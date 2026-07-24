//! File hashing and byte-level content comparison

use std::{
    fs::File,
    io::{self, Read as _},
    ops::Range,
    os::unix::fs::FileExt as _,
    path::Path,
};

use xxhash_rust::xxh3;

/// File read chunk size, in bytes
pub(crate) const READ_BUFFER_SIZE: usize = 256 * 1024;

/// Compute an XXH3-64 non-cryptographic file hash
pub(crate) fn hash_file(
    file: &File,
    hasher: &mut xxh3::Xxh3,
    buffer: &mut Vec<u8>,
) -> Result<u64, io::Error> {
    hasher.reset();
    loop {
        // Unlike a bare read, read_to_end fills the whole chunk, resuming when a signal interrupts it
        buffer.clear();
        file.take(READ_BUFFER_SIZE as u64).read_to_end(buffer)?;
        if buffer.is_empty() {
            break;
        }
        hasher.update(buffer);
    }
    Ok(hasher.digest())
}

/// Test if two files have the same content
///
/// Both files must be the same size: reading stops at the end of `first`, so a longer `second` compares equal.
pub(crate) fn same_content(first: &Path, second: &Path) -> Result<bool, io::Error> {
    let first = File::open(first)?;
    let second = File::open(second)?;
    // One range spanning the whole file, which same_ranges clamps to its actual size
    let whole_file = 0..u64::MAX;
    same_ranges(&first, &second, &[whole_file])
}

/// Test if two files of the same size have the same content over every given range
///
/// Extent lengths are block aligned, so a range reaching past the end of the files is clamped to it.
pub(crate) fn same_ranges(
    first: &File,
    second: &File,
    ranges: &[Range<u64>],
) -> Result<bool, io::Error> {
    let size = first.metadata()?.len();
    debug_assert_eq!(size, second.metadata()?.len());
    let mut buffer1 = Vec::new();
    let mut buffer2 = Vec::new();
    for range in ranges {
        let mut offset = range.start;
        let end = range.end.min(size);
        while offset < end {
            let chunk = (end - offset).min(READ_BUFFER_SIZE as u64);
            let length = usize::try_from(chunk).map_err(io::Error::other)?;
            buffer1.resize(length, 0);
            buffer2.resize(length, 0);
            first.read_exact_at(&mut buffer1, offset)?;
            second.read_exact_at(&mut buffer2, offset)?;
            if buffer1 != buffer2 {
                return Ok(false);
            }
            offset += chunk;
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::{
        extent::ambiguous_ranges,
        tests::{
            btrfs_test_dir, incompressible_bytes, pair_paths, path_extents, write_compressed,
            EXTENT_SIZE,
        },
    };

    /// Write two files and compare their content, as the candidate reporting does
    fn compare_content(first_content: &[u8], second_content: &[u8]) -> bool {
        let dir = tempfile::TempDir::new().unwrap();
        let (first, second) = pair_paths(dir.path());
        fs::write(&first, first_content).unwrap();
        fs::write(&second, second_content).unwrap();

        same_content(&first, &second).unwrap()
    }

    #[test]
    fn same_content_identical_spanning_chunks() {
        let content = incompressible_bytes(READ_BUFFER_SIZE * 2 + 100);

        assert!(compare_content(&content, &content));
    }

    #[test]
    fn same_content_differing_at_chunk_boundary() {
        let content = incompressible_bytes(READ_BUFFER_SIZE * 2);
        let mut other = content.clone();
        other[READ_BUFFER_SIZE] ^= 0xff;

        assert!(!compare_content(&content, &other));
    }

    #[test]
    fn same_content_differing_in_last_partial_chunk() {
        let content = incompressible_bytes(READ_BUFFER_SIZE + 100);
        let mut other = content.clone();
        *other.last_mut().unwrap() ^= 0xff;

        assert!(!compare_content(&content, &other));
    }

    #[test]
    fn same_content_empty() {
        assert!(compare_content(b"", b""));
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "assertion `left == right` failed")]
    fn same_content_rejects_different_sizes() {
        compare_content(b"aa", b"a");
    }

    #[test]
    fn same_ranges_clamps_to_file_end() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        // An unaligned size makes the tail extent reach past the bytes the file holds
        let size = EXTENT_SIZE + 100;
        write_compressed(&first, size);
        fs::copy(&first, &second).unwrap();

        let ranges = ambiguous_ranges(&path_extents(&first));
        assert!(ranges.iter().any(|range| range.end > size as u64));
        assert!(same_ranges(
            &File::open(&first).unwrap(),
            &File::open(&second).unwrap(),
            &ranges
        )
        .unwrap());
    }

    #[test]
    fn same_ranges_checks_later_disjoint_ranges() {
        let dir = tempfile::TempDir::new().unwrap();
        let (first, second) = pair_paths(dir.path());
        fs::write(&first, b"abc").unwrap();
        fs::write(&second, b"abx").unwrap();

        // The first range matches, so the difference in the second is only caught by checking every range
        assert!(!same_ranges(
            &File::open(first).unwrap(),
            &File::open(second).unwrap(),
            &[0..2, 2..3],
        )
        .unwrap());
    }
}
