//! FIEMAP extent mapping and the extent-level analysis reflink detection relies on

use std::{fs::File, io, mem::zeroed, ops::Range, os::fd::AsRawFd as _};

use linux_raw_sys::ioctl;

/// Number of extents fetched per FIEMAP ioctl
///
/// Btrfs caps a compressed extent at 128 KiB, so mapping a large compressed file takes many extents, and a batch
/// this size still fits comfortably on the stack.
const FIEMAP_BATCH_SIZE: u32 = 512;

/// Extent flags marking a physical location that does not identify the underlying data
const UNRESOLVED_EXTENT_FLAGS: u32 = ioctl::FIEMAP_EXTENT_UNKNOWN | ioctl::FIEMAP_EXTENT_DELALLOC;

/// Extent flags marking a location that can never establish sharing
///
/// Inlined data, which Btrfs copies into the destination rather than sharing, and a location left unresolved are
/// both placeholders identical between unrelated files.
const UNSHAREABLE_EXTENT_FLAGS: u32 = UNRESOLVED_EXTENT_FLAGS | ioctl::FIEMAP_EXTENT_DATA_INLINE;

/// Largest span of file bytes one compressed Btrfs extent carries
pub(crate) const BTRFS_MAX_UNCOMPRESSED: u64 = 128 * 1024;

/// One mapped extent, as the FIEMAP ioctl reports it
// Field names follow the kernel ABI
#[expect(clippy::struct_field_names)]
#[repr(C)]
#[derive(Clone, Copy)]
#[cfg_attr(test, derive(Default))]
pub(crate) struct FiemapExtent {
    /// Offset of the extent in the file
    fe_logical: u64,
    /// Location of the extent on the device
    fe_physical: u64,
    /// Length of the extent
    fe_length: u64,
    /// Reserved by the ABI
    fe_reserved64: [u64; 2],
    /// `FIEMAP_EXTENT_*` flags
    fe_flags: u32,
    /// Reserved by the ABI
    fe_reserved: [u32; 3],
}

/// Header of a FIEMAP ioctl request, which the extent buffer directly follows in memory
// Field names follow the kernel ABI
#[expect(clippy::struct_field_names)]
#[repr(C)]
struct FiemapHeader {
    /// First file offset to map
    fm_start: u64,
    /// Length of the file range to map
    fm_length: u64,
    /// `FIEMAP_FLAG_*` request flags
    fm_flags: u32,
    /// Number of extents the kernel filled
    fm_mapped_extents: u32,
    /// Capacity of the extent buffer
    fm_extent_count: u32,
    /// Reserved by the ABI
    fm_reserved: u32,
}

/// A whole FIEMAP request, the header followed by the extent buffer it announces
#[repr(C)]
struct FiemapRequest<const N: usize> {
    /// Request header
    header: FiemapHeader,
    /// Extent buffer the kernel fills
    extents: [FiemapExtent; N],
}

/// Offset of one extent in the file, its location on the device, and its length
pub(crate) type ExtentLocation = (u64, u64, u64);

nix::ioctl_readwrite!(
    /// Map the extents of an open file, taking the request header the extent buffer follows
    fs_fiemap,
    b'f',
    11,
    FiemapHeader
);

/// Map the extents of an open file, having the kernel flush pending writes first when `sync` is set
///
/// Data under delayed allocation is reported without a location, which only a flush resolves, hiding whether it
/// will be inlined.
pub(crate) fn file_extents(file: &File, sync: bool) -> Result<Vec<FiemapExtent>, io::Error> {
    let mut extents: Vec<FiemapExtent> = Vec::new();
    loop {
        let start = extents
            .last()
            .map_or(0, |last| last.fe_logical + last.fe_length);
        // One flush resolves delayed allocation for the whole file, so further batches skip it
        let flags = if sync && extents.is_empty() {
            ioctl::FIEMAP_FLAG_SYNC
        } else {
            0
        };
        let batch = map_extents::<{ FIEMAP_BATCH_SIZE as usize }>(file, start, u64::MAX, flags)?;
        let mapped = batch.len();
        extents.extend(batch);
        if mapped < FIEMAP_BATCH_SIZE as usize
            || extents
                .last()
                .is_some_and(|last| last.fe_flags & ioctl::FIEMAP_EXTENT_LAST != 0)
        {
            return split_merged(file, extents);
        }
    }
}

/// Map up to `N` extents intersecting the logical range of `length` bytes beginning at `start`
fn map_extents<const N: usize>(
    file: &File,
    start: u64,
    length: u64,
    flags: u32,
) -> Result<impl ExactSizeIterator<Item = FiemapExtent>, io::Error> {
    // SAFETY: every field is an integer, for which zero is a valid value
    let mut request: FiemapRequest<N> = unsafe { zeroed() };
    request.header.fm_start = start;
    request.header.fm_length = length;
    request.header.fm_flags = flags;
    request.header.fm_extent_count = u32::try_from(N).map_err(io::Error::other)?;
    // SAFETY: the ioctl writes at most `fm_extent_count` extents into the buffer following the header, which
    // `FiemapRequest` lays out with that capacity
    unsafe { fs_fiemap(file.as_raw_fd(), &raw mut request.header) }?;
    let mapped = request.header.fm_mapped_extents as usize;
    Ok(request.extents.into_iter().take(mapped))
}

/// Re-map compressed entries at least `BTRFS_MAX_UNCOMPRESSED` bytes long into their underlying extents
fn split_merged(file: &File, extents: Vec<FiemapExtent>) -> Result<Vec<FiemapExtent>, io::Error> {
    let mut split = Vec::with_capacity(extents.len());
    for extent in extents {
        // A shorter entry is ambiguous already, whatever extents it covers
        if extent.fe_flags & ioctl::FIEMAP_EXTENT_ENCODED == 0
            || extent.fe_length < BTRFS_MAX_UNCOMPRESSED
        {
            split.push(extent);
            continue;
        }
        let end = extent.fe_logical + extent.fe_length;
        let mut start = extent.fe_logical;
        while start < end {
            // Mapping a single byte ends the request at the extent holding it, before a neighbour can merge in
            let piece = map_extents::<1>(file, start, 1, 0)?
                .next()
                .filter(|piece| piece.fe_logical == start && piece.fe_length > 0)
                .ok_or_else(|| io::Error::other("Extent map changed while mapping it"))?;
            start += piece.fe_length;
            split.push(piece);
        }
    }
    Ok(split)
}

/// Whether a file stores its data in filesystem metadata rather than in a data extent
pub(crate) fn is_inlined(extents: &[FiemapExtent]) -> bool {
    extents
        .iter()
        .any(|extent| extent.fe_flags & ioctl::FIEMAP_EXTENT_DATA_INLINE != 0)
}

/// Identify the data a file maps to, `None` when its extents can not establish sharing
///
/// Files sharing a key hold the same bytes, save for compressed extents, which several contents can share. The
/// logical offset is part of it, as the same extent mapped elsewhere in the file puts its bytes elsewhere too.
pub(crate) fn extent_key(extents: &[FiemapExtent]) -> Option<Vec<ExtentLocation>> {
    extents
        .iter()
        .map(|extent| {
            (extent.fe_flags & UNSHAREABLE_EXTENT_FLAGS == 0).then_some((
                extent.fe_logical,
                extent.fe_physical,
                extent.fe_length,
            ))
        })
        .collect()
}

/// Identify the ranges of the file whose extents leave the bytes unproven between files sharing them
///
/// The map must come from `file_extents`, with merged compressed entries split.
pub(crate) fn ambiguous_ranges(extents: &[FiemapExtent]) -> Vec<Range<u64>> {
    extents
        .iter()
        .filter(|extent| {
            extent.fe_flags & ioctl::FIEMAP_EXTENT_ENCODED != 0
                && extent.fe_length != BTRFS_MAX_UNCOMPRESSED
        })
        .map(|extent| extent.fe_logical..extent.fe_logical + extent.fe_length)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{Seek as _, Write as _},
    };

    use super::*;
    use crate::tests::{
        btrfs_test_dir, clone_range, files_have_same_extent_key, incompressible_bytes, pair_paths,
        path_extents, write_compressed, write_flushed, write_unflushed, CLONE_RANGE_SIZE,
        EXTENT_SIZE, INLINE_SIZE,
    };

    #[test]
    fn ambiguous_ranges_of_compressed_tail() {
        let Some(dir) = btrfs_test_dir() else { return };
        let path = dir.path().join("file");
        write_compressed(&path, EXTENT_SIZE);

        let extents = path_extents(&path);
        assert!(extents.len() >= 3);
        // Only the tail extent falls short of the largest compressed extent, whose full coverage is unambiguous
        let tail_start = EXTENT_SIZE as u64 / BTRFS_MAX_UNCOMPRESSED * BTRFS_MAX_UNCOMPRESSED;
        assert_eq!(
            ambiguous_ranges(&extents),
            vec![tail_start..EXTENT_SIZE as u64]
        );
    }

    #[test]
    fn ambiguous_ranges_none_for_incompressible_data() {
        let Some(dir) = btrfs_test_dir() else { return };
        let path = dir.path().join("file");
        write_flushed(&path, EXTENT_SIZE);

        assert_eq!(ambiguous_ranges(&path_extents(&path)), vec![]);
    }

    #[test]
    fn files_have_same_extent_key_not_partially_rewritten_reflink() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        write_flushed(&first, EXTENT_SIZE);
        fs::copy(&first, &second).unwrap();
        // Rewriting identical bytes breaks sharing for that range, leaving the files identical but no longer
        // fully reflinked
        let mut file = File::options().write(true).open(&second).unwrap();
        file.write_all(&incompressible_bytes(EXTENT_SIZE)[..4096])
            .unwrap();
        file.sync_data().unwrap();

        assert!(!files_have_same_extent_key(&first, &second));
    }

    #[test]
    fn files_have_same_extent_key_not_sparse_and_dense() {
        let Some(dir) = btrfs_test_dir() else { return };
        let sparse = dir.path().join("sparse");
        let dense = dir.path().join("dense");
        let tail = incompressible_bytes(EXTENT_SIZE);

        // A hole is not reported as an extent, unlike the zeroes it reads as
        let mut sparse_file = File::create(&sparse).unwrap();
        sparse_file
            .seek(io::SeekFrom::Start(EXTENT_SIZE as u64))
            .unwrap();
        sparse_file.write_all(&tail).unwrap();
        sparse_file.sync_data().unwrap();

        let mut dense_file = File::create(&dense).unwrap();
        dense_file.write_all(&vec![0; EXTENT_SIZE]).unwrap();
        dense_file.write_all(&tail).unwrap();
        dense_file.sync_data().unwrap();

        assert!(!files_have_same_extent_key(&sparse, &dense));
    }

    #[test]
    fn files_have_same_extent_key_not_slices_of_consecutive_compressed_extents() {
        let Some(dir) = btrfs_test_dir() else { return };
        let base = dir.path().join("base");
        // The slices only line up from two extents less than a whole extent apart on disk, which neighbours in a
        // large write usually are
        let (low, high) = (0..10)
            .find_map(|_| {
                write_compressed(&base, 16 * usize::try_from(BTRFS_MAX_UNCOMPRESSED).unwrap());
                let mut extents = path_extents(&base);
                extents.sort_unstable_by_key(|extent| extent.fe_physical);
                extents
                    .windows(2)
                    .find(|pair| pair[1].fe_physical - pair[0].fe_physical < BTRFS_MAX_UNCOMPRESSED)
                    .map(|pair| (pair[0], pair[1]))
            })
            .unwrap();
        let gap = high.fe_physical - low.fe_physical;
        let (slices, whole) = pair_paths(dir.path());
        // Combine slices at the same relative offsets their source extents have on disk
        clone_range(&base, low.fe_logical, gap, &slices, 0);
        let tail_length = BTRFS_MAX_UNCOMPRESSED - gap;
        clone_range(&base, high.fe_logical, tail_length, &slices, gap);
        clone_range(&base, low.fe_logical, BTRFS_MAX_UNCOMPRESSED, &whole, 0);
        assert_ne!(fs::read(&slices).unwrap(), fs::read(&whole).unwrap());

        assert!(!files_have_same_extent_key(&slices, &whole));
        assert_eq!(
            extent_key(&path_extents(&slices)),
            Some(vec![
                (0, low.fe_physical, gap),
                (gap, high.fe_physical, tail_length)
            ])
        );
    }

    #[test]
    fn split_merged_rejects_disappeared_extent() {
        let Some(dir) = btrfs_test_dir() else { return };
        let path = dir.path().join("file");
        write_compressed(&path, usize::try_from(BTRFS_MAX_UNCOMPRESSED).unwrap());
        let file = File::options().read(true).write(true).open(&path).unwrap();
        let extents = map_extents::<1>(&file, 0, u64::MAX, 0).unwrap().collect();
        file.set_len(0).unwrap();

        let error = split_merged(&file, extents).err().unwrap();
        assert_eq!(error.to_string(), "Extent map changed while mapping it");
    }

    #[test]
    fn extent_key_includes_logical_offset() {
        let Some(dir) = btrfs_test_dir() else { return };
        let path = dir.path().join("file");
        write_flushed(&path, EXTENT_SIZE);

        // Only a synthesized map can hold the same extent at another offset, which a hole opening ahead of the
        // data would produce while changing the bytes the file reads as
        let extents = path_extents(&path);
        let mut shifted = extents.clone();
        shifted[0].fe_logical += EXTENT_SIZE as u64;

        assert_ne!(extent_key(&shifted), extent_key(&extents));
    }

    #[test]
    fn extent_key_none_for_inlined_data() {
        let Some(dir) = btrfs_test_dir() else { return };
        let inlined = dir.path().join("inlined");
        let truncated = dir.path().join("truncated");
        write_flushed(&inlined, INLINE_SIZE);
        // Truncating below the inline threshold keeps the allocated extent, so files of equal size and content
        // can differ in layout
        write_flushed(&truncated, EXTENT_SIZE);
        let file = File::options().write(true).open(&truncated).unwrap();
        file.set_len(INLINE_SIZE as u64).unwrap();
        file.sync_data().unwrap();

        let inlined_extents = path_extents(&inlined);
        let truncated_extents = path_extents(&truncated);
        assert!(is_inlined(&inlined_extents));
        assert!(!is_inlined(&truncated_extents));

        // Reflinking the inlined file onto the other releases the extent it holds alone
        assert!(extent_key(&inlined_extents).is_none());
        assert_ne!(extent_key(&inlined_extents), extent_key(&truncated_extents));
    }

    #[test]
    fn extent_key_includes_extent_length() {
        let Some(dir) = btrfs_test_dir() else { return };
        let whole = dir.path().join("whole");
        let part = dir.path().join("part");
        write_flushed(&whole, EXTENT_SIZE);
        fs::copy(&whole, &part).unwrap();
        // Truncating keeps the start of the reflinked extent, shortening it in place
        let file = File::options().write(true).open(&part).unwrap();
        file.set_len(EXTENT_SIZE as u64 / 2).unwrap();
        file.sync_data().unwrap();

        let whole_extents = path_extents(&whole);
        let part_extents = path_extents(&part);
        assert_eq!(whole_extents.len(), part_extents.len());
        assert_eq!(whole_extents[0].fe_physical, part_extents[0].fe_physical);
        assert_ne!(whole_extents[0].fe_length, part_extents[0].fe_length);

        assert_ne!(extent_key(&whole_extents), extent_key(&part_extents));
    }

    #[test]
    fn extent_key_includes_entire_map() {
        let Some(dir) = btrfs_test_dir() else { return };
        let path = dir.path().join("file");
        write_flushed(&path, EXTENT_SIZE);
        // Appending to a flushed file allocates a further extent
        let mut file = File::options().append(true).open(&path).unwrap();
        file.write_all(&incompressible_bytes(EXTENT_SIZE)).unwrap();
        file.sync_data().unwrap();

        let extents = path_extents(&path);
        assert!(extents.len() > 1);

        // Every extent of the shorter map is shared, yet the files are not
        assert_ne!(extent_key(&extents[..1]), extent_key(&extents));
    }

    #[test]
    fn extent_key_none_for_unresolved_locations() {
        // Covers a write landing between the flush and the mapping, which the flush in file_extents makes
        // otherwise unreachable
        let extents = [FiemapExtent {
            fe_length: EXTENT_SIZE as u64,
            fe_flags: ioctl::FIEMAP_EXTENT_DELALLOC | ioctl::FIEMAP_EXTENT_UNKNOWN,
            ..FiemapExtent::default()
        }];

        // Unresolved locations match between unrelated files, so a key would group them
        assert!(extent_key(&extents).is_none());

        // A resolved prefix does not make up for a pending tail
        let resolved = FiemapExtent {
            fe_physical: 4096,
            fe_length: EXTENT_SIZE as u64,
            ..FiemapExtent::default()
        };
        let pending = FiemapExtent {
            fe_logical: resolved.fe_length,
            ..extents[0]
        };
        assert!(extent_key(&[resolved, pending]).is_none());
    }

    #[test]
    fn file_extents_resolves_delayed_allocation() {
        let Some(dir) = btrfs_test_dir() else { return };
        let path = dir.path().join("file");
        write_unflushed(&path, EXTENT_SIZE);

        let extents = path_extents(&path);
        assert!(!extents.is_empty());
        assert!(!extents
            .iter()
            .any(|extent| extent.fe_flags & UNRESOLVED_EXTENT_FLAGS != 0));
    }

    #[test]
    fn file_extents_reports_inlined_data_of_unflushed_file() {
        let Some(dir) = btrfs_test_dir() else { return };
        let path = dir.path().join("file");
        write_unflushed(&path, INLINE_SIZE);

        let extents = path_extents(&path);
        assert!(extents
            .iter()
            .any(|extent| extent.fe_flags & ioctl::FIEMAP_EXTENT_DATA_INLINE != 0));
    }

    #[test]
    fn file_extents_reads_past_one_ioctl_batch() {
        let Some(dir) = btrfs_test_dir() else { return };
        let path = dir.path().join("file");
        let mut file = File::create(&path).unwrap();
        // A byte in every other clone-sized slot leaves a hole between the writes, so they never coalesce into
        // fewer extents than one batch holds
        let last_offset = u64::from(FIEMAP_BATCH_SIZE) * 2 * CLONE_RANGE_SIZE;
        for index in 0..=FIEMAP_BATCH_SIZE {
            file.seek(io::SeekFrom::Start(u64::from(index) * 2 * CLONE_RANGE_SIZE))
                .unwrap();
            file.write_all(b"x").unwrap();
        }
        file.sync_data().unwrap();

        let extents = path_extents(&path);
        assert!(extents.len() > FIEMAP_BATCH_SIZE as usize);
        assert!(extents.last().unwrap().fe_logical >= last_offset);
    }
}
