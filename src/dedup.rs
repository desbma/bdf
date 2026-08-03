//! Grouping candidate files by the data copy they hold and reporting the reflink candidates

use std::{
    collections::hash_map::{Entry, HashMap},
    fs::File,
    io::{self, Write},
    iter,
    ops::Range,
    os::unix::{ffi::OsStrExt as _, io::AsRawFd as _},
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Mutex},
};

use nix::errno::Errno;
use rayon::iter::{ParallelBridge as _, ParallelIterator as _};

use crate::{
    content::{same_content, same_ranges},
    extent::{ambiguous_ranges, extent_key, file_extents, is_inlined, ExtentLocation},
    ProgressCounters,
};

// FICLONE: _IOW(0x94, 9, int)
nix::ioctl_write_int!(ficlone, 0x94, 9);

/// Attempt to reflink `source` into `dest` using FICLONE
///
/// Returns `Ok(())` on success, `Err` if the ioctl fails (e.g. cross-filesystem,
/// cross-subvolume on kernels < 5.18, or read-only destination).
fn try_ficlone(source: &File, dest: &File) -> Result<(), Errno> {
    let src_fd = source.as_raw_fd();
    let dst_fd = dest.as_raw_fd();
    // SAFETY: ioctl write
    unsafe { ficlone(dst_fd, src_fd.try_into().unwrap_or_default()) }.map(|_| ())
}

/// Representative of each data copy seen so far, the file kept for hashing while later files found mapping to the
/// same extents hold its bytes and are therefore spared hashing, keyed by file size and extent key
pub(crate) type SharedIndex = HashMap<(u64, Vec<ExtentLocation>), (PathBuf, Vec<PathBuf>)>;

/// Record `file` in the index of content seen so far, attaching `path` to the entry sharing its bytes if any
///
/// Returns whether the file was attached, sparing its hashing; a fresh key starts a new entry instead, for later
/// files to attach to. Sharing every extent proves the bytes shared, except over ambiguous ranges, which are
/// compared before trusting the entry; a mismatch, an error on this file alone, or a comparison that would read
/// at least as much as hashing all leave the file to hashing.
pub(crate) fn try_attach_shared(
    shared_index: &Mutex<SharedIndex>,
    file: &File,
    path: &Path,
    file_size: u64,
) -> anyhow::Result<bool> {
    // Pending writes are not flushed: delayed allocation yields no key, and the file simply falls through to
    // hashing
    let extents = match file_extents(file, false) {
        Ok(extents) => extents,
        Err(e) => {
            log::warn!("Error while reading extents of {path:?}: {e}");
            return Ok(false);
        }
    };
    let Some(key) = extent_key(&extents) else {
        return Ok(false);
    };
    let ambiguous = ambiguous_ranges(&extents);
    drop(extents);

    let (index_key, representative) = {
        let mut shared_index = shared_index
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned lock: {e}"))?;
        let mut known = match shared_index.entry((file_size, key)) {
            Entry::Occupied(known) => known,
            Entry::Vacant(slot) => {
                slot.insert((path.to_path_buf(), Vec::new()));
                return Ok(false);
            }
        };
        if ambiguous.is_empty() {
            known.get_mut().1.push(path.to_path_buf());
            return Ok(true);
        }
        // Confirming reads the ranges out of both files, so from half the file on, hashing reads no more
        let ambiguous_total: u64 = ambiguous.iter().map(|range| range.end - range.start).sum();
        if ambiguous_total * 2 >= file_size {
            return Ok(false);
        }
        (known.key().clone(), known.get().0.clone())
    };

    // The comparison reads both files, which would stall the other workers if it held the index lock
    let Ok(shares) = File::open(&representative)
        .and_then(|rep| same_ranges(&rep, file, &ambiguous))
        .inspect_err(|e| log::warn!("Error while comparing {path:?} with {representative:?}: {e}"))
    else {
        return Ok(false);
    };
    if !shares {
        log::warn!(
            "Files {representative:?} and {path:?} share a compressed extent but hold different data"
        );
        return Ok(false);
    }
    shared_index
        .lock()
        .map_err(|e| anyhow::anyhow!("Poisoned lock: {e}"))?
        .get_mut(&index_key)
        // Entries are never removed, so the entry just found is still there
        .ok_or_else(|| anyhow::anyhow!("Shared index entry vanished"))?
        .1
        .push(path.to_path_buf());
    Ok(true)
}

/// One copy of a file content on disk, and the files already sharing it
#[cfg_attr(test, derive(Debug, Eq, PartialEq))]
struct DataCopy<'p> {
    /// First file holding the copy, standing for all of them
    path: &'p Path,
    /// Whether the data sits in filesystem metadata, which reflinking can not share
    inlined: bool,
    /// Further files that map to the extents of `path` and hold its data
    shared: Vec<&'p Path>,
}

/// Find the index in `copies` of the variant among `variants` holding the data of `file`, or `None` for a
/// fresh content
///
/// Files sharing a key hold identical bytes save over ambiguous compressed ranges, so an unambiguous file joins
/// the sole variant a key can then have, while a compressed one is compared over those ranges against each.
fn matching_variant(
    copies: &[DataCopy<'_>],
    variants: &[usize],
    file: &File,
    ambiguous: &[Range<u64>],
) -> Result<Option<usize>, io::Error> {
    if ambiguous.is_empty() {
        return Ok(variants.first().copied());
    }
    for &index in variants {
        // Variants hold indices filled from `copies.len()`, so they always resolve
        #[expect(clippy::indexing_slicing)]
        let representative = File::open(copies[index].path)?;
        if same_ranges(&representative, file, ambiguous)? {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

/// Group candidate files sharing a size by the copy of the data they hold
///
/// Files mapping to the same extents hold the same bytes, so grouping them ahead of any further reading spares
/// comparing data that is already shared. Compression is the exception: files sharing a key can differ over the
/// ranges its short extents cover, so those are compared to keep genuinely distinct contents in their own copy.
fn data_copies(filepaths: &[PathBuf]) -> Result<Vec<DataCopy<'_>>, io::Error> {
    let mut copies: Vec<DataCopy<'_>> = Vec::new();
    // Copies indexed by the data they hold, several variants to a key when compressed extents make it ambiguous
    let mut by_key: HashMap<Vec<ExtentLocation>, Vec<usize>> = HashMap::new();

    for filepath in filepaths {
        let path = filepath.as_path();
        // A file that can not be opened is skipped rather than fatal, as it is when hashing
        let Ok(file) = File::open(filepath).inspect_err(|e| {
            log::warn!("Error while opening {path:?}: {e}");
        }) else {
            continue;
        };
        let extents = file_extents(&file, true)?;
        let inlined = is_inlined(&extents);
        let Some(key) = extent_key(&extents) else {
            // Extents identifying nothing keep the file to itself, even against another file mapping the same way
            copies.push(DataCopy {
                path,
                inlined,
                shared: Vec::new(),
            });
            continue;
        };
        let ambiguous = ambiguous_ranges(&extents);
        drop(extents);

        let variants = by_key.entry(key).or_default();
        if let Some(index) = matching_variant(&copies, variants, &file, &ambiguous)? {
            // Variants hold indices filled from `copies.len()`, so they always resolve
            #[expect(clippy::indexing_slicing)]
            copies[index].shared.push(path);
        } else {
            variants.push(copies.len());
            copies.push(DataCopy {
                path,
                inlined,
                shared: Vec::new(),
            });
        }
    }
    Ok(copies)
}

/// Partition the copies of one candidate group into classes of identical content
///
/// Each class is its first copy, standing for the whole class, followed by the others. In a hashed group, more
/// than one class means the hash collided; in an unhashed pair, that the size matched by chance.
fn content_classes<'c, 'p>(
    copies: &'c [DataCopy<'p>],
) -> Result<Vec<(&'c DataCopy<'p>, Vec<&'c DataCopy<'p>>)>, io::Error> {
    let mut classes: Vec<(&DataCopy<'_>, Vec<&DataCopy<'_>>)> = Vec::new();
    'copy: for copy in copies {
        for (representative, others) in &mut classes {
            // Content equality is transitive, so comparing against one member settles the whole class
            if same_content(representative.path, copy.path)? {
                others.push(copy);
                continue 'copy;
            }
        }
        classes.push((copy, Vec::new()));
    }
    Ok(classes)
}

/// Deduplicate a pair of files
fn dedup_pair(src_path: &Path, dst_path: &Path) {
    if let (Ok(src), Ok(dst)) = (
        File::open(src_path),
        File::options().write(true).open(dst_path),
    ) {
        let mod_time = dst.metadata().and_then(|md| md.modified()).ok();
        match try_ficlone(&src, &dst) {
            Ok(()) => log::info!("Reflinked {src_path:?} -> {dst_path:?}"),
            Err(e) => {
                let desc = e.desc();
                log::warn!("FICLONE({src_path:?} -> {dst_path:?}) failed: {desc}");
            }
        }
        if let Some(mt) = mod_time {
            let _ = dst.set_modified(mt);
        }
    } else {
        log::warn!("Could not open files for FICLONE({src_path:?} -> {dst_path:?})");
    }
}

/// Write a NUL terminated duplicate pair
fn write_pair<W>(writer: &mut W, first: &Path, second: &Path) -> Result<(), io::Error>
where
    W: Write,
{
    writer.write_all(first.as_os_str().as_bytes())?;
    writer.write_all(b"\0")?;
    writer.write_all(second.as_os_str().as_bytes())?;
    writer.write_all(b"\0")
}

/// Report the duplicate pairs of one group of files sharing a size, and a hash unless the size alone held it to
/// two files
fn report_group<W>(
    filepaths: &[PathBuf],
    hashed: bool,
    dedup: bool,
    counters: &ProgressCounters,
    writer: &Mutex<&mut W>,
) -> Result<(), io::Error>
where
    W: Write,
{
    let copies = data_copies(filepaths)?;
    let classes = content_classes(&copies)?;
    // An unhashed pair holding two contents is a size shared by chance, not a collision
    if hashed && classes.len() > 1 {
        log::warn!(
            "Files {filepaths:?} have the same size and hash but {count} distinct contents",
            count = classes.len()
        );
        counters
            .hash_collision
            .fetch_add(classes.len() - 1, Ordering::Relaxed);
    }

    for (first, others) in classes {
        let first_path = first.path;
        for &shared in &first.shared {
            log::debug!("Files {first_path:?} and {shared:?} are already reflinked");
            counters.reflinked.fetch_add(1, Ordering::Relaxed);
        }
        for other in others {
            let other_path = other.path;
            // Reflinking an inlined file onto one holding a regular extent releases that extent, so only a
            // pair inlined on both sides has nothing to gain
            if first.inlined && other.inlined {
                log::debug!(
                    "Files {first_path:?} and {other_path:?} have inlined data, reflinking would not free space"
                );
                counters.inlined.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            // Every file of the other copy needs its own reflink, sharing among them leaving the copy behind
            for path in iter::once(other_path).chain(other.shared.iter().copied()) {
                log::debug!("Files {first_path:?} and {path:?} are duplicates");
                counters.duplicate_candidate.fetch_add(1, Ordering::Relaxed);
                if dedup {
                    dedup_pair(first_path, path);
                }
                let mut writer = writer
                    .lock()
                    .map_err(|e| io::Error::other(format!("Poisoned lock: {e}")))?;
                write_pair(&mut **writer, first_path, path)?;
            }
        }
    }
    Ok(())
}

/// Fold every file spared hashing into the group of the representative that stands for it, dropping a group left
/// holding a single unshared file
pub(crate) fn merge_shared_members(
    files: &mut HashMap<(u64, u64), Vec<PathBuf>>,
    shared_index: SharedIndex,
) {
    // Index the files spared hashing by their hashed representative
    let shared_members: HashMap<PathBuf, Vec<PathBuf>> = shared_index
        .into_values()
        .filter(|(_path, members)| !members.is_empty())
        .collect();

    // Remove unique hashes, folding every file spared hashing into the group of the representative standing for
    // it, and keeping a lone hash whose file stands for shared ones
    files.retain(|_key, filepaths| {
        let members: Vec<PathBuf> = filepaths
            .iter()
            .filter_map(|path| shared_members.get(path))
            .flatten()
            .cloned()
            .collect();
        let keep = filepaths.len() > 1 || !members.is_empty();
        filepaths.extend(members);
        keep
    });
}

/// Report the duplicate pairs of every hashed group and unhashed size pair, spreading groups over a Rayon pool
///
/// A pair names the file to reflink onto, then the one to replace, so the output feeds `cp --reflink` directly.
pub(crate) fn report_duplicates<W>(
    files: &HashMap<(u64, u64), Vec<PathBuf>>,
    pair_groups: &[Vec<PathBuf>],
    dedup: bool,
    counters: &ProgressCounters,
    writer: &mut W,
) -> anyhow::Result<()>
where
    W: Write + Send,
{
    let writer = Mutex::new(writer);
    // Tag each group with whether it was hashed, telling a hash collision apart from a coincidental size match
    files
        .values()
        .map(|filepaths| (filepaths, true))
        .chain(pair_groups.iter().map(|filepaths| (filepaths, false)))
        .par_bridge()
        .try_for_each(|(filepaths, hashed)| {
            report_group(filepaths, hashed, dedup, counters, &writer)
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsStr, fs};

    use super::*;
    use crate::{extent::BTRFS_MAX_UNCOMPRESSED, tests::*};

    /// Build an extent backed copy holding the given files
    fn plain_copy<'p>(path: &'p Path, shared: Vec<&'p Path>) -> DataCopy<'p> {
        DataCopy {
            path,
            inlined: false,
            shared,
        }
    }

    /// Build one copy per path, holding a single file, as a tree with no sharing would produce
    fn unshared_copies(paths: &[PathBuf]) -> Vec<DataCopy<'_>> {
        paths
            .iter()
            .map(|path| plain_copy(path, Vec::new()))
            .collect()
    }

    /// Write the given contents, in order, as files of a same size and hash group
    fn write_group(dir: &Path, contents: &[&[u8]]) -> Vec<PathBuf> {
        contents
            .iter()
            .enumerate()
            .map(|(index, content)| {
                let path = dir.join(index.to_string());
                fs::write(&path, content).unwrap();
                path
            })
            .collect()
    }

    /// Clone two differing ranges of one compressed extent, which Btrfs maps identically, into a pair of files
    fn clone_compressed_ranges(dir: &Path) -> (PathBuf, PathBuf) {
        let (first, second) = pair_paths(dir);
        let base = dir.join("base");
        write_compressed(&base, EXTENT_SIZE);
        // Btrfs compresses in 128 KiB units, so both ranges fall inside one extent
        clone_range(&base, 0, CLONE_RANGE_SIZE, &first, 0);
        clone_range(&base, CLONE_RANGE_SIZE, CLONE_RANGE_SIZE, &second, 0);

        let first_extents = path_extents(&first);
        assert!(!ambiguous_ranges(&first_extents).is_empty());
        assert_eq!(
            extent_key(&first_extents),
            extent_key(&path_extents(&second))
        );
        assert_ne!(fs::read(&first).unwrap(), fs::read(&second).unwrap());
        (first, second)
    }

    /// Run `try_attach_shared` for a file over an index, as a hash worker does
    fn attached(index: &Mutex<SharedIndex>, path: &Path) -> bool {
        let file = File::open(path).unwrap();
        let file_size = file.metadata().unwrap().len();
        try_attach_shared(index, &file, path, file_size).unwrap()
    }

    /// Report the given hashed group and unhashed pair groups, decoding the pairs written
    fn decoded_report(
        files: &HashMap<(u64, u64), Vec<PathBuf>>,
        pair_groups: &[Vec<PathBuf>],
        counters: &ProgressCounters,
    ) -> Vec<(PathBuf, PathBuf)> {
        let mut out = Vec::new();
        report_duplicates(files, pair_groups, false, counters, &mut out).unwrap();

        out.split(|&byte| byte == 0)
            .filter(|path| !path.is_empty())
            .map(|path| PathBuf::from(OsStr::from_bytes(path)))
            .collect::<Vec<_>>()
            .chunks(2)
            .map(|pair| (pair[0].clone(), pair[1].clone()))
            .collect()
    }

    /// Report the duplicates of one group of files sharing a size and hash, decoding the pairs written
    fn reported_pairs(group: Vec<PathBuf>, counters: &ProgressCounters) -> Vec<(PathBuf, PathBuf)> {
        // The size and hash are what grouped the files, which the reporting never reads back
        decoded_report(&HashMap::from([((0, 0), group)]), &[], counters)
    }

    /// Report the duplicates of one unhashed pair, held to a direct comparison by its size alone
    fn reported_pair_group(
        pair: [PathBuf; 2],
        counters: &ProgressCounters,
    ) -> Vec<(PathBuf, PathBuf)> {
        decoded_report(&HashMap::new(), &[pair.into()], counters)
    }

    #[test]
    fn content_classes_identical_files_form_one_class() {
        let dir = tempfile::TempDir::new().unwrap();
        let paths = write_group(dir.path(), &[b"dup", b"dup", b"dup"]);
        let copies = unshared_copies(&paths);

        assert_eq!(
            content_classes(&copies).unwrap(),
            vec![(&copies[0], vec![&copies[1], &copies[2]])]
        );
    }

    #[test]
    fn content_classes_collision_keeps_duplicates_of_other_classes() {
        let dir = tempfile::TempDir::new().unwrap();
        // The first file colliding with the rest is what hides the duplicates behind it
        let paths = write_group(dir.path(), &[b"odd", b"dup", b"dup"]);
        let copies = unshared_copies(&paths);

        assert_eq!(
            content_classes(&copies).unwrap(),
            vec![(&copies[0], vec![]), (&copies[1], vec![&copies[2]]),]
        );
    }

    #[test]
    fn content_classes_compares_one_file_per_copy() {
        let dir = tempfile::TempDir::new().unwrap();
        let paths = write_group(dir.path(), &[b"dup", b"dup"]);
        // A file already sharing its data is never read, so a path that can not be opened stands in for one
        let missing = dir.path().join("missing");
        let copies = vec![
            plain_copy(&paths[0], vec![missing.as_path()]),
            plain_copy(&paths[1], vec![]),
        ];

        assert_eq!(
            content_classes(&copies).unwrap(),
            vec![(&copies[0], vec![&copies[1]])]
        );
    }

    #[test]
    fn data_copies_reflinked_files_form_one_copy() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        write_flushed(&first, EXTENT_SIZE);
        fs::copy(&first, &second).unwrap();
        let paths = vec![first.clone(), second.clone()];

        assert_eq!(
            data_copies(&paths).unwrap(),
            vec![plain_copy(&first, vec![second.as_path()])]
        );
    }

    #[test]
    fn data_copies_skips_missing_files_and_groups_remaining_reflinks() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        write_flushed(&first, EXTENT_SIZE);
        fs::copy(&first, &second).unwrap();
        // A path that can not be opened is skipped, and does not abort the group forming behind it
        let paths = vec![dir.path().join("missing"), first.clone(), second.clone()];

        assert_eq!(
            data_copies(&paths).unwrap(),
            vec![plain_copy(&first, vec![second.as_path()])]
        );
    }

    #[test]
    fn data_copies_indexes_interleaved_reflink_sets() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        let first_shared = dir.path().join("first-shared");
        let second_shared = dir.path().join("second-shared");
        write_flushed(&first, EXTENT_SIZE);
        write_flushed(&second, EXTENT_SIZE);
        fs::copy(&first, &first_shared).unwrap();
        fs::copy(&second, &second_shared).unwrap();
        // Interleaving two sets of reflinks leaves the second copy behind the first in the index
        let paths = vec![
            first.clone(),
            second.clone(),
            first_shared.clone(),
            second_shared.clone(),
        ];

        assert_eq!(
            data_copies(&paths).unwrap(),
            vec![
                plain_copy(&first, vec![first_shared.as_path()]),
                plain_copy(&second, vec![second_shared.as_path()]),
            ]
        );
    }

    #[test]
    fn data_copies_splits_differing_compressed_ranges() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = clone_compressed_ranges(dir.path());
        let paths = vec![first.clone(), second.clone()];

        // A compressed extent is reported without the offset its data sits at, so the shared key alone would
        // group the files although they differ, which comparing the ambiguous ranges tells apart
        assert_eq!(
            data_copies(&paths).unwrap(),
            vec![plain_copy(&first, vec![]), plain_copy(&second, vec![])]
        );
    }

    #[test]
    fn data_copies_inlined_files_stand_apart() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        write_flushed(&first, INLINE_SIZE);
        // Btrfs copies inline data into the destination instead of sharing it
        fs::copy(&first, &second).unwrap();
        let paths = vec![first.clone(), second.clone()];

        assert_eq!(
            data_copies(&paths).unwrap(),
            vec![
                DataCopy {
                    inlined: true,
                    ..plain_copy(&first, vec![])
                },
                DataCopy {
                    inlined: true,
                    ..plain_copy(&second, vec![])
                },
            ]
        );
    }

    #[test]
    fn try_attach_shared_attaches_a_reflinked_file() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        write_flushed(&first, EXTENT_SIZE);
        fs::copy(&first, &second).unwrap();
        let index = Mutex::new(HashMap::new());

        assert!(!attached(&index, &first));
        assert!(attached(&index, &second));
        let members: Vec<_> = index.into_inner().unwrap().into_values().collect();
        assert_eq!(members, vec![(first, vec![second])]);
    }

    #[test]
    fn try_attach_shared_confirms_a_compressed_tail() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        write_compressed(&first, EXTENT_SIZE);
        fs::copy(&first, &second).unwrap();
        assert!(files_have_same_extent_key(&first, &second));
        let index = Mutex::new(HashMap::new());

        assert!(!attached(&index, &first));
        assert!(attached(&index, &second));
        let members: Vec<_> = index.into_inner().unwrap().into_values().collect();
        assert_eq!(members, vec![(first, vec![second])]);
    }

    #[test]
    fn try_attach_shared_rejects_a_differing_compressed_tail() {
        let Some(dir) = btrfs_test_dir() else { return };
        let base = dir.path().join("base");
        write_compressed(&base, 2 * usize::try_from(BTRFS_MAX_UNCOMPRESSED).unwrap());
        let (first, second) = pair_paths(dir.path());
        // Both hold the first extent in full and a partial tail of the second one, at offsets the mapping does
        // not report, so their keys match while their tails differ
        clone_range(
            &base,
            0,
            BTRFS_MAX_UNCOMPRESSED + CLONE_RANGE_SIZE,
            &first,
            0,
        );
        clone_range(&base, 0, BTRFS_MAX_UNCOMPRESSED, &second, 0);
        clone_range(
            &base,
            BTRFS_MAX_UNCOMPRESSED + CLONE_RANGE_SIZE,
            CLONE_RANGE_SIZE,
            &second,
            BTRFS_MAX_UNCOMPRESSED,
        );
        assert!(files_have_same_extent_key(&first, &second));
        assert_ne!(fs::read(&first).unwrap(), fs::read(&second).unwrap());
        let index = Mutex::new(HashMap::new());

        assert!(!attached(&index, &first));
        assert!(!attached(&index, &second));
        let members: Vec<_> = index.into_inner().unwrap().into_values().collect();
        assert_eq!(members, vec![(first, vec![])]);
    }

    #[test]
    fn try_attach_shared_leaves_a_mostly_ambiguous_file_to_hashing() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = clone_compressed_ranges(dir.path());
        let index = Mutex::new(HashMap::new());

        assert!(!attached(&index, &first));
        // The keys match, but confirming would read more than hashing, so the file is left to it
        assert!(!attached(&index, &second));
        let members: Vec<_> = index.into_inner().unwrap().into_values().collect();
        assert_eq!(members, vec![(first, vec![])]);
    }

    #[test]
    fn report_duplicates_reports_independently_allocated_files() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        write_flushed(&first, EXTENT_SIZE);
        write_flushed(&second, EXTENT_SIZE);

        assert_eq!(
            reported_pairs(
                vec![first.clone(), second.clone()],
                &ProgressCounters::default()
            ),
            vec![(first, second)]
        );
    }

    #[test]
    fn report_duplicates_counts_reflinked_files_instead_of_reporting_them() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        write_flushed(&first, EXTENT_SIZE);
        fs::copy(&first, &second).unwrap();
        let counters = ProgressCounters::default();

        assert_eq!(reported_pairs(vec![first, second], &counters), vec![]);
        assert_eq!(
            counters.to_string(),
            "0 files, 0 hashes, 0 hash collisions, 1 already reflinked, 0 inlined, 0 duplicates"
        );
    }

    #[test]
    fn report_duplicates_counts_inlined_pairs_instead_of_reporting_them() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        write_flushed(&first, INLINE_SIZE);
        write_flushed(&second, INLINE_SIZE);
        let counters = ProgressCounters::default();

        assert_eq!(reported_pairs(vec![first, second], &counters), vec![]);
        assert_eq!(
            counters.to_string(),
            "0 files, 0 hashes, 0 hash collisions, 0 already reflinked, 1 inlined, 0 duplicates"
        );
    }

    #[test]
    fn report_duplicates_reports_an_inlined_file_against_an_extent() {
        let Some(dir) = btrfs_test_dir() else { return };
        let inlined = dir.path().join("inlined");
        let truncated = dir.path().join("truncated");
        write_flushed(&inlined, INLINE_SIZE);
        // Truncating below the inline threshold keeps the allocated extent, so the pair holds one of each, and
        // reflinking releases that extent
        write_flushed(&truncated, EXTENT_SIZE);
        let file = File::options().write(true).open(&truncated).unwrap();
        file.set_len(INLINE_SIZE as u64).unwrap();
        file.sync_data().unwrap();

        assert_eq!(
            reported_pairs(
                vec![inlined.clone(), truncated.clone()],
                &ProgressCounters::default()
            ),
            vec![(inlined, truncated)]
        );
    }

    #[test]
    fn report_duplicates_reports_every_file_of_a_shared_copy() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        let shared = dir.path().join("shared");
        write_flushed(&first, EXTENT_SIZE);
        write_flushed(&second, EXTENT_SIZE);
        fs::copy(&second, &shared).unwrap();

        // Reflinking second onto first leaves shared holding the copy second stood for, so it needs its own pair
        assert_eq!(
            reported_pairs(
                vec![first.clone(), second.clone(), shared.clone()],
                &ProgressCounters::default()
            ),
            vec![(first.clone(), second), (first, shared)]
        );
    }

    #[test]
    fn report_duplicates_confirms_a_member_past_a_differing_variant() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (same, different) = clone_compressed_ranges(dir.path());
        let same_shared = dir.path().join("same-shared");
        fs::copy(&same, &same_shared).unwrap();
        assert!(files_have_same_extent_key(&same, &same_shared));
        // An independent file leads the class, so the compressed copy is the one whose members get confirmed
        let independent = dir.path().join("independent");
        fs::write(&independent, fs::read(&same).unwrap()).unwrap();
        File::open(&independent).unwrap().sync_data().unwrap();

        // The differing variant is registered first under the shared key, so confirming a member has to scan past it
        assert_eq!(
            reported_pairs(
                vec![
                    independent.clone(),
                    different,
                    same.clone(),
                    same_shared.clone()
                ],
                &ProgressCounters::default()
            ),
            vec![(independent.clone(), same), (independent, same_shared)]
        );
    }

    #[test]
    fn report_duplicates_reports_a_collision_duplicate_hidden_by_shared_extents() {
        let Some(dir) = btrfs_test_dir() else { return };
        // The clones share a compressed extent yet hold different bytes; only a hash collision could group them,
        // which the fake hash key of reported_pairs stands in for
        let (colliding, hidden) = clone_compressed_ranges(dir.path());
        // An independent copy of the second clone, the duplicate the shared extent must not hide behind the first
        let duplicate = dir.path().join("duplicate");
        fs::write(&duplicate, fs::read(&hidden).unwrap()).unwrap();
        File::open(&duplicate).unwrap().sync_data().unwrap();
        let counters = ProgressCounters::default();

        assert_eq!(
            reported_pairs(
                vec![colliding, hidden.clone(), duplicate.clone()],
                &counters
            ),
            vec![(hidden, duplicate)]
        );
        assert_eq!(
            counters.to_string(),
            "0 files, 0 hashes, 1 hash collisions, 0 already reflinked, 0 inlined, 1 duplicates"
        );
    }

    #[test]
    fn report_duplicates_counts_collision_and_reports_later_class() {
        let Some(dir) = btrfs_test_dir() else { return };
        let duplicate = incompressible_bytes(EXTENT_SIZE);
        let mut odd = duplicate.clone();
        *odd.first_mut().unwrap() ^= 0xff;
        let mut odder = duplicate.clone();
        *odder.last_mut().unwrap() ^= 0xff;
        // The colliding files lead, so reporting has to carry on past the classes they form alone, and a third
        // content makes the count differ from the number of collided groups
        let paths = write_group(dir.path(), &[&odd, &odder, &duplicate, &duplicate]);
        let counters = ProgressCounters::default();

        assert_eq!(
            reported_pairs(paths.clone(), &counters),
            vec![(paths[2].clone(), paths[3].clone())]
        );
        assert_eq!(
            counters.to_string(),
            "0 files, 0 hashes, 2 hash collisions, 0 already reflinked, 0 inlined, 1 duplicates"
        );
    }

    #[test]
    fn report_duplicates_pair_group_reports_unhashed_duplicates() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        write_flushed(&first, EXTENT_SIZE);
        fs::write(&second, fs::read(&first).unwrap()).unwrap();
        File::open(&second).unwrap().sync_data().unwrap();
        let counters = ProgressCounters::default();

        assert_eq!(
            reported_pair_group([first.clone(), second.clone()], &counters),
            vec![(first, second)]
        );
    }

    #[test]
    fn report_duplicates_pair_group_of_distinct_contents_reports_nothing() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        write_flushed(&first, EXTENT_SIZE);
        let mut other = incompressible_bytes(EXTENT_SIZE);
        *other.first_mut().unwrap() ^= 0xff;
        fs::write(&second, other).unwrap();
        File::open(&second).unwrap().sync_data().unwrap();
        let counters = ProgressCounters::default();

        assert_eq!(
            reported_pair_group([first.clone(), second.clone()], &counters),
            vec![]
        );
        // Two contents behind one size is the normal outcome of an unhashed pair, not a hash collision
        assert_eq!(
            counters.to_string(),
            "0 files, 0 hashes, 0 hash collisions, 0 already reflinked, 0 inlined, 0 duplicates"
        );
    }

    #[test]
    fn report_duplicates_pair_group_of_compressed_clones_reports_nothing() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = clone_compressed_ranges(dir.path());
        let counters = ProgressCounters::default();

        assert_eq!(reported_pair_group([first, second], &counters), vec![]);
        // The shared extent does not prove the pair equal, so it counts neither as reflinked nor as duplicate
        assert_eq!(
            counters.to_string(),
            "0 files, 0 hashes, 0 hash collisions, 0 already reflinked, 0 inlined, 0 duplicates"
        );
    }

    #[test]
    fn report_duplicates_pair_group_counts_reflinked_compressed_pair() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        write_compressed(&first, EXTENT_SIZE);
        fs::copy(&first, &second).unwrap();
        assert!(files_have_same_extent_key(&first, &second));
        let counters = ProgressCounters::default();

        assert_eq!(reported_pair_group([first, second], &counters), vec![]);
        assert_eq!(
            counters.to_string(),
            "0 files, 0 hashes, 0 hash collisions, 1 already reflinked, 0 inlined, 0 duplicates"
        );
    }

    #[test]
    fn write_pair_utf8() {
        let mut out = Vec::new();
        write_pair(&mut out, Path::new("/a/b"), Path::new("/c/d")).unwrap();
        assert_eq!(out, b"/a/b\0/c/d\0");
    }

    #[test]
    fn write_pair_non_utf8() {
        let mut out = Vec::new();
        write_pair(
            &mut out,
            Path::new(OsStr::from_bytes(b"/a\xff/b")),
            Path::new(OsStr::from_bytes(b"/c\xfe/d")),
        )
        .unwrap();
        assert_eq!(out, b"/a\xff/b\0/c\xfe/d\0");
    }

    #[test]
    fn try_ficlone_reflinks_a_pair() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        write_flushed(&first, EXTENT_SIZE);
        fs::write(&second, fs::read(&first).unwrap()).unwrap();
        File::open(&second).unwrap().sync_data().unwrap();

        let src = File::open(&first).unwrap();
        let dst = File::options().write(true).open(&second).unwrap();
        assert!(try_ficlone(&src, &dst).is_ok());
        assert!(files_have_same_extent_key(&first, &second));
    }

    #[test]
    fn report_duplicates_with_dedup_ficlone_reflinks_pairs() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
        write_flushed(&first, EXTENT_SIZE);
        fs::write(&second, fs::read(&first).unwrap()).unwrap();
        File::open(&second).unwrap().sync_data().unwrap();
        let counters = ProgressCounters::default();

        // Before dedup, files are independent
        assert!(!files_have_same_extent_key(&first, &second));

        let mut out = Vec::new();
        report_duplicates(
            &HashMap::from([((0, 0), vec![first.clone(), second.clone()])]),
            &[],
            true,
            &counters,
            &mut out,
        )
        .unwrap();

        // After dedup, files should share extents
        assert!(files_have_same_extent_key(&first, &second));
        assert_eq!(
            counters.to_string(),
            "0 files, 0 hashes, 0 hash collisions, 0 already reflinked, 0 inlined, 1 duplicates"
        );
    }
}
