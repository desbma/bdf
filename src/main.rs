//! Btrfs Duplicate Finder

use std::{
    cmp::max,
    collections::hash_map::{Entry, HashMap},
    ffi::OsStr,
    fmt,
    fs::{self, File},
    io::{self, BufRead, BufReader, Read as _, Write},
    mem::zeroed,
    os::{
        fd::AsRawFd as _,
        unix::{ffi::OsStrExt as _, fs::MetadataExt as _},
    },
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use anyhow::Context as _;
use clap::Parser;
use linux_raw_sys::btrfs::btrfs_ioctl_fs_info_args;
use multimap::MultiMap;
use xxhash_rust::xxh3;

/// File read chunk size, in bytes
const READ_BUFFER_SIZE: usize = 256 * 1024;

/// Extent flags marking a physical location that does not identify the underlying data
const UNRESOLVED_EXTENT_FLAGS: fiemap::FiemapExtentFlags =
    fiemap::FiemapExtentFlags::UNKNOWN.union(fiemap::FiemapExtentFlags::DELALLOC);

/// Convenience type for a pair of crossbeam channel ends
type CrossbeamChannel<T> = (crossbeam_channel::Sender<T>, crossbeam_channel::Receiver<T>);

/// Identifier of a Btrfs filesystem, the same for all of its subvolumes
type BtrfsFsid = [u8; 16];

nix::ioctl_read!(
    /// Query the Btrfs filesystem holding an open file
    btrfs_fs_info,
    0x94,
    31,
    btrfs_ioctl_fs_info_args
);

/// Return the identifier of the Btrfs filesystem holding `path`, `None` if it is not on Btrfs
fn btrfs_fsid(path: &Path) -> Result<Option<BtrfsFsid>, io::Error> {
    let file = File::open(path)?;
    // SAFETY: every field is an integer, for which zero is a valid value
    let mut args: btrfs_ioctl_fs_info_args = unsafe { zeroed() };
    // SAFETY: the ioctl writes at most `size_of::<btrfs_ioctl_fs_info_args>()` bytes to `args`
    match unsafe { btrfs_fs_info(file.as_raw_fd(), &raw mut args) } {
        Ok(_) => Ok(Some(args.fsid)),
        // Any other filesystem leaves the ioctl unimplemented
        Err(nix::errno::Errno::ENOTTY) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// A single Btrfs filesystem, which reflinks can not reach outside of
struct BtrfsFilesystem {
    /// Identifier of the filesystem
    fsid: BtrfsFsid,
    /// Whether a device belongs to the filesystem, cached to keep the ioctl off the per file path
    devices: HashMap<u64, bool>,
}

impl BtrfsFilesystem {
    /// Identify the Btrfs filesystem holding `path`, if any
    fn containing(path: &Path) -> Result<Option<Self>, io::Error> {
        Ok(btrfs_fsid(path)?.map(|fsid| Self {
            fsid,
            devices: HashMap::new(),
        }))
    }

    /// Test whether `path`, sitting on device `device`, is on this filesystem
    ///
    /// Each subvolume has its own device, so the device alone does not settle it.
    fn holds(&mut self, path: &Path, device: u64) -> Result<bool, io::Error> {
        if let Some(&known) = self.devices.get(&device) {
            return Ok(known);
        }
        let holds = btrfs_fsid(path)? == Some(self.fsid);
        self.devices.insert(device, holds);
        Ok(holds)
    }
}

/// Walk a directory tree, without leaving the Btrfs filesystem it starts on
///
/// Subvolumes of a filesystem each have their own device, so pruning on a device change would skip them, while
/// reflinking across them is perfectly valid.
fn walk_btrfs_dir(
    input_dir: &Path,
) -> anyhow::Result<impl Iterator<Item = walkdir::Result<walkdir::DirEntry>>> {
    let mut filesystem = BtrfsFilesystem::containing(input_dir)
        .with_context(|| format!("Failed to identify the filesystem of {input_dir:?}"))?
        .with_context(|| format!("{input_dir:?} is not on a Btrfs filesystem"))?;
    Ok(walkdir::WalkDir::new(input_dir)
        .into_iter()
        .filter_entry(move |entry| {
            // A regular file can be a mount point of its own, so checking directories alone would let one through
            let file_type = entry.file_type();
            if !file_type.is_dir() && !file_type.is_file() {
                return true;
            }
            let path = entry.path();
            entry
                .metadata()
                .map_err(io::Error::from)
                .and_then(|metadata| filesystem.holds(path, metadata.dev()))
                .unwrap_or_else(|e| {
                    log::warn!("Error while identifying the filesystem of {path:?}: {e}");
                    false
                })
        }))
}

/// Command line arguments
#[derive(Debug, Parser)]
#[command(
    version,
    about = "Find identical files, candidates for reflinking, on Btrfs filesystems."
)]
pub struct CommandLineOpts {
    /// Input directory tree, if not set will take NUL terminated paths from stdin
    pub dir: Option<PathBuf>,

    /// Minimum file size in bytes to consider
    #[structopt(short, long)]
    pub min_size: Option<u64>,
}

/// Compute an XXH3-64 non-cryptographic file hash
fn hash_file(path: &Path, hasher: &mut xxh3::Xxh3, buffer: &mut Vec<u8>) -> Result<u64, io::Error> {
    let mut reader = BufReader::new(File::open(path)?);
    hasher.reset();
    loop {
        // Unlike a bare read, read_to_end fills the whole chunk, resuming when a signal interrupts it
        buffer.clear();
        reader
            .by_ref()
            .take(READ_BUFFER_SIZE as u64)
            .read_to_end(buffer)?;
        if buffer.is_empty() {
            break;
        }
        hasher.update(buffer);
    }
    Ok(hasher.digest())
}

/// Processing progress counters
#[derive(Default)]
struct ProgressCounters {
    /// Number of files that were targeted for analysis
    file: AtomicUsize,
    /// Number of files that were hashed
    hash: AtomicUsize,
    /// Number of extra contents found among files sharing a size and hash
    hash_collision: AtomicUsize,
    /// Number of identical files already reflinked
    reflinked: AtomicUsize,
    /// Number of identical files with inlined data, which reflinking can not share
    inlined: AtomicUsize,
    /// Number of duplicate files, candidates for reflinking
    duplicate_candidate: AtomicUsize,
}

impl fmt::Display for ProgressCounters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} files, {} hashes, {} hash collisions, {} already reflinked, {} inlined, {} duplicates",
            self.file.load(Ordering::Relaxed),
            self.hash.load(Ordering::Relaxed),
            self.hash_collision.load(Ordering::Relaxed),
            self.reflinked.load(Ordering::Relaxed),
            self.inlined.load(Ordering::Relaxed),
            self.duplicate_candidate.load(Ordering::Relaxed),
        )
    }
}

/// Test if two files have the same content
///
/// Both files must be the same size: reading stops at the end of `first`, so a longer `second` compares equal.
fn same_content(first: &Path, second: &Path) -> Result<bool, io::Error> {
    let file1 = File::open(first)?;
    let file2 = File::open(second)?;
    debug_assert_eq!(file1.metadata()?.len(), file2.metadata()?.len());
    let mut reader1 = BufReader::new(file1);
    let mut reader2 = BufReader::new(file2);
    let mut buffer1 = Vec::with_capacity(READ_BUFFER_SIZE);
    let mut buffer2 = Vec::with_capacity(READ_BUFFER_SIZE);
    loop {
        // Unlike a bare read, read_to_end fills the whole chunk, resuming when a signal interrupts it
        buffer1.clear();
        reader1
            .by_ref()
            .take(READ_BUFFER_SIZE as u64)
            .read_to_end(&mut buffer1)?;
        if buffer1.is_empty() {
            break;
        }
        buffer2.clear();
        reader2
            .by_ref()
            .take(READ_BUFFER_SIZE as u64)
            .read_to_end(&mut buffer2)?;
        if buffer1 != buffer2 {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Partition files sharing a size and hash into classes of identical content
///
/// Each class is its first member, standing for the whole class, followed by the others. More than one class means
/// the hash collided.
fn content_classes(filepaths: &[PathBuf]) -> Result<Vec<(&Path, Vec<&Path>)>, io::Error> {
    let mut classes: Vec<(&Path, Vec<&Path>)> = Vec::new();
    'filepath: for filepath in filepaths {
        for (representative, others) in &mut classes {
            // Content equality is transitive, so comparing against one member settles the whole class
            if same_content(representative, filepath)? {
                others.push(filepath);
                continue 'filepath;
            }
        }
        classes.push((filepath, Vec::new()));
    }
    Ok(classes)
}

/// How two identical files relate on disk
#[cfg_attr(test, derive(Debug, Eq, PartialEq))]
enum ExtentSharing {
    /// Both files map to the same extents
    Shared,
    /// Files map to distinct extents, reflinking them would free space
    Distinct,
    /// Both files have their data inlined in filesystem metadata, which reflinking can not share
    Inlined,
}

/// Map the extents of a file, flushing pending writes first
fn file_extents(path: &Path) -> Result<Vec<fiemap::FiemapExtent>, io::Error> {
    let file = File::open(path)?;
    // Data under delayed allocation is reported without a location, hiding whether it will be inlined
    file.sync_data()?;
    fiemap::Fiemap::new(&file).collect()
}

/// Whether a file stores its data in filesystem metadata rather than in a data extent
fn is_inlined(extents: &[fiemap::FiemapExtent]) -> bool {
    extents.iter().any(|extent| {
        extent
            .fe_flags
            .contains(fiemap::FiemapExtentFlags::DATA_INLINE)
    })
}

/// Compare the extent maps of two identical files
fn compare_extents(
    extents1: &[fiemap::FiemapExtent],
    extents2: &[fiemap::FiemapExtent],
) -> ExtentSharing {
    // Reflinking an inlined file onto one holding a regular extent releases that extent, so only a pair inlined
    // on both sides has nothing to gain
    if is_inlined(extents1) && is_inlined(extents2) {
        return ExtentSharing::Inlined;
    }
    if extents1.len() != extents2.len() {
        return ExtentSharing::Distinct;
    }
    for (extent1, extent2) in extents1.iter().zip(extents2.iter()) {
        // A location left unresolved, by a write landing after the flush, is a placeholder identical between
        // unrelated files, so it can never establish sharing
        if (extent1.fe_flags | extent2.fe_flags).intersects(UNRESOLVED_EXTENT_FLAGS)
            || extent1.fe_physical != extent2.fe_physical
            || extent1.fe_length != extent2.fe_length
        {
            return ExtentSharing::Distinct;
        }
    }
    ExtentSharing::Shared
}

/// Read a NUL terminated path into `buf`, stripping the terminator, `None` at end of input
fn read_nul_path<'b, R>(reader: &mut R, buf: &'b mut Vec<u8>) -> Result<Option<&'b Path>, io::Error>
where
    R: BufRead,
{
    buf.clear();
    if reader.read_until(0, buf)? == 0 {
        return Ok(None);
    }
    // Last path may not be terminated if input does not end with a separator
    if buf.last() == Some(&0) {
        buf.pop();
    }
    Ok(Some(Path::new(OsStr::from_bytes(buf))))
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

/// Read path metadata without following symlinks, warning when it can not be read
fn path_metadata(path: &Path) -> Option<fs::Metadata> {
    fs::symlink_metadata(path)
        .inspect_err(|e| log::warn!("Error while reading metadata of {path:?}: {e}"))
        .ok()
}

/// Return the size when a file this large should be hashed
fn wanted_file_size(file_size: u64, min_size: Option<u64>) -> Option<u64> {
    // Don't bother for empty files
    (file_size != 0 && min_size.is_none_or(|minimum| file_size >= minimum)).then_some(file_size)
}

#[expect(clippy::too_many_lines)]
fn main() -> anyhow::Result<()> {
    // Init logger
    simple_logger::SimpleLogger::new()
        .init()
        .context("Failed to init logger")?;

    // Parse command line opts
    let cl_opts = CommandLineOpts::parse();
    log::trace!("{cl_opts:?}");
    let dir_walk = cl_opts.dir.as_deref().map(walk_btrfs_dir).transpose()?;

    // Get usable core count
    let cpu_count = thread::available_parallelism()?.get();

    // Channels
    let (to_hashed_tx, to_hashed_rx): CrossbeamChannel<(PathBuf, u64)> =
        crossbeam_channel::unbounded();
    let (hashed_tx, hashed_rx): CrossbeamChannel<(PathBuf, u64, u64)> =
        crossbeam_channel::unbounded();

    // File hash map
    let mut files: MultiMap<(u64, u64), PathBuf> = MultiMap::new();

    // Progress
    let progress = indicatif::ProgressBar::new_spinner();
    progress.enable_steady_tick(Duration::from_millis(300));
    let progress_counters = Arc::new(ProgressCounters::default());

    thread::scope(|scope| -> anyhow::Result<()> {
        // Worker threads
        let worker_count = max(cpu_count - 1, 1);
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            // Per thread clones
            let to_hashed_rx = to_hashed_rx.clone();
            let hashed_tx = hashed_tx.clone();
            let progress = progress.clone();
            let progress_counters = Arc::clone(&progress_counters);

            workers.push(scope.spawn(move || -> anyhow::Result<()> {
                let mut hasher = xxh3::Xxh3::new();
                let mut buffer = Vec::with_capacity(READ_BUFFER_SIZE);
                while let Ok((path, file_size)) = to_hashed_rx.recv() {
                    let Ok(hash) = hash_file(&path, &mut hasher, &mut buffer).inspect_err(|e| {
                        log::warn!("Error while hashing {path:?}: {e}");
                    }) else {
                        continue;
                    };

                    log::debug!("{path:?} {hash:016x}");
                    progress_counters.hash.fetch_add(1, Ordering::AcqRel);
                    progress.set_message(format!("{progress_counters}"));

                    hashed_tx.send((path, file_size, hash))?;
                }

                Ok(())
            }));
        }
        drop(to_hashed_rx);
        drop(hashed_tx);

        // Iterate over files
        let mut entry_map: HashMap<u64, Option<walkdir::DirEntry>> = HashMap::new();
        if let Some(dir_walk) = dir_walk {
            for entry in dir_walk {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(e) => {
                        log::warn!("{e}");
                        continue;
                    }
                };
                if !entry.file_type().is_file() {
                    continue;
                }
                let Some(metadata) = path_metadata(entry.path()) else {
                    continue;
                };
                let Some(file_size) = wanted_file_size(metadata.len(), cl_opts.min_size) else {
                    continue;
                };
                let path = entry.path();
                log::debug!("{path:?}");
                progress_counters.file.fetch_add(1, Ordering::AcqRel);
                progress.set_message(format!("{progress_counters}"));

                // Decide what to to depending on whether or not we have already seen some files for this size
                // This allows saving some hash computations for the common case
                match entry_map.entry(file_size) {
                    Entry::Vacant(e) => {
                        // First file for this size, keep entry and move along
                        e.insert(Some(entry));
                    }
                    Entry::Occupied(mut e) => {
                        // The first file of a size is held back until a second one shows up, so it is only hashed
                        // once it can have a duplicate
                        if let Some(prev_entry) = e.get_mut().take() {
                            to_hashed_tx.send((prev_entry.path().to_path_buf(), file_size))?;
                        }
                        to_hashed_tx.send((path.to_path_buf(), file_size))?;
                    }
                }
            }
        } else {
            let mut stdin_locked = io::stdin().lock();
            let mut buf = Vec::new();
            // Reflinks can not cross filesystems, so every input has to be on the one the first input is on
            let mut filesystem: Option<BtrfsFilesystem> = None;
            while let Some(path) = read_nul_path(&mut stdin_locked, &mut buf)? {
                let Some(metadata) = path_metadata(path) else {
                    continue;
                };
                if !metadata.is_file() {
                    log::warn!("{path:?} is not a file, ignoring it");
                    continue;
                }
                // A file that can not be opened is skipped rather than fatal, as it would be when hashing it
                let filesystem = match filesystem {
                    Some(ref mut filesystem) => filesystem,
                    None => match BtrfsFilesystem::containing(path) {
                        Ok(Some(found)) => filesystem.insert(found),
                        Ok(None) => {
                            anyhow::bail!("Input file {path:?} is not on a Btrfs filesystem")
                        }
                        Err(e) => {
                            log::warn!("Error while identifying the filesystem of {path:?}: {e}");
                            continue;
                        }
                    },
                };
                match filesystem.holds(path, metadata.dev()) {
                    Ok(true) => {}
                    Ok(false) => anyhow::bail!(
                        "Input file {path:?} is not on the same Btrfs filesystem as the first input file"
                    ),
                    Err(e) => {
                        log::warn!("Error while identifying the filesystem of {path:?}: {e}");
                        continue;
                    }
                }
                let Some(file_size) = wanted_file_size(metadata.len(), cl_opts.min_size) else {
                    continue;
                };
                log::debug!("{path:?}");
                progress_counters.file.fetch_add(1, Ordering::AcqRel);
                progress.set_message(format!("{progress_counters}"));

                to_hashed_tx.send((path.to_path_buf(), file_size))?;
            }
        }
        drop(to_hashed_tx);

        // Fill hashmap
        for (filepath, file_size, hash) in &hashed_rx {
            files.insert((file_size, hash), filepath);
        }

        // Workers have all completed once their channel ends were dropped above, so this does not block
        workers.into_iter().try_for_each(|worker| {
            worker
                .join()
                .map_err(|e| anyhow::anyhow!("Worker thread panicked: {e:?}"))?
        })
    })?;

    // Remove unique hashes
    for key in files
        .keys()
        .filter(|k| !files.is_vec(k))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>()
    {
        files.remove(&key);
    }

    // Find candidates
    let mut stdout = io::stdout().lock();
    for ((_file_size, _file_hash), filepaths) in files.iter_all() {
        let classes = content_classes(filepaths)?;
        if classes.len() > 1 {
            log::warn!(
                "Files {filepaths:?} have the same size and hash but {count} distinct contents",
                count = classes.len()
            );
            progress_counters
                .hash_collision
                .fetch_add(classes.len() - 1, Ordering::AcqRel);
            progress.set_message(format!("{progress_counters}"));
        }

        for (first, others) in classes {
            for other in others {
                match compare_extents(&file_extents(first)?, &file_extents(other)?) {
                    ExtentSharing::Shared => {
                        log::debug!("Files {first:?} and {other:?} are already reflinked");
                        progress_counters.reflinked.fetch_add(1, Ordering::AcqRel);
                    }
                    ExtentSharing::Inlined => {
                        log::debug!(
                            "Files {first:?} and {other:?} have inlined data, reflinking would not free space"
                        );
                        progress_counters.inlined.fetch_add(1, Ordering::AcqRel);
                    }
                    ExtentSharing::Distinct => {
                        log::debug!("Files {first:?} and {other:?} are duplicates");
                        progress_counters
                            .duplicate_candidate
                            .fetch_add(1, Ordering::AcqRel);
                        write_pair(&mut stdout, first, other)?;
                    }
                }
                progress.set_message(format!("{progress_counters}"));
            }
        }
    }

    progress.finish();

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        io::Seek as _,
        process::{Command, Stdio},
    };

    use super::*;

    /// Size of a file small enough for Btrfs to store its data inline in metadata
    const INLINE_SIZE: usize = 500;

    /// Size of a file too large for Btrfs to store its data inline in metadata
    const EXTENT_SIZE: usize = 300 * 1024;

    /// Temporary directory under the build directory, `None` when it is not on a Btrfs filesystem
    ///
    /// Extent layout and subvolumes are filesystem specific, so tests relying on them can only run on Btrfs.
    fn btrfs_test_dir() -> Option<tempfile::TempDir> {
        let base = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/target"));
        if btrfs_fsid(base).unwrap().is_some() {
            Some(tempfile::TempDir::new_in(base).unwrap())
        } else {
            eprintln!("Skipping test, {base:?} is not on a Btrfs filesystem");
            None
        }
    }

    /// Create a Btrfs subvolume named `name` under `parent`, which needs the `btrfs` binary
    fn create_subvolume(parent: &Path, name: &str) -> PathBuf {
        let path = parent.join(name);
        let status = Command::new("btrfs")
            .args(["subvolume", "create"])
            .arg(&path)
            .stdout(Stdio::null())
            .status()
            .unwrap();
        assert!(status.success());
        path
    }

    /// Build incompressible bytes, so that transparent compression does not alter the extent layout
    fn incompressible_bytes(size: usize) -> Vec<u8> {
        (0..size.div_ceil(8))
            .flat_map(|index| xxh3::xxh3_64(&index.to_le_bytes()).to_le_bytes())
            .take(size)
            .collect()
    }

    /// Write a file of `size` bytes, leaving its data under delayed allocation
    fn write_unflushed(path: &Path, size: usize) {
        fs::write(path, incompressible_bytes(size)).unwrap();
    }

    /// Write a file of `size` bytes and flush it, so its extents are allocated
    fn write_flushed(path: &Path, size: usize) {
        write_unflushed(path, size);
        File::open(path).unwrap().sync_data().unwrap();
    }

    /// Map both files and compare them, as the candidate reporting does
    fn compare_files(first: &Path, second: &Path) -> ExtentSharing {
        compare_extents(
            &file_extents(first).unwrap(),
            &file_extents(second).unwrap(),
        )
    }

    /// Map a file without flushing it, leaving delayed allocation unresolved
    fn unflushed_extents(path: &Path) -> Vec<fiemap::FiemapExtent> {
        fiemap::fiemap(path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    /// Write two files and compare their content, as the candidate reporting does
    fn compare_content(first_content: &[u8], second_content: &[u8]) -> bool {
        let dir = tempfile::TempDir::new().unwrap();
        let first = dir.path().join("first");
        let second = dir.path().join("second");
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

    #[test]
    fn content_classes_identical_files_form_one_class() {
        let dir = tempfile::TempDir::new().unwrap();
        let paths = write_group(dir.path(), &[b"dup", b"dup", b"dup"]);

        assert_eq!(
            content_classes(&paths).unwrap(),
            vec![(
                paths[0].as_path(),
                vec![paths[1].as_path(), paths[2].as_path()]
            )]
        );
    }

    #[test]
    fn content_classes_collision_keeps_duplicates_of_other_classes() {
        let dir = tempfile::TempDir::new().unwrap();
        // The first file colliding with the rest is what hides the duplicates behind it
        let paths = write_group(dir.path(), &[b"odd", b"dup", b"dup"]);

        assert_eq!(
            content_classes(&paths).unwrap(),
            vec![
                (paths[0].as_path(), vec![]),
                (paths[1].as_path(), vec![paths[2].as_path()]),
            ]
        );
    }

    #[test]
    fn content_classes_all_distinct_files_are_singletons() {
        let dir = tempfile::TempDir::new().unwrap();
        let paths = write_group(dir.path(), &[b"aaa", b"bbb", b"ccc"]);

        assert_eq!(
            content_classes(&paths).unwrap(),
            vec![
                (paths[0].as_path(), vec![]),
                (paths[1].as_path(), vec![]),
                (paths[2].as_path(), vec![]),
            ]
        );
    }

    #[test]
    fn compare_extents_reflinked_is_shared() {
        let Some(dir) = btrfs_test_dir() else { return };
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        write_flushed(&first, EXTENT_SIZE);
        // On Btrfs a plain copy is reflinked, sharing the extents of the source
        fs::copy(&first, &second).unwrap();

        assert_eq!(compare_files(&first, &second), ExtentSharing::Shared);
    }

    #[test]
    fn compare_extents_independently_written_is_distinct() {
        let Some(dir) = btrfs_test_dir() else { return };
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        write_flushed(&first, EXTENT_SIZE);
        write_flushed(&second, EXTENT_SIZE);

        assert_eq!(compare_files(&first, &second), ExtentSharing::Distinct);
    }

    #[test]
    fn compare_extents_unflushed_is_distinct() {
        let Some(dir) = btrfs_test_dir() else { return };
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        write_unflushed(&first, EXTENT_SIZE);
        write_unflushed(&second, EXTENT_SIZE);

        // Both files map to a placeholder extent until their delayed allocation is resolved, which would make
        // them compare as shared
        assert_eq!(compare_files(&first, &second), ExtentSharing::Distinct);
    }

    #[test]
    fn compare_extents_partially_rewritten_reflink_is_distinct() {
        let Some(dir) = btrfs_test_dir() else { return };
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        write_flushed(&first, EXTENT_SIZE);
        fs::copy(&first, &second).unwrap();
        // Rewriting identical bytes breaks sharing for that range, leaving the files identical but no longer
        // fully reflinked
        let mut file = File::options().write(true).open(&second).unwrap();
        file.write_all(&incompressible_bytes(EXTENT_SIZE)[..4096])
            .unwrap();
        file.sync_data().unwrap();

        assert_eq!(compare_files(&first, &second), ExtentSharing::Distinct);
    }

    #[test]
    fn compare_extents_sparse_and_dense_are_distinct() {
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

        assert_eq!(compare_files(&sparse, &dense), ExtentSharing::Distinct);
    }

    #[test]
    fn compare_extents_independently_written_small_is_inlined() {
        let Some(dir) = btrfs_test_dir() else { return };
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        write_flushed(&first, INLINE_SIZE);
        write_flushed(&second, INLINE_SIZE);

        assert_eq!(compare_files(&first, &second), ExtentSharing::Inlined);
    }

    #[test]
    fn compare_extents_reflinked_small_is_inlined() {
        let Some(dir) = btrfs_test_dir() else { return };
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        write_flushed(&first, INLINE_SIZE);
        // Btrfs copies inline data into the destination instead of sharing it
        fs::copy(&first, &second).unwrap();

        assert_eq!(compare_files(&first, &second), ExtentSharing::Inlined);
    }

    #[test]
    fn compare_extents_unflushed_small_is_inlined() {
        let Some(dir) = btrfs_test_dir() else { return };
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        write_unflushed(&first, INLINE_SIZE);
        write_unflushed(&second, INLINE_SIZE);

        // Delayed allocation hides that the data will be inlined, which would report the pair as a candidate
        assert_eq!(compare_files(&first, &second), ExtentSharing::Inlined);
    }

    #[test]
    fn compare_extents_same_size_inline_and_extent_is_distinct() {
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

        let inlined_extents = file_extents(&inlined).unwrap();
        let truncated_extents = file_extents(&truncated).unwrap();
        assert!(is_inlined(&inlined_extents));
        assert!(!is_inlined(&truncated_extents));

        // Reflinking the inlined file onto the other releases the extent it holds alone
        assert_eq!(
            compare_extents(&inlined_extents, &truncated_extents),
            ExtentSharing::Distinct
        );
        assert_eq!(
            compare_extents(&truncated_extents, &inlined_extents),
            ExtentSharing::Distinct
        );
    }

    #[test]
    fn compare_extents_partially_shared_extent_is_distinct() {
        let Some(dir) = btrfs_test_dir() else { return };
        let whole = dir.path().join("whole");
        let part = dir.path().join("part");
        write_flushed(&whole, EXTENT_SIZE);
        fs::copy(&whole, &part).unwrap();
        // Truncating keeps the start of the reflinked extent, shortening it in place
        let file = File::options().write(true).open(&part).unwrap();
        file.set_len(EXTENT_SIZE as u64 / 2).unwrap();
        file.sync_data().unwrap();

        let whole_extents = file_extents(&whole).unwrap();
        let part_extents = file_extents(&part).unwrap();
        assert_eq!(whole_extents.len(), part_extents.len());
        assert_eq!(whole_extents[0].fe_physical, part_extents[0].fe_physical);
        assert_ne!(whole_extents[0].fe_length, part_extents[0].fe_length);

        assert_eq!(
            compare_extents(&whole_extents, &part_extents),
            ExtentSharing::Distinct
        );
    }

    #[test]
    fn compare_extents_prefix_map_is_distinct() {
        let Some(dir) = btrfs_test_dir() else { return };
        let path = dir.path().join("file");
        write_flushed(&path, EXTENT_SIZE);
        // Appending to a flushed file allocates a further extent
        let mut file = File::options().append(true).open(&path).unwrap();
        file.write_all(&incompressible_bytes(EXTENT_SIZE)).unwrap();
        file.sync_data().unwrap();

        let extents = file_extents(&path).unwrap();
        assert!(extents.len() > 1);

        // Every extent of the shorter map is shared, yet the files are not
        assert_eq!(
            compare_extents(&extents[..1], &extents),
            ExtentSharing::Distinct
        );
    }

    #[test]
    fn compare_extents_unresolved_locations_are_distinct() {
        let Some(dir) = btrfs_test_dir() else { return };
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        write_unflushed(&first, EXTENT_SIZE);
        write_unflushed(&second, EXTENT_SIZE);

        // Covers a write landing between the flush and the mapping, which the flush in file_extents makes
        // otherwise unreachable
        let extents1 = unflushed_extents(&first);
        let extents2 = unflushed_extents(&second);
        assert!(extents1
            .iter()
            .chain(extents2.iter())
            .all(|extent| extent.fe_flags.intersects(UNRESOLVED_EXTENT_FLAGS)));

        assert_eq!(
            compare_extents(&extents1, &extents2),
            ExtentSharing::Distinct
        );
    }

    #[test]
    fn compare_extents_one_sided_unresolved_location_is_distinct() {
        let Some(dir) = btrfs_test_dir() else { return };
        let path = dir.path().join("file");
        write_flushed(&path, EXTENT_SIZE);

        // Only a synthesized map can hold an unresolved location while matching an allocated one everywhere else
        let extents = file_extents(&path).unwrap();
        let mut unresolved = extents.clone();
        unresolved[0].fe_flags.insert(UNRESOLVED_EXTENT_FLAGS);

        assert_eq!(
            compare_extents(&unresolved, &extents),
            ExtentSharing::Distinct
        );
        assert_eq!(
            compare_extents(&extents, &unresolved),
            ExtentSharing::Distinct
        );
    }

    #[test]
    fn file_extents_resolves_delayed_allocation() {
        let Some(dir) = btrfs_test_dir() else { return };
        let path = dir.path().join("file");
        write_unflushed(&path, EXTENT_SIZE);

        let extents = file_extents(&path).unwrap();
        assert!(!extents.is_empty());
        assert!(!extents
            .iter()
            .any(|extent| extent.fe_flags.intersects(UNRESOLVED_EXTENT_FLAGS)));
    }

    #[test]
    fn file_extents_reports_inlined_data_of_unflushed_file() {
        let Some(dir) = btrfs_test_dir() else { return };
        let path = dir.path().join("file");
        write_unflushed(&path, INLINE_SIZE);

        let extents = file_extents(&path).unwrap();
        assert!(extents.iter().any(|extent| extent
            .fe_flags
            .contains(fiemap::FiemapExtentFlags::DATA_INLINE)));
    }

    #[test]
    fn read_nul_path_terminated() {
        let mut reader = &b"/a/b\0/c/d\0"[..];
        let mut buf = Vec::new();
        assert_eq!(
            read_nul_path(&mut reader, &mut buf).unwrap(),
            Some(Path::new("/a/b"))
        );
        assert_eq!(
            read_nul_path(&mut reader, &mut buf).unwrap(),
            Some(Path::new("/c/d"))
        );
        assert_eq!(read_nul_path(&mut reader, &mut buf).unwrap(), None);
    }

    #[test]
    fn read_nul_path_unterminated_last() {
        let mut reader = &b"/a/b\0/c/d"[..];
        let mut buf = Vec::new();
        assert_eq!(
            read_nul_path(&mut reader, &mut buf).unwrap(),
            Some(Path::new("/a/b"))
        );
        assert_eq!(
            read_nul_path(&mut reader, &mut buf).unwrap(),
            Some(Path::new("/c/d"))
        );
        assert_eq!(read_nul_path(&mut reader, &mut buf).unwrap(), None);
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
    fn read_nul_path_non_utf8() {
        let mut reader = &b"/a\xff/b\0"[..];
        let mut buf = Vec::new();
        assert_eq!(
            read_nul_path(&mut reader, &mut buf).unwrap(),
            Some(Path::new(OsStr::from_bytes(b"/a\xff/b")))
        );
    }

    #[test]
    fn btrfs_fsid_of_other_filesystem_is_none() {
        assert_eq!(btrfs_fsid(Path::new("/proc")).unwrap(), None);
    }

    #[test]
    fn btrfs_fsid_of_regular_file_of_other_filesystem_is_none() {
        assert_eq!(btrfs_fsid(Path::new("/proc/self/status")).unwrap(), None);
    }

    #[test]
    fn btrfs_fsid_is_shared_by_subvolumes_holding_distinct_devices() {
        let Some(dir) = btrfs_test_dir() else { return };
        let subvolume = create_subvolume(dir.path(), "subvolume");

        assert_ne!(
            fs::metadata(&subvolume).unwrap().dev(),
            fs::metadata(dir.path()).unwrap().dev()
        );
        assert_eq!(
            btrfs_fsid(&subvolume).unwrap(),
            btrfs_fsid(dir.path()).unwrap()
        );
    }

    #[test]
    fn walk_btrfs_dir_descends_into_subvolumes() {
        let Some(dir) = btrfs_test_dir() else { return };
        let subvolume = create_subvolume(dir.path(), "subvolume");
        let nested = create_subvolume(&subvolume, "nested");
        fs::write(dir.path().join("outside"), b"content").unwrap();
        fs::write(subvolume.join("inside"), b"content").unwrap();
        fs::write(nested.join("deeper"), b"content").unwrap();

        let mut walked: Vec<PathBuf> = walk_btrfs_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().into_path())
            .filter(|path| path.is_file())
            .collect();
        walked.sort();

        assert_eq!(
            walked,
            vec![
                dir.path().join("outside"),
                subvolume.join("inside"),
                nested.join("deeper"),
            ]
        );
    }
}
