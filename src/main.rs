//! Btrfs Duplicate Finder

use std::{
    cmp::max,
    collections::hash_map::{Entry, HashMap},
    ffi::OsStr,
    fmt,
    fs::{self, File},
    io::{self, BufRead, Read as _, Write},
    iter,
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
use xxhash_rust::xxh3;

/// File read chunk size, in bytes
const READ_BUFFER_SIZE: usize = 256 * 1024;

/// Extent flags marking a physical location that does not identify the underlying data
const UNRESOLVED_EXTENT_FLAGS: fiemap::FiemapExtentFlags =
    fiemap::FiemapExtentFlags::UNKNOWN.union(fiemap::FiemapExtentFlags::DELALLOC);

/// Extent flags marking a location that can never establish sharing
///
/// Inlined data, which Btrfs copies into the destination rather than sharing, and a location left unresolved are
/// both placeholders identical between unrelated files.
const UNSHAREABLE_EXTENT_FLAGS: fiemap::FiemapExtentFlags =
    UNRESOLVED_EXTENT_FLAGS.union(fiemap::FiemapExtentFlags::DATA_INLINE);

/// Convenience type for a pair of crossbeam channel ends
type CrossbeamChannel<T> = (crossbeam_channel::Sender<T>, crossbeam_channel::Receiver<T>);

/// Identifier of a Btrfs filesystem, the same for all of its subvolumes
type BtrfsFsid = [u8; 16];

/// Offset of one extent in the file, its location on the device, and its length
type ExtentLocation = (u64, u64, u64);

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
    let file = File::open(path)?;
    hasher.reset();
    loop {
        // Unlike a bare read, read_to_end fills the whole chunk, resuming when a signal interrupts it
        buffer.clear();
        (&file).take(READ_BUFFER_SIZE as u64).read_to_end(buffer)?;
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
    let mut buffer1 = Vec::with_capacity(READ_BUFFER_SIZE);
    let mut buffer2 = Vec::with_capacity(READ_BUFFER_SIZE);
    loop {
        // Unlike a bare read, read_to_end fills the whole chunk, resuming when a signal interrupts it
        buffer1.clear();
        (&file1)
            .take(READ_BUFFER_SIZE as u64)
            .read_to_end(&mut buffer1)?;
        if buffer1.is_empty() {
            break;
        }
        buffer2.clear();
        (&file2)
            .take(READ_BUFFER_SIZE as u64)
            .read_to_end(&mut buffer2)?;
        if buffer1 != buffer2 {
            return Ok(false);
        }
    }
    Ok(true)
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

/// Whether a file holds compressed data, whose offset within its extent the mapping leaves out
fn is_encoded(extents: &[fiemap::FiemapExtent]) -> bool {
    extents
        .iter()
        .any(|extent| extent.fe_flags.contains(fiemap::FiemapExtentFlags::ENCODED))
}

/// Identify the data a file maps to, `None` when its extents can not establish sharing
///
/// Files sharing a key hold the same bytes, save for compressed extents, which several contents can share. The
/// logical offset is part of it, as the same extent mapped elsewhere in the file puts its bytes elsewhere too.
fn extent_key(extents: &[fiemap::FiemapExtent]) -> Option<Vec<ExtentLocation>> {
    extents
        .iter()
        .map(|extent| {
            (!extent.fe_flags.intersects(UNSHAREABLE_EXTENT_FLAGS)).then_some((
                extent.fe_logical,
                extent.fe_physical,
                extent.fe_length,
            ))
        })
        .collect()
}

/// One copy of a file content on disk, and the files already sharing it
#[cfg_attr(test, derive(Debug, Eq, PartialEq))]
struct DataCopy<'p> {
    /// First file holding the copy, standing for all of them
    path: &'p Path,
    /// Whether the data sits in filesystem metadata, which reflinking can not share
    inlined: bool,
    /// Whether the data is compressed, which the extents alone can not tell apart from another content
    encoded: bool,
    /// Further files mapping to the extents of `path`, holding its data unless `encoded` leaves that to confirm
    shared: Vec<&'p Path>,
}

/// Group files sharing a size and hash by the copy of the data they hold
///
/// Files mapping to the same extents hold the same bytes, so grouping them ahead of any content comparison spares
/// reading data that is already shared. Compression is the exception, which `confirm_shared` settles for the files
/// that end up reported.
fn data_copies(filepaths: &[PathBuf]) -> Result<Vec<DataCopy<'_>>, io::Error> {
    let mut copies: Vec<DataCopy<'_>> = Vec::new();
    // Copies indexed by the data they hold, sparing a scan of them all for every file
    let mut by_key: HashMap<Vec<ExtentLocation>, usize> = HashMap::new();

    for filepath in filepaths {
        let path = filepath.as_path();
        let extents = file_extents(filepath)?;
        let copy = DataCopy {
            path,
            inlined: is_inlined(&extents),
            encoded: is_encoded(&extents),
            shared: Vec::new(),
        };
        let Some(key) = extent_key(&extents) else {
            // Extents identifying nothing keep the file to itself, even against another file mapping the same way
            copies.push(copy);
            continue;
        };
        drop(extents);

        // The index is inserted below from `copies.len()`, so it always resolves
        #[expect(clippy::indexing_slicing)]
        match by_key.entry(key) {
            Entry::Occupied(known) => {
                let known = &mut copies[*known.get()];
                // A key carries no compression flag, so the copy takes that of every file joining it
                known.encoded |= copy.encoded;
                known.shared.push(path);
            }
            Entry::Vacant(slot) => {
                slot.insert(copies.len());
                copies.push(copy);
            }
        }
    }
    Ok(copies)
}

/// Files sharing the extents of `other` that hold the content of `first`
///
/// A compressed extent is reported without the offset its data sits at, so files grouped on one can differ. The
/// first file of a copy needs no confirming, the content classes having compared it.
fn confirm_shared<'p>(first_path: &Path, other: &DataCopy<'p>) -> Result<Vec<&'p Path>, io::Error> {
    if !other.encoded {
        return Ok(other.shared.clone());
    }
    let mut confirmed = Vec::new();
    for &path in &other.shared {
        if same_content(first_path, path)? {
            confirmed.push(path);
        } else {
            log::warn!(
                "Files {first_path:?} and {path:?} share a compressed extent but hold different data"
            );
        }
    }
    Ok(confirmed)
}

/// Partition the copies of files sharing a size and hash into classes of identical content
///
/// Each class is its first copy, standing for the whole class, followed by the others. More than one class means
/// the hash collided.
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

/// Report the duplicate pairs of every group of files sharing a size and hash
///
/// A pair names the file to reflink onto, then the one to replace, so the output feeds `cp --reflink` directly.
fn report_duplicates<W>(
    files: &HashMap<(u64, u64), Vec<PathBuf>>,
    counters: &ProgressCounters,
    writer: &mut W,
) -> Result<(), io::Error>
where
    W: Write,
{
    for filepaths in files.values() {
        let copies = data_copies(filepaths)?;
        let classes = content_classes(&copies)?;
        if classes.len() > 1 {
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
            for shared in &first.shared {
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
                for path in iter::once(other_path).chain(confirm_shared(first_path, other)?) {
                    log::debug!("Files {first_path:?} and {path:?} are duplicates");
                    counters.duplicate_candidate.fetch_add(1, Ordering::Relaxed);
                    write_pair(writer, first_path, path)?;
                }
            }
        }
    }
    Ok(())
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

/// Tracks the sizes seen so far, withholding the first file of each until a second file of that size shows up
///
/// Identical files have identical sizes, so a file whose size no other file shares can not have a duplicate, and
/// hashing it would read it in full for nothing.
#[derive(Default)]
struct SizeTracker(HashMap<u64, Option<PathBuf>>);

impl SizeTracker {
    /// Take in a file, returning the files whose size is now known to be shared, and which are therefore worth
    /// hashing: none while the size is unique so far, both the withheld file and this one when it completes a first
    /// pair, this one alone afterwards
    fn track(&mut self, path: PathBuf, file_size: u64) -> [Option<PathBuf>; 2] {
        match self.0.entry(file_size) {
            Entry::Vacant(e) => {
                e.insert(Some(path));
                [None, None]
            }
            Entry::Occupied(mut e) => [e.get_mut().take(), Some(path)],
        }
    }
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
    let mut files: HashMap<(u64, u64), Vec<PathBuf>> = HashMap::new();

    // Progress
    let progress_counters = Arc::new(ProgressCounters::default());
    let progress = indicatif::ProgressBar::new_spinner().with_style(
        indicatif::ProgressStyle::with_template("{spinner} {counters}")
            .context("Invalid progress template")?
            .with_key("counters", {
                let progress_counters = Arc::clone(&progress_counters);
                // Rendering the counters when a frame is drawn, rather than when they change, keeps formatting and
                // its allocation off the per file path
                move |_: &indicatif::ProgressState, writer: &mut dyn fmt::Write| {
                    // The writer collects into a string, and the style has no way to report a failure anyway
                    let _ = write!(writer, "{progress_counters}");
                }
            }),
    );
    progress.enable_steady_tick(Duration::from_millis(300));

    thread::scope(|scope| -> anyhow::Result<()> {
        // Worker threads
        let worker_count = max(cpu_count - 1, 1);
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            // Per thread clones
            let to_hashed_rx = to_hashed_rx.clone();
            let hashed_tx = hashed_tx.clone();
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
                    progress_counters.hash.fetch_add(1, Ordering::Relaxed);

                    hashed_tx.send((path, file_size, hash))?;
                }

                Ok(())
            }));
        }
        drop(to_hashed_rx);
        drop(hashed_tx);

        // Iterate over files
        let mut size_tracker = SizeTracker::default();
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
                progress_counters.file.fetch_add(1, Ordering::Relaxed);

                let tracked = size_tracker.track(path.to_path_buf(), file_size);
                for to_hash in tracked.into_iter().flatten() {
                    to_hashed_tx.send((to_hash, file_size))?;
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
                progress_counters.file.fetch_add(1, Ordering::Relaxed);

                let tracked = size_tracker.track(path.to_path_buf(), file_size);
                for to_hash in tracked.into_iter().flatten() {
                    to_hashed_tx.send((to_hash, file_size))?;
                }
            }
        }
        drop(to_hashed_tx);

        // Fill hashmap
        for (filepath, file_size, hash) in &hashed_rx {
            files.entry((file_size, hash)).or_default().push(filepath);
        }

        // Workers have all completed once their channel ends were dropped above, so this does not block
        workers.into_iter().try_for_each(|worker| {
            worker
                .join()
                .map_err(|e| anyhow::anyhow!("Worker thread panicked: {e:?}"))?
        })
    })?;

    // Remove unique hashes
    files.retain(|_key, filepaths| filepaths.len() > 1);

    // Find candidates
    let mut stdout = io::stdout().lock();
    report_duplicates(&files, &progress_counters, &mut stdout)?;

    progress.finish();

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        io::Seek as _,
        process::{Command, Stdio},
    };

    use linux_raw_sys::btrfs::{file_clone_range, FS_COMPR_FL};

    use super::*;

    nix::ioctl_write_ptr!(
        /// Clone a range of one file into another, as `cp --reflink` does for whole files
        btrfs_clone_range,
        0x94,
        13,
        file_clone_range
    );

    nix::ioctl_read!(
        /// Read the inode flags of a file, as `lsattr` does
        get_inode_flags,
        b'f',
        1,
        nix::libc::c_long
    );

    nix::ioctl_write_ptr!(
        /// Set the inode flags of a file, as `chattr` does
        set_inode_flags,
        b'f',
        2,
        nix::libc::c_long
    );

    /// Size of a file small enough for Btrfs to store its data inline in metadata
    const INLINE_SIZE: usize = 500;

    /// Size of a file too large for Btrfs to store its data inline in metadata
    const EXTENT_SIZE: usize = 300 * 1024;

    /// Length of a cloned range, the largest Btrfs sector size a clone has to align to
    const CLONE_RANGE_SIZE: u64 = 64 * 1024;

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

    /// Write a file of `size` bytes Btrfs stores compressed, whatever the mount options
    ///
    /// Every block holds distinct bytes, so that two ranges of the extent read differently.
    fn write_compressed(path: &Path, size: usize) {
        let file = File::create(path).unwrap();
        let mut flags: nix::libc::c_long = 0;
        // SAFETY: the ioctl writes one c_long to flags
        unsafe { get_inode_flags(file.as_raw_fd(), &raw mut flags) }.unwrap();
        flags |= nix::libc::c_long::from(FS_COMPR_FL);
        // SAFETY: the ioctl reads one c_long from flags
        unsafe { set_inode_flags(file.as_raw_fd(), &raw const flags) }.unwrap();

        let data: Vec<u8> = (0..size)
            .map(|offset| u8::try_from((offset / 4096) % 256).unwrap())
            .collect();
        fs::write(path, data).unwrap();
        File::open(path).unwrap().sync_data().unwrap();
    }

    /// Clone `length` bytes of `source` at `offset` into a new file at `path`
    fn clone_range(source: &Path, offset: u64, length: u64, path: &Path) {
        let source_file = File::open(source).unwrap();
        let file = File::create(path).unwrap();
        let args = file_clone_range {
            src_fd: source_file.as_raw_fd().into(),
            src_offset: offset,
            src_length: length,
            dest_offset: 0,
        };
        // SAFETY: the ioctl reads `size_of::<file_clone_range>()` bytes from `args`
        unsafe { btrfs_clone_range(file.as_raw_fd(), &raw const args) }.unwrap();
        file.sync_data().unwrap();
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

    /// Build the pair of paths a two file test writes to
    fn pair_paths(dir: &Path) -> (PathBuf, PathBuf) {
        (dir.join("first"), dir.join("second"))
    }

    /// Map both files and test whether they already share their data, as the candidate reporting does
    fn files_share_extents(first: &Path, second: &Path) -> bool {
        let key = extent_key(&file_extents(first).unwrap());
        key.is_some() && key == extent_key(&file_extents(second).unwrap())
    }

    /// Build an unencoded, extent backed copy holding the given files
    fn plain_copy<'p>(path: &'p Path, shared: Vec<&'p Path>) -> DataCopy<'p> {
        DataCopy {
            path,
            inlined: false,
            encoded: false,
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

    /// Clone two differing ranges of one compressed extent, which Btrfs maps identically, into a pair of files
    fn clone_compressed_ranges(dir: &Path) -> (PathBuf, PathBuf) {
        let (first, second) = pair_paths(dir);
        let base = dir.join("base");
        write_compressed(&base, EXTENT_SIZE);
        // Btrfs compresses in 128 KiB units, so both ranges fall inside one extent
        clone_range(&base, 0, CLONE_RANGE_SIZE, &first);
        clone_range(&base, CLONE_RANGE_SIZE, CLONE_RANGE_SIZE, &second);

        let first_extents = file_extents(&first).unwrap();
        assert!(is_encoded(&first_extents));
        assert_eq!(
            extent_key(&first_extents),
            extent_key(&file_extents(&second).unwrap())
        );
        assert_ne!(fs::read(&first).unwrap(), fs::read(&second).unwrap());
        (first, second)
    }

    #[test]
    fn data_copies_ranges_of_one_compressed_extent_form_one_copy() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = clone_compressed_ranges(dir.path());
        let paths = vec![first.clone(), second.clone()];

        // A compressed extent is reported without the offset its data sits at, so the extents group the files
        // although they differ, leaving them for confirm_shared to tell apart
        assert_eq!(
            data_copies(&paths).unwrap(),
            vec![DataCopy {
                encoded: true,
                ..plain_copy(&first, vec![second.as_path()])
            }]
        );
    }

    #[test]
    fn confirm_shared_does_not_read_unencoded_members() {
        let dir = tempfile::TempDir::new().unwrap();
        let (first, second) = pair_paths(dir.path());
        fs::write(&first, b"dup").unwrap();
        fs::write(&second, b"dup").unwrap();
        // Extents settle sharing on their own unless compressed, so a path that can not be opened stands in for a
        // file taken on its extents alone
        let missing = dir.path().join("missing");
        let other = DataCopy {
            path: &second,
            inlined: false,
            encoded: false,
            shared: vec![missing.as_path()],
        };

        assert_eq!(
            confirm_shared(&first, &other).unwrap(),
            vec![missing.as_path()]
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

    /// Report the duplicates of one group of files sharing a size and hash, decoding the pairs written
    fn reported_pairs(group: Vec<PathBuf>, counters: &ProgressCounters) -> Vec<(PathBuf, PathBuf)> {
        let mut out = Vec::new();
        // The size and hash are what grouped the files, which the reporting never reads back
        report_duplicates(&HashMap::from([((0, 0), group)]), counters, &mut out).unwrap();

        out.split(|&byte| byte == 0)
            .filter(|path| !path.is_empty())
            .map(|path| PathBuf::from(OsStr::from_bytes(path)))
            .collect::<Vec<_>>()
            .chunks(2)
            .map(|pair| (pair[0].clone(), pair[1].clone()))
            .collect()
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
    fn report_duplicates_confirms_each_member_of_a_compressed_copy() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (same, different) = clone_compressed_ranges(dir.path());
        let same_shared = dir.path().join("same-shared");
        fs::copy(&same, &same_shared).unwrap();
        assert!(files_share_extents(&same, &same_shared));
        // An independent file leads the class, so the compressed copy is the one whose members get confirmed
        let independent = dir.path().join("independent");
        fs::write(&independent, fs::read(&same).unwrap()).unwrap();
        File::open(&independent).unwrap().sync_data().unwrap();

        // The differing range comes first among the shared files, so confirming has to look past it
        assert_eq!(
            reported_pairs(
                vec![
                    independent.clone(),
                    same.clone(),
                    different,
                    same_shared.clone()
                ],
                &ProgressCounters::default()
            ),
            vec![(independent.clone(), same), (independent, same_shared)]
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
    fn progress_counters_display_reports_every_counter() {
        let counters = ProgressCounters::default();
        counters.file.fetch_add(6, Ordering::Relaxed);
        counters.hash.fetch_add(5, Ordering::Relaxed);
        counters.hash_collision.fetch_add(4, Ordering::Relaxed);
        counters.reflinked.fetch_add(3, Ordering::Relaxed);
        counters.inlined.fetch_add(2, Ordering::Relaxed);
        counters.duplicate_candidate.fetch_add(1, Ordering::Relaxed);

        assert_eq!(
            counters.to_string(),
            "6 files, 5 hashes, 4 hash collisions, 3 already reflinked, 2 inlined, 1 duplicates"
        );
    }

    #[test]
    fn size_tracker_withholds_a_size_seen_once() {
        let mut size_tracker = SizeTracker::default();

        assert_eq!(size_tracker.track(PathBuf::from("a"), 1), [None, None]);
        assert_eq!(size_tracker.track(PathBuf::from("b"), 2), [None, None]);
    }

    #[test]
    fn size_tracker_releases_both_files_completing_a_pair() {
        let mut size_tracker = SizeTracker::default();
        size_tracker.track(PathBuf::from("a"), 1);

        assert_eq!(
            size_tracker.track(PathBuf::from("b"), 1),
            [Some(PathBuf::from("a")), Some(PathBuf::from("b"))]
        );
    }

    #[test]
    fn size_tracker_releases_only_the_new_file_of_an_already_shared_size() {
        let mut size_tracker = SizeTracker::default();
        size_tracker.track(PathBuf::from("a"), 1);
        size_tracker.track(PathBuf::from("b"), 1);

        assert_eq!(
            size_tracker.track(PathBuf::from("c"), 1),
            [None, Some(PathBuf::from("c"))]
        );
    }

    #[test]
    fn size_tracker_keeps_sizes_independent() {
        let mut size_tracker = SizeTracker::default();
        size_tracker.track(PathBuf::from("a"), 1);
        size_tracker.track(PathBuf::from("b"), 2);

        assert_eq!(
            size_tracker.track(PathBuf::from("c"), 2),
            [Some(PathBuf::from("b")), Some(PathBuf::from("c"))]
        );
    }

    #[test]
    fn files_share_extents_not_partially_rewritten_reflink() {
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

        assert!(!files_share_extents(&first, &second));
    }

    #[test]
    fn files_share_extents_not_sparse_and_dense() {
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

        assert!(!files_share_extents(&sparse, &dense));
    }

    #[test]
    fn extent_key_includes_logical_offset() {
        let Some(dir) = btrfs_test_dir() else { return };
        let path = dir.path().join("file");
        write_flushed(&path, EXTENT_SIZE);

        // Only a synthesized map can hold the same extent at another offset, which a hole opening ahead of the
        // data would produce while changing the bytes the file reads as
        let extents = file_extents(&path).unwrap();
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

        let inlined_extents = file_extents(&inlined).unwrap();
        let truncated_extents = file_extents(&truncated).unwrap();
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

        let whole_extents = file_extents(&whole).unwrap();
        let part_extents = file_extents(&part).unwrap();
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

        let extents = file_extents(&path).unwrap();
        assert!(extents.len() > 1);

        // Every extent of the shorter map is shared, yet the files are not
        assert_ne!(extent_key(&extents[..1]), extent_key(&extents));
    }

    #[test]
    fn extent_key_none_for_unresolved_locations() {
        let Some(dir) = btrfs_test_dir() else { return };
        let (first, second) = pair_paths(dir.path());
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

        // Both map the same way, so a key would group them
        assert!(extent_key(&extents1).is_none());
        assert!(extent_key(&extents2).is_none());
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
