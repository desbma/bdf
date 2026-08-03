//! Btrfs Duplicate Finder

mod btrfs;
mod content;
mod dedup;
mod extent;
mod size_tracker;

use std::{
    cmp::max,
    collections::HashMap,
    ffi::OsStr,
    fmt,
    fs::{self, File},
    io::{self, BufRead as _},
    mem,
    os::unix::{ffi::OsStrExt as _, fs::MetadataExt as _},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use anyhow::Context as _;
use clap::Parser;
use xxhash_rust::xxh3;

use crate::{
    btrfs::{walk_btrfs_dir, BtrfsFilesystem},
    content::{hash_file, READ_BUFFER_SIZE},
    dedup::{merge_shared_members, report_duplicates, try_attach_shared, SharedIndex},
    size_tracker::{wanted_file_size, SizeTracker},
};

/// Convenience type for a pair of crossbeam channel ends
type CrossbeamChannel<T> = (crossbeam_channel::Sender<T>, crossbeam_channel::Receiver<T>);

/// Maximum number of files grouped into one channel message
const BATCH_LEN: usize = 16;

/// Combined file size at or above which a batch is sent without waiting for `BATCH_LEN`
const BATCH_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Groups items into batches before sending them, amortizing the per item channel cost
///
/// A batch is sent once it holds `BATCH_LEN` items or their combined weight reaches `BATCH_MAX_BYTES`, so a run of
/// large files is not piled into one lopsided message.
struct BatchSender<T> {
    /// Channel the batches are sent on
    sender: crossbeam_channel::Sender<Vec<T>>,
    /// Items awaiting a full batch
    batch: Vec<T>,
    /// Combined weight of the items in `batch`
    batch_bytes: u64,
}

impl<T> BatchSender<T> {
    /// Wrap a channel sender
    fn new(sender: crossbeam_channel::Sender<Vec<T>>) -> Self {
        Self {
            sender,
            batch: Vec::with_capacity(BATCH_LEN),
            batch_bytes: 0,
        }
    }

    /// Queue an item weighing `weight` bytes, sending the batch once it fills by count or weight
    fn send(&mut self, item: T, weight: u64) -> Result<(), crossbeam_channel::SendError<Vec<T>>> {
        self.batch.push(item);
        self.batch_bytes += weight;
        if self.batch.len() == BATCH_LEN || self.batch_bytes >= BATCH_MAX_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    /// Send the pending items, if any
    fn flush(&mut self) -> Result<(), crossbeam_channel::SendError<Vec<T>>> {
        if !self.batch.is_empty() {
            self.sender
                .send(mem::replace(&mut self.batch, Vec::with_capacity(BATCH_LEN)))?;
            self.batch_bytes = 0;
        }
        Ok(())
    }
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

    /// Attempt to reflink each duplicate pair using FICLONE, logging the result.
    #[structopt(long)]
    pub dedup: bool,
}

/// Processing progress counters
#[derive(Default)]
pub(crate) struct ProgressCounters {
    /// Number of files that were targeted for analysis
    pub file: AtomicUsize,
    /// Number of files that were hashed
    pub hash: AtomicUsize,
    /// Number of extra contents found among files sharing a size and hash
    pub hash_collision: AtomicUsize,
    /// Number of identical files already reflinked
    pub reflinked: AtomicUsize,
    /// Number of identical files with inlined data, which reflinking can not share
    pub inlined: AtomicUsize,
    /// Number of duplicate files, candidates for reflinking
    pub duplicate_candidate: AtomicUsize,
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

/// Build the spinner that renders the progress counters on each drawn frame
fn build_progress_bar(counters: &Arc<ProgressCounters>) -> anyhow::Result<indicatif::ProgressBar> {
    let progress = indicatif::ProgressBar::new_spinner().with_style(
        indicatif::ProgressStyle::with_template("{spinner} {counters}")
            .context("Invalid progress template")?
            .with_key("counters", {
                let counters = Arc::clone(counters);
                // Rendering the counters when a frame is drawn, rather than when they change, keeps formatting and
                // its allocation off the per file path
                move |_: &indicatif::ProgressState, writer: &mut dyn fmt::Write| {
                    // The writer collects into a string, and the style has no way to report a failure anyway
                    let _ = write!(writer, "{counters}");
                }
            }),
    );
    progress.enable_steady_tick(Duration::from_millis(300));
    Ok(progress)
}

/// Hash each file arriving on `to_hashed_rx`, forwarding it with its size and hash, skipping any already found to
/// share the extents of a tracked file
fn hash_worker(
    to_hashed_rx: &crossbeam_channel::Receiver<Vec<(PathBuf, u64)>>,
    hashed_tx: &crossbeam_channel::Sender<Vec<(PathBuf, u64, u64)>>,
    shared_index: &Mutex<SharedIndex>,
    counters: &ProgressCounters,
) -> anyhow::Result<()> {
    let mut hasher = xxh3::Xxh3::new();
    let mut buffer = Vec::with_capacity(READ_BUFFER_SIZE);
    while let Ok(batch) = to_hashed_rx.recv() {
        let mut hashed_batch = Vec::with_capacity(batch.len());
        for (path, file_size) in batch {
            let Ok(file) = File::open(&path).inspect_err(|e| {
                log::warn!("Error while hashing {path:?}: {e}");
            }) else {
                continue;
            };

            // Files mapping to the extents of an already tracked file hold its bytes, so probing the
            // extents first spares reading data that is already shared
            if try_attach_shared(shared_index, &file, &path, file_size)? {
                continue;
            }

            let Ok(hash) = hash_file(&file, &mut hasher, &mut buffer).inspect_err(|e| {
                log::warn!("Error while hashing {path:?}: {e}");
            }) else {
                continue;
            };

            log::debug!("{path:?} {hash:016x}");
            counters.hash.fetch_add(1, Ordering::Relaxed);

            hashed_batch.push((path, file_size, hash));
        }
        if !hashed_batch.is_empty() {
            hashed_tx.send(hashed_batch)?;
        }
    }
    Ok(())
}

/// Track each walked file, sending those that need hashing
fn feed_walk(
    walk: impl Iterator<Item = (PathBuf, u64)>,
    size_tracker: &mut SizeTracker,
    to_hashed_tx: &mut BatchSender<(PathBuf, u64)>,
    counters: &ProgressCounters,
) -> anyhow::Result<()> {
    for (path, file_size) in walk {
        log::debug!("{path:?}");
        counters.file.fetch_add(1, Ordering::Relaxed);

        let tracked = size_tracker.track(path, file_size);
        for to_hash in tracked.into_iter().flatten() {
            to_hashed_tx.send((to_hash, file_size), file_size)?;
        }
    }
    Ok(())
}

/// Read NUL terminated paths from stdin, tracking each valid file and sending those that need hashing
fn feed_stdin(
    min_size: Option<u64>,
    size_tracker: &mut SizeTracker,
    to_hashed_tx: &mut BatchSender<(PathBuf, u64)>,
    counters: &ProgressCounters,
) -> anyhow::Result<()> {
    // Reflinks can not cross filesystems, so every input has to be on the one the first input is on
    let mut filesystem: Option<BtrfsFilesystem> = None;
    for bytes in io::stdin().lock().split(0) {
        let bytes = bytes?;
        let path = Path::new(OsStr::from_bytes(&bytes));
        let Ok(metadata) = fs::symlink_metadata(path).inspect_err(|e| {
            log::warn!("Error while reading metadata of {path:?}: {e}");
        }) else {
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
        let Some(file_size) = wanted_file_size(metadata.len(), min_size) else {
            continue;
        };
        log::debug!("{path:?}");
        counters.file.fetch_add(1, Ordering::Relaxed);

        let tracked = size_tracker.track(path.to_path_buf(), file_size);
        for to_hash in tracked.into_iter().flatten() {
            to_hashed_tx.send((to_hash, file_size), file_size)?;
        }
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    // Init logger
    simple_logger::SimpleLogger::new()
        .init()
        .context("Failed to init logger")?;

    // Parse command line opts
    let cl_opts = CommandLineOpts::parse();
    log::trace!("{cl_opts:?}");
    let dir_walk = cl_opts
        .dir
        .as_deref()
        .map(|dir| walk_btrfs_dir(dir, cl_opts.min_size))
        .transpose()?;

    // Get usable core count
    let cpu_count = thread::available_parallelism()?.get();

    // Channels
    let (to_hashed_tx, to_hashed_rx): CrossbeamChannel<Vec<(PathBuf, u64)>> =
        crossbeam_channel::unbounded();
    let (hashed_tx, hashed_rx): CrossbeamChannel<Vec<(PathBuf, u64, u64)>> =
        crossbeam_channel::unbounded();

    // File hash map
    let mut files: HashMap<(u64, u64), Vec<PathBuf>> = HashMap::new();

    // Files found to share the extents of an already tracked file, which hashing can skip
    let shared_index: Mutex<SharedIndex> = Mutex::new(HashMap::new());

    // Progress
    let progress_counters = Arc::new(ProgressCounters::default());
    let progress = build_progress_bar(&progress_counters)?;

    let pair_groups = thread::scope(|scope| -> anyhow::Result<Vec<Vec<PathBuf>>> {
        // Worker threads
        let worker_count = max(cpu_count - 1, 1);
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            // Per thread clones
            let to_hashed_rx = to_hashed_rx.clone();
            let hashed_tx = hashed_tx.clone();
            let progress_counters = Arc::clone(&progress_counters);
            let shared_index = &shared_index;

            workers.push(scope.spawn(move || {
                hash_worker(&to_hashed_rx, &hashed_tx, shared_index, &progress_counters)
            }));
        }
        drop(to_hashed_rx);
        drop(hashed_tx);

        // Iterate over files
        let mut size_tracker = SizeTracker::default();
        let mut to_hashed_tx = BatchSender::new(to_hashed_tx);
        if let Some(walk) = dir_walk {
            feed_walk(
                walk,
                &mut size_tracker,
                &mut to_hashed_tx,
                &progress_counters,
            )?;
        } else {
            feed_stdin(
                cl_opts.min_size,
                &mut size_tracker,
                &mut to_hashed_tx,
                &progress_counters,
            )?;
        }
        to_hashed_tx.flush()?;
        drop(to_hashed_tx);

        // Sizes left holding exactly two files go to a direct comparison, which their hashing would only repeat
        let pair_groups: Vec<Vec<PathBuf>> = size_tracker.into_pairs().collect();

        // Fill hashmap
        for (filepath, file_size, hash) in hashed_rx.into_iter().flatten() {
            files.entry((file_size, hash)).or_default().push(filepath);
        }

        // Workers have all completed once their channel ends were dropped above, so this does not block
        workers.into_iter().try_for_each(|worker| {
            worker
                .join()
                .map_err(|e| anyhow::anyhow!("Worker thread panicked: {e:?}"))?
        })?;
        Ok(pair_groups)
    })?;

    let shared_index = shared_index
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Poisoned lock: {e}"))?;
    merge_shared_members(&mut files, shared_index);

    // Find candidates
    let mut stdout = io::stdout();
    report_duplicates(
        &files,
        &pair_groups,
        cl_opts.dedup,
        &progress_counters,
        &mut stdout,
    )?;

    progress.finish();

    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use std::os::fd::AsRawFd as _;

    use linux_raw_sys::btrfs::{file_clone_range, FS_COMPR_FL};
    // The test build of the binary links every dev-dependency, and this one only serves the benches
    use walkdir as _;

    use super::*;
    use crate::{
        btrfs::btrfs_fsid,
        extent::{extent_key, file_extents, FiemapExtent},
    };

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
    pub(crate) const INLINE_SIZE: usize = 500;

    /// Size of a file too large for Btrfs to store its data inline in metadata
    pub(crate) const EXTENT_SIZE: usize = 300 * 1024;

    /// Length of a cloned range, the largest Btrfs sector size a clone has to align to
    pub(crate) const CLONE_RANGE_SIZE: u64 = 64 * 1024;

    /// Temporary directory under the build directory, `None` when it is not on a Btrfs filesystem
    ///
    /// Extent layout and subvolumes are filesystem specific, so tests relying on them can only run on Btrfs.
    pub(crate) fn btrfs_test_dir() -> Option<tempfile::TempDir> {
        let base = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/target"));
        if btrfs_fsid(base).unwrap().is_some() {
            Some(tempfile::TempDir::new_in(base).unwrap())
        } else {
            eprintln!("Skipping test, {base:?} is not on a Btrfs filesystem");
            None
        }
    }

    /// Build incompressible bytes, so that transparent compression does not alter the extent layout
    pub(crate) fn incompressible_bytes(size: usize) -> Vec<u8> {
        (0..size.div_ceil(8))
            .flat_map(|index| xxh3::xxh3_64(&index.to_le_bytes()).to_le_bytes())
            .take(size)
            .collect()
    }

    /// Write a file of `size` bytes Btrfs stores compressed, whatever the mount options
    ///
    /// Every block holds distinct bytes, so that two ranges of the extent read differently.
    pub(crate) fn write_compressed(path: &Path, size: usize) {
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

    /// Clone `length` bytes of `source` at `offset` into `path` at `dest_offset`, creating the file if needed
    pub(crate) fn clone_range(
        source: &Path,
        offset: u64,
        length: u64,
        path: &Path,
        dest_offset: u64,
    ) {
        let source_file = File::open(source).unwrap();
        let file = File::options()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .unwrap();
        let args = file_clone_range {
            src_fd: source_file.as_raw_fd().into(),
            src_offset: offset,
            src_length: length,
            dest_offset,
        };
        // SAFETY: the ioctl reads `size_of::<file_clone_range>()` bytes from `args`
        unsafe { btrfs_clone_range(file.as_raw_fd(), &raw const args) }.unwrap();
        file.sync_data().unwrap();
    }

    /// Write a file of `size` bytes, leaving its data under delayed allocation
    pub(crate) fn write_unflushed(path: &Path, size: usize) {
        fs::write(path, incompressible_bytes(size)).unwrap();
    }

    /// Write a file of `size` bytes and flush it, so its extents are allocated
    pub(crate) fn write_flushed(path: &Path, size: usize) {
        write_unflushed(path, size);
        File::open(path).unwrap().sync_data().unwrap();
    }

    /// Build the pair of paths a two file test writes to
    pub(crate) fn pair_paths(dir: &Path) -> (PathBuf, PathBuf) {
        (dir.join("first"), dir.join("second"))
    }

    /// Map a file by path, flushing it first, as the candidate reporting does
    pub(crate) fn path_extents(path: &Path) -> Vec<FiemapExtent> {
        file_extents(&File::open(path).unwrap(), true).unwrap()
    }

    /// Map both files and test whether their extents yield the same key, as the candidate reporting does
    pub(crate) fn files_have_same_extent_key(first: &Path, second: &Path) -> bool {
        let key = extent_key(&path_extents(first));
        key.is_some() && key == extent_key(&path_extents(second))
    }

    #[test]
    fn batch_sender_sends_full_batch_and_flushes_remainder() {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let mut sender = BatchSender::new(sender);

        // A batch short of BATCH_LEN stays buffered
        for item in 0..BATCH_LEN - 1 {
            sender.send(item, 0).unwrap();
        }
        assert!(receiver.is_empty());

        // The item reaching BATCH_LEN releases the whole batch
        sender.send(BATCH_LEN - 1, 0).unwrap();
        assert_eq!(
            receiver.try_recv().unwrap(),
            (0..BATCH_LEN).collect::<Vec<_>>()
        );

        // A trailing item waits for flush rather than a full count
        sender.send(BATCH_LEN, 0).unwrap();
        assert!(receiver.is_empty());
        sender.flush().unwrap();
        assert_eq!(receiver.try_recv().unwrap(), vec![BATCH_LEN]);
    }

    #[test]
    fn batch_sender_sends_at_combined_weight_limit() {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let mut sender = BatchSender::new(sender);

        // The batch is held while its combined weight stays below BATCH_MAX_BYTES
        sender.send("first", BATCH_MAX_BYTES - 1).unwrap();
        assert!(receiver.is_empty());

        // Reaching BATCH_MAX_BYTES sends both accumulated items at once
        sender.send("second", 1).unwrap();
        assert_eq!(receiver.try_recv().unwrap(), vec!["first", "second"]);

        // The weight resets with the batch, so a light item stays buffered again
        sender.send("third", 1).unwrap();
        assert!(receiver.is_empty());
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
}
