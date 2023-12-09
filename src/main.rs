//! Btrfs Duplicate Finder

use std::{
    cmp::max,
    collections::hash_map::{Entry, HashMap},
    ffi::OsStr,
    fmt,
    fs::File,
    io::{self, BufRead, BufReader, Read},
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use anyhow::Context;
use clap::Parser;
use multimap::MultiMap;
use xxhash_rust::xxh3;

/// File read chunk size, in bytes
const READ_BUFFER_SIZE: usize = 256 * 1024;

/// Convenience type for a pair of crossbeam channel ends
type CrossbeamChannel<T> = (crossbeam_channel::Sender<T>, crossbeam_channel::Receiver<T>);

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

/// Compute XXH3-64 non cryptographic hash
fn compute_xxh(
    hasher: &mut xxh3::Xxh3,
    reader: &mut BufReader<File>,
    buffer: &mut [u8],
) -> Result<u64, io::Error> {
    hasher.reset();
    loop {
        let rd_count = reader.read(buffer)?;
        hasher.update(&buffer[..rd_count]);
        if rd_count == 0 {
            break;
        }
    }
    Ok(hasher.digest())
}

/// Processing progress counters
struct ProgressCounters {
    /// Number of files that were targeted for analysis
    file_count: AtomicUsize,
    /// Number of files that were hashed
    hash_count: AtomicUsize,
    /// Number of hash collisions
    hash_collision_count: AtomicUsize,
    /// Number of identical files already reflinked
    reflinked_count: AtomicUsize,
    /// Number of duplicate files, candidates for reflinking
    duplicate_candidate_count: AtomicUsize,
}

impl ProgressCounters {
    /// Constructor
    fn new() -> Self {
        Self {
            file_count: AtomicUsize::new(0),
            hash_count: AtomicUsize::new(0),
            hash_collision_count: AtomicUsize::new(0),
            reflinked_count: AtomicUsize::new(0),
            duplicate_candidate_count: AtomicUsize::new(0),
        }
    }
}

impl fmt::Display for ProgressCounters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} files, {} hashes, {} hash collisions, {} already reflinked, {} duplicates",
            self.file_count.load(Ordering::Relaxed),
            self.hash_count.load(Ordering::Relaxed),
            self.hash_collision_count.load(Ordering::Relaxed),
            self.reflinked_count.load(Ordering::Relaxed),
            self.duplicate_candidate_count.load(Ordering::Relaxed),
        )
    }
}

/// Test if two files of the same size have the same content
fn same_content(first: &Path, second: &Path) -> Result<bool, io::Error> {
    let file1 = File::open(first)?;
    let file2 = File::open(second)?;
    let mut reader1 = BufReader::new(file1);
    let mut reader2 = BufReader::new(file2);
    let mut buffer1 = [0; READ_BUFFER_SIZE];
    let mut buffer2 = [0; READ_BUFFER_SIZE];
    loop {
        let rd_count = reader1.read(&mut buffer1)?;
        if rd_count == 0 {
            break;
        }
        reader2.read_exact(&mut buffer2[0..rd_count])?;
        if buffer1[0..rd_count] != buffer2[0..rd_count] {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Test if two identical files share the same extents
fn same_extents(first: &Path, second: &Path) -> Result<bool, io::Error> {
    let extents1: Vec<fiemap::FiemapExtent> =
        fiemap::fiemap(first)?.collect::<Result<Vec<_>, _>>()?;
    let extents2: Vec<fiemap::FiemapExtent> =
        fiemap::fiemap(second)?.collect::<Result<Vec<_>, _>>()?;
    if extents1.len() != extents2.len() {
        return Ok(false);
    }
    for (extent1, extent2) in extents1.iter().zip(extents2.iter()) {
        if (extent1.fe_physical != extent2.fe_physical) || (extent1.fe_length != extent2.fe_length)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Return true if path is on a Btrfs filesystem
fn is_on_btrfs(path: &Path) -> nix::Result<bool> {
    let statfs = nix::sys::statfs::statfs(path)?;
    Ok(statfs.filesystem_type() == nix::sys::statfs::BTRFS_SUPER_MAGIC)
}

fn main() -> anyhow::Result<()> {
    // Init logger
    simple_logger::SimpleLogger::new()
        .init()
        .context("Failed to init logger")?;

    // Parse command line opts
    let cl_opts = CommandLineOpts::parse();
    log::trace!("{:?}", cl_opts);
    if let Some(input_dir) = cl_opts.dir.as_ref() {
        anyhow::ensure!(
            is_on_btrfs(input_dir)?,
            "Input directory {:?} is not on a Btrfs filesystem",
            input_dir
        );
    }

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
    let progress_counters = Arc::new(ProgressCounters::new());

    crossbeam_utils::thread::scope(|scope| -> anyhow::Result<()> {
        // Worker threads
        for _ in 0..max(cpu_count - 1, 1) {
            // Per thread clones
            let to_hashed_rx = to_hashed_rx.clone();
            let hashed_tx = hashed_tx.clone();
            let progress = progress.clone();
            let progress_counters = Arc::clone(&progress_counters);

            scope.spawn(move |_| -> anyhow::Result<()> {
                let mut hasher = xxh3::Xxh3::new();
                let mut buffer = [0; READ_BUFFER_SIZE];
                while let Ok((path, file_size)) = to_hashed_rx.recv() {
                    let file = match File::open(&path) {
                        Ok(file) => file,
                        Err(e) => {
                            log::warn!("Error while opening {:?}: {}", path, e);
                            continue;
                        }
                    };

                    let mut reader = BufReader::new(file);
                    let hash = compute_xxh(&mut hasher, &mut reader, &mut buffer)?;

                    log::debug!("{:?} {:016x}", path, hash);
                    progress_counters.hash_count.fetch_add(1, Ordering::AcqRel);
                    progress.set_message(format!("{progress_counters}"));

                    hashed_tx.send((path, file_size, hash))?;
                }

                Ok(())
            });
        }
        drop(to_hashed_rx);
        drop(hashed_tx);

        // Iterate over files
        let mut entry_map: HashMap<u64, Option<walkdir::DirEntry>> = HashMap::new();
        if let Some(input_dir) = cl_opts.dir {
            for entry in walkdir::WalkDir::new(input_dir)
                .same_file_system(true)
                .into_iter()
            {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(e) => {
                        log::warn!("{}", e);
                        continue;
                    }
                };
                if !entry.file_type().is_file() {
                    continue;
                }
                let file_size = entry.metadata()?.len();
                if file_size == 0 {
                    // Don't bother for empty files
                    continue;
                }
                if let Some(min_size) = cl_opts.min_size {
                    if file_size < min_size {
                        continue;
                    }
                }
                let path = entry.path();
                log::debug!("{:?}", path);
                progress_counters.file_count.fetch_add(1, Ordering::AcqRel);
                progress.set_message(format!("{progress_counters}"));

                // Decide what to to depending on whether or not we have already seen some files for this size
                // This allows saving some hash computations for the common case
                match entry_map.entry(file_size) {
                    Entry::Vacant(e) => {
                        // First file for this size, keep entry and move along
                        e.insert(Some(entry));
                    }
                    Entry::Occupied(e) => {
                        match e.get() {
                            Some(_) => {
                                // Second file for this size, send this one and the previous to the channel, and set map
                                // so the next ones will be sent immediately
                                let prev_entry = e.into_mut().take().unwrap();
                                to_hashed_tx.send((prev_entry.path().to_path_buf(), file_size))?;
                                to_hashed_tx.send((path.to_path_buf(), file_size))?;
                            }
                            None => {
                                // Not the first file not second for this size, send it to channel immediately
                                to_hashed_tx.send((path.to_path_buf(), file_size))?;
                            }
                        }
                    }
                }
            }
        } else {
            let mut stdin_locked = io::stdin().lock();
            let mut buf = Vec::new();
            let mut first = false;
            loop {
                buf.clear();
                if stdin_locked.read_until(0, &mut buf)? == 0 {
                    break;
                }
                buf.truncate(buf.len() - 1);
                let path = Path::new(OsStr::from_bytes(&buf));
                let entry = match walkdir::WalkDir::new(path)
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Woot"))?
                {
                    Ok(entry) => entry,
                    Err(e) => {
                        log::warn!("{}", e);
                        continue;
                    }
                };
                if !entry.file_type().is_file() {
                    log::warn!("{:?} is not a file, ignoring it", path);
                    continue;
                }
                if !first {
                    anyhow::ensure!(
                        is_on_btrfs(path)?,
                        "Input file {:?} is not on a Btrfs filesystem",
                        path
                    );
                    first = false;
                }
                let file_size = entry.metadata()?.len();
                if file_size == 0 {
                    // Don't bother for empty files
                    continue;
                }
                if let Some(min_size) = cl_opts.min_size {
                    if file_size < min_size {
                        continue;
                    }
                }
                log::debug!("{:?}", path);
                progress_counters.file_count.fetch_add(1, Ordering::AcqRel);
                progress.set_message(format!("{progress_counters}"));

                to_hashed_tx.send((path.to_path_buf(), file_size))?;
            }
        }
        drop(to_hashed_tx);

        // Fill hashmap
        for (filepath, file_size, hash) in hashed_rx.iter() {
            files.insert((file_size, hash), filepath);
        }
        Ok(())
    })
    .map_err(|e| anyhow::anyhow!("Worker thread error: {:?}", e))??;

    // Remove unique hashes
    for key in files
        .keys()
        .filter(|k| !files.is_vec(k))
        .map(|k| k.to_owned())
        .collect::<Vec<_>>()
    {
        files.remove(&key);
    }

    // Find candidates
    for ((_file_size, _file_hash), filepaths) in files.iter_all_mut() {
        let first = filepaths.first().unwrap();
        for other in filepaths.iter().skip(1) {
            if !same_content(first, other)? {
                log::warn!(
                    "Files {:?} and {:?} have the same size and hash but not the same content",
                    first,
                    other
                );
                progress_counters
                    .hash_collision_count
                    .fetch_add(1, Ordering::AcqRel);
                progress.set_message(format!("{progress_counters}"));
                continue;
            }

            if same_extents(first, other)? {
                log::debug!("Files {:?} and {:?} are already reflinked", first, other);
                progress_counters
                    .reflinked_count
                    .fetch_add(1, Ordering::AcqRel);
                progress.set_message(format!("{progress_counters}"));
                continue;
            }

            log::debug!(
                "Files {:?} and {:?} are duplicates",
                first.to_str().unwrap(),
                other.to_str().unwrap()
            );
            progress_counters
                .duplicate_candidate_count
                .fetch_add(1, Ordering::AcqRel);
            progress.set_message(format!("{progress_counters}"));
            print!("{}\0{}\0", first.to_str().unwrap(), other.to_str().unwrap());
        }
    }

    progress.finish();

    Ok(())
}
