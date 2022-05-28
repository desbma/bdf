//! Btrfs Duplicate Finder

use std::cmp::max;
use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Context;
use multimap::MultiMap;
use structopt::StructOpt;
use xxhash_rust::xxh3;

/// File read chunk size, in bytes
const READ_BUFFER_SIZE: usize = 256 * 1024;

/// Convenience type for a pair of crossbeam channel ends
type CrossbeamChannel<T> = (crossbeam_channel::Sender<T>, crossbeam_channel::Receiver<T>);

/// Command line arguments
#[derive(Debug, StructOpt)]
#[structopt(version=env!("CARGO_PKG_VERSION"), about="Find identical files, candidates for reflinking, on Btrfs filesystems.")]
pub struct CommandLineOpts {
    /// Input directory tree
    pub dir: PathBuf,
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

fn main() -> anyhow::Result<()> {
    // Init logger
    simple_logger::SimpleLogger::new()
        .init()
        .context("Failed to init logger")?;

    // Parse command line opts
    let cl_opts = CommandLineOpts::from_args();
    log::trace!("{:?}", cl_opts);

    // Get usable core count
    let cpu_count = num_cpus::get();

    // Channels
    let (entries_tx, entries_rx): CrossbeamChannel<walkdir::DirEntry> =
        crossbeam_channel::unbounded();
    let (hash_tx, hash_rx): CrossbeamChannel<(PathBuf, u64, u64)> = crossbeam_channel::unbounded();

    // File hash map
    let mut files: MultiMap<(u64, u64), PathBuf> = MultiMap::new();

    // Progress
    let progress = indicatif::ProgressBar::new_spinner();
    progress.set_draw_delta(1);
    //progress.set_draw_rate(1);
    progress.enable_steady_tick(300);
    let progress_counters = Arc::new(ProgressCounters::new());

    crossbeam_utils::thread::scope(|scope| -> anyhow::Result<()> {
        // Worker threads
        for _ in 0..max(cpu_count - 1, 1) {
            // Per thread clones
            let entries_rx = entries_rx.clone();
            let hash_tx = hash_tx.clone();
            let progress = progress.clone();
            let progress_counters = Arc::clone(&progress_counters);

            scope.spawn(move |_| -> anyhow::Result<()> {
                let mut hasher = xxh3::Xxh3::new();
                let mut buffer = [0; READ_BUFFER_SIZE];
                while let Ok(entry) = entries_rx.recv() {
                    let path = entry.path();
                    let file = match File::open(path) {
                        Ok(file) => file,
                        Err(e) => {
                            log::warn!("Error while opening {:?}: {}", path, e);
                            continue;
                        }
                    };
                    let file_size = entry.metadata()?.len();
                    if file_size == 0 {
                        // Don't bother for empty files
                        continue;
                    }
                    let mut reader = BufReader::new(file);

                    let hash = compute_xxh(&mut hasher, &mut reader, &mut buffer)?;

                    log::debug!("{:?} {:016x}", path, hash);
                    progress_counters.hash_count.fetch_add(1, Ordering::AcqRel);
                    progress.set_message(format!("{}", progress_counters));

                    hash_tx.send((path.to_path_buf(), file_size, hash))?;
                }

                Ok(())
            });
        }
        drop(entries_rx);
        drop(hash_tx);

        // Iterate over files
        for entry in walkdir::WalkDir::new(cl_opts.dir)
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
            log::debug!("{:?}", entry.path());
            progress_counters.file_count.fetch_add(1, Ordering::AcqRel);
            progress.set_message(format!("{}", progress_counters));

            entries_tx.send(entry)?;
        }
        drop(entries_tx);

        // Fill hashmap
        for (filepath, file_size, hash) in hash_rx.iter() {
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
                progress.set_message(format!("{}", progress_counters));
                continue;
            }

            if same_extents(first, other)? {
                log::debug!("Files {:?} and {:?} are already reflinked", first, other);
                progress_counters
                    .reflinked_count
                    .fetch_add(1, Ordering::AcqRel);
                progress.set_message(format!("{}", progress_counters));
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
            progress.set_message(format!("{}", progress_counters));
            print!("{}\0{}\0", first.to_str().unwrap(), other.to_str().unwrap());
        }
    }

    progress.finish();

    Ok(())
}
