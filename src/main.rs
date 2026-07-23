//! Btrfs Duplicate Finder

use std::{
    cmp::max,
    collections::hash_map::{Entry, HashMap},
    ffi::OsStr,
    fmt,
    fs::{self, File},
    io::{self, BufRead as _, Read as _, Write},
    iter,
    mem::{self, zeroed},
    ops::Range,
    os::{
        fd::AsRawFd as _,
        unix::{
            ffi::OsStrExt as _,
            fs::{FileExt as _, MetadataExt as _},
        },
    },
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
use linux_raw_sys::{btrfs::btrfs_ioctl_fs_info_args, ioctl};
use rayon::iter::{ParallelBridge as _, ParallelIterator as _};
use xxhash_rust::xxh3;

/// File read chunk size, in bytes
const READ_BUFFER_SIZE: usize = 256 * 1024;

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
const BTRFS_MAX_UNCOMPRESSED: u64 = 128 * 1024;

/// One mapped extent, as the FIEMAP ioctl reports it
// Field names follow the kernel ABI
#[expect(clippy::struct_field_names)]
#[repr(C)]
#[derive(Clone, Copy)]
struct FiemapExtent {
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
struct FiemapRequest {
    /// Request header
    header: FiemapHeader,
    /// Extent buffer the kernel fills
    extents: [FiemapExtent; FIEMAP_BATCH_SIZE as usize],
}

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

/// Identifier of a Btrfs filesystem, the same for all of its subvolumes
type BtrfsFsid = [u8; 16];

/// Offset of one extent in the file, its location on the device, and its length
type ExtentLocation = (u64, u64, u64);

/// Representative of each data copy seen so far, the file kept for hashing while later files found mapping to the
/// same extents hold its bytes and are therefore spared hashing, keyed by file size and extent key
type SharedIndex = HashMap<(u64, Vec<ExtentLocation>), (PathBuf, Vec<PathBuf>)>;

nix::ioctl_read!(
    /// Query the Btrfs filesystem holding an open file
    btrfs_fs_info,
    0x94,
    31,
    btrfs_ioctl_fs_info_args
);

nix::ioctl_readwrite!(
    /// Map the extents of an open file, taking the request header the extent buffer follows
    fs_fiemap,
    b'f',
    11,
    FiemapHeader
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

/// Number of directory walker threads
const WALKER_THREADS: usize = 16;

/// Walk a directory tree in parallel, yielding each regular file eligible for analysis along with its size,
/// without leaving the Btrfs filesystem the tree starts on
///
/// Subvolumes of a filesystem each have their own device, so pruning on a device change would skip them, while
/// reflinking across them is perfectly valid.
fn walk_btrfs_dir(
    input_dir: &Path,
    min_size: Option<u64>,
) -> anyhow::Result<impl Iterator<Item = (PathBuf, u64)>> {
    let filesystem = Mutex::new(
        BtrfsFilesystem::containing(input_dir)
            .with_context(|| format!("Failed to identify the filesystem of {input_dir:?}"))?
            .with_context(|| format!("{input_dir:?} is not on a Btrfs filesystem"))?,
    );

    // An eligible file carries its size, a directory carries nothing but is kept to descend into
    let walk = jwalk::WalkDirGeneric::<((), Option<u64>)>::new(input_dir)
        // Dotfiles are ordinary candidates, against jwalk's default of skipping them
        .skip_hidden(false)
        .parallelism(jwalk::Parallelism::RayonNewPool(WALKER_THREADS))
        .process_read_dir(move |depth, _dir, _state, entries| {
            entries.retain_mut(|result| {
                let Ok(entry) = result else {
                    return true;
                };
                let path = entry.path();
                // The root, at depth none, is reached through a symlink it may be named as, while entries below it
                // are stated unfollowed; the stat also drops a regular file that is its own mount point, and sizes it
                let metadata = if depth.is_none() {
                    fs::metadata(&path)
                } else {
                    fs::symlink_metadata(&path)
                };
                let Ok(metadata) = metadata.inspect_err(|e| {
                    log::warn!("Error while reading metadata of {path:?}: {e}");
                }) else {
                    return false;
                };
                if !metadata.is_dir() && !metadata.is_file() {
                    return false;
                }
                // The lock guards only this quick filesystem test, which never panics, so it can not be poisoned
                let holds = match filesystem.lock() {
                    Ok(mut fs) => fs.holds(&path, metadata.dev()),
                    Err(_) => return false,
                };
                let Ok(true) = holds.inspect_err(|e| {
                    log::warn!("Error while identifying the filesystem of {path:?}: {e}");
                }) else {
                    return false;
                };
                entry.client_state = metadata
                    .is_file()
                    .then(|| wanted_file_size(metadata.len(), min_size))
                    .flatten();
                metadata.is_dir() || entry.client_state.is_some()
            });
        })
        .try_into_iter()?;

    Ok(walk.filter_map(|result| {
        let entry = result
            .inspect_err(|e| log::warn!("Error while walking the tree: {e}"))
            .ok()?;
        entry.client_state.map(|size| (entry.path(), size))
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
fn hash_file(file: &File, hasher: &mut xxh3::Xxh3, buffer: &mut Vec<u8>) -> Result<u64, io::Error> {
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
    let first = File::open(first)?;
    let second = File::open(second)?;
    // One range spanning the whole file, which same_ranges clamps to its actual size
    let whole_file = 0..u64::MAX;
    same_ranges(&first, &second, &[whole_file])
}

/// Test if two files of the same size have the same content over every given range
///
/// Extent lengths are block aligned, so a range reaching past the end of the files is clamped to it.
fn same_ranges(first: &File, second: &File, ranges: &[Range<u64>]) -> Result<bool, io::Error> {
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

/// Map the extents of an open file, having the kernel flush pending writes first when `sync` is set
///
/// Data under delayed allocation is reported without a location, which only a flush resolves, hiding whether it
/// will be inlined.
fn file_extents(file: &File, sync: bool) -> Result<Vec<FiemapExtent>, io::Error> {
    let mut extents: Vec<FiemapExtent> = Vec::new();
    // SAFETY: every field is an integer, for which zero is a valid value
    let mut request: FiemapRequest = unsafe { zeroed() };
    loop {
        request.header = FiemapHeader {
            fm_start: extents
                .last()
                .map_or(0, |last| last.fe_logical + last.fe_length),
            fm_length: u64::MAX,
            // One flush resolves delayed allocation for the whole file, so further batches skip it
            fm_flags: if sync && extents.is_empty() {
                ioctl::FIEMAP_FLAG_SYNC
            } else {
                0
            },
            fm_mapped_extents: 0,
            fm_extent_count: FIEMAP_BATCH_SIZE,
            fm_reserved: 0,
        };
        // SAFETY: the ioctl writes at most `fm_extent_count` extents into the buffer following the header, which
        // `FiemapRequest` lays out with that capacity
        unsafe { fs_fiemap(file.as_raw_fd(), &raw mut request.header) }?;
        let mapped = request.header.fm_mapped_extents as usize;
        extents.extend(request.extents.iter().take(mapped).copied());
        if mapped < FIEMAP_BATCH_SIZE as usize
            || extents
                .last()
                .is_some_and(|last| last.fe_flags & ioctl::FIEMAP_EXTENT_LAST != 0)
        {
            return Ok(extents);
        }
    }
}

/// Whether a file stores its data in filesystem metadata rather than in a data extent
fn is_inlined(extents: &[FiemapExtent]) -> bool {
    extents
        .iter()
        .any(|extent| extent.fe_flags & ioctl::FIEMAP_EXTENT_DATA_INLINE != 0)
}

/// Identify the data a file maps to, `None` when its extents can not establish sharing
///
/// Files sharing a key hold the same bytes, save for compressed extents, which several contents can share. The
/// logical offset is part of it, as the same extent mapped elsewhere in the file puts its bytes elsewhere too.
fn extent_key(extents: &[FiemapExtent]) -> Option<Vec<ExtentLocation>> {
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
/// The mapping omits the offset of the data inside a compressed extent, so an extent shorter than the largest
/// Btrfs writes may cover any range of a larger extent, and files mapping to it can hold different bytes.
fn ambiguous_ranges(extents: &[FiemapExtent]) -> Vec<Range<u64>> {
    extents
        .iter()
        .filter(|extent| {
            extent.fe_flags & ioctl::FIEMAP_EXTENT_ENCODED != 0
                && extent.fe_length != BTRFS_MAX_UNCOMPRESSED
        })
        .map(|extent| extent.fe_logical..extent.fe_logical + extent.fe_length)
        .collect()
}

/// Record `file` in the index of content seen so far, attaching `path` to the entry sharing its bytes if any
///
/// Returns whether the file was attached, sparing its hashing; a fresh key starts a new entry instead, for later
/// files to attach to. Sharing every extent proves the bytes shared, except over ambiguous ranges, which are
/// compared before trusting the entry; a mismatch, an error on this file alone, or a comparison that would read
/// at least as much as hashing all leave the file to hashing.
fn try_attach_shared(
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
                let mut writer = writer
                    .lock()
                    .map_err(|e| io::Error::other(format!("Poisoned lock: {e}")))?;
                write_pair(&mut **writer, first_path, path)?;
            }
        }
    }
    Ok(())
}

/// Report the duplicate pairs of every hashed group and unhashed size pair, spreading groups over a Rayon pool
///
/// A pair names the file to reflink onto, then the one to replace, so the output feeds `cp --reflink` directly.
fn report_duplicates<W>(
    files: &HashMap<(u64, u64), Vec<PathBuf>>,
    pair_groups: &[Vec<PathBuf>],
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
        .try_for_each(|(filepaths, hashed)| report_group(filepaths, hashed, counters, &writer))?;
    Ok(())
}

/// Return the size when a file this large should be considered
fn wanted_file_size(file_size: u64, min_size: Option<u64>) -> Option<u64> {
    // Don't bother for empty files
    (file_size != 0 && min_size.is_none_or(|minimum| file_size >= minimum)).then_some(file_size)
}

/// Files seen so far for one size
enum SizeEntry {
    /// A single file, which can not have a duplicate yet
    One(PathBuf),
    /// Exactly two files, withheld for a direct comparison, which reads less than hashing them
    Pair(PathBuf, PathBuf),
    /// Three files or more, whose hashes partition the size into content groups cheaper than comparisons would
    Hashing,
}

/// Tracks the sizes seen so far, withholding files until enough share a size to settle how to check them
///
/// Identical files have identical sizes, so a file whose size no other file shares can not have a duplicate, and
/// hashing it would read it in full for nothing. A size shared by exactly two files is settled by comparing them
/// directly, so hashing only pays once a third file shows up.
#[derive(Default)]
struct SizeTracker(HashMap<u64, SizeEntry>);

impl SizeTracker {
    /// Take in a file, returning the files whose size is now known to need hashing: none while the size holds two
    /// files or fewer, the three withheld files when a third one ends the direct comparison plan, this one alone
    /// afterwards
    fn track(&mut self, path: PathBuf, file_size: u64) -> [Option<PathBuf>; 3] {
        match self.0.entry(file_size) {
            Entry::Vacant(e) => {
                e.insert(SizeEntry::One(path));
                [None, None, None]
            }
            Entry::Occupied(mut e) => match mem::replace(e.get_mut(), SizeEntry::Hashing) {
                SizeEntry::One(first) => {
                    *e.get_mut() = SizeEntry::Pair(first, path);
                    [None, None, None]
                }
                SizeEntry::Pair(first, second) => [Some(first), Some(second), Some(path)],
                SizeEntry::Hashing => [None, None, Some(path)],
            },
        }
    }

    /// Drain the sizes holding exactly two files, which a direct comparison settles without hashing
    fn into_pairs(self) -> impl Iterator<Item = Vec<PathBuf>> {
        self.0.into_values().filter_map(|entry| match entry {
            SizeEntry::Pair(first, second) => Some(vec![first, second]),
            SizeEntry::One(_) | SizeEntry::Hashing => None,
        })
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

            workers.push(scope.spawn(move || -> anyhow::Result<()> {
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

                        let Ok(hash) =
                            hash_file(&file, &mut hasher, &mut buffer).inspect_err(|e| {
                                log::warn!("Error while hashing {path:?}: {e}");
                            })
                        else {
                            continue;
                        };

                        log::debug!("{path:?} {hash:016x}");
                        progress_counters.hash.fetch_add(1, Ordering::Relaxed);

                        hashed_batch.push((path, file_size, hash));
                    }
                    if !hashed_batch.is_empty() {
                        hashed_tx.send(hashed_batch)?;
                    }
                }

                Ok(())
            }));
        }
        drop(to_hashed_rx);
        drop(hashed_tx);

        // Iterate over files
        let mut size_tracker = SizeTracker::default();
        let mut to_hashed_tx = BatchSender::new(to_hashed_tx);
        if let Some(walk) = dir_walk {
            for (path, file_size) in walk {
                log::debug!("{path:?}");
                progress_counters.file.fetch_add(1, Ordering::Relaxed);

                let tracked = size_tracker.track(path, file_size);
                for to_hash in tracked.into_iter().flatten() {
                    to_hashed_tx.send((to_hash, file_size), file_size)?;
                }
            }
        } else {
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
                let Some(file_size) = wanted_file_size(metadata.len(), cl_opts.min_size) else {
                    continue;
                };
                log::debug!("{path:?}");
                progress_counters.file.fetch_add(1, Ordering::Relaxed);

                let tracked = size_tracker.track(path.to_path_buf(), file_size);
                for to_hash in tracked.into_iter().flatten() {
                    to_hashed_tx.send((to_hash, file_size), file_size)?;
                }
            }
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

    // Index the files spared hashing by their hashed representative
    let shared_members: HashMap<PathBuf, Vec<PathBuf>> = shared_index
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Poisoned lock: {e}"))?
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

    // Find candidates
    let mut stdout = io::stdout();
    report_duplicates(&files, &pair_groups, &progress_counters, &mut stdout)?;

    progress.finish();

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        io::Seek as _,
        os::unix::fs::symlink,
        process::{Command, Stdio},
    };

    use linux_raw_sys::btrfs::{file_clone_range, FS_COMPR_FL};
    // The test build of the binary links every dev-dependency, and this one only serves the benches
    use walkdir as _;

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

    /// Clone `length` bytes of `source` at `offset` into `path` at `dest_offset`, creating the file if needed
    fn clone_range(source: &Path, offset: u64, length: u64, path: &Path, dest_offset: u64) {
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

    /// Map a file by path, flushing it first, as the candidate reporting does
    fn path_extents(path: &Path) -> Vec<FiemapExtent> {
        file_extents(&File::open(path).unwrap(), true).unwrap()
    }

    /// Map both files and test whether their extents yield the same key, as the candidate reporting does
    fn files_have_same_extent_key(first: &Path, second: &Path) -> bool {
        let key = extent_key(&path_extents(first));
        key.is_some() && key == extent_key(&path_extents(second))
    }

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

    /// Map a file without flushing it, leaving delayed allocation unresolved
    fn unflushed_extents(path: &Path) -> Vec<FiemapExtent> {
        file_extents(&File::open(path).unwrap(), false).unwrap()
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

    /// Run `try_attach_shared` for a file over an index, as a hash worker does
    fn attached(index: &Mutex<SharedIndex>, path: &Path) -> bool {
        let file = File::open(path).unwrap();
        let file_size = file.metadata().unwrap().len();
        try_attach_shared(index, &file, path, file_size).unwrap()
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

    /// Report the given hashed group and unhashed pair groups, decoding the pairs written
    fn decoded_report(
        files: &HashMap<(u64, u64), Vec<PathBuf>>,
        pair_groups: &[Vec<PathBuf>],
        counters: &ProgressCounters,
    ) -> Vec<(PathBuf, PathBuf)> {
        let mut out = Vec::new();
        report_duplicates(files, pair_groups, counters, &mut out).unwrap();

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

    /// Collect the pairs a tracker still withholds, sorted for stable comparison
    fn tracked_pairs(size_tracker: SizeTracker) -> Vec<Vec<PathBuf>> {
        let mut pairs: Vec<_> = size_tracker.into_pairs().collect();
        pairs.sort();
        pairs
    }

    /// Take in a file named `path` of the given size, as the size iteration does
    fn track(size_tracker: &mut SizeTracker, path: &str, file_size: u64) -> [Option<PathBuf>; 3] {
        size_tracker.track(path.into(), file_size)
    }

    #[test]
    fn size_tracker_withholds_a_pair_for_direct_comparison() {
        let mut size_tracker = SizeTracker::default();

        assert_eq!(track(&mut size_tracker, "a", 1), [None, None, None]);
        assert_eq!(track(&mut size_tracker, "b", 1), [None, None, None]);
        assert_eq!(
            tracked_pairs(size_tracker),
            vec![vec![PathBuf::from("a"), PathBuf::from("b")]]
        );
    }

    #[test]
    fn size_tracker_transitions_from_pair_to_hashing() {
        let mut size_tracker = SizeTracker::default();
        assert_eq!(track(&mut size_tracker, "a", 1), [None, None, None]);
        assert_eq!(track(&mut size_tracker, "b", 1), [None, None, None]);

        // The third file of a size ends the direct comparison plan, releasing the two withheld files with it
        assert_eq!(
            track(&mut size_tracker, "c", 1),
            [Some("a".into()), Some("b".into()), Some("c".into())]
        );
        // Once hashing, each further file of the size is released on its own
        assert_eq!(
            track(&mut size_tracker, "d", 1),
            [None, None, Some("d".into())]
        );
        assert_eq!(tracked_pairs(size_tracker), Vec::<Vec<PathBuf>>::new());
    }

    #[test]
    fn size_tracker_keeps_sizes_independent() {
        let mut size_tracker = SizeTracker::default();
        track(&mut size_tracker, "a", 1);
        track(&mut size_tracker, "b", 2);
        track(&mut size_tracker, "c", 2);

        // The lone file of size 1 stays withheld while size 2 crosses into hashing
        assert_eq!(
            track(&mut size_tracker, "d", 2),
            [Some("b".into()), Some("c".into()), Some("d".into())]
        );
        assert_eq!(tracked_pairs(size_tracker), Vec::<Vec<PathBuf>>::new());
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
    fn wanted_file_size_keeps_the_minimum_and_drops_empty_files() {
        assert_eq!(wanted_file_size(4, Some(4)), Some(4));
        assert_eq!(wanted_file_size(3, Some(4)), None);
        assert_eq!(wanted_file_size(4, None), Some(4));
        // An empty file has no duplicate worth reflinking, whatever the threshold
        assert_eq!(wanted_file_size(0, None), None);
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
            .all(|extent| extent.fe_flags & UNRESOLVED_EXTENT_FLAGS != 0));

        // Both map the same way, so a key would group them
        assert!(extent_key(&extents1).is_none());
        assert!(extent_key(&extents2).is_none());
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
    fn walk_btrfs_dir_follows_a_root_symlink() {
        let Some(dir) = btrfs_test_dir() else { return };
        let tree = dir.path().join("tree");
        fs::create_dir(&tree).unwrap();
        fs::write(tree.join("file"), b"content").unwrap();
        let link = dir.path().join("link");
        symlink(&tree, &link).unwrap();

        let walked: Vec<PathBuf> = walk_btrfs_dir(&link, None)
            .unwrap()
            .map(|(path, _size)| path)
            .collect();

        assert_eq!(walked, vec![link.join("file")]);
    }

    #[test]
    fn walk_btrfs_dir_does_not_follow_nested_symlinks() {
        let Some(dir) = btrfs_test_dir() else { return };
        let tree = dir.path().join("tree");
        let outside = dir.path().join("outside");
        fs::create_dir(&tree).unwrap();
        fs::create_dir(&outside).unwrap();
        fs::write(tree.join("inside"), b"content").unwrap();
        fs::write(outside.join("escaped"), b"content").unwrap();
        symlink(&outside, tree.join("link")).unwrap();

        let walked: Vec<PathBuf> = walk_btrfs_dir(&tree, None)
            .unwrap()
            .map(|(path, _size)| path)
            .collect();

        assert_eq!(walked, vec![tree.join("inside")]);
    }

    #[test]
    fn walk_btrfs_dir_includes_hidden_files() {
        let Some(dir) = btrfs_test_dir() else { return };
        let hidden = dir.path().join(".hidden");
        let regular = dir.path().join("regular");
        fs::write(&hidden, b"content").unwrap();
        fs::write(&regular, b"content").unwrap();

        let mut walked: Vec<PathBuf> = walk_btrfs_dir(dir.path(), None)
            .unwrap()
            .map(|(path, _size)| path)
            .collect();
        walked.sort();

        assert_eq!(walked, vec![hidden, regular]);
    }

    #[test]
    fn walk_btrfs_dir_descends_into_subvolumes() {
        let Some(dir) = btrfs_test_dir() else { return };
        let subvolume = create_subvolume(dir.path(), "subvolume");
        let nested = create_subvolume(&subvolume, "nested");
        fs::write(dir.path().join("outside"), b"content").unwrap();
        fs::write(subvolume.join("inside"), b"content").unwrap();
        fs::write(nested.join("deeper"), b"content").unwrap();

        let mut walked: Vec<PathBuf> = walk_btrfs_dir(dir.path(), None)
            .unwrap()
            .map(|(path, _size)| path)
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
