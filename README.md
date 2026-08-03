# bdf

[![Build status](https://github.com/desbma/bdf/actions/workflows/ci.yml/badge.svg)](https://github.com/desbma/bdf/actions)
[![AUR version](https://img.shields.io/aur/version/bdf.svg?style=flat)](https://aur.archlinux.org/packages/bdf/)
[![License](https://img.shields.io/github/license/desbma/bdf.svg?style=flat)](https://github.com/desbma/bdf/blob/master/LICENSE)

Btrfs Duplicate Finder

`bdf` is a simple tool to **efficiently** find identical files, candidates for [reflinking](https://btrfs.readthedocs.io/en/latest/Reflink.html), on Btrfs filesystems.

In keeping with the Unix philosophy "_do one thing and do it well_", it finds duplicates not yet reflinked, but does not deduplicate files. It is up to you to decide what to do with the identical files. If you are looking for an easy way to deduplicate that "just works", it is very easy to use `bdf`'s output in a pipeline to deduplicate though, see [Auto deduplication](#auto-deduplication).

Compared to alternative solutions, `bdf` is fast and simple. It does not store any state or database locally, and does not touch your files, you can even run it on a read only mounted filesystem.

## Algorithm summary

1. Walk the input tree with parallel threads, and get the size of every file \*
2. For sizes shared by three files or more, compute the [XXH3-64](https://github.com/Cyan4973/xxHash) of each file \*\*
3. For files with the same hash and size, or pairs of files alone sharing a size, ask Btrfs where each file stores its data (using [fiemap](https://www.kernel.org/doc/html/latest/filesystems/fiemap.html)), and group the files that already share it
4. Compare one file per copy of the data to confirm the content is identical, which for hashed groups also catches the extremely unlikely but possible case of a hash collision
5. Files holding the same content, but each in its own copy of the data, are considered duplicates candidate for reflinking

_\* Files whose size no other file shares can not be duplicates, and are dismissed without reading their data (the common case), which leads to a major overall speedup. A size shared by exactly two files skips hashing entirely: directly comparing the pair reads less than hashing both files._

_\*\* Hashes are computed in separate threads to make use of multi core CPUs. Before a file is read, its extents are mapped: a file mapping to the same extents as a file already picked for hashing holds the same bytes, and is attached to it without hashing it (compressed extents whose location does not pin the bytes down are first compared directly). Steps 3 and 4 also run on parallel worker threads, spare reading files whose extents show they already share their data, and read only one file per data copy for the comparison._

## Installation

### From source

You need a Rust build environment for example from [rustup](https://rustup.rs/).

```
cargo build --release
install -Dm 755 -t /usr/local/bin target/release/bdf
```

### From the AUR

Arch Linux users can install the [bdf AUR package](https://aur.archlinux.org/packages/bdf/).

## Usage

`bdf` outputs progress information on _stderr_, and NUL (`'\0'`) terminated pairs of filepaths to deduplicate on _stdout_, for easy and safe usage in shell scripts.

See `bdf -h` for complete command line reference.

### Auto deduplication

To automatically deduplicate files in directory `target_dir`, run `bdf` with the `--dedup` command line argument.

Reflinking a pair whose files sit in subvolumes mounted separately needs Linux 5.18 or later. Earlier kernels restrict `FICLONE` to a single mount point, and `cp` fails with `EXDEV` even though both files belong to the same filesystem.

## License

[GPLv3](./LICENSE)
