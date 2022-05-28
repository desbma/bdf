# bdf

[![Build status](https://github.com/desbma/bdf/actions/workflows/ci.yml/badge.svg)](https://github.com/desbma/bdf/actions)
[![License](https://img.shields.io/github/license/desbma/bdf.svg?style=flat)](https://github.com/desbma/bdf/blob/master/LICENSE)

`bdf` (Btrfs Duplicate Finder)

`bdf` is a simple tool to efficiently find identical files, candidates for [reflinking](https://btrfs.readthedocs.io/en/latest/Reflink.html), on Btrfs filesystems.

In keeping with Unix philosophy, it does only one thing: find duplicates not yet reflinked. It does not deduplicate files, it is up to you to decide what to do with the identical files. It is very easy to use `bdf`'s output to deduplicate tghough, see [Auto deduplication](### Auto deduplication).

Compared to alternative solutions, `bdf` is fast and simple. It does not store any state or database locally, and does not touch your files, you can even run it on a read only mounted filesystem.

## Algorithm

1. For all files, get the file size and compute the [XXH3-64](https://github.com/Cyan4973/xxHash)
2. For files with similar hashes and size, check the file content (for the extremely unlikely but possible case of a hash collision)
3. For files with similar content, check if some Btrfs file extents are different.
4. If some extents are not shared, the files are considered duplicates candidate for reflinking

## Installation

### From source

You need a Rust build environment for example from [rustup](https://rustup.rs/).

```
cargo build --release
strip --strip-all target/release/bdf
install -Dm 755 -t /usr/local/bin target/release/bdf
```

## Usage

`bdf` outputs progress information on _stderr_, and NUL (`'\0'`) terminated pairs of filepaths to deduplicate on _stdout_, for easy and safe usage in shell scripts.

See `bdf -h` for complete command line reference.

### Auto deduplication

To automatically deduplicate files, run `bdf` with `xargs`, and deduplicate with `cp`:

```
bdf target_dir | xargs -0 -n 2 cp -v --reflink=always --preserve=all
```

## License

[GPLv3](./LICENSE)
