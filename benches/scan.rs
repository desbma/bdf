//! End to end benchmark of the whole binary, walking the tree itself

// The binary is driven as a subprocess, so none of its dependencies are linked here
#![expect(unused_crate_dependencies)]

mod common;

use std::{
    path::Path,
    process::{Command, Output},
};

use anyhow::Context as _;

/// Run the binary over `dir`, letting it walk the tree
fn run(dir: &Path, min_size: u64) -> anyhow::Result<Output> {
    Command::new(common::BIN)
        .arg("--min-size")
        .arg(min_size.to_string())
        .arg(dir)
        .output()
        .context("Failed to run the binary")
}

fn main() -> anyhow::Result<()> {
    common::bench(run)
}
