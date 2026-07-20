//! End to end benchmark of the whole binary, fed NUL terminated paths on stdin
//!
//! The benched tree must hold no foreign mount point: unlike the directory walk, which filters them out, the binary
//! rejects an input path that is not on the filesystem of the first one.

// The binary is driven as a subprocess, so most of its dependencies are not linked here
#![expect(unused_crate_dependencies)]

mod common;

use std::{
    io::Write as _,
    os::unix::ffi::OsStrExt as _,
    path::Path,
    process::{Command, Output, Stdio},
    thread,
};

use anyhow::Context as _;

/// Run the binary over the regular files of `dir`, feeding their paths on stdin
fn run(dir: &Path, min_size: u64) -> anyhow::Result<Output> {
    let mut child = Command::new(common::BIN)
        .arg("--min-size")
        .arg(min_size.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        // Capturing stderr hides the progress bar, which is only drawn to a terminal, and makes the message of a
        // failed run available to report
        .stderr(Stdio::piped())
        .spawn()
        .context("Failed to run the binary")?;
    let mut stdin = child.stdin.take().context("Missing stdin pipe")?;

    thread::scope(|scope| {
        // Feeding from its own thread, so that reading the reported pairs never waits on the paths still to be written
        let feeder = scope.spawn(move || -> anyhow::Result<()> {
            for entry in walkdir::WalkDir::new(dir) {
                let entry = entry?;
                if !entry.file_type().is_file() {
                    continue;
                }
                stdin.write_all(entry.path().as_os_str().as_bytes())?;
                stdin.write_all(&[0])?;
            }
            Ok(())
        });
        // Draining stdout first, as the binary only exits once the feeder is done and has dropped its end of stdin
        let output = child
            .wait_with_output()
            .context("Failed to run the binary")?;
        feeder
            .join()
            .map_err(|e| anyhow::anyhow!("Feeder thread panicked: {e:?}"))??;
        Ok(output)
    })
}

fn main() -> anyhow::Result<()> {
    common::bench(run)
}
