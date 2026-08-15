// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    println!("cargo::rerun-if-env-changed=PIPEWIRE_SOURCE_DIR");
    println!("cargo::rerun-if-changed=c/activation_abi.c");

    let source = pipewire_source();
    let source_include = source.join("src");
    let spa_source_include = source.join("spa/include");
    let spa = pkg_config::Config::new()
        .cargo_metadata(false)
        .probe("libspa-0.2")
        .expect("libspa-0.2 development headers are required");
    let pipewire = pkg_config::Config::new()
        .cargo_metadata(false)
        .probe("libpipewire-0.3")
        .expect("libpipewire-0.3 development headers are required");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo must set OUT_DIR"));
    let probe = out_dir.join("activation-abi-probe");

    let mut command = Command::new(env::var_os("CC").unwrap_or_else(|| "cc".into()));
    command
        .arg("-std=c11")
        .arg("-D_GNU_SOURCE")
        .arg("-DPW_ACTIVATION_ABI_PROBE")
        .arg("c/activation_abi.c")
        .arg("-o")
        .arg(&probe)
        .arg(format!("-I{}", source_include.display()))
        .arg(format!("-I{}", spa_source_include.display()));
    for include in spa.include_paths.iter().chain(&pipewire.include_paths) {
        command.arg(format!("-I{}", include.display()));
    }
    let status = command
        .status()
        .expect("failed to execute activation ABI C compiler");
    assert!(status.success(), "failed to compile activation ABI C probe");

    let output = Command::new(&probe)
        .output()
        .expect("failed to execute activation ABI C probe");
    assert!(output.status.success(), "activation ABI C probe failed");
    let values = String::from_utf8(output.stdout).expect("C ABI probe output was not UTF-8");
    fs::write(out_dir.join("activation_abi.rs"), values)
        .expect("failed to write generated activation ABI constants");

    let mut build = cc::Build::new();
    build
        .file("c/activation_abi.c")
        .define("_GNU_SOURCE", None)
        .include(source_include)
        .include(spa_source_include);
    for include in spa.include_paths.iter().chain(&pipewire.include_paths) {
        build.include(include);
    }
    build.compile("activation-abi");
}

fn pipewire_source() -> PathBuf {
    if let Some(path) = env::var_os("PIPEWIRE_SOURCE_DIR") {
        let path = PathBuf::from(path);
        assert_private_header(&path);
        return path;
    }

    if let Some(home) = env::var_os("HOME") {
        let path = PathBuf::from(home).join("archive/pipewire/pipewire");
        if path.join("src/pipewire/private.h").is_file() {
            return path;
        }
    }

    panic!(
        "PipeWire source checkout not found; set PIPEWIRE_SOURCE_DIR to the pinned upstream tree"
    );
}

fn assert_private_header(path: &Path) {
    assert!(
        path.join("src/pipewire/private.h").is_file(),
        "PIPEWIRE_SOURCE_DIR does not contain src/pipewire/private.h"
    );
}
