// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

fn main() {
    match pkg_config::get_variable("libspa-0.2", "plugindir") {
        Ok(plugindir) => println!("cargo::rustc-env=SPA_DEFAULT_PLUGINDIR={plugindir}"),
        Err(e) => {
            println!("cargo::warning=pkg-config error: {e})");
            println!("cargo::warning=Could not find SPA plugindir from pkg-config, using default");
            println!("cargo::rustc-env=SPA_DEFAULT_PLUGINDIR=/usr/lib64/spa-0.2");
        }
    }
    let out_dir = std::env::var_os("OUT_DIR").unwrap();
    let dest_path = std::path::Path::new(&out_dir).join("conf_dir.rs");
    let conf_dir = std::env::var("PIPEWIRE_CONFIG_DIR").unwrap_or("/etc/pipewire".to_string());
    let conf_data_dir =
        std::env::var("PIPEWIRE_CONFIG_DATA_DIR").unwrap_or("/usr/share/pipewire".to_string());

    std::fs::write(
        &dest_path,
        format!(
            "
            const PIPEWIRE_CONFIG_DIR: &str = \"{}\";
            const PIPEWIRE_CONFIG_DATA_DIR: &str = \"{}\";
            ",
            conf_dir, conf_data_dir
        ),
    )
    .unwrap();
}
