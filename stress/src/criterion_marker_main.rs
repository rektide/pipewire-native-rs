use std::{env, process};

use pipewire_native_stress::marker::Marker;

fn main() {
    let mut args = env::args().skip(1);
    let profile = args.next().unwrap_or_else(|| {
        eprintln!("usage: pw-criterion-marker PROFILE [CRITERION_ARGS...]");
        process::exit(2);
    });
    let marker = Marker::collect(profile, args.collect()).unwrap_or_else(|error| {
        eprintln!("error: cannot collect Criterion marker: {error}");
        process::exit(2);
    });
    println!("{}", marker.history_id);
    println!("{}", marker.history_description.replace(['\r', '\n'], " "));
    println!(
        "{}",
        serde_json::to_string(&marker).expect("marker serialization failed")
    );
}
