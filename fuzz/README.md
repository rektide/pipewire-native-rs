# SPA POD fuzzing

This independent Cargo workspace fuzzes the implemented typed and raw SPA POD
decode/parser entry points. Malformed PODs normally return errors; only panics,
timeouts, sanitizer findings, or excessive resource use are failures.

Install `cargo-fuzz`, then run from the repository root:

```sh
cargo install cargo-fuzz
cargo +nightly fuzz run --manifest-path fuzz/Cargo.toml pod_decode \
  fuzz/corpus/pod_decode -- -max_len=65536 -timeout=5
```

Re-run one saved input with:

```sh
cargo +nightly fuzz run --manifest-path fuzz/Cargo.toml pod_decode \
  fuzz/artifacts/pod_decode/<input>
```

The harness also truncates each input to 64 KiB so peer-declared data cannot make
the target allocate without a fixed bound. Keep minimized reproductions in
`corpus/pod_decode/`; transient generated inputs and crash artifacts are ignored.
