use std::{env, hint::black_box, process, time::Duration};

use criterion::{BenchmarkId, Criterion, Throughput};
use pipewire_native_stress::benchmark::{cases, Profile};

fn main() {
    let profile_name = env::var("PW_CRITERION_PROFILE").unwrap_or_else(|_| "smoke".into());
    let profile = profile_name.parse::<Profile>().unwrap_or_else(|error| {
        eprintln!("error: {error}");
        process::exit(2);
    });

    // Keep local defaults short. Criterion CLI arguments remain authoritative.
    let mut criterion = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(250))
        .measurement_time(Duration::from_secs(1))
        .configure_from_args();

    for case in cases(profile) {
        let id = case.id();
        let prepared = case
            .prepare()
            .unwrap_or_else(|error| panic!("failed to prepare {}/{}: {error}", case.group(), id));
        let mut group = criterion.benchmark_group(case.group());
        group.throughput(Throughput::Elements(
            prepared
                .operations()
                .try_into()
                .expect("operation count does not fit u64"),
        ));
        group.bench_function(BenchmarkId::from_parameter(id), |b| {
            b.iter(|| {
                let verified = prepared
                    .execute_verify()
                    .expect("benchmark execution or verification failed");
                black_box(verified)
            });
        });
        group.finish();
    }
    criterion.final_summary();
}
