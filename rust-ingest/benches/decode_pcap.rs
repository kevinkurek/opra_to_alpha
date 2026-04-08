use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "../src/opra_decoder.rs"]
mod opra_decoder;

/// Find PCAP files for benchmarking (env override first, then known sample files).
fn resolve_pcap_paths() -> Vec<(String, PathBuf)> {
    let from_env = std::env::var("OPRA_BENCH_PCAP").ok().map(PathBuf::from);
    if let Some(path) = from_env {
        if path.exists() {
            return vec![("env".to_string(), path)];
        }
    }

    let candidates = [
        ("10k", PathBuf::from("pcap_samples/ny4-small-10k.pcap")),
        ("1m", PathBuf::from("pcap_samples/ny4-small-1m.pcap")),
        ("10m", PathBuf::from("pcap_samples/ny4-small-10m.pcap")),
    ];

    candidates
        .into_iter()
        .filter_map(|(label, path)| {
            if path.exists() {
                Some((label.to_string(), path))
            } else {
                None
            }
        })
        .collect()
}

/// Read one benchmark PCAP into memory.
fn load_pcap(path: &Path) -> Option<Vec<u8>> {
    fs::read(path).ok()
}

/// Criterion benchmark for header decode throughput across available sample sizes.
fn decode_bench(c: &mut Criterion) {
    let pcap_paths = resolve_pcap_paths();
    if pcap_paths.is_empty() {
        eprintln!(
            "Skipping decode benchmark: no PCAP found. Set OPRA_BENCH_PCAP=/absolute/path/to/file.pcap"
        );
        return;
    }

    let rayon_threads = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);

    for (label, pcap_path) in pcap_paths {
        let Some(pcap_bytes) = load_pcap(&pcap_path) else {
            eprintln!(
                "Skipping decode benchmark for {}: failed to read PCAP at {}",
                label,
                pcap_path.display()
            );
            continue;
        };

        let mut group = c.benchmark_group(format!("decode_pcap/{label}"));
        group.throughput(Throughput::Bytes(pcap_bytes.len() as u64));

        group.bench_function(
            BenchmarkId::new("rayon", format!("threads={rayon_threads}")),
            |b| {
                b.iter(|| {
                    let result = opra_decoder::decode_pcap_headers_schema(
                        black_box(&pcap_bytes),
                        rayon_threads,
                    );
                    if result.is_err() {
                        std::hint::black_box(result.err());
                    }
                });
            },
        );

        group.finish();
    }
}

criterion_group!(benches, decode_bench);
criterion_main!(benches);
