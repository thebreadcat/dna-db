//! Stage 1 encode benchmarks (Criterion). Run from `engine/`:
//! `cargo bench --bench stage1_encode`

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dnadb_engine::encoding::{
    encode_bytes_to_codons, encode_bytes_to_codons_avx2, encode_bytes_to_codons_scalar,
};

fn bench_encode_variants(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_bytes_to_codons");
    for size in [32, 64, 256, 4096, 65_536] {
        let data: Vec<u8> = (0..size)
            .map(|i: usize| (i.wrapping_mul(251)) as u8)
            .collect();
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("default", size), &data, |b, d| {
            b.iter(|| encode_bytes_to_codons(black_box(d.as_slice())));
        });
        group.bench_with_input(BenchmarkId::new("scalar", size), &data, |b, d| {
            b.iter(|| encode_bytes_to_codons_scalar(black_box(d.as_slice())));
        });
        group.bench_with_input(BenchmarkId::new("avx2_or_scalar", size), &data, |b, d| {
            b.iter(|| encode_bytes_to_codons_avx2(black_box(d.as_slice())));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_encode_variants);
criterion_main!(benches);
