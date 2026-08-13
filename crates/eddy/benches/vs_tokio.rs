use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;

fn spawn_join_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("spawn-join");
    for n in [1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::new("eddy", n), &n, |b, &n| {
            b.iter(|| {
                let runtime = eddy::Builder::new_current_thread().build();
                runtime.block_on(async {
                    let mut set = eddy::future::JoinSet::new();
                    let handle = eddy::Handle::current();
                    for i in 0..n {
                        set.spawn(&handle, async move { i });
                    }
                    let mut sum = 0usize;
                    while let Some(Ok(v)) = set.join_next().await {
                        sum += v;
                    }
                    black_box(sum);
                })
            })
        });
    }
    group.finish();
}

criterion_group!(benches, spawn_join_throughput);
criterion_main!(benches);
