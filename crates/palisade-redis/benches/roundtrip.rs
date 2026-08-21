//! Criterion benchmarks for the Redis backend (requires a live Redis).
//!
//! Run: cargo bench -p palisade-redis

use std::time::Duration;

use criterion::{Criterion, Throughput};
use palisade_core::LockHandle;
use palisade_redis::{RedisConfig, RedisLockManager};

fn url() -> String {
    std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

fn bench_acquire_release(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mgr = rt
        .block_on(RedisLockManager::connect(RedisConfig::new(url())))
        .expect("bench requires a live redis");

    let mut group = c.benchmark_group("palisade_redis");
    group.throughput(Throughput::Elements(1));
    group.sample_size(200);

    group.bench_function("acquire_release", |b| {
        b.iter(|| {
            rt.block_on(async {
                let h = mgr.try_lock("palisade-bench:key").await.expect("grant");
                h.release().await.expect("release");
            });
        });
    });

    group.bench_function("try_lock_held_fast_fail", |b| {
        let _holder = rt
            .block_on(mgr.try_lock("palisade-bench:held"))
            .expect("holder");
        b.iter(|| {
            rt.block_on(async {
                let _ = mgr.try_lock("palisade-bench:held").await;
            });
        });
    });

    group.bench_function("extend", |b| {
        let h = rt.block_on(async {
            mgr.try_lock_with(
                "palisade-bench:ext",
                &palisade_core::LockOptions::default().with_ttl(Duration::from_secs(60)),
            )
            .await
            .expect("grant")
        });
        b.iter(|| {
            rt.block_on(async {
                h.extend(Duration::from_secs(60)).await.expect("extend");
            });
        });
        rt.block_on(h.release()).expect("cleanup");
    });

    group.finish();
}

criterion::criterion_group!(benches, bench_acquire_release);
criterion::criterion_main!(benches);
