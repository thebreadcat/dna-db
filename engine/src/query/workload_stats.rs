//! Lightweight counters for observability (Phase 1 “adaptive layer” stub).
//!
//! Full workload profiling (co-access matrices, circadian phases) is deferred; this module
//! provides stable hooks for gates and benchmarks.

use std::sync::atomic::{AtomicU64, Ordering};

static FETCH_CALLS: AtomicU64 = AtomicU64::new(0);
static FETCH_STREAM_CALLS: AtomicU64 = AtomicU64::new(0);

#[inline]
pub fn record_fetch() {
    FETCH_CALLS.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_fetch_stream() {
    FETCH_STREAM_CALLS.fetch_add(1, Ordering::Relaxed);
}

pub fn snapshot() -> WorkloadSnapshot {
    WorkloadSnapshot {
        fetch_calls: FETCH_CALLS.load(Ordering::Relaxed),
        fetch_stream_calls: FETCH_STREAM_CALLS.load(Ordering::Relaxed),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkloadSnapshot {
    pub fetch_calls: u64,
    pub fetch_stream_calls: u64,
}
