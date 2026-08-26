use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub const ROUTE_LATENCY_BOUNDS_US: &[u64] = &[
    5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000,
    500_000, 1_000_000, 2_500_000, 5_000_000,
];

/// Lock-free, fixed-bucket latency histogram intended for hot-path observability.
/// Each observation increments one non-cumulative bucket. Snapshots turn those
/// buckets into Prometheus-compatible cumulative counts.
pub struct LatencyHistogram {
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    sum_ns: AtomicU64,
}

impl LatencyHistogram {
    pub fn new() -> Self {
        let buckets = (0..=ROUTE_LATENCY_BOUNDS_US.len())
            .map(|_| AtomicU64::new(0))
            .collect();
        Self {
            buckets,
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn record(&self, duration: Duration) {
        let micros = duration.as_micros().min(u64::MAX as u128) as u64;
        let bucket = ROUTE_LATENCY_BOUNDS_US
            .iter()
            .position(|bound| micros <= *bound)
            .unwrap_or(ROUTE_LATENCY_BOUNDS_US.len());
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(
            duration.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub fn sum_seconds(&self) -> f64 {
        self.sum_ns.load(Ordering::Relaxed) as f64 / 1_000_000_000.0
    }

    pub fn cumulative_buckets(&self) -> Vec<(Option<u64>, u64)> {
        let mut cumulative = 0_u64;
        self.buckets
            .iter()
            .enumerate()
            .map(|(index, bucket)| {
                cumulative = cumulative.saturating_add(bucket.load(Ordering::Relaxed));
                let bound = ROUTE_LATENCY_BOUNDS_US.get(index).copied();
                (bound, cumulative)
            })
            .collect()
    }

    /// Approximate quantile using the upper bound of the fixed bucket in which
    /// the quantile falls. Overflow observations are reported as the final bound.
    pub fn quantile_seconds(&self, quantile: f64) -> f64 {
        let count = self.count();
        if count == 0 {
            return 0.0;
        }
        let target = ((count as f64 * quantile.clamp(0.0, 1.0)).ceil() as u64).max(1);
        let mut cumulative = 0_u64;
        for (index, bucket) in self.buckets.iter().enumerate() {
            cumulative = cumulative.saturating_add(bucket.load(Ordering::Relaxed));
            if cumulative >= target {
                let micros = ROUTE_LATENCY_BOUNDS_US
                    .get(index)
                    .copied()
                    .unwrap_or_else(|| *ROUTE_LATENCY_BOUNDS_US.last().unwrap());
                return micros as f64 / 1_000_000.0;
            }
        }
        *ROUTE_LATENCY_BOUNDS_US.last().unwrap() as f64 / 1_000_000.0
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_use_bucket_upper_bounds() {
        let histogram = LatencyHistogram::new();
        for _ in 0..50 {
            histogram.record(Duration::from_micros(8));
        }
        for _ in 0..45 {
            histogram.record(Duration::from_micros(80));
        }
        for _ in 0..5 {
            histogram.record(Duration::from_micros(800));
        }
        assert_eq!(histogram.count(), 100);
        assert_eq!(histogram.quantile_seconds(0.50), 0.000_010);
        assert_eq!(histogram.quantile_seconds(0.95), 0.000_100);
        assert_eq!(histogram.quantile_seconds(0.99), 0.001_000);
    }
}
