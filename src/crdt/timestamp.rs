use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::crdt::operation::Timestamp;

/// Returns the current time in nanoseconds since the Unix epoch.
fn current_timestamp_nanos() -> Timestamp {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

static LAST_TIMESTAMP: AtomicU64 = AtomicU64::new(0);

/// Generates a monotonically increasing timestamp.
pub fn next_monotonic_timestamp() -> Timestamp {
    let mut ts = current_timestamp_nanos();

    loop {
        let last = LAST_TIMESTAMP.load(Ordering::Relaxed);
        if ts > last {
            match LAST_TIMESTAMP.compare_exchange(last, ts, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => return ts,
                Err(_) => ts = current_timestamp_nanos(),
            }
        } else {
            let candidate = last.wrapping_add(1);
            if candidate == 0 {
                ts = current_timestamp_nanos();
                continue;
            }
            match LAST_TIMESTAMP.compare_exchange(
                last,
                candidate,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return candidate,
                Err(_) => ts = current_timestamp_nanos(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::next_monotonic_timestamp;

    #[test]
    fn timestamps_are_monotonic() {
        let a = next_monotonic_timestamp();
        let b = next_monotonic_timestamp();
        let c = next_monotonic_timestamp();

        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn handles_same_tick() {
        let mut last = next_monotonic_timestamp();
        for _ in 0..100 {
            let current = next_monotonic_timestamp();
            assert!(current > last);
            last = current;
        }
    }
}
