//! A tiny, dependency-free rate limiter built on GCRA (the Generic Cell Rate Algorithm).
//!
//! GCRA tracks a single timestamp per limiter, the *theoretical arrival time* (TAT),
//! instead of a token count plus a last-refill time. That makes every check one
//! compare-and-swap on an `AtomicU64`: no locks, no background refill task.
//!
//! ```
//! use gcra_lite::{Quota, RateLimiter};
//!
//! let limiter = RateLimiter::new(Quota::per_second(5));
//! for _ in 0..5 {
//!     assert!(limiter.check().is_ok());
//! }
//! let denied = limiter.check().unwrap_err();
//! assert!(denied.retry_after.as_millis() <= 200);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// How many requests are allowed: up to `burst` at once, refilling evenly over `period`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Quota {
    burst: u32,
    period: Duration,
}

impl Quota {
    /// # Panics
    /// If `burst` is zero, or `period` is too short to give each request at least 1ns.
    pub fn new(burst: u32, period: Duration) -> Self {
        assert!(burst > 0, "burst must be at least 1");
        assert!(
            period.as_nanos() >= u128::from(burst),
            "period too short for this burst"
        );
        Self { burst, period }
    }

    pub fn per_second(n: u32) -> Self {
        Self::new(n, Duration::from_secs(1))
    }

    pub fn per_minute(n: u32) -> Self {
        Self::new(n, Duration::from_secs(60))
    }

    pub fn per_hour(n: u32) -> Self {
        Self::new(n, Duration::from_secs(3600))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Allowed {
    /// Requests still allowed right now, after this one.
    pub remaining: u32,
    /// Time until the limiter is back to a full burst (useful for `RateLimit-Reset`).
    pub reset_after: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Denied {
    /// Wait this long before the same request would be allowed (`Retry-After`).
    /// `Duration::MAX` when the request costs more than the whole burst and can never fit.
    pub retry_after: Duration,
}

/// The GCRA decision on plain nanosecond timestamps, shared by every limiter type.
#[derive(Clone, Copy, Debug)]
struct Gcra {
    /// Emission interval: the time one request "costs".
    t: u64,
    /// Burst tolerance: how far ahead of now the TAT may run.
    tau: u64,
}

impl Gcra {
    fn new(quota: Quota) -> Self {
        let period = u64::try_from(quota.period.as_nanos()).expect("period over 584 years");
        let t = period / u64::from(quota.burst);
        Self {
            t,
            tau: t * u64::from(quota.burst),
        }
    }

    /// Returns the new TAT on success. `n = 0` peeks without consuming anything.
    fn decide(self, tat: u64, now: u64, n: u32) -> Result<(u64, Allowed), Denied> {
        let cost = self.t.saturating_mul(u64::from(n));
        if cost > self.tau {
            return Err(Denied {
                retry_after: Duration::MAX,
            });
        }
        let new_tat = tat.max(now) + cost;
        let ahead = new_tat - now;
        if ahead > self.tau {
            return Err(Denied {
                retry_after: Duration::from_nanos(ahead - self.tau),
            });
        }
        let allowed = Allowed {
            remaining: ((self.tau - ahead) / self.t) as u32,
            reset_after: Duration::from_nanos(ahead),
        };
        Ok((new_tat, allowed))
    }
}

/// A single lock-free limiter, e.g. one per process or per upstream API.
#[derive(Debug)]
pub struct RateLimiter {
    gcra: Gcra,
    epoch: Instant,
    /// TAT as nanoseconds since `epoch`.
    tat: AtomicU64,
}

impl RateLimiter {
    pub fn new(quota: Quota) -> Self {
        Self {
            gcra: Gcra::new(quota),
            epoch: Instant::now(),
            tat: AtomicU64::new(0),
        }
    }

    pub fn check(&self) -> Result<Allowed, Denied> {
        self.check_n_at(1, Instant::now())
    }

    /// Takes `n` requests' worth of quota at once (e.g. bytes or batch size), or none.
    pub fn check_n(&self, n: u32) -> Result<Allowed, Denied> {
        self.check_n_at(n, Instant::now())
    }

    /// Like [`check_n`](Self::check_n) with an explicit clock, for tests and simulations.
    pub fn check_n_at(&self, n: u32, now: Instant) -> Result<Allowed, Denied> {
        let now = nanos_since(self.epoch, now);
        let mut tat = self.tat.load(Ordering::Acquire);
        loop {
            let (new_tat, allowed) = self.gcra.decide(tat, now, n)?;
            match self
                .tat
                .compare_exchange_weak(tat, new_tat, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(allowed),
                Err(current) => tat = current,
            }
        }
    }
}

fn nanos_since(epoch: Instant, now: Instant) -> u64 {
    u64::try_from(now.saturating_duration_since(epoch).as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn allows_a_burst_then_denies_with_exact_retry_after() {
        let lim = RateLimiter::new(Quota::per_second(5));
        let t0 = Instant::now();
        let remaining: Vec<u32> = (0..5)
            .map(|_| lim.check_n_at(1, t0).unwrap().remaining)
            .collect();
        assert_eq!(remaining, [4, 3, 2, 1, 0]);
        assert_eq!(
            lim.check_n_at(1, t0),
            Err(Denied {
                retry_after: 200 * MS
            })
        );
        // Denials don't consume quota: the wait is still exactly one emission interval.
        assert_eq!(
            lim.check_n_at(1, t0 + 100 * MS),
            Err(Denied {
                retry_after: 100 * MS
            })
        );
    }

    #[test]
    fn refills_one_request_per_emission_interval() {
        let lim = RateLimiter::new(Quota::per_second(5));
        let t0 = Instant::now();
        for _ in 0..5 {
            lim.check_n_at(1, t0).unwrap();
        }
        assert_eq!(lim.check_n_at(1, t0 + 200 * MS).unwrap().remaining, 0);
        assert!(lim.check_n_at(1, t0 + 200 * MS).is_err());
        assert_eq!(
            lim.check_n_at(1, t0 + Duration::from_secs(10))
                .unwrap()
                .remaining,
            4
        );
    }

    #[test]
    fn reset_after_counts_down_to_a_full_burst() {
        let lim = RateLimiter::new(Quota::per_second(5));
        let t0 = Instant::now();
        assert_eq!(lim.check_n_at(5, t0).unwrap().reset_after, 1000 * MS);
        assert_eq!(
            lim.check_n_at(0, t0 + 400 * MS).unwrap().reset_after,
            600 * MS
        );
    }

    #[test]
    fn weighted_requests_take_several_cells_or_none() {
        let lim = RateLimiter::new(Quota::per_second(5));
        let t0 = Instant::now();
        assert_eq!(lim.check_n_at(3, t0).unwrap().remaining, 2);
        assert_eq!(
            lim.check_n_at(3, t0),
            Err(Denied {
                retry_after: 200 * MS
            })
        );
        assert_eq!(lim.check_n_at(2, t0).unwrap().remaining, 0);
    }

    #[test]
    fn a_request_bigger_than_the_burst_never_fits() {
        let lim = RateLimiter::new(Quota::per_second(5));
        assert_eq!(
            lim.check_n(6),
            Err(Denied {
                retry_after: Duration::MAX
            })
        );
        assert_eq!(lim.check().unwrap().remaining, 4, "and it consumed nothing");
    }

    #[test]
    fn zero_peeks_without_consuming() {
        let lim = RateLimiter::new(Quota::per_minute(10));
        let t0 = Instant::now();
        assert_eq!(lim.check_n_at(0, t0).unwrap().remaining, 10);
        assert_eq!(lim.check_n_at(0, t0).unwrap().remaining, 10);
    }

    #[test]
    fn concurrent_checks_never_over_admit() {
        // Hour-long period: no refill can happen during the test, so exactly `burst` may pass.
        let lim = Arc::new(RateLimiter::new(Quota::per_hour(100)));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let lim = Arc::clone(&lim);
                thread::spawn(move || (0..500).filter(|_| lim.check().is_ok()).count())
            })
            .collect();
        let allowed: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(allowed, 100);
    }

    #[test]
    #[should_panic(expected = "burst must be at least 1")]
    fn zero_burst_is_rejected() {
        Quota::per_second(0);
    }
}
