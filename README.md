# gcra-lite

[![ci](https://github.com/Urthella/gcra-lite/actions/workflows/ci.yml/badge.svg)](https://github.com/Urthella/gcra-lite/actions/workflows/ci.yml)

A tiny, dependency-free rate limiter for Rust built on **GCRA** (the Generic Cell Rate Algorithm), the algorithm behind [redis-cell](https://github.com/brandur/redis-cell).

- **Lock-free**: one `AtomicU64` per limiter, every check is a single compare-and-swap
- **No refill task**: GCRA stores one timestamp, not a token count, so time does the refilling
- **HTTP-ready answers**: `remaining` and `reset_after` on success, exact `retry_after` on denial
- **Weighted requests**: `check_n(n)` takes several cells atomically, or none
- **Zero dependencies**, `std` only

## Use

```rust
use gcra_lite::{Quota, RateLimiter};

let limiter = RateLimiter::new(Quota::per_second(5)); // burst of 5, one more every 200ms

match limiter.check() {
    Ok(ok) => println!("allowed, {} left", ok.remaining),
    Err(denied) => println!("slow down, retry in {:?}", denied.retry_after),
}
```

## How GCRA works

Instead of a bucket of tokens, GCRA keeps one value: the **theoretical arrival time** (TAT), the moment the limiter would be "caught up" if requests arrived exactly at the allowed rate.

```text
T   = period / burst       time one request costs (emission interval)
tau = T * burst            how far TAT may run ahead of now (burst tolerance)

new_tat = max(tat, now) + n * T
allow if new_tat - now <= tau
  remaining   = (tau - (new_tat - now)) / T
  reset_after = new_tat - now
else
  retry_after = (new_tat - now) - tau
```

With `Quota::per_second(5)`: `T = 200ms`, `tau = 1s`. Five requests at once push TAT 1s ahead; the sixth would push it to 1.2s, so it's denied with `retry_after = 200ms`.

Because the whole state is one number, the update is a CAS loop on an `AtomicU64`: no mutex, and no background thread topping up tokens.

## Develop

```bash
cargo test
```

`nix develop` (or direnv) gives you the same Rust toolchain. CI runs fmt, clippy and tests on Linux, macOS and Windows.

## License

[MIT](LICENSE)
