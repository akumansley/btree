/// Prefetch a node's memory into L1 cache, targeting the regions
/// accessed during binary search.
///
/// Node layout (storage starts shortly after the node pointer):
///   NodeHeader (height + lock) — needed for lock_optimistic
///   num_keys — checked first during search
///   heads array — primary search target, placed early for cache efficiency
///   keys array — touched on head-match fallback
///
/// We prefetch the first 1024 bytes (16 cache lines) which covers header,
/// num_keys, heads, and the start of the keys array.
#[inline(always)]
pub fn prefetch_node(ptr: *const u8) {
    #[cfg(not(miri))]
    unsafe {
        let mut off = 0;
        while off < 1024 {
            prefetch_line(ptr.add(off));
            off += 64;
        }
    }
    #[cfg(miri)]
    {
        let _ = ptr;
    }
}

#[cfg(not(miri))]
#[inline(always)]
unsafe fn prefetch_line(ptr: *const u8) {
    #[cfg(target_arch = "aarch64")]
    std::arch::asm!(
        "prfm pldl1keep, [{ptr}]",
        ptr = in(reg) ptr,
        options(readonly, nostack, preserves_flags)
    );
    #[cfg(target_arch = "x86_64")]
    std::arch::asm!(
        "prefetcht0 [{}]",
        in(reg) ptr,
        options(readonly, nostack, preserves_flags)
    );
}

pub trait UnwrapEither {
    type Item;
    fn unwrap_either(self) -> Self::Item;
}

impl<T> UnwrapEither for Result<T, T> {
    type Item = T;

    fn unwrap_either(self) -> T {
        match self {
            Ok(t) | Err(t) => t,
        }
    }
}

/// Runs a closure N times, returning the first Ok result or the last Err if all attempts fail.
/// The closure is passed the current attempt number (0-based).
pub fn retry<T, E, F, const NUM_TRIES: usize>(mut f: F) -> Result<T, E>
where
    F: FnMut() -> Result<T, E>,
{
    let mut last_err = None;
    for _ in 0..NUM_TRIES {
        match f() {
            Ok(val) => return Ok(val),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.expect("retry_with_count called with num_retries = 0"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_success_first_try() {
        let result = retry::<_, _, _, 3>(|| Ok::<_, &str>("success"));
        assert_eq!(result, Ok("success"));
    }

    #[test]
    fn test_retry_success_after_failure() {
        let mut attempts = 0;
        let result = retry::<_, _, _, 3>(|| {
            attempts += 1;
            if attempts < 2 {
                Err("failed")
            } else {
                Ok("success")
            }
        });
        assert_eq!(result, Ok("success"));
        assert_eq!(attempts, 2);
    }

    #[test]
    fn test_retry_all_failures() {
        let mut attempts = 0;
        let result = retry::<_, _, _, 3>(|| {
            attempts += 1;
            Err::<&str, _>("failed")
        });
        assert_eq!(result, Err("failed"));
        assert_eq!(attempts, 3);
    }

    #[test]
    #[should_panic(expected = "retry_with_count called with num_retries = 0")]
    fn test_retry_zero_attempts() {
        let _result: Result<(), ()> = retry::<_, _, _, 0>(|| Ok(()));
    }
}
