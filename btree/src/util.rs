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
