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
