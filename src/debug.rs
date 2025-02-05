#[macro_export]
macro_rules! debug_println {
    ($($arg:tt)*) => {
        #[cfg(feature = "extra_verbose_debug_logging")]
        println!("{:?}: {}", std::thread::current().id(), format!($($arg)*));
    };
}
