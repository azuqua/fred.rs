

macro_rules! flame_start(
  ($($arg:tt)*) => {
    ::flame::start_guard($($arg)*)
  }
);