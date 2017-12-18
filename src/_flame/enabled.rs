

macro_rules! flame_start(
  ($($arg:tt)*) => { {
    ::flame::start($($arg)*)
  } }
);

macro_rules! flame_end(
  ($($arg:tt)*) => { {
    ::flame::end($($arg)*)
  } }
);