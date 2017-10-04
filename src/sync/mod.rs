
mod utils;
mod commands;

/// A remote interface for a `RedisClient` that takes ownership over `self` on each command,
/// ideal for chaining commands together.
pub mod owned;

/// A remote interface for a `RedisClient` that borrows `self` on each command. This is ideal for
/// use within a wrapping struct that manages ownership over inner values.
pub mod borrowed;