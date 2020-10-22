#[cfg(feature = "async-ng")]
pub use azuqua_core_async::ng::*;

#[cfg(not(feature = "async-ng"))]
pub use azuqua_core_async::legacy::*;
