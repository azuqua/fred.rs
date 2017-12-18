
#[cfg(feature="enable-flame")]
#[macro_use]
pub mod enabled;

#[cfg(not(feature="enable-flame"))]
#[macro_use]
pub mod disabled;