pub mod clock;
pub mod config;
pub mod engine;
pub mod media;
pub mod network;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn version() -> String {
    format!("weft-core v{VERSION}")
}
