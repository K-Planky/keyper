//! Building blocks for Keyper's in-memory Redis-compatible service.
//!
//! Only configuration is implemented so far. Transport and database operations
//! will live in the library; the executable owns arguments, diagnostics, and exit
//! status. No API currently starts a listener.

pub mod config;
