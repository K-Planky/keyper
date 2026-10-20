//! Building blocks for Keyper's in-memory Redis-compatible service.
//!
//! RESP2 framing, command/session validation, exclusive state ownership, and
//! supervised TCP I/O are separate from executable argument/signal handling.

mod command;
pub mod config;
pub mod resp;
pub mod server;
mod state;
