//! Building blocks for Keyper's in-memory Redis-compatible service.
//!
//! RESP2 framing, PING validation, and the supervised TCP service are separate
//! from executable argument/signal handling. Database commands are not yet present.

pub mod command;
pub mod config;
pub mod resp;
pub mod server;
