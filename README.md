# Keyper

A small, in-memory Redis-compatible service being built in Rust on Tokio.

**Current state: project setup only.** Configuration parsing, a two-worker Tokio
entry point, and development checks exist. There is **no TCP listener, RESP
transport, database, or implemented Redis command** yet. This version cannot be
used with redis-cli or the crawler. Startup deliberately fails rather than
pretending to serve requests.

## Prerequisites and build

Install Rust **1.99.0**, including rustfmt and Clippy, with
[rustup](https://rustup.rs/):

```sh
rustup toolchain install 1.99.0 --profile minimal --component rustfmt --component clippy
```

A platform linker is also required (for example, the system C toolchain on Linux).
The checked-in `rust-toolchain.toml` selects this toolchain automatically.
`Cargo.lock` is tracked; use `--locked` to avoid unintended dependency updates.
The first build needs crates.io access unless the locked dependencies are cached.

From the project directory:

```sh
cargo build --locked
cargo run --locked -- --help
```

The only direct dependency is Tokio **1.53.1**, pinned exactly, with
`default-features = false`. Only `macros` and `rt-multi-thread` are enabled for
the entry point; network, I/O, synchronization, and timer features will be added
when used. No Redis installation, sibling repository, Lua toolchain, or local
planning documents are needed for the basic build/tests.

## Configuration boundary

```sh
cargo run --locked -- --port 6380
```

**This currently exits with status 1** and explains that no listener was started.
It is not a working server-start command yet.

- No arguments selects port **6379**.
- `--port NUMBER` accepts decimal integers from 0 through 65535. Port 0 is
  intended to request an OS-assigned port when networking is added.
- The intended bind address is **127.0.0.1**; there is no bind-address option yet.
- Missing, invalid, repeated, or unknown options exit with status **2**.
  Only the separate `--port NUMBER` spelling is accepted.
- `-h` / `--help` prints help and exits successfully, short-circuiting parsing.

This is intended for trusted local use, not exposure to an untrusted network.
No bind or occupied-port behavior has been implemented or verified yet.

## Development checks

Run all four gates after code changes:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
cargo build --locked
```

Use `cargo fmt --all` to apply formatting. Keep Clippy warnings actionable rather
than suppressing them. Configuration unit tests cover defaults, port boundaries,
argument errors, and non-UTF-8 arguments on Unix. Process tests check help and
honest startup/configuration failures, with bounded waits and child cleanup.
These are **scaffolding checks**, not protocol, concurrency, or crawler coverage.

### Layout and conventions

- `src/lib.rs`: testable service-library boundary.
- `src/config.rs`: configuration types and argument validation, without network I/O.
- `src/main.rs`: arguments, Tokio entry point, diagnostics, and exit status.
- `tests/cli.rs`: executable-level tests.

Use focused modules as behavior arrives: framing, command validation, state
ownership, and connection lifecycle should remain distinguishable. Return normal
client/configuration errors instead of panicking, and avoid logging raw request
payloads. Keep tests self-contained; future socket tests should use owned servers,
ephemeral ports, bounded waits, and cleanup. Keep build artifacts out of Git and
retain the application lockfile.

## Intended service contracts (not implemented)

The target is RESP2 with sixteen independent databases (0–15), database 0 by
default, and SELECT allowed only as the first complete command attempt. Core
commands will be PING, SELECT, GET, bare SET, EXISTS, LPUSH, RPUSH, BLPOP, and BRPOP;
blocking pops accept one to five keys and a double-precision timeout.

The planned linearizability boundary is one dedicated state-owner thread for all
databases and waiters, separate from Tokio networking. A command, transaction, or
Lua script runs as one non-interleaved state unit. Blocking pops atomically check
and register, then release the owner while waiting; they recheck keys left to
right after complete atomic units, with one terminal outcome per waiter. No state
ownership may span socket I/O or a blocking wait. These are design commitments,
**not a verified implementation or proof**. PING is excluded from linearizability
histories.

Memory is lost on stop; there is no persistence or crash recovery. Owner failure
must stop the service, not silently replace state. A lost response may follow an
applied write; no automatic rollback or retry is promised.

Later work includes ordered pipelining, audited extra commands, genuine Lua 5.1,
and MULTI/EXEC/DISCARD for an **unmodified** external crawler. Crawler sources and
Git state must not be changed; compatibility code and harnesses belong here.
Neither crawler interoperability nor any live demo has been verified. Broad
Redis compatibility, RESP3, authentication, pub/sub, and public-network hardening
are not claimed.
