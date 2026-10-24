//! Command-line configuration, independent of the network runtime.

use std::ffi::OsString;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr};

/// Loopback is the default; non-loopback access requires explicit opt-in.
pub const DEFAULT_BIND: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// The Redis-compatible default listening port.
pub const DEFAULT_PORT: u16 = 6379;

/// Server configuration. Port zero requests an OS-assigned port.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Config {
    pub bind: IpAddr,
    pub port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: DEFAULT_BIND,
            port: DEFAULT_PORT,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Cli {
    Help,
    Serve(Config),
}

/// Argument errors deliberately omit raw argument values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    UnknownArgument,
    MissingBind,
    InvalidBind,
    DuplicateBind,
    MissingPort,
    InvalidPort,
    DuplicatePort,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnknownArgument => {
                "unknown argument; supported options are --bind, --port, and --help"
            }
            Self::MissingBind => "--bind requires a value",
            Self::InvalidBind => "--bind must be an IPv4 or IPv6 address (no hostname or port)",
            Self::DuplicateBind => "--bind may only be specified once",
            Self::MissingPort => "--port requires a value",
            Self::InvalidPort => "--port must be an integer from 0 through 65535",
            Self::DuplicatePort => "--port may only be specified once",
        })
    }
}

impl std::error::Error for ConfigError {}

/// Parse arguments excluding the executable name. Help short-circuits parsing.
/// Options require separate values: `--bind IP` and `--port NUMBER`.
pub fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Cli, ConfigError> {
    let mut args = args.into_iter();
    let mut bind = None;
    let mut port = None;
    while let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            return Ok(Cli::Help);
        }
        if arg == "--bind" {
            if bind.is_some() {
                return Err(ConfigError::DuplicateBind);
            }
            let value = args.next().ok_or(ConfigError::MissingBind)?;
            let text = value.to_str().ok_or(ConfigError::InvalidBind)?;
            bind = Some(
                text.parse::<IpAddr>()
                    .map_err(|_| ConfigError::InvalidBind)?,
            );
            continue;
        }
        if arg != "--port" {
            return Err(ConfigError::UnknownArgument);
        }
        if port.is_some() {
            return Err(ConfigError::DuplicatePort);
        }
        let value = args.next().ok_or(ConfigError::MissingPort)?;
        let text = value.to_str().ok_or(ConfigError::InvalidPort)?;
        if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(ConfigError::InvalidPort);
        }
        port = Some(text.parse::<u16>().map_err(|_| ConfigError::InvalidPort)?);
    }

    Ok(Cli::Serve(Config {
        bind: bind.unwrap_or(DEFAULT_BIND),
        port: port.unwrap_or(DEFAULT_PORT),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Cli, ConfigError> {
        parse_args(args.iter().map(OsString::from))
    }

    #[test]
    fn default_port_and_explicit_boundaries() {
        assert_eq!(Config::default().bind, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(Config::default().port, 6379);
        assert_eq!(parse(&[]), Ok(Cli::Serve(Config::default())));
        for (text, port) in [("0", 0), ("1", 1), ("6380", 6380), ("65535", 65535)] {
            assert_eq!(
                parse(&["--port", text]),
                Ok(Cli::Serve(Config {
                    port,
                    ..Config::default()
                }))
            );
        }
    }

    #[test]
    fn bind_accepts_ip_literals_and_composes_with_port_in_either_order() {
        for value in [
            "127.0.0.1",
            "192.168.1.10",
            "0.0.0.0",
            "::1",
            "::",
            "2001:db8::10",
        ] {
            let bind = value.parse::<IpAddr>().unwrap();
            assert_eq!(
                parse(&["--bind", value]),
                Ok(Cli::Serve(Config {
                    bind,
                    port: DEFAULT_PORT
                }))
            );
            for port in ["0", "6380", "65535"] {
                let expected = Ok(Cli::Serve(Config {
                    bind,
                    port: port.parse().unwrap(),
                }));
                assert_eq!(parse(&["--bind", value, "--port", port]), expected);
                assert_eq!(parse(&["--port", port, "--bind", value]), expected);
            }
        }
    }

    #[test]
    fn invalid_bind_values_are_rejected() {
        for value in [
            "",
            "localhost",
            "*",
            "127.0.0.1:6379",
            "[::1]",
            "[::1]:6379",
            "fe80::1%eth0",
            "256.0.0.1",
            "127.1",
            "127.0.0.01",
            "::gg",
            " 127.0.0.1",
            "127.0.0.1 ",
            "--port",
        ] {
            assert_eq!(parse(&["--bind", value]), Err(ConfigError::InvalidBind));
        }
    }

    #[test]
    fn invalid_port_values_are_rejected() {
        for value in [
            "",
            "-1",
            "+1",
            "65536",
            "99999999999999999999",
            "1.5",
            " 1",
            "1 ",
            "１",
        ] {
            assert_eq!(parse(&["--port", value]), Err(ConfigError::InvalidPort));
        }
    }

    #[test]
    fn invalid_argument_shapes_are_rejected() {
        assert_eq!(parse(&["--bind"]), Err(ConfigError::MissingBind));
        assert_eq!(
            parse(&["--bind", "127.0.0.1", "--bind", "::1"]),
            Err(ConfigError::DuplicateBind)
        );
        assert_eq!(parse(&["--port"]), Err(ConfigError::MissingPort));
        assert_eq!(
            parse(&["--port", "1", "--port", "2"]),
            Err(ConfigError::DuplicatePort)
        );
        for arg in ["--unknown", "6379", "--port=6380", "--bind=127.0.0.1"] {
            assert_eq!(parse(&[arg]), Err(ConfigError::UnknownArgument));
        }
    }

    #[test]
    fn help_is_explicit_and_short_circuits() {
        for args in [
            vec!["--help"],
            vec!["-h"],
            vec!["--help", "--unknown"],
            vec!["--bind", "::1", "--help", "--unknown"],
        ] {
            assert_eq!(parse(&args), Ok(Cli::Help));
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_arguments_are_errors_not_panics() {
        use std::os::unix::ffi::OsStringExt;

        let invalid = OsString::from_vec(vec![0xff]);
        assert_eq!(
            parse_args([invalid.clone()]),
            Err(ConfigError::UnknownArgument)
        );
        assert_eq!(
            parse_args([OsString::from("--port"), invalid.clone()]),
            Err(ConfigError::InvalidPort)
        );
        assert_eq!(
            parse_args([OsString::from("--bind"), invalid]),
            Err(ConfigError::InvalidBind)
        );
    }
}
