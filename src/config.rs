//! Command-line configuration, independent of the network runtime.

use std::ffi::OsString;
use std::fmt;

/// The Redis-compatible default listening port.
pub const DEFAULT_PORT: u16 = 6379;

/// Server configuration. Port zero requests an OS-assigned port. The bind
/// address is loopback only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Config {
    pub port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self { port: DEFAULT_PORT }
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
    MissingPort,
    InvalidPort,
    DuplicatePort,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnknownArgument => "unknown argument; supported options are --port and --help",
            Self::MissingPort => "--port requires a value",
            Self::InvalidPort => "--port must be an integer from 0 through 65535",
            Self::DuplicatePort => "--port may only be specified once",
        })
    }
}

impl std::error::Error for ConfigError {}

/// Parse arguments excluding the executable name. Help short-circuits parsing.
/// Only the separate `--port NUMBER` form is accepted.
pub fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Cli, ConfigError> {
    let mut args = args.into_iter();
    let mut port = None;
    while let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            return Ok(Cli::Help);
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
        assert_eq!(Config::default().port, 6379);
        assert_eq!(parse(&[]), Ok(Cli::Serve(Config::default())));
        for (text, port) in [("0", 0), ("1", 1), ("6380", 6380), ("65535", 65535)] {
            assert_eq!(parse(&["--port", text]), Ok(Cli::Serve(Config { port })));
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
        assert_eq!(parse(&["--port"]), Err(ConfigError::MissingPort));
        assert_eq!(
            parse(&["--port", "1", "--port", "2"]),
            Err(ConfigError::DuplicatePort)
        );
        for arg in ["--unknown", "6379", "--port=6380"] {
            assert_eq!(parse(&[arg]), Err(ConfigError::UnknownArgument));
        }
    }

    #[test]
    fn help_is_explicit_and_short_circuits() {
        for args in [vec!["--help"], vec!["-h"], vec!["--help", "--unknown"]] {
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
            parse_args([OsString::from("--port"), invalid]),
            Err(ConfigError::InvalidPort)
        );
    }
}
