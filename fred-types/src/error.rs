use std::error::Error;
use std::fmt;
use std::io::Error as IoError;


use std::num::ParseFloatError;
use std::num::ParseIntError;
use std::string::FromUtf8Error;

#[cfg(feature = "legacy")]
use url::ParseError;

#[cfg(feature = "legacy")]
use futures::sync::mpsc::SendError;
#[cfg(feature = "legacy")]
use futures::Canceled;
#[cfg(feature = "legacy")]
use redis_protocol::types::RedisProtocolError;
#[cfg(feature = "legacy")]
use redis_protocol::types::Frame;
#[cfg(feature = "legacy")]
use tokio_timer_patched::TimerError;

/// An enum representing the type of error from Redis.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RedisErrorKind {
    /// An authentication error.
    Auth,
    /// An IO error with the underlying connection.
    IO,
    /// An invalid command, such as trying to perform a `set` command on a client after calling `subscribe`.
    InvalidCommand,
    /// An invalid argument or set of arguments to a command.
    InvalidArgument,
    /// An invalid URL error.
    UrlError,
    /// A protocol error.
    ProtocolError,
    /// An error indicating the request was canceled.
    Canceled,
    /// An unknown error.
    Unknown,
    /// A timeout error.
    Timeout,
    /// An internal error used to indicate that the cluster's state has changed.
    #[doc(hidden)]
    Cluster,
}

impl RedisErrorKind {
    pub fn to_str(&self) -> &'static str {
        match *self {
            RedisErrorKind::Auth => "Authentication Error",
            RedisErrorKind::IO => "IO Error",
            RedisErrorKind::InvalidArgument => "Invalid Argument",
            RedisErrorKind::InvalidCommand => "Invalid Command",
            RedisErrorKind::UrlError => "Url Error",
            RedisErrorKind::ProtocolError => "Protocol Error",
            RedisErrorKind::Unknown => "Unknown Error",
            RedisErrorKind::Canceled => "Canceled",
            RedisErrorKind::Cluster => "Cluster Error",
            RedisErrorKind::Timeout => "Timeout Error",
        }
    }
}

/// A struct representing an error from Redis.
#[derive(Clone, Eq, PartialEq)]
pub struct RedisError {
    /// Details about the specific error condition.
    details: String,
    /// The kind of error.
    kind: RedisErrorKind,
}

impl fmt::Debug for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "[Redis Error - kind: {:?}, desc: {}, details: {}]",
            self.kind,
            self.kind.to_str(),
            self.details
        )
    }
}

#[cfg(feature = "legacy")]
impl<'a> From<RedisProtocolError<'a>> for RedisError {
    fn from(e: RedisProtocolError) -> Self {
        RedisError::new(RedisErrorKind::ProtocolError, format!("{}", e))
    }
}

impl From<()> for RedisError {
    fn from(_: ()) -> Self {
        RedisError::new(RedisErrorKind::Canceled, "Empty error.")
    }
}

#[cfg(feature = "legacy")]
impl<T: Into<RedisError>> From<SendError<T>> for RedisError {
    fn from(e: SendError<T>) -> Self {
        RedisError::new(RedisErrorKind::Unknown, format!("{}", e))
    }
}

#[cfg(feature = "legacy")]
impl From<TimerError> for RedisError {
    fn from(e: TimerError) -> Self {
        RedisError::new(RedisErrorKind::Unknown, format!("{:?}", e))
    }
}

impl From<IoError> for RedisError {
    fn from(e: IoError) -> Self {
        RedisError::new(RedisErrorKind::IO, format!("{:?}", e))
    }
}

#[cfg(feature = "legacy")]
impl From<ParseError> for RedisError {
    fn from(e: ParseError) -> Self {
        RedisError::new(RedisErrorKind::UrlError, format!("{:?}", e))
    }
}

impl From<ParseFloatError> for RedisError {
    fn from(_: ParseFloatError) -> Self {
        RedisError::new(RedisErrorKind::Unknown, "Invalid floating point number.")
    }
}

impl From<ParseIntError> for RedisError {
    fn from(_: ParseIntError) -> Self {
        RedisError::new(RedisErrorKind::Unknown, "Invalid integer string.")
    }
}

impl From<FromUtf8Error> for RedisError {
    fn from(_: FromUtf8Error) -> Self {
        RedisError::new(RedisErrorKind::Unknown, "Invalid UTF8 string.")
    }
}

impl From<fmt::Error> for RedisError {
    fn from(e: fmt::Error) -> Self {
        RedisError::new(RedisErrorKind::Unknown, format!("{:?}", e))
    }
}

#[cfg(feature = "legacy")]
impl From<Canceled> for RedisError {
    fn from(e: Canceled) -> Self {
        RedisError::new(RedisErrorKind::Canceled, format!("{}", e))
    }
}


#[cfg(feature = "legacy")]
impl From<Frame> for RedisError {
    fn from(e: Frame) -> Self {
        match e {
            Frame::SimpleString(s) => match s.as_ref() {
                "Canceled" => RedisError::new_canceled(),
                _ => RedisError::new(RedisErrorKind::Unknown, "Unknown frame error."),
            },
            _ => RedisError::new(RedisErrorKind::Unknown, "Unknown frame error."),
        }
    }
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.kind.to_str(), self.details)
    }
}

impl Error for RedisError {
    fn description(&self) -> &str {
        self.kind.to_str()
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

impl RedisError {
    pub fn new<T: Into<String>>(kind: RedisErrorKind, details: T) -> RedisError {
        RedisError {
            kind,
            details: details.into(),
        }
    }

    pub fn kind(&self) -> &RedisErrorKind {
        &self.kind
    }

    pub fn details(&self) -> &str {
        &self.details
    }

    pub fn to_string(&self) -> String {
        format!("{}: {}", &self.kind.to_str(), &self.details)
    }

    pub fn new_canceled() -> RedisError {
        RedisError::new(RedisErrorKind::Canceled, "Canceled.")
    }

    pub fn new_timeout() -> RedisError {
        RedisError::new(RedisErrorKind::Timeout, "")
    }

    pub fn is_canceled(&self) -> bool {
        match self.kind {
            RedisErrorKind::Canceled => true,
            _ => false,
        }
    }
}
