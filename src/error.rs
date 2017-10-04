

use std::io::Error as IoError;
use std::error::Error;
use std::fmt;

use url::ParseError;
use std::string::FromUtf8Error;
use std::num::ParseFloatError;
use std::num::ParseIntError;
use protocol::types::Frame;
use futures::sync::mpsc::SendError;
use futures::Canceled;
use tokio_timer::TimeoutError;
use tokio_timer::TimerError;

/// An enum representing the type of error from Redis.
#[derive(Debug)]
pub enum RedisErrorKind {
  /// An authentication error.
  Auth,
  /// An IO error with the underlying connection.
  IO(IoError),
  /// An invalid command, such as trying to perform a `set` command on a client after calling `subscribe`.
  InvalidCommand,
  /// An invalid argument or set of arguments to a command.
  InvalidArgument,
  /// An invalid URL error.
  UrlError(ParseError),
  /// A protocol error.
  ProtocolError,
  /// An error indicating the request was canceled.
  Canceled,
  /// An unknown error.
  Unknown,
  /// An internal error used to indicate that the cluster's state has changed.
  #[doc(hidden)]
  Cluster
}

impl PartialEq for RedisErrorKind {
  fn eq(&self, other: &RedisErrorKind) -> bool {
    match *self {
      RedisErrorKind::Auth => match *other {
        RedisErrorKind::Auth => true,
        _ => false
      },
      RedisErrorKind::IO(_) => match *other {
        RedisErrorKind::IO(_) => true,
        _ => false
      },
      RedisErrorKind::InvalidArgument => match *other {
        RedisErrorKind::InvalidArgument => true,
        _ => false
      },
      RedisErrorKind::InvalidCommand => match *other {
        RedisErrorKind::InvalidCommand => true,
        _ => false
      },
      RedisErrorKind::UrlError(_) => match *other {
        RedisErrorKind::UrlError(_) => true,
        _ => false
      },
      RedisErrorKind::ProtocolError => match *other {
        RedisErrorKind::ProtocolError => true,
        _ => false
      },
      RedisErrorKind::Unknown => match *other {
        RedisErrorKind::Unknown => true,
        _ => false
      },
      RedisErrorKind::Canceled => match *other {
        RedisErrorKind::Canceled => true,
        _ => false
      },
      RedisErrorKind::Cluster => match *other {
        RedisErrorKind::Cluster => true,
        _ => false
      }
    }
  }
}

impl Eq for RedisErrorKind {}

// See https://github.com/rust-lang/rust/issues/24135
impl Clone for RedisErrorKind {
  fn clone(&self) -> Self {
    match *self {
      RedisErrorKind::Auth            => RedisErrorKind::Auth,
      RedisErrorKind::InvalidArgument => RedisErrorKind::InvalidArgument,
      RedisErrorKind::InvalidCommand  => RedisErrorKind::InvalidCommand,
      RedisErrorKind::UrlError(ref e) => RedisErrorKind::UrlError(e.clone()),
      RedisErrorKind::ProtocolError   => RedisErrorKind::ProtocolError,
      RedisErrorKind::Unknown         => RedisErrorKind::Unknown,
      RedisErrorKind::Canceled        => RedisErrorKind::Canceled,
      RedisErrorKind::Cluster         => RedisErrorKind::Cluster,
      RedisErrorKind::IO(ref e)       => {
        match e.raw_os_error() {
          Some(c) => RedisErrorKind::IO(IoError::from_raw_os_error(c)),
          None => RedisErrorKind::IO(IoError::new(
            e.kind(), format!("{}", e)
          ))
        }
      }
    }
  }
}

/// A struct representing an error from Redis. 
#[derive(Clone)]
pub struct RedisError {
  /// A high level description of the error.
  desc: &'static str,
  /// Details about the specific error condition.
  details: String,
  /// The kind of error.
  kind: RedisErrorKind
}

impl fmt::Debug for RedisError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[Redis Error - kind: {:?}, desc: {}, details: {}]", self.kind, self.desc, self.details)
  }
}

impl PartialEq for RedisError {
  fn eq(&self, other: &Self) -> bool {
    self.kind == other.kind && self.desc == other.desc && self.details == other.details
  }
}

impl Eq for RedisError {}

impl From<()> for RedisError {
  fn from(_: ()) -> Self {
    RedisError::new(RedisErrorKind::Canceled, "Empty error.")
  }
}

impl<T: Into<RedisError>> From<SendError<T>> for RedisError {
  fn from(e: SendError<T>) -> Self {
    RedisError::new(RedisErrorKind::Unknown, format!("{}", e))
  }
}

impl From<IoError> for RedisError {
  fn from(e: IoError) -> Self {
    RedisError::new(RedisErrorKind::IO(e), "IO Error.")
  }
}

impl From<ParseError> for RedisError {
  fn from(e: ParseError) -> Self {
    RedisError::new(RedisErrorKind::UrlError(e), "Url Error.")
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
    RedisError::new(RedisErrorKind::Unknown, format!("Format error: {}", e))
  }
}

impl From<Canceled> for RedisError {
  fn from(e: Canceled) -> Self {
    RedisError::new(RedisErrorKind::Canceled, format!("{}", e))
  }
}

impl From<Frame> for RedisError {
  fn from(e: Frame) -> Self {
    match e {
      Frame::Canceled => RedisError::new(RedisErrorKind::Canceled, ""),
      _ => RedisError::new(RedisErrorKind::Unknown, "Unknown frame error.")
    }
  }
}

impl From<TimerError> for RedisError {
  fn from(e: TimerError) -> Self {
    RedisError::new(RedisErrorKind::Unknown, format!("Timer error: {}", e))
  }
}

impl<T: Into<RedisError>> From<TimeoutError<T>> for RedisError {
  fn from(e: TimeoutError<T>) -> Self {
    RedisError::new(RedisErrorKind::Unknown, format!("Timeout error: {}", e))
  }
}

impl fmt::Display for RedisError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self.cause() {
      Some(c) => write!(f, "{}: {} ({})", self.desc, &self.details, c),
      None => write!(f, "{}: {}", self.desc, &self.details)
    }
  }
}

impl Error for RedisError {
  
  fn description(&self) -> &str {
    self.desc
  }

  fn cause(&self) -> Option<&Error> {
    match self.kind {
      RedisErrorKind::IO(ref e)       => Some(e),
      RedisErrorKind::UrlError(ref e) => Some(e),
      _ => None
    }
  }

}

impl RedisError {

  pub fn new<T: Into<String>>(kind: RedisErrorKind, details: T) -> RedisError {
    let desc = match kind {
      RedisErrorKind::Auth            => "Authentication Error",
      RedisErrorKind::IO(_)           => "IO Error",
      RedisErrorKind::InvalidArgument => "Invalid Argument",
      RedisErrorKind::InvalidCommand  => "Invalid Command",
      RedisErrorKind::UrlError(_)     => "Url Error",
      RedisErrorKind::ProtocolError   => "Protocol Error",
      RedisErrorKind::Unknown         => "Unknown Error",
      RedisErrorKind::Canceled        => "Canceled",
      RedisErrorKind::Cluster         => "Cluster Error"
    };

    RedisError {
      kind: kind,
      desc: desc,
      details: details.into()
    }
  }

  pub fn kind(&self) -> &RedisErrorKind {
    &self.kind
  }

  pub fn details(&self) -> &str {
    &self.details
  }

  pub fn to_string(&self) -> String {
    format!("{}: {}", &self.desc, &self.details)
  }

  pub fn new_canceled() -> RedisError {
    RedisError::new(RedisErrorKind::Canceled, "Canceled.")
  }

}

