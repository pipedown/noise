use std::io;
use std::num::ParseFloatError;
use std::num::ParseIntError;
use std::{error, fmt};

use noise_storage::StorageError;

#[derive(Debug)]
pub enum Error {
    Parse(String),
    Shred(String),
    Storage(StorageError),
    Write(String),
    Io(io::Error),
}

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        match (self, other) {
            (Error::Parse(a), Error::Parse(b)) => a == b,
            (Error::Shred(a), Error::Shred(b)) => a == b,
            (Error::Storage(a), Error::Storage(b)) => a.to_string() == b.to_string(),
            (Error::Write(a), Error::Write(b)) => a == b,
            (Error::Io(a), Error::Io(b)) => a.to_string() == b.to_string(),
            _ => false,
        }
    }
}

impl error::Error for Error {
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::Parse(_) => None,
            Error::Shred(_) => None,
            Error::Storage(_) => None,
            Error::Write(_) => None,
            Error::Io(ref err) => Some(err as &dyn error::Error),
        }
    }
}

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Error {
        Error::Storage(err)
    }
}

impl From<ParseIntError> for Error {
    fn from(err: ParseIntError) -> Error {
        Error::Parse(err.to_string())
    }
}

impl From<ParseFloatError> for Error {
    fn from(err: ParseFloatError) -> Error {
        Error::Parse(err.to_string())
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Parse(ref err) => write!(f, "Parse error: {}", err),
            Error::Shred(ref err) => write!(f, "Shred error: {}", err),
            Error::Storage(ref err) => write!(f, "Storage error: {}", err),
            Error::Write(ref err) => write!(f, "Write error: {}", err),
            Error::Io(ref err) => write!(f, "Io error: {}", err),
        }
    }
}
