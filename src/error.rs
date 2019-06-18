extern crate rocksdb;

use std::{error, fmt};
use std::num::ParseIntError;
use std::num::ParseFloatError;
use std::io;


#[derive(Debug)]
pub enum Error {
    Parse(String),
    Shred(String),
    Rocks(rocksdb::Error),
    Write(String),
    Io(io::Error),
}

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        self == other
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Parse(ref description) => description,
            Error::Shred(ref description) => description,
            // XXX vmx 2016-11-07: It should be fixed on the RocksDB wrapper
            // that it has the std::error:Error implemented and hence
            // and err.description()
            Error::Rocks(_) => "This is an rocksdb error",
            Error::Write(ref description) => description,
            Error::Io(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::Parse(_) => None,
            Error::Shred(_) => None,
            // NOTE vmx 2016-11-07: Looks like the RocksDB Wrapper needs to be
            // patched to be based on the std::error::Error trait
            Error::Rocks(_) => None,
            Error::Write(_) => None,
            Error::Io(ref err) => Some(err as &dyn error::Error),
        }
    }
}

impl From<rocksdb::Error> for Error {
    fn from(err: rocksdb::Error) -> Error {
        Error::Rocks(err)
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
            Error::Rocks(ref err) => write!(f, "RocksDB error: {}", err),
            Error::Write(ref err) => write!(f, "Write error: {}", err),
            Error::Io(ref err) => write!(f, "Io error: {}", err),
        }
    }
}
