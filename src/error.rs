#[derive(Debug)]
pub enum Error {
    // Only used on delete and update, search returns None instead
    KeyNotFound(String),
    UnexpectedError(String),
    InvalidRootOffset,
    InternalNodeNoChild,
    ImpossibleSplit,
    InvalidNodeKind,
    NodeParseError,
    KeyParseError,
    KeyOverflowError,
    FileSystemError(std::io::Error),
}

impl std::convert::From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::FileSystemError(e)
    }
}
