use failure::Fail;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "{}", _0)]
    Block(libipld::error::BlockError),
    #[fail(display = "{}", _0)]
    Db(sled::Error),
    #[fail(display = "{}", _0)]
    Io(std::io::Error),
    #[fail(display = "{}", _0)]
    Cbor(libipld::cbor::CborError),
}

impl From<libipld::error::BlockError> for Error {
    fn from(err: libipld::error::BlockError) -> Self {
        Self::Block(err)
    }
}

impl From<sled::Error> for Error {
    fn from(err: sled::Error) -> Self {
        Self::Db(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<libipld::cbor::CborError> for Error {
    fn from(err: libipld::cbor::CborError) -> Self {
        Self::Cbor(err)
    }
}
