use async_std::{
    fs::{self, File, Permissions},
    io::Write,
    os::unix::net::{UnixListener, UnixStream},
    stream::Stream,
    task,
};
use caps::CapSet;
use exitfailure::ExitFailure;
use failure::Fail;
use ipld_daemon::{cid_store_path, DB_PATH, PIN_PER_USER_PATH, SOCKET_PATH, STORE_PATH, VAR_PATH};
use libipld::cbor::{CborError, ReadCbor, WriteCbor};
use libipld::{decode_ipld, references, validate, BlockError, Cid};
use sled::Db;
use slog::{o, Drain, Logger};
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn main() -> Result<(), ExitFailure> {
    task::block_on(run())
}

async fn run() -> Result<(), ExitFailure> {
    // Drop capabilities
    caps::clear(None, CapSet::Permitted)?;

    // Setup logger
    let drain = if atty::is(atty::Stream::Stderr) {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        slog_async::Async::new(drain).build().fuse()
    } else {
        let drain = slog_journald::JournaldDrain.ignore_res();
        slog_async::Async::new(drain).build().fuse()
    };
    let log = slog::Logger::root(drain, o!());

    // Setup signal handlers
    let sigterm = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::SIGTERM, sigterm.clone())?;

    // Setup store
    fs::create_dir_all(STORE_PATH).await?;
    fs::create_dir_all(PIN_PER_USER_PATH).await?;
    fs::create_dir_all(VAR_PATH).await?;
    let db = Db::open(DB_PATH)?;

    // Setup socket
    fs::remove_file(SOCKET_PATH).await.ok();
    let listener = UnixListener::bind(SOCKET_PATH).await?;
    let perms = Permissions::from_mode(0o777);
    fs::set_permissions(SOCKET_PATH, perms).await?;
    let mut incoming = listener.incoming();
    slog::info!(log, "listening at {}", SOCKET_PATH);

    // When SIGTERM is received, shut down the daemon and exit cleanly
    while !sigterm.load(Ordering::Relaxed) {
        if let Some(stream) = incoming.next().await {
            slog::info!(log, "client connected");
            task::spawn(handle_client(log.clone(), db.clone(), stream?));
        }
    }

    Ok(())
}

#[derive(Debug, Fail)]
enum Error {
    #[fail(display = "{}", _0)]
    Block(BlockError),
    #[fail(display = "{}", _0)]
    Db(sled::Error),
    #[fail(display = "{}", _0)]
    Io(std::io::Error),
    #[fail(display = "{}", _0)]
    Cbor(libipld::cbor::CborError),
}

impl From<BlockError> for Error {
    fn from(err: BlockError) -> Self {
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

async fn add_to_store(log: &Logger, db: &Db, cid: &Cid, data: &[u8]) -> Result<(), Error> {
    // Early exit if block is already in store
    if db.get(&cid.to_bytes())?.is_some() {
        slog::info!(log, "block exists");
        return Ok(());
    }

    // Verify block
    validate(cid, data)?;
    let ipld = decode_ipld(cid, data).await?;

    // Add block and it's references to db.
    let cids: Vec<Cid> = references(&ipld).into_iter().collect();
    let mut bytes = Vec::new();
    cids.write_cbor(&mut bytes);
    if let Ok(()) = db.cas(&cid.to_bytes(), None as Option<&[u8]>, Some(bytes))? {
        slog::info!(log, "writing block to disk");
        // Add block to fs.
        let path = cid_store_path(cid);
        let mut file = File::create(&path).await?;
        file.write_all(&data).await?;
        file.sync_data().await?;
    }

    Ok(())
}

async fn handle_client(log: Logger, db: Db, mut stream: UnixStream) {
    loop {
        if let Err(e) = inner_handle_client(&log, &db, &mut stream).await {
            if let Error::Cbor(CborError::UnexpectedEof) = e {
                break;
            }
            slog::error!(log, "{}", e);
        }
    }
}

async fn inner_handle_client(log: &Logger, db: &Db, stream: &mut UnixStream) -> Result<(), Error> {
    let cid: Cid = ReadCbor::read_cbor(stream).await?;
    let data: Box<[u8]> = ReadCbor::read_cbor(stream).await?;
    slog::info!(log, "received block");
    add_to_store(log, db, &cid, &data).await?;
    Ok(())
}
