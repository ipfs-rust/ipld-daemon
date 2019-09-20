use async_std::{
    fs::{self, File, Permissions},
    io::Write,
    os::unix::net::{UnixListener, UnixStream},
    stream::Stream,
    task,
};
use caps::CapSet;
use exitfailure::ExitFailure;
use failure::Error;
use ipld_daemon::{cid_path, SOCKET_PATH, STORE_PATH, VAR_PATH};
use libipld::cbor::ReadCbor;
use libipld::{decode_ipld, validate, BlockError, Cid};
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
    create_store().await?;

    // Setup socket
    fs::remove_file(SOCKET_PATH).await?;
    slog::info!(log, "listening at {}", SOCKET_PATH);
    let listener = UnixListener::bind(SOCKET_PATH).await?;
    let perms = Permissions::from_mode(0o777);
    fs::set_permissions(SOCKET_PATH, perms).await?;
    let mut incoming = listener.incoming();

    // When SIGTERM is received, shut down the daemon and exit cleanly
    while !sigterm.load(Ordering::Relaxed) {
        if let Some(stream) = incoming.next().await {
            slog::info!(log, "client connected");
            task::spawn(handle_client(log.clone(), stream?));
        }
    }

    Ok(())
}

async fn create_store() -> Result<(), Error> {
    fs::create_dir_all(STORE_PATH).await?;
    fs::create_dir_all(VAR_PATH).await?;
    // TODO create db
    Ok(())
}

async fn add_to_store(cid: &Cid, data: &[u8]) -> Result<(), BlockError> {
    validate(cid, data)?;
    let _ipld = decode_ipld(cid, data).await?;
    // TODO add links to db
    let path = cid_path(cid);
    let mut file = File::create(&path).await?;
    file.write_all(&data).await?;
    file.sync_data().await?;
    Ok(())
}

async fn handle_client(log: Logger, mut stream: UnixStream) {
    loop {
        if let Err(e) = inner_handle_client(&log, &mut stream).await {
            match e {
                BlockError::UnexpectedEof => break,
                _ => slog::error!(log, "{}", e),
            }
        }
    }
}

async fn inner_handle_client(log: &Logger, stream: &mut UnixStream) -> Result<(), BlockError> {
    let cid: Cid = ReadCbor::read_cbor(stream).await?;
    let data: Box<[u8]> = ReadCbor::read_cbor(stream).await?;
    slog::info!(log, "received block");
    add_to_store(&cid, &data).await?;
    Ok(())
}
