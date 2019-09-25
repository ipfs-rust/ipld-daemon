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
use ipld_daemon::paths::Paths;
use libipld::cbor::{CborError, ReadCbor, WriteCbor};
use libipld::{decode_ipld, references, validate, BlockError, Cid};
use sled::Db;
use slog::{o, Drain, Logger};
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct Opts {
    #[structopt(short = "p", long = "prefix", default_value = "/tmp")]
    prefix: String,
}

fn main() -> Result<(), ExitFailure> {
    let opts = Opts::from_args();
    task::block_on(run(opts))
}

async fn run(opts: Opts) -> Result<(), ExitFailure> {
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
    let paths = Paths::new(&opts.prefix);
    fs::create_dir_all(paths.blocks()).await?;
    fs::create_dir_all(paths.per_user()).await?;
    fs::create_dir_all(paths.var()).await?;
    let db = Db::open(paths.db())?;

    // Setup socket
    fs::remove_file(paths.socket()).await.ok();
    let listener = UnixListener::bind(paths.socket()).await?;
    let perms = Permissions::from_mode(0o777);
    fs::set_permissions(paths.socket(), perms).await?;
    let mut incoming = listener.incoming();
    slog::info!(log, "listening at {:?}", paths.socket());

    // When SIGTERM is received, shut down the daemon and exit cleanly
    while !sigterm.load(Ordering::Relaxed) {
        if let Some(stream) = incoming.next().await {
            slog::info!(log, "client connected");
            let task = Task::new(&log, &paths, &db, stream?);
            task::spawn(task.run());
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

pub struct Task {
    log: Logger,
    paths: Paths,
    db: Db,
    stream: UnixStream,
}

impl Task {
    fn new(log: &Logger, paths: &Paths, db: &Db, stream: UnixStream) -> Self {
        Self {
            log: log.clone(),
            db: db.clone(),
            paths: paths.clone(),
            stream,
        }
    }

    async fn add_to_store(&self, cid: &Cid, data: &[u8]) -> Result<(), Error> {
        // Early exit if block is already in store
        if self.db.get(&cid.to_bytes())?.is_some() {
            slog::info!(self.log, "block exists");
            return Ok(());
        }

        // Verify block
        validate(cid, data)?;
        let ipld = decode_ipld(cid, data).await?;

        // Add block and it's references to db.
        let cids: Vec<Cid> = references(&ipld).into_iter().collect();
        let mut bytes = Vec::new();
        cids.write_cbor(&mut bytes);
        if let Ok(()) = self
            .db
            .cas(&cid.to_bytes(), None as Option<&[u8]>, Some(bytes))?
        {
            slog::info!(self.log, "writing block to disk");
            // Add block to fs.
            let mut file = File::create(&self.paths.block(cid)).await?;
            file.write_all(&data).await?;
            file.sync_data().await?;
        }

        Ok(())
    }

    async fn request(&self) -> Result<(), Error> {
        let cid: Cid = ReadCbor::read_cbor(&mut &self.stream).await?;
        let data: Box<[u8]> = ReadCbor::read_cbor(&mut &self.stream).await?;
        slog::info!(self.log, "received block");
        self.add_to_store(&cid, &data).await?;
        Ok(())
    }

    async fn run(self) {
        loop {
            if let Err(e) = self.request().await {
                if let Error::Cbor(CborError::UnexpectedEof) = e {
                    break;
                }
                slog::error!(self.log, "{}", e);
            }
        }
    }
}
