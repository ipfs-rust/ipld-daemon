use crate::task::Task;
use async_std::{
    fs::{self, Permissions},
    os::unix::net::UnixListener,
    stream::Stream,
    task,
};
use caps::CapSet;
use exitfailure::ExitFailure;
use ipld_daemon_common::paths::Paths;
use sled::Db;
use slog::{o, Drain, Logger};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct Service {
    log: Logger,
    paths: Paths,
    db: Db,
    sigterm: Arc<AtomicBool>,
    socket: UnixListener,
}

impl Service {
    pub async fn setup<P: AsRef<Path>>(prefix: P) -> Result<Self, ExitFailure> {
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
        let paths = Paths::new(prefix);
        fs::create_dir_all(paths.blocks()).await?;
        fs::create_dir_all(paths.per_user()).await?;
        fs::create_dir_all(paths.var()).await?;
        let db = Db::open(paths.db())?;

        // Setup socket
        fs::remove_file(paths.socket()).await.ok();
        let socket = UnixListener::bind(paths.socket()).await?;
        slog::info!(log, "listening at {:?}", paths.socket());

        // Set permissions on per_user dir and socket.
        let perms = Permissions::from_mode(0o777);
        fs::set_permissions(paths.socket(), perms.clone()).await?;
        fs::set_permissions(paths.per_user(), perms).await?;

        Ok(Self {
            log,
            paths,
            db,
            sigterm,
            socket,
        })
    }

    pub async fn run(self) -> Result<(), ExitFailure> {
        let mut incoming = self.socket.incoming();
        while !self.sigterm.load(Ordering::Relaxed) {
            if let Some(stream) = incoming.next().await {
                slog::debug!(self.log, "client connected");
                let task = Task::new(&self.log, &self.paths, &self.db, stream?);
                task::spawn(task.run());
            }
        }
        Ok(())
    }
}
