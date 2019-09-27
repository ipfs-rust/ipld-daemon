use crate::task::Task;
use async_std::{fs, os::unix::net::UnixListener, prelude::*, task};
use exitfailure::ExitFailure;
use ipld_daemon_common::{paths::Paths, utils};
use sled::Db;
use slog::{o, Drain, Logger};
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

fn setup_tty_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

#[cfg(target_os = "linux")]
fn setup_journald_logger() -> Logger {
    let drain = slog_journald::JournaldDrain.ignore_res();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

fn setup_no_tty_logger() -> Logger {
    #[cfg(target_os = "linux")]
    return setup_journald_logger();
    #[cfg(not(target_os = "linux"))]
    setup_tty_logger()
}

impl Service {
    pub async fn setup<P: AsRef<Path>>(prefix: P) -> Result<Self, ExitFailure> {
        // Drop capabilities
        #[cfg(target_os = "linux")]
        caps::clear(None, caps::CapSet::Permitted)?;

        // Setup logger
        let log = if atty::is(atty::Stream::Stderr) {
            setup_tty_logger()
        } else {
            setup_no_tty_logger()
        };

        // Setup signal handlers
        let sigterm = Arc::new(AtomicBool::new(false));
        signal_hook::flag::register(signal_hook::SIGTERM, sigterm.clone())?;

        // Setup store
        let paths = Paths::new(prefix.as_ref());
        fs::create_dir_all(paths.blocks()).await?;
        utils::create_dir_with_perm(paths.per_user(), 0o777).await?; // rwx-rwx-rwx
        utils::create_file_with_perm(paths.lock(), 0o666).await?; // rw-rw-rw
        fs::create_dir_all(paths.var()).await?;
        let db = Db::open(paths.db())?;

        // Setup socket
        utils::remove_file(paths.socket()).await.ok();
        let socket = UnixListener::bind(paths.socket()).await?;
        utils::set_permissions(paths.socket(), 0o777).await?; // rwx-rwx-rwx

        slog::info!(log, "listening at {:?}", paths.socket());

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
