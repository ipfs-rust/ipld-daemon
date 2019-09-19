use async_std::task;
use caps::CapSet;
use exitfailure::ExitFailure;
use slog::{info, o, Drain};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
struct Opts {
    /// The etc path.
    #[structopt(long = "etc", default_value = "/etc")]
    etc: PathBuf,
    /// The var path.
    #[structopt(long = "var", default_value = "/var/ipfs")]
    var: PathBuf,
    /// The store path.
    #[structopt(long = "store", default_value = "/ipfs/blocks")]
    store: PathBuf,
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

    // Parse cli args
    let opts = Opts::from_args();
    info!(log, "etc: {}", &opts.etc.to_str().unwrap());
    info!(log, "var: {}", &opts.var.to_str().unwrap());
    info!(log, "store: {}", &opts.store.to_str().unwrap());

    // Setup signal handlers
    let sigterm = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::SIGTERM, sigterm.clone())?;
    let sighup = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::SIGHUP, sighup.clone())?;

    // Load config
    // TODO load config

    // When SIGTERM is received, shut down the daemon and exit cleanly
    while !sigterm.load(Ordering::Relaxed) {
        // If SIGHUP is received, reload the configuration files
        if sighup.load(Ordering::Relaxed) {
            sighup.store(false, Ordering::SeqCst);
            // TODO reload config
            info!(log, "reloading config");
        }
        //return Err(failure::format_err!("bailing").into());
    }

    Ok(())
}

fn main() -> Result<(), ExitFailure> {
    task::block_on(run())
}
