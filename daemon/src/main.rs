use async_std::task;
use exitfailure::ExitFailure;
use ipld_daemon::Service;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct Opts {
    #[structopt(short = "p", long = "prefix", default_value = "/tmp")]
    prefix: String,
}

fn main() -> Result<(), ExitFailure> {
    let opts = Opts::from_args();
    let service = task::block_on(Service::setup(&opts.prefix))?;
    task::block_on(service.run())
}
