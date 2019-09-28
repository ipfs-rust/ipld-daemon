use crate::error::Error;
use async_std::os::unix::net::UnixStream;
use ipld_daemon_common::{paths::Paths, utils};
use libipld::block::{decode_ipld, validate};
use libipld::cbor::{CborError, ReadCbor, WriteCbor};
use libipld::gc::references;
use libipld::Cid;
use sled::Db;
use slog::Logger;

pub struct Task {
    log: Logger,
    paths: Paths,
    db: Db,
    stream: UnixStream,
}

impl Task {
    pub fn new(log: &Logger, paths: &Paths, db: &Db, stream: UnixStream) -> Self {
        Self {
            log: log.clone(),
            db: db.clone(),
            paths: paths.clone(),
            stream,
        }
    }

    pub async fn add_to_store(&self, cid: &Cid, data: &[u8]) -> Result<(), Error> {
        // Early exit if block is already in store
        if self.db.get(&cid.to_bytes())?.is_some() {
            slog::debug!(self.log, "block exists");
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
            slog::debug!(self.log, "writing block to disk");
            // Add block atomically to fs.
            utils::atomic_write_file(&self.paths.blocks(), &self.paths.cid(cid), &data).await?;
        }

        Ok(())
    }

    pub async fn request(&self) -> Result<(), Error> {
        let cid: Cid = ReadCbor::read_cbor(&mut &self.stream).await?;
        let data: Box<[u8]> = ReadCbor::read_cbor(&mut &self.stream).await?;
        slog::debug!(self.log, "received block");
        self.add_to_store(&cid, &data).await?;
        Ok(())
    }

    pub async fn run(self) {
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
