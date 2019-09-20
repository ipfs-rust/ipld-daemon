use async_std::{fs, os::unix::net::UnixStream};
use async_trait::async_trait;
use libipld::cbor::WriteCbor;
use libipld::{Cid, Result, Store};
use multibase::Base;
use std::path::{Path, PathBuf};

/// The STORE_PATH
pub const STORE_PATH: &str = "/tmp/ipfs/blocks";
/// The VAR_PATH
pub const VAR_PATH: &str = "/tmp/var/ipfs";
/// The SOCKET_PATH
pub const SOCKET_PATH: &str = "/tmp/ipld.sock";

/// Path for block.
pub fn cid_path(cid: &Cid) -> Box<Path> {
    let base64 = multibase::encode(Base::Base64UrlUpperNoPad, cid.to_bytes());
    let mut buf = PathBuf::from(STORE_PATH);
    buf.push(base64);
    buf.into_boxed_path()
}

/// The block store.
pub struct BlockStore(UnixStream);

impl BlockStore {
    pub async fn connect() -> Result<Self> {
        let stream = UnixStream::connect(SOCKET_PATH).await?;
        Ok(Self(stream))
    }
}

#[async_trait]
impl Store for BlockStore {
    async fn read(&self, cid: &Cid) -> Result<Box<[u8]>> {
        let path = cid_path(cid);
        let bytes = fs::read(path).await?;
        Ok(bytes.into_boxed_slice())
    }

    async fn write(&self, cid: &Cid, data: Box<[u8]>) -> Result<()> {
        cid.write_cbor(&mut &self.0).await?;
        data.write_cbor(&mut &self.0).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;

    #[test]
    #[ignore]
    fn store_works() {
        task::block_on(async {
            let store = BlockStore::connect().await.unwrap();
            let cid = Cid::random();
            let data = vec![0, 1, 2, 3].into_boxed_slice();
            store.write(&cid, data.clone()).await.unwrap();
            store.write(&cid, data.clone()).await.unwrap();
            //let data2 = store.read(&cid).await.unwrap();
            //assert_eq!(data, data2);
        });
    }
}
