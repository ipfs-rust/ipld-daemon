use async_std::fs::{self, File};
use async_std::io::Write;
use async_std::os::unix::net::UnixStream;
use async_trait::async_trait;
use ipld_daemon_common::paths::{AppPaths, Paths};
use ipld_daemon_common::utils;
use libipld::cbor::WriteCbor;
use libipld::{BlockError, Cid, DefaultHash as H, Hash, Result, Store};
use multibase::Base;
use std::convert::TryFrom;
use std::io::ErrorKind;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

/// The block store.
pub struct BlockStore {
    socket: UnixStream,
    paths: AppPaths,
    _lock: File,
}

impl BlockStore {
    pub async fn connect<P: AsRef<Path>>(prefix: P, app: &str) -> Result<Self> {
        let paths = Paths::new(prefix);
        let user = whoami::username();
        let paths = paths.to_app_paths(&user, app);

        let socket = UnixStream::connect(paths.socket()).await?;
        fs::create_dir_all(&paths.app()).await?;
        utils::create_dir(&paths.pins()).await?;
        utils::create_dir(&paths.auto()).await?;
        utils::create_dir(&paths.links()).await?;
        let lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(paths.lock())
            .await?;

        Ok(Self {
            socket,
            paths,
            _lock: lock,
        })
    }
}

#[async_trait]
impl Store for BlockStore {
    async fn read(&self, cid: &Cid) -> Result<Option<Box<[u8]>>> {
        let res = utils::read_file(&self.paths.block(cid)).await?;
        Ok(res)
    }

    async fn write(&self, cid: &Cid, data: Box<[u8]>) -> Result<()> {
        cid.write_cbor(&mut &self.socket).await?;
        data.write_cbor(&mut &self.socket).await?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        (&mut &self.socket).flush().await?;
        Ok(())
    }

    async fn gc(&self) -> Result<()> {
        Ok(())
    }

    async fn pin(&self, cid: &Cid) -> Result<()> {
        let block = self.paths.block(cid);
        // Needs a shared lock, to prevent a race with the garbage collector.
        //self.lock_file.lock_shared();
        utils::atomic_symlink(&block, &self.paths.pins(), &self.paths.cid(cid)).await?;
        Ok(())
    }

    async fn unpin(&self, cid: &Cid) -> Result<()> {
        utils::remove_file(&self.paths.pin(cid)).await?;
        Ok(())
    }

    async fn autopin(&self, cid: &Cid, auto_path: &Path) -> Result<()> {
        let store_path = self.paths.block(cid);

        let (auto_path, auto_parent, auto_name) = {
            let parent = auto_path.parent();
            let name = auto_path
                .file_name()
                .map(|n| n.to_str())
                .unwrap_or_default();
            let (parent, name) = if let (Some(parent), Some(name)) = (parent, name) {
                (parent, name)
            } else {
                return Err(BlockError::InvalidLink);
            };
            let parent = fs::canonicalize(parent).await?;
            let path = parent.join(name);
            (path, parent, name)
        };

        let bytes = auto_path.as_os_str().as_bytes();
        let hash = H::digest(bytes).to_bytes();
        let link_name = multibase::encode(Base::Base64UrlUpperNoPad, hash);

        utils::atomic_symlink(&store_path, &auto_parent, auto_name).await?;
        utils::atomic_symlink(&auto_path, &self.paths.auto(), &link_name).await?;
        Ok(())
    }

    async fn write_link(&self, link: &str, cid: &Cid) -> Result<()> {
        utils::atomic_symlink(&self.paths.pin(cid), &self.paths.links(), link).await?;
        Ok(())
    }

    async fn read_link(&self, link: &str) -> Result<Option<Cid>> {
        match fs::read_link(self.paths.link(link)).await {
            Ok(path) => {
                if let Some(file_name) = path.file_name() {
                    if let Some(file_name) = file_name.to_str() {
                        return Ok(Some(Cid::try_from(file_name)?));
                    }
                }
                Err(BlockError::InvalidLink)
            }
            Err(err) => match err.kind() {
                ErrorKind::NotFound => Ok(None),
                _ => Err(err.into()),
            },
        }
    }

    async fn remove_link(&self, link: &str) -> Result<()> {
        // Remove symlink if it exists
        utils::remove_file(&self.paths.link(link)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use core::future::Future;
    use futures::join;
    use ipld_daemon::Service;
    use libipld::{create_cbor_block, create_raw_block, ipld, DefaultHash as H, MemStore};
    use model::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tempdir::TempDir;

    async fn setup() -> (TempDir, Arc<BlockStore>, AppPaths) {
        let tmp = TempDir::new("ipld-daemon").unwrap();
        let service = Service::setup(tmp.path()).await.unwrap();
        task::spawn(service.run());
        let store = Arc::new(BlockStore::connect(tmp.path(), "test").await.unwrap());
        let paths = Paths::new(tmp.path()).to_app_paths(&whoami::username(), "test");
        (tmp, store, paths)
    }

    #[test]
    fn read_write_block() {
        task::block_on(async {
            let (_tmp, store, paths) = setup().await;

            // Send invalid block
            let cid = Cid::random();
            let data: Box<[u8]> = vec![0, 1, 2, 3].into_boxed_slice();
            store.write(&cid, data.clone()).await.unwrap();
            store.flush().await.unwrap();
            assert!(utils::read_file(&paths.block(&cid))
                .await
                .unwrap()
                .is_none());

            // Send valid block
            let ipld = ipld!("hello world!");
            let (cid, data) = create_cbor_block::<H, _>(&ipld).await.unwrap();
            assert!(store.read(&cid).await.unwrap().is_none());
            store.write(&cid, data.clone()).await.unwrap();

            // Check that the block was written to disk.
            task::sleep(Duration::from_millis(100)).await;
            let data2 = store.read(&cid).await.unwrap().unwrap();
            assert_eq!(data, data2);
        });
    }

    #[test]
    fn pin_unpin_block() {
        task::block_on(async {
            // setup
            let (_tmp, store, paths) = setup().await;
            let cid = Cid::random();
            assert!(fs::read_link(&paths.pin(&cid)).await.is_err());

            store.pin(&cid).await.unwrap();
            let res = fs::read_link(&paths.pin(&cid)).await.unwrap();
            assert_eq!(res, paths.block(&cid));

            // Pin should not error if already pinned.
            store.pin(&cid).await.unwrap();

            store.unpin(&cid).await.unwrap();
            assert!(fs::read_link(&paths.pin(&cid)).await.is_err());

            // Unpin should not error if not pinned
            store.unpin(&cid).await.unwrap();
        });
    }

    #[test]
    fn autopin_block() {
        task::block_on(async {
            // setup
            let (_tmp, store, paths) = setup().await;
            let cid = Cid::random();
            let auto_path = Path::new("/tmp/autolink");
            let hash_plain = b"/tmp/autolink";
            let hash = H::digest(hash_plain);
            let name = multibase::encode(Base::Base64UrlUpperNoPad, hash);

            store.autopin(&cid, &auto_path).await.unwrap();
            let res = fs::read_link(&auto_path).await.unwrap();
            assert_eq!(res, paths.block(&cid));
            let res = fs::read_link(&paths.auto().join(name)).await.unwrap();
            assert_eq!(&res, auto_path);

            // Autopin should not error if already pinned
            store.autopin(&cid, &auto_path).await.unwrap();
        });
    }

    #[test]
    fn create_read_remove_link() {
        task::block_on(async {
            // setup
            let (_tmp, store, paths) = setup().await;
            let cid = Cid::random();
            assert!(fs::read_link(&paths.link("link")).await.is_err());

            // create
            store.write_link("link", &cid).await.unwrap();
            let res = fs::read_link(&paths.link("link")).await.unwrap();
            assert_eq!(res, paths.pin(&cid));
            assert_eq!(store.read_link("link").await.unwrap().as_ref(), Some(&cid));

            // update
            store.write_link("link", &Cid::random()).await.unwrap();

            // remove
            store.remove_link("link").await.unwrap();
            assert!(fs::read_link(&paths.link("link")).await.is_err());
            assert!(store.read_link("link").await.unwrap().is_none());
            store.remove_link("link").await.unwrap();
        });
    }

    fn create_block_raw(n: usize) -> (Cid, Box<[u8]>) {
        let data = n.to_ne_bytes().to_vec().into_boxed_slice();
        create_raw_block::<H>(data).unwrap()
    }

    fn join<T: Send>(
        f1: impl Future<Output = Result<T>> + Send,
        f2: impl Future<Output = Result<T>> + Send,
    ) -> (T, T) {
        task::block_on(async {
            let f1_u = async { f1.await.unwrap() };
            let f2_u = async { f2.await.unwrap() };
            join!(f1_u, f2_u)
        })
    }

    #[test]
    #[ignore]
    fn mem_buf_store_eqv() {
        const LEN: usize = 4;
        let blocks: Vec<_> = (0..LEN).into_iter().map(create_block_raw).collect();
        model! {
            Model => let mem_store = MemStore::default(),
            Implementation => let (_tmp, store, _) = task::block_on(setup()),
            Read(usize)(i in 0..LEN) => {
                let (cid, _) = &blocks[i];
                let mem = mem_store.read(cid);
                let st = store.read(cid);
                let (mem, st) = join(mem, st);
                // Writes might not be flushed to disk yet.
                if !(mem.is_some() && st.is_none()) {
                    assert_eq!(mem, st);
                }
            },
            Write(usize)(i in 0..LEN) => {
                let (cid, data) = &blocks[i];
                let mem = mem_store.write(cid, data.clone());
                let st = store.write(cid, data.clone());
                join(mem, st);
                // The flush behaviour is different.
                // In the mem store writes are not written until
                // flushed, in the block store they may or may not
                // have happened.
                let mem = mem_store.flush();
                let st = store.flush();
                join(mem, st);
            },
            /*Gc(usize)(_ in 0..LEN) => {
                let mem = mem_store.gc();
                let buf = buf_store.gc();
                join(mem, buf);
            },*/
            Pin(usize)(i in 0..LEN) => {
                let (cid, _) = &blocks[i];
                let mem = mem_store.pin(&cid);
                let st = store.pin(&cid);
                join(mem, st);
            },
            Unpin(usize)(i in 0..LEN) => {
                let (cid, _) = &blocks[i];
                let mem = mem_store.unpin(&cid);
                let st = store.unpin(&cid);
                join(mem, st);
            }
        }
    }

    macro_rules! linearizable_store {
        ($store:expr) => {
            const LEN: usize = 4;
            let blocks: Vec<_> = (0..LEN).into_iter().map(create_block_raw).collect();
            let blocks = Shared::new(blocks);
            const LLEN: usize = 3;
            let links = Shared::new(["a", "b", "c"]);
            linearizable! {
                Implementation => let store = Shared::new($store),
                Read(usize)(i in 0..LEN) -> Option<Box<[u8]>> {
                    let (cid, _) = &blocks[i];
                    task::block_on(store.read(cid)).unwrap()
                },
                Write(usize)(i in 0..LEN) -> () {
                    let (cid, data) = &blocks[i];
                    task::block_on(store.write(cid, data.clone())).unwrap()
                },
                Flush(usize)(_ in 0..LEN) -> () {
                    task::block_on(store.flush()).unwrap()
                },
                Gc(usize)(_ in 0..LEN) -> () {
                    task::block_on(store.gc()).unwrap()
                },
                Pin(usize)(i in 0..LEN) -> () {
                    let (cid, _) = &blocks[i];
                    task::block_on(store.pin(cid)).unwrap()
                },
                Unpin(usize)(i in 0..LEN) -> () {
                    let (cid, _) = &blocks[i];
                    task::block_on(store.unpin(cid)).unwrap()
                },
                WriteLink((usize, usize))((i1, i2) in (0..LLEN, 0..LEN)) -> () {
                    let link = &links[i1];
                    let (cid, _) = &blocks[i2];
                    task::block_on(store.write_link(link, cid)).unwrap()
                },
                ReadLink(usize)(i in 0..LLEN) -> Option<Cid> {
                    let link = &links[i];
                    task::block_on(store.read_link(link)).unwrap()
                },
                RemoveLink(usize)(i in 0..LLEN) -> () {
                    let link = &links[i];
                    task::block_on(store.remove_link(link)).unwrap()
                }
            }
        };
    }

    #[test]
    #[ignore]
    fn linearizable() {
        let (_tmp, store, _) = task::block_on(setup());
        linearizable_store!(store.clone());
    }
}
