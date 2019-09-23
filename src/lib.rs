#![feature(const_str_as_bytes, const_str_len)]
use async_std::fs::{self, File};
use async_std::io::Write;
use async_std::os::unix::{fs::symlink, net::UnixStream};
use async_trait::async_trait;
use const_concat::const_concat;
use libipld::cbor::WriteCbor;
use libipld::{BlockError, Cid, DefaultHash as H, Hash, Result, Store};
use multibase::Base;
use std::convert::TryFrom;
use std::io::{ErrorKind, Result as IoResult};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

/// The PREFIX_PATH
pub const PREFIX_PATH: &str = "/tmp";

/// The DATA_PATH
pub const DATA_PATH: &str = const_concat!(PREFIX_PATH, "/ipfs");
/// The STORE_PATH
pub const STORE_PATH: &str = const_concat!(DATA_PATH, "/blocks");
/// The PIN_PATH
pub const PIN_PATH: &str = const_concat!(DATA_PATH, "/pins");
/// The PIN_LOCK_PATH
pub const PIN_LOCK_PATH: &str = const_concat!(PIN_PATH, "/lock");
/// The PIN_PER_USER_PATH
pub const PIN_PER_USER_PATH: &str = const_concat!(PIN_PATH, "/per-user");

/// The VAR_PATH
pub const VAR_PATH: &str = const_concat!(PREFIX_PATH, "/var/ipfs");
/// The SOCKET_PATH
pub const SOCKET_PATH: &str = const_concat!(VAR_PATH, "/ipld.sock");
/// The DB_PATH
pub const DB_PATH: &str = const_concat!(VAR_PATH, "/db");

/// Cid to file name
pub fn cid_file_name(cid: &Cid) -> String {
    multibase::encode(Base::Base64UrlUpperNoPad, cid.to_bytes())
}

/// Store path for block.
pub fn cid_store_path(cid: &Cid) -> Box<Path> {
    Path::new(STORE_PATH)
        .join(cid_file_name(cid))
        .into_boxed_path()
}

/// Pin path for block.
pub fn cid_pin_path(user: &str, app: &str, cid: &Cid) -> Box<Path> {
    Path::new(PIN_PER_USER_PATH)
        .join(user)
        .join(app)
        .join("pins")
        .join(cid_file_name(cid))
        .into_boxed_path()
}

/// Link path.
pub fn link_path(user: &str, app: &str, link: &str) -> Box<Path> {
    Path::new(PIN_PER_USER_PATH)
        .join(user)
        .join(app)
        .join("links")
        .join(link)
        .into_boxed_path()
}

/// Create directory.
pub async fn create_dir(path: &Path) -> IoResult<()> {
    match fs::create_dir(path).await {
        Ok(()) => Ok(()),
        Err(err) => match err.kind() {
            ErrorKind::AlreadyExists => Ok(()),
            _ => Err(err),
        },
    }
}

/// Read file.
pub async fn read_file(path: &Path) -> IoResult<Option<Box<[u8]>>> {
    match fs::read(path).await {
        Ok(bytes) => Ok(Some(bytes.into_boxed_slice())),
        Err(err) => match err.kind() {
            ErrorKind::NotFound => Ok(None),
            _ => Err(err),
        },
    }
}

/// Remove file.
pub async fn remove_file(path: &Path) -> IoResult<()> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(err) => match err.kind() {
            ErrorKind::NotFound => Ok(()),
            _ => Err(err),
        },
    }
}

/// Atomic symlink.
///
/// To atomically create a symlink we first create a new symlink
/// with a random name and then rename it to it's final name.
pub async fn atomic_symlink(src: &Path, dst: &Path, name: &str) -> IoResult<()> {
    let dst_new = dst.join(name.to_owned() + ".new");
    symlink(src, &dst_new).await?;
    fs::rename(&dst_new, dst.join(name)).await?;
    Ok(())
}

/// The block store.
pub struct BlockStore {
    socket: UnixStream,
    pin_dir: Box<Path>,
    auto_dir: Box<Path>,
    link_dir: Box<Path>,
    lock_file: File,
}

impl BlockStore {
    pub async fn connect(app_name: &str) -> Result<Self> {
        let socket = UnixStream::connect(SOCKET_PATH).await?;

        let app_dir = Path::new(PIN_PER_USER_PATH)
            .join(whoami::username())
            .join(app_name);
        fs::create_dir_all(&app_dir).await?;

        let pin_dir = app_dir.join("pins");
        create_dir(&pin_dir).await?;

        let auto_dir = app_dir.join("auto");
        create_dir(&auto_dir).await?;

        let link_dir = app_dir.join("links");
        create_dir(&link_dir).await?;

        let lock_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(PIN_LOCK_PATH)
            .await?;

        Ok(Self {
            socket,
            pin_dir: pin_dir.into_boxed_path(),
            auto_dir: auto_dir.into_boxed_path(),
            link_dir: link_dir.into_boxed_path(),
            lock_file,
        })
    }
}

#[async_trait]
impl Store for BlockStore {
    async fn read(&self, cid: &Cid) -> Result<Option<Box<[u8]>>> {
        let path = cid_store_path(cid);
        let res = read_file(&path).await?;
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

    async fn pin(&self, cid: &Cid) -> Result<()> {
        let file_name = cid_file_name(cid);
        let store_path = Path::new(STORE_PATH).join(&file_name);
        // Needs a shared lock, to prevent a race with the garbage collector.
        //self.lock_file.lock_shared();
        atomic_symlink(&store_path, &self.pin_dir, &file_name).await?;
        Ok(())
    }

    async fn unpin(&self, cid: &Cid) -> Result<()> {
        let file_name = cid_file_name(cid);
        let pin_path = self.pin_dir.join(&file_name);
        remove_file(&pin_path).await?;
        Ok(())
    }

    async fn autopin(&self, cid: &Cid, auto_path: &Path) -> Result<()> {
        let file_name = cid_file_name(cid);
        let store_path = Path::new(STORE_PATH).join(&file_name);

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

        atomic_symlink(&store_path, &auto_parent, auto_name).await?;
        atomic_symlink(&auto_path, &self.auto_dir, &link_name).await?;
        Ok(())
    }

    async fn write_link(&self, link: &str, cid: &Cid) -> Result<()> {
        let file_name = cid_file_name(cid);
        let pin_path = self.pin_dir.join(&file_name);
        atomic_symlink(&pin_path, &self.link_dir, link).await?;
        Ok(())
    }

    async fn read_link(&self, link: &str) -> Result<Option<Cid>> {
        let link_path = self.link_dir.join(link);

        match fs::read_link(link_path).await {
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
        let link_path = self.link_dir.join(link);
        // Remove symlink if it exists
        remove_file(&link_path).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use core::result::Result;
    use failure::Error;
    use libipld::{create_cbor_block, ipld, DefaultHash as H};
    use std::time::Duration;

    async fn run_read_write_block() -> Result<(), Error> {
        let store = BlockStore::connect("test").await?;

        // Send invalid block
        let cid = Cid::random();
        let data = vec![0, 1, 2, 3].into_boxed_slice();
        store.write(&cid, data.clone()).await?;
        store.flush().await?;

        // Send valid block
        let ipld = ipld!("hello world!");
        let (cid, data) = create_cbor_block::<H, _>(&ipld).await?;
        remove_file(&cid_store_path(&cid)).await?;
        assert!(store.read(&cid).await?.is_none());
        store.write(&cid, data.clone()).await?;

        // Check that the block was written to disk.
        task::sleep(Duration::from_millis(100)).await;
        let data2 = store.read(&cid).await?.unwrap();
        assert_eq!(data, data2);

        Ok(())
    }

    #[test]
    #[ignore]
    fn read_write_block() {
        task::block_on(run_read_write_block()).unwrap();
    }

    async fn run_pin_unpin_block() -> Result<(), Error> {
        // setup
        let store = BlockStore::connect("test").await?;
        let user = whoami::username();
        let cid = Cid::random();
        let path = cid_pin_path(&user, "test", &cid);
        assert!(fs::read_link(&path).await.is_err());

        store.pin(&cid).await?;
        let res = fs::read_link(&path).await?;
        assert_eq!(res.into_boxed_path(), cid_store_path(&cid));

        // Pin should not error if already pinned.
        store.pin(&cid).await?;

        store.unpin(&cid).await?;
        assert!(fs::read_link(&path).await.is_err());

        // Unpin should not error if not pinned
        store.unpin(&cid).await?;

        Ok(())
    }

    #[test]
    #[ignore]
    fn pin_unpin_block() {
        task::block_on(run_pin_unpin_block()).unwrap();
    }

    async fn run_autopin_block() -> Result<(), Error> {
        // setup
        let store = BlockStore::connect("test").await?;
        let user = whoami::username();
        let auto_dir = Path::new(PIN_PER_USER_PATH)
            .join(user)
            .join("test")
            .join("auto");
        let cid = Cid::random();
        let auto_path = Path::new("/tmp/autolink");
        let hash_plain = b"/tmp/autolink";
        let hash = H::digest(hash_plain);
        let name = multibase::encode(Base::Base64UrlUpperNoPad, hash);

        store.autopin(&cid, &auto_path).await?;
        let res = fs::read_link(&auto_path).await?;
        assert_eq!(res.into_boxed_path(), cid_store_path(&cid));
        let res = fs::read_link(&auto_dir.join(name)).await?;
        assert_eq!(&res, auto_path);

        // Autopin should not error if already pinned
        store.autopin(&cid, &auto_path).await?;

        Ok(())
    }

    #[test]
    #[ignore]
    fn autopin_block() {
        task::block_on(run_autopin_block()).unwrap();
    }

    async fn run_create_read_remove_link() -> Result<(), Error> {
        // setup
        let store = BlockStore::connect("test").await?;
        let user = whoami::username();
        let cid = Cid::random();
        let pin_path = cid_pin_path(&user, "test", &cid);
        let link_path = link_path(&user, "test", "link");
        assert!(fs::read_link(&link_path).await.is_err());

        // create
        store.write_link("link", &cid).await?;
        let res = fs::read_link(&link_path).await?;
        assert_eq!(res.into_boxed_path(), pin_path);
        assert_eq!(store.read_link("link").await?.as_ref(), Some(&cid));

        // update
        store.write_link("link", &Cid::random()).await?;

        // remove
        store.remove_link("link").await?;
        assert!(fs::read_link(&link_path).await.is_err());
        assert!(store.read_link("link").await?.is_none());
        store.remove_link("link").await?;

        Ok(())
    }

    #[test]
    #[ignore]
    fn create_read_remove_link() {
        task::block_on(run_create_read_remove_link()).unwrap();
    }
}
