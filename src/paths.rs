use libipld::Cid;
use multibase::Base;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct Paths {
    store: Box<Path>,
    var: Box<Path>,
}

impl Paths {
    pub fn new<P: AsRef<Path>>(prefix: P) -> Self {
        Self {
            store: prefix.as_ref().join("ipfs").into_boxed_path(),
            var: prefix.as_ref().join("var/ipfs").into_boxed_path(),
        }
    }

    pub fn store(&self) -> &Path {
        &self.store
    }

    pub fn blocks(&self) -> PathBuf {
        self.store.join("blocks")
    }

    pub fn block(&self, cid: &Cid) -> PathBuf {
        self.blocks().join(cid_file_name(cid))
    }

    pub fn per_user(&self) -> PathBuf {
        self.store.join("per-user")
    }

    pub fn user(&self, user: &str) -> PathBuf {
        self.per_user().join(user)
    }

    pub fn per_app(&self, user: &str) -> PathBuf {
        self.user(user).join("per-app")
    }

    pub fn app(&self, user: &str, app: &str) -> PathBuf {
        self.per_app(user).join(app)
    }

    pub fn pins(&self, user: &str, app: &str) -> PathBuf {
        self.app(user, app).join("pins")
    }

    pub fn pin(&self, user: &str, app: &str, cid: &Cid) -> PathBuf {
        self.pins(user, app).join(cid_file_name(cid))
    }

    pub fn links(&self, user: &str, app: &str) -> PathBuf {
        self.app(user, app).join("links")
    }

    pub fn link(&self, user: &str, app: &str, link: &str) -> PathBuf {
        self.links(user, app).join(link)
    }

    pub fn auto(&self, user: &str, app: &str) -> PathBuf {
        self.app(user, app).join("auto")
    }

    pub fn lock(&self) -> PathBuf {
        self.per_user().join("lock")
    }

    pub fn var(&self) -> &Path {
        &self.var
    }

    pub fn db(&self) -> PathBuf {
        self.var.join("db")
    }

    pub fn socket(&self) -> PathBuf {
        self.var.join("ipld.sock")
    }

    pub fn to_app_paths(&self, user: &str, app: &str) -> AppPaths {
        AppPaths::new(&self, user, app)
    }

    pub fn cid(&self, cid: &Cid) -> String {
        cid_file_name(cid)
    }
}

#[derive(Clone, Debug)]
pub struct AppPaths {
    blocks: Box<Path>,
    app: Box<Path>,
    socket: Box<Path>,
    lock: Box<Path>,
}

impl AppPaths {
    fn new(paths: &Paths, user: &str, app: &str) -> Self {
        Self {
            blocks: paths.blocks().into_boxed_path(),
            app: paths.app(user, app).into_boxed_path(),
            socket: paths.socket().into_boxed_path(),
            lock: paths.lock().into_boxed_path(),
        }
    }

    pub fn blocks(&self) -> &Path {
        &self.blocks
    }

    pub fn block(&self, cid: &Cid) -> PathBuf {
        self.blocks.join(cid_file_name(cid))
    }

    pub fn app(&self) -> &Path {
        &self.app
    }

    pub fn pins(&self) -> PathBuf {
        self.app.join("pins")
    }

    pub fn pin(&self, cid: &Cid) -> PathBuf {
        self.pins().join(cid_file_name(cid))
    }

    pub fn links(&self) -> PathBuf {
        self.app.join("links")
    }

    pub fn link(&self, link: &str) -> PathBuf {
        self.links().join(link)
    }

    pub fn auto(&self) -> PathBuf {
        self.app.join("auto")
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub fn lock(&self) -> &Path {
        &self.lock
    }

    pub fn cid(&self, cid: &Cid) -> String {
        cid_file_name(cid)
    }
}

#[inline]
fn cid_file_name(cid: &Cid) -> String {
    multibase::encode(Base::Base64UrlUpperNoPad, cid.to_bytes())
}
