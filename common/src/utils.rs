use async_std::fs::{self, File};
use async_std::io::Write;
use async_std::os::unix::fs::symlink;
use std::io::{ErrorKind, Result};
use std::path::Path;

/// Create directory.
pub async fn create_dir(path: &Path) -> Result<()> {
    match fs::create_dir(path).await {
        Ok(()) => Ok(()),
        Err(err) => match err.kind() {
            ErrorKind::AlreadyExists => Ok(()),
            _ => Err(err),
        },
    }
}

/// Read file.
pub async fn read_file(path: &Path) -> Result<Option<Box<[u8]>>> {
    match fs::read(path).await {
        Ok(bytes) => Ok(Some(bytes.into_boxed_slice())),
        Err(err) => match err.kind() {
            ErrorKind::NotFound => Ok(None),
            _ => Err(err),
        },
    }
}

/// Remove file.
pub async fn remove_file(path: &Path) -> Result<()> {
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
pub async fn atomic_symlink(src: &Path, dst: &Path, name: &str) -> Result<()> {
    let dst_new = dst.join(name.to_owned() + ".new");
    symlink(src, &dst_new).await?;
    fs::rename(&dst_new, dst.join(name)).await?;
    Ok(())
}

/// Atomic write file.
pub async fn atomic_write_file(path: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let path_new = path.join(name.to_owned() + ".new");
    let mut file = File::create(&path_new).await?;
    file.write_all(bytes).await?;
    file.sync_data().await?;
    fs::rename(&path_new, path.join(name)).await?;
    Ok(())
}
