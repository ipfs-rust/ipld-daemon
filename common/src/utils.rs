use async_std::fs::{self, File, Permissions};
use async_std::os::unix::fs::symlink;
use async_std::prelude::*;
use std::io::{ErrorKind, Result};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// Set permissions
pub async fn set_permissions<P: AsRef<Path>>(path: P, perm: u32) -> Result<()> {
    fs::set_permissions(path, Permissions::from_mode(perm)).await
}

/// Create directory.
pub async fn create_dir<P: AsRef<Path>>(path: P) -> Result<()> {
    match fs::create_dir(path).await {
        Ok(()) => Ok(()),
        Err(err) => match err.kind() {
            ErrorKind::AlreadyExists => Ok(()),
            _ => Err(err),
        },
    }
}

/// Create directory with permissions.
pub async fn create_dir_with_perm<P: AsRef<Path>>(path: P, perm: u32) -> Result<()> {
    create_dir(path.as_ref()).await?;
    set_permissions(path, perm).await?;
    Ok(())
}

/// Create file with permissions.
pub async fn create_file_with_perm<P: AsRef<Path>>(path: P, perm: u32) -> Result<()> {
    File::create(path.as_ref()).await?;
    set_permissions(path, perm).await?;
    Ok(())
}

/// Read file.
pub async fn read_file<P: AsRef<Path>>(path: P) -> Result<Option<Box<[u8]>>> {
    match fs::read(path).await {
        Ok(bytes) => Ok(Some(bytes.into_boxed_slice())),
        Err(err) => match err.kind() {
            ErrorKind::NotFound => Ok(None),
            _ => Err(err),
        },
    }
}

/// Remove file.
pub async fn remove_file<P: AsRef<Path>>(path: P) -> Result<()> {
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
pub async fn atomic_symlink<P1: AsRef<Path>, P2: AsRef<Path>>(
    src: P1,
    dst: P2,
    name: &str,
) -> Result<()> {
    let dst_new = dst.as_ref().join(name.to_owned() + ".new");
    symlink(src, &dst_new).await?;
    fs::rename(&dst_new, dst.as_ref().join(name)).await?;
    Ok(())
}

/// Atomic write file.
pub async fn atomic_write_file<P: AsRef<Path>>(path: P, name: &str, bytes: &[u8]) -> Result<()> {
    let path_new = path.as_ref().join(name.to_owned() + ".new");
    let mut file = File::create(&path_new).await?;
    file.write_all(bytes).await?;
    file.sync_data().await?;
    fs::rename(&path_new, path.as_ref().join(name)).await?;
    Ok(())
}
