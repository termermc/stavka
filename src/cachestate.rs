use std::path::Path;
use crate::hash::FileBlockHash;

struct CacheState {
    // TODO Store a lockless map (find a good implementation) with keys being file block keys.
    // When a request comes in, just transparently proxy it if there's a pending write; the next
    // request will most likely hit the cache.
}

struct PendingFileBlockWrite {
    // Include the domain here? Maybe in the key? We'll figure it out.

    pub key: FileBlockHash,
    pub file: tokio_uring::fs::File,
}

impl Drop for PendingFileBlockWrite {
    fn drop(&mut self) {
        // TODO Close file and remove key from pending
    }
}

/// Creates and opens a block file for writing.
/// The file will be atomically created. An error will be returned if the file already exists.
async fn create_and_open_block_file(hash: FileBlockHash, cache_root: &Path) -> Result<tokio_uring::fs::File, std::io::Error> {
    let seg1 = hash.get(..2).expect("BUG: hash is too short");
    let seg2 = hash.get(2..4).expect("BUG: hash is too short");
    let filename = hash.get(4..6).expect("BUG: hash is too short");

    // Try to create directories.
    let containing_dir = cache_root.join(seg1).join(seg2);
    tokio_uring::fs::create_dir_all(&containing_dir).await?;

    let file_path = containing_dir.join(filename);

    let file = tokio_uring::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(file_path)
        .await?;

    Ok(file)
}
