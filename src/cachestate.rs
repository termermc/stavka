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
    pub file: monoio::fs::File,
}

impl Drop for PendingFileBlockWrite {
    fn drop(&mut self) {
        // TODO Create async drop equivalent to close the file
    }
}

/// Creates and opens a block file for writing.
/// The file will be atomically created. An error will be returned if the file already exists.
async fn create_and_open_block_file(hash: FileBlockHash, cache_root: &Path) -> Result<monoio::fs::File, std::io::Error> {
    let seg1 = hash.get(..2).expect("BUG: hash is too short");
    let seg2 = hash.get(2..4).expect("BUG: hash is too short");
    let filename = hash.get(4..6).expect("BUG: hash is too short");

    // Try to create directories.
    let containing_dir = cache_root.join(seg1).join(seg2);
    monoio::fs::DirBuilder::new()
        .recursive(true)
        .create(&containing_dir)
        .await?;

    let file_path = containing_dir.join(filename);

    let file = monoio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(file_path)
        .await?;

    Ok(file)
}
