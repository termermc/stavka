use std::hash::Hasher;

// TODO NUKE EVERYTHING BELOW!
//
// The xxhash part is useful, though.
// Basically, turn URLs into hashes, which will be used as part of on-disk filenames.
// So, http://stavka.localhost/freemoney.mp4 would become xyz.
// From there, you can check, for example, /cache/meta.xyz (meta file) and /cache/fb.xyz.1 (block one).

/// Information about a file block.
pub struct FileBlockInfo {
    /// The block size used.
    /// This is not specific to the block, is it the size used for all blocks.
    pub(crate) block_size: u32,

    /// The block number.
    /// Block numbers start at 0.
    pub(crate) block_num: u16,
}

/// The hash of a file block.
/// Can be used as a key to identify a file block.
pub type FileBlockHash = String;

/// Creates a hash for a file block.
pub fn create_file_block_hash(path: &str, block: FileBlockInfo) -> FileBlockHash {
    // Hash with xxh3.
    let mut hasher = xxhash_rust::xxh3::Xxh3::with_seed(0);

    hasher.write(path.as_bytes());

    let hash = hasher.finish();
    let hash = hash.to_le_bytes();

    // The average length of the filename trailer.
    // The trailer includes the block size and the block number.
    const FILENAME_PREFIX: &str = ".fb";
    const FILENAME_TRAILER_AVG_LEN: usize = FILENAME_PREFIX.len() + 8 + 1 + 4; // .fb99999999-9999;
    let mut hash_str = String::with_capacity((hash.len() * 2) + FILENAME_TRAILER_AVG_LEN);

    // Start with hex.
    for byte in hash {
        let hex = format!("{:02x}", byte);
        hash_str.push_str(&hex);
    }

    // Push trailer.
    hash_str.push_str(FILENAME_PREFIX);
    hash_str.push_str(&block.block_size.to_string());
    hash_str.push('-');
    hash_str.push_str(&block.block_num.to_string());

    FileBlockHash::from(hash_str)
}
