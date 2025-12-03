use crate::cachestate::ObjectMetaVersion::V0;
use crate::constant::MAX_COVERAGE_BLOCK_SKIP_SIZE;
use crate::hash::FileBlockHash;
use monoio::buf::IoBufMut;
use std::cmp::max;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
use std::path::Path;

#[repr(u8)]
enum ObjectMetaVersion {
    V0,
}

impl TryFrom<u8> for ObjectMetaVersion {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(V0),
            _ => Err(()),
        }
    }
}

const OBJECT_META_SERIAL_VER: ObjectMetaVersion = V0;

pub struct ObjectMetaPreamble {
    pub exp_ts: u64,
    pub size_bytes: u64,
    pub block_size: u32,
    pub headers: Vec<(String, String)>,
    // TODO Store ETag as its own Option field?
    // We have to invalidate the cache if the ETag changes.
    // We also need to invalidate the cache if the origin reports a different size, even if the ETag is the same.
    // pub etag: Option<String>,
}

/// A loaded cache block coverage map.
struct LoadedCoverageMap(pub Vec<bool>);

impl LoadedCoverageMap {
    /// Returns whether the block at the following index is covered.
    #[inline]
    pub fn is_covered(&self, block_num: u64) -> bool {
        self.0[block_num as usize]
    }

    /// Marks the block at the following index as covered.
    /// Only does so in memory; does not write to the underlying coverage map on disk.
    #[inline]
    fn mark_covered(&mut self, block_num: u64) {
        self.0[block_num as usize] = true;
    }
}

pub struct ObjectMeta {
    pub preamble: ObjectMetaPreamble,
    pub coverage_map_offset: u64,
    pub coverage_map: LoadedCoverageMap,
}

impl ObjectMeta {
    pub fn from_bytes(buf: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let (preamble, _, offset) = Self::deserialize_preamble(buf)?;

        let coverage_map = &buf[offset..];

        Ok(ObjectMeta {
            preamble,
            coverage_map_offset: offset as u64,
            coverage_map: LoadedCoverageMap(coverage_map.iter().map(|&b| b == 1).collect()),
        })
    }

    pub async fn from_file(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let file = monoio::fs::read(path).await?;
        Self::from_bytes(&file).map_err(|e| e.into())
    }

    fn deserialize_preamble_v1(buf: &[u8]) -> Result<(ObjectMetaPreamble, usize), String> {
        let mut offset: usize = 0;

        // exp_ts
        if buf.len() < (offset + 8) as usize {
            return Err("buffer too small for exp_ts".into());
        }
        let exp_ts = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // size_bytes
        if buf.len() < offset + 8 {
            return Err("buffer too small for size_bytes".into());
        }
        let size_bytes = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // block_size
        if buf.len() < offset + 4 {
            return Err("buffer too small for block_size".into());
        }
        let block_size = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // headers count
        if buf.len() < offset + 2 {
            return Err("buffer too small for headers count".into());
        }
        let headers_count =
            u16::from_le_bytes(buf[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;

        // headers
        let mut headers = Vec::with_capacity(headers_count);

        while offset < buf.len() {
            // Check if there's enough for a u16 length
            if buf.len() < offset + 2 {
                break; // stop before coverage_map
            }
            let name_len = u16::from_le_bytes(buf[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;

            if buf.len() < offset + name_len {
                break;
            }
            let name = String::from_utf8(buf[offset..offset + name_len].to_vec())
                .map_err(|_| "invalid utf8 in header name")?;
            offset += name_len;

            if buf.len() < offset + 2 {
                return Err("buffer ended unexpectedly in header value length".into());
            }
            let value_len =
                u16::from_le_bytes(buf[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;

            if buf.len() < offset + value_len {
                return Err("buffer ended unexpectedly in header value".into());
            }
            let value = String::from_utf8(buf[offset..offset + value_len].to_vec())
                .map_err(|_| "invalid utf8 in header value")?;
            offset += value_len;

            headers.push((name, value));
        }

        Ok((
            ObjectMetaPreamble {
                exp_ts,
                size_bytes,
                block_size,
                headers,
            },
            offset, // where coverage_map begins
        ))
    }

    pub fn deserialize_preamble(
        buf: &[u8],
    ) -> Result<(ObjectMetaPreamble, ObjectMetaVersion, usize), String> {
        let mut offset = 0;

        // Version
        if buf.len() < 1 {
            return Err("buffer too small for version".into());
        }

        let version_u8 = buf[offset];
        let version: ObjectMetaVersion = version_u8.try_into().map_err(|_| "invalid version")?;

        offset += 1;

        match version {
            V0 => {
                let (preamble, offset) = Self::deserialize_preamble_v1(&buf[offset..])?;

                Ok((preamble, version, offset))
            }
        }
    }

    pub fn serialize_preamble(self) -> Vec<u8> {
        const VER_LEN: usize = size_of::<ObjectMetaVersion>();
        const EXP_TS_LEN: usize = size_of::<u64>();
        const SIZE_BYTES_LEN: usize = size_of::<u64>();
        const BLOCK_SIZE_LEN: usize = size_of::<u32>();
        const HEADERS_COUNT_LEN: usize = size_of::<u16>();

        let mut serial_len =
            VER_LEN + EXP_TS_LEN + SIZE_BYTES_LEN + BLOCK_SIZE_LEN + HEADERS_COUNT_LEN;

        const STR_PREFIX_LEN: usize = size_of::<u16>();

        let mut headers_count: u16 = 0;
        for (name, value) in &self.preamble.headers {
            serial_len += STR_PREFIX_LEN + name.len() + STR_PREFIX_LEN + value.len();
            headers_count += 1;
        }

        let mut vec = Vec::with_capacity(serial_len);

        // Version
        vec.push(OBJECT_META_SERIAL_VER as u8);

        // exp_ts
        vec.extend_from_slice(&self.preamble.exp_ts.to_le_bytes());

        // size_bytes
        vec.extend_from_slice(&self.preamble.size_bytes.to_le_bytes());

        // block_size
        vec.extend_from_slice(&self.preamble.block_size.to_le_bytes());

        // headers count
        vec.extend_from_slice(&headers_count.to_le_bytes());

        // headers
        for (name, value) in &self.preamble.headers {
            let name_bytes = name.as_bytes();
            let value_bytes = value.as_bytes();

            // u16 length prefix (little endian)
            vec.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
            vec.extend_from_slice(name_bytes);

            vec.extend_from_slice(&(value_bytes.len() as u16).to_le_bytes());
            vec.extend_from_slice(value_bytes);
        }

        debug_assert_eq!(vec.len(), serial_len);
        vec
    }
}

struct OpenObjectMeta {
    meta: ObjectMeta,
    file: monoio::fs::File,
    coverage_map_offset: u64,
}

impl OpenObjectMeta {
    async fn from_file(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let file = monoio::fs::File::open(path).await?;

        let sys_file = unsafe { std::fs::File::from_raw_fd(file.as_raw_fd()) };
        let size = sys_file.metadata()?.len() as usize;
        let _ = sys_file.into_raw_fd();

        let (res, buf) = file
            .read_exact_at(Vec::with_capacity(size).slice_mut(0..size), 0)
            .await;
        res?;

        let meta = ObjectMeta::from_bytes(&buf)?;
        let offset = meta.coverage_map_offset;

        Ok(Self {
            meta,
            file,
            coverage_map_offset: offset,
        })
    }

    async fn mark_block_covered(mut self, block_num: u64) -> Result<(), std::io::Error> {
        let byte_idx = self.coverage_map_offset + block_num;
        self.file.write_at(&[1], byte_idx).await.0.map(|_| ())?;
        self.meta.coverage_map.mark_covered(block_num as u64);
        Ok(())
    }

    async fn close(self) -> Result<(), std::io::Error> {
        self.file.close().await
    }
}

/// Creates and opens a block file for writing.
/// The file will be atomically created. An error will be returned if the file already exists.
async fn create_and_open_block_file(
    hash: FileBlockHash,
    cache_root: &Path,
) -> Result<monoio::fs::File, std::io::Error> {
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

/// The kind of file read step.
enum FileReadPlanStepKind {
    /// Read cached blocks.
    /// The starting and ending block numbers are to be read from.
    CACHE,

    /// Read from origin.
    ORIGIN {
        /// The starting byte to read from the origin (inclusive).
        byte_start: u64,

        /// The ending byte to read from the origin (inclusive).
        byte_end: u64,
    },
}

/// A file read plan step.
/// Steps instruct the caller on how to read data from the file.
/// For example, a step may be a cache read, then the next step might be an origin read.
struct FileReadPlanStep {
    /// The step kind.
    pub kind: FileReadPlanStepKind,

    /// The relevant starting block number (inclusive).
    pub block_start_num: u64,

    /// The relevant ending block number (inclusive).
    pub block_end_num: u64,

    /// The offset within the returned data to start returning data to the client.
    /// This only applies to the response to return to the client, not the origin request or cache block writes.
    client_start_offset: u64,

    /// The offset within the returned data to stop returning data to the client.
    /// This only applies to the response to return to the client, not the origin request or cache block writes.
    client_end_offset: u64,
}

/// The struct that manages the read plan for a file.
/// Each iteration instructs the caller on what type of read to make.
/// For example, if it can read from the cache for the first 10 blocks, then the first iteration will return a step that reads 10 blocks,
/// then if the next 5 blocks need to be read from origin, the second iteration will return a step that reads 5 blocks from origin.
struct FileReadPlan {
    start_byte: u64,
    end_byte: u64,
    file_size: u64,
    block_size: u64,
    coverage_map: LoadedCoverageMap,

    cur_byte: u64,
}

impl FileReadPlan {
    /// Creates a new FileReadPlan for the specified range and parameters.
    pub fn new(
        start_byte: u64,
        end_byte: u64,
        file_size: u64,
        block_size: u64,
        coverage_map: LoadedCoverageMap,
    ) -> Self {
        Self {
            start_byte,
            end_byte,
            file_size,
            block_size,
            coverage_map,
            cur_byte: start_byte,
        }
    }
}

impl Iterator for FileReadPlan {
    type Item = FileReadPlanStep;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_byte >= self.file_size - 1 {
            return None;
        }

        let start_block = self.cur_byte / self.block_size;
        let max_block = max(
            self.end_byte / self.block_size,
            (self.coverage_map.0.len() - 1) as u64,
        );

        // Bytes pulled from disk and origin operate on fixed-sized blocks.
        // However, client requests may not align with block boundaries.
        // These values store the offsets to send to the client based on the original range request.
        let client_start_offset: u64;
        let client_end_offset: u64;
        if self.cur_byte == self.start_byte {
            client_start_offset = self.start_byte % self.block_size;
        } else {
            client_start_offset = 0;
        }
        if self.end_byte - self.cur_byte < self.block_size {
            client_end_offset = self.end_byte % self.block_size;
        } else {
            client_end_offset = self.block_size - 1;
        }

        let res: FileReadPlanStep;
        let first_block_covered = self.coverage_map.0[start_block as usize];

        if first_block_covered {
            // Figure out how many cached blocks we can read consecutively.

            let mut last_covered = start_block;
            while last_covered <= max_block {
                let is_covered = self.coverage_map.0[last_covered as usize];
                if !is_covered {
                    break;
                }

                last_covered += 1;
            }

            res = FileReadPlanStep {
                kind: FileReadPlanStepKind::CACHE,
                block_start_num: start_block,
                block_end_num: last_covered,
                client_start_offset: client_start_offset,
                client_end_offset: client_end_offset,
            };
        } else {
            // Figure out how many blocks we need to fetch from origin.

            let mut last_uncovered = start_block;
            let mut skipped: u64 = 0;
            while last_uncovered <= max_block {
                let is_covered = self.coverage_map.0[last_uncovered as usize];
                if is_covered {
                    skipped += 1;

                    if skipped * self.block_size > MAX_COVERAGE_BLOCK_SKIP_SIZE {
                        // Break if it's over the acceptable coverage skip size.
                        last_uncovered -= skipped;
                        break;
                    }

                    last_uncovered += 1;
                } else {
                    skipped = 0;
                    last_uncovered += 1;
                }
            }

            res = FileReadPlanStep {
                kind: FileReadPlanStepKind::ORIGIN {
                    byte_start: start_block * self.block_size,
                    byte_end: max(last_uncovered * self.block_size, self.file_size),
                },
                block_start_num: start_block,
                block_end_num: last_uncovered,
                client_start_offset: client_start_offset,
                client_end_offset: client_end_offset,
            };
        }

        self.cur_byte = res.block_end_num * self.block_size;

        Some(res)
    }
}
