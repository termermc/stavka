use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
use std::path::Path;
use monoio::buf::IoBufMut;
use monoio_http_client::Error;
use crate::cachestate::ObjectMetaVersion::V0;
use crate::hash::FileBlockHash;

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
}

pub struct ObjectMeta {
    pub preamble: ObjectMetaPreamble,
    pub coverage_map_offset: u64,
    pub coverage_map: Vec<bool>
}

impl ObjectMeta {
    pub fn from_bytes(buf: &[u8]) -> Result<Self, Box<dyn std::error::Error> > {
        let (preamble, _, offset) = Self::deserialize_preamble(buf)?;

        let coverage_map = &buf[offset..];

        Ok(ObjectMeta {
            preamble,
            coverage_map_offset: offset as u64,
            coverage_map: coverage_map.iter().map(|&b| b == 1).collect(),
        })
    }

    pub async fn from_file(path: &Path) -> Result<Self, Box<dyn std::error::Error> > {
        let file = monoio::fs::read(path).await?;
        Self::from_bytes(&file).map_err(|e| e.into())
    }

    fn deserialize_preamble_v1(buf: &[u8]) -> Result<(ObjectMetaPreamble, usize), String> {
        let mut offset: usize = 0;

        // exp_ts
        if buf.len() < (offset + 8) as usize {
            return Err("buffer too small for exp_ts".into());
        }
        let exp_ts = u64::from_le_bytes(
            buf[offset..offset + 8].try_into().unwrap(),
        );
        offset += 8;

        // size_bytes
        if buf.len() < offset + 8 {
            return Err("buffer too small for size_bytes".into());
        }
        let size_bytes = u64::from_le_bytes(
            buf[offset..offset + 8].try_into().unwrap(),
        );
        offset += 8;

        // block_size
        if buf.len() < offset + 4 {
            return Err("buffer too small for block_size".into());
        }
        let block_size = u32::from_le_bytes(
            buf[offset..offset + 4].try_into().unwrap(),
        );
        offset += 4;

        // headers count
        if buf.len() < offset + 2 {
            return Err("buffer too small for headers count".into());
        }
        let headers_count = u16::from_le_bytes(
            buf[offset..offset + 2].try_into().unwrap(),
        ) as usize;
        offset += 2;

        // headers
        let mut headers = Vec::with_capacity(headers_count);

        while offset < buf.len() {
            // Check if there's enough for a u16 length
            if buf.len() < offset + 2 {
                break; // stop before coverage_map
            }
            let name_len = u16::from_le_bytes(
                buf[offset..offset + 2].try_into().unwrap(),
            ) as usize;
            offset += 2;

            if buf.len() < offset + name_len {
                break;
            }
            let name =
                String::from_utf8(buf[offset..offset + name_len].to_vec())
                    .map_err(|_| "invalid utf8 in header name")?;
            offset += name_len;

            if buf.len() < offset + 2 {
                return Err("buffer ended unexpectedly in header value length".into());
            }
            let value_len = u16::from_le_bytes(
                buf[offset..offset + 2].try_into().unwrap(),
            ) as usize;
            offset += 2;

            if buf.len() < offset + value_len {
                return Err("buffer ended unexpectedly in header value".into());
            }
            let value =
                String::from_utf8(buf[offset..offset + value_len].to_vec())
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

    pub fn deserialize_preamble(buf: &[u8]) -> Result<(ObjectMetaPreamble, ObjectMetaVersion, usize), String> {
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

        let mut serial_len = VER_LEN + EXP_TS_LEN + SIZE_BYTES_LEN + BLOCK_SIZE_LEN + HEADERS_COUNT_LEN;

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

    async fn mark_block_covered(self, block_num: u64) -> Result<(), std::io::Error> {
        let byte_idx = self.coverage_map_offset + block_num;
        self.file.write_at(&[1], byte_idx).await.0.map(|_| ())
    }

    async fn close(self) -> Result<(), std::io::Error> {
        self.file.close().await
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
