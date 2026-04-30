use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use bytemuck::{Pod, Zeroable};
use chrono::Utc;

pub const MAGIC_NUMBER: u32 = 0xDEADBEEF;
pub const MAX_VECTOR_SIZE: usize = 10_000;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

impl TryFrom<u8> for Operation {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Operation::Insert),
            1 => Ok(Operation::Update),
            2 => Ok(Operation::Delete),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid operation",
            )),
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
struct LogHeader {
    timestamp: i64,
    vector_id: u64,
    magic: u32,
    vector_len: u32,
    checksum: u32,
    operation: u8,
    _padding: [u8; 3],
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub timestamp: i64,
    pub id: u64,
    pub vector: Vec<f32>,
    pub operation: Operation,
}

pub struct Wal {
    path: PathBuf,
    writer: Arc<Mutex<WalWriter>>,
}

impl Wal {
    pub fn new<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let writer = WalWriter::new(&path)?;
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    pub fn append(&self, id: u64, vector: &[f32], operation: Operation) -> io::Result<()> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| io::Error::other("WAL writer's lock is poisoned"))?;
        writer.append(id, vector, operation)
    }

    pub fn get_reader(&self) -> io::Result<WalReader> {
        WalReader::new(&self.path)
    }
}

struct WalWriter {
    writer: BufWriter<File>,
}

impl WalWriter {
    fn new<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        let writer = BufWriter::new(file);
        Ok(Self { writer })
    }

    fn append(&mut self, id: u64, vector: &[f32], operation: Operation) -> io::Result<()> {
        let now = Utc::now().timestamp();
        let payload = bytemuck::cast_slice(vector);
        let checksum = Hasher::hash(now, id, vector.len() as u32, operation as u8, payload);

        let header = LogHeader {
            timestamp: now,
            vector_id: id,
            magic: MAGIC_NUMBER,
            vector_len: vector.len() as u32,
            checksum,
            operation: operation as u8,
            _padding: [0; 3],
        };
        let header_bytes = bytemuck::bytes_of(&header);

        let mut combined = Vec::with_capacity(header_bytes.len() + payload.len());
        combined.extend_from_slice(header_bytes);
        combined.extend_from_slice(payload);
        self.writer.write_all(&combined)?;

        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;

        Ok(())
    }
}

struct WalReader {
    reader: BufReader<File>,
}

impl WalReader {
    fn new<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = File::open(path)?;
        let buffer = BufReader::new(file);
        Ok(Self { reader: buffer })
    }

    fn read_next_entry(&mut self) -> io::Result<(LogHeader, Vec<f32>)> {
        let mut header_bytes = [0u8; size_of::<LogHeader>()];
        self.reader.read_exact(&mut header_bytes)?;
        let header: LogHeader = *bytemuck::from_bytes(&header_bytes);

        if header.magic != MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic bytes",
            ));
        }

        let vector_len = header.vector_len as usize;
        if vector_len > MAX_VECTOR_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "vector_len exceeds sanity limit",
            ));
        }

        let payload_len = size_of::<f32>() * vector_len;
        let mut payload = vec![0u8; payload_len];
        self.reader.read_exact(&mut payload)?;

        let checksum = Hasher::hash(
            header.timestamp,
            header.vector_id,
            header.vector_len,
            header.operation,
            &payload,
        );

        if checksum != header.checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "checksum mismatch",
            ));
        }

        let vector: &[f32] = bytemuck::try_cast_slice(&payload).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "memory alignment error during vector recovery",
            )
        })?;

        Ok((header, vector.to_vec()))
    }
}

impl Iterator for WalReader {
    type Item = io::Result<LogEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next_entry() {
            Ok((header, vector)) => Operation::try_from(header.operation).ok().map(|operation| {
                Ok(LogEntry {
                    timestamp: header.timestamp,
                    id: header.vector_id,
                    vector,
                    operation,
                })
            }),
            Err(e) => {
                if e.kind() == ErrorKind::UnexpectedEof {
                    None
                } else {
                    Some(Err(e))
                }
            }
        }
    }
}

struct Hasher;

impl Hasher {
    fn hash(timestamp: i64, vector_id: u64, vector_len: u32, operation: u8, payload: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&vector_id.to_le_bytes());
        hasher.update(&vector_len.to_le_bytes());
        hasher.update(&[operation]);
        hasher.update(payload);
        hasher.finalize()
    }
}
