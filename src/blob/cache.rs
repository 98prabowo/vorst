use std::{fs::File, io, num::NonZeroUsize, path::Path, sync::Mutex};

use lru::LruCache;
use uuid::Uuid;

struct FileCache {
    cache: Mutex<LruCache<Uuid, File>>,
}

impl FileCache {
    pub fn new(capacity: usize) -> io::Result<Self> {
        let cap =
            NonZeroUsize::new(capacity).ok_or_else(|| io::Error::other("invalid capacity"))?;
        Ok(Self {
            cache: Mutex::new(LruCache::new(cap)),
        })
    }

    fn open<P>(&self, id: Uuid, path: P) -> io::Result<File>
    where
        P: AsRef<Path>,
    {
        let mut cache = self
            .cache
            .lock()
            .map_err(|e| io::Error::other(format!("cache poisoned: {e}")))?;

        if let Some(file) = cache.get(&id) {
            return file.try_clone();
        }

        let file_to_cache = File::open(path)?;
        let file_to_return = file_to_cache.try_clone()?;

        cache.put(id, file_to_cache);

        Ok(file_to_return)
    }
}

pub struct FileCachePool {
    shards: Vec<FileCache>,
    shard_mask: usize,
}

impl FileCachePool {
    pub fn new(num_shards: usize, capacity_per_shard: usize) -> io::Result<Self> {
        if num_shards == 0 || (num_shards & (num_shards - 1)) != 0 {
            return Err(io::Error::other("num_shards must be a power of two"));
        }

        let shards: Vec<FileCache> = (0..num_shards)
            .map(|_| FileCache::new(capacity_per_shard))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            shards,
            shard_mask: num_shards - 1,
        })
    }

    pub fn get<P>(&self, id: Uuid, path: P) -> io::Result<File>
    where
        P: AsRef<Path>,
    {
        let hash = id.as_u128() as usize;
        let shard_idx = hash & self.shard_mask;
        self.shards[shard_idx].open(id, path)
    }
}
