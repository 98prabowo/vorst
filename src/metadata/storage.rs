use std::sync::Arc;

use chrono::Utc;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use uuid::Uuid;

use crate::{
    blob::{ObjectLocation, ObjectOffset},
    config::MetadataStorageConfig,
    metadata::error::{Error, Result},
};

type RawUuid = [u8; 16];
type RawObjectLocation = (RawUuid, u64, u16, u32, u32); // (Segment ID, Offset, Flags, Length, Checksum)

/// Table: Object Location (The Blob Pointer)
/// Key: Object ID (The UUID of the specific data chunk)
/// Value: (Segment ID, Offset, Flags, Length, Checksum)
const OBJECT_TABLE: TableDefinition<RawUuid, RawObjectLocation> =
    TableDefinition::new("object_meta");

/// Table: File Layout (Big File Chunk Pointer)
/// Key: (File ID, Chunk Index)
/// Value: Vector ID
const FILE_TABLE: TableDefinition<(RawUuid, u32), RawUuid> = TableDefinition::new("file_layout");

/// Table: Tombstone (Deleted objects)
/// Key: File ID
/// Value: Deleted At Timestamp
const TOMBSTONE_TABLE: TableDefinition<RawUuid, i64> = TableDefinition::new("tombstone");

pub struct MetadataStorage {
    db: Arc<Database>,
    retention_policy: i64,
}

impl MetadataStorage {
    pub fn open(config: MetadataStorageConfig) -> Result<Self> {
        let db = Database::create(config.path)?;

        let write_tx = db.begin_write()?;
        {
            write_tx.open_table(OBJECT_TABLE)?;
            write_tx.open_table(FILE_TABLE)?;
            write_tx.open_table(TOMBSTONE_TABLE)?;
        }
        write_tx.commit()?;

        Ok(Self {
            db: Arc::new(db),
            retention_policy: config.retention_policy,
        })
    }

    pub fn record_multi_chunk_ingestion(
        &self,
        file_id: Uuid,
        chunks: Vec<ObjectLocation>,
    ) -> Result<()> {
        let write_tx = self.db.begin_write()?;
        {
            let mut object_table = write_tx.open_table(OBJECT_TABLE)?;
            let mut file_table = write_tx.open_table(FILE_TABLE)?;

            for (idx, location) in chunks.iter().enumerate() {
                let obj_raw = location.object_offset.object_id.into_bytes();
                let seg_raw = location.segment_id.into_bytes();
                let file_raw = file_id.into_bytes();
                let offset = location.object_offset.offset;
                let flags = location.object_offset.flags;
                let length = location.length;
                let checksum = location.checksum;

                object_table.insert(obj_raw, (seg_raw, offset, flags, length, checksum))?;
                file_table.insert((file_raw, idx as u32), obj_raw)?;
            }
        }
        write_tx.commit()?;

        Ok(())
    }

    pub fn update_locations(&self, updates: Vec<(Uuid, ObjectLocation)>) -> Result<()> {
        let write_tx = self.db.begin_write()?;
        {
            let mut object_table = write_tx.open_table(OBJECT_TABLE)?;
            for (id, location) in updates {
                let obj_raw = id.into_bytes();
                let seg_raw = location.segment_id.into_bytes();
                let offset = location.object_offset.offset;
                let flags = location.object_offset.flags;
                let length = location.length;
                let checksum = location.checksum;
                object_table.insert(obj_raw, (seg_raw, offset, flags, length, checksum))?;
            }
        }
        write_tx.commit()?;

        Ok(())
    }

    pub fn get_file_object_ids(&self, file_id: Uuid) -> Result<Vec<Uuid>> {
        let read_tx = self.db.begin_read()?;
        let file_table = read_tx.open_table(FILE_TABLE)?;

        let file_raw = file_id.into_bytes();

        let range = file_table.range((file_raw, 0)..(file_raw, u32::MAX))?;

        let mut object_ids = Vec::new();
        for entry in range {
            let (_key, value_guard) = entry?;
            let obj_id = Uuid::from_bytes(value_guard.value());
            object_ids.push(obj_id);
        }

        Ok(object_ids)
    }

    pub fn get_object_location(&self, object_id: Uuid) -> Result<ObjectLocation> {
        let read_tx = self.db.begin_read()?;
        let object_table = read_tx.open_table(OBJECT_TABLE)?;

        let guard = object_table
            .get(object_id.into_bytes())?
            .ok_or(Error::ObjectNotFound(object_id))?;

        let loc = guard.value();

        Ok(ObjectLocation {
            segment_id: Uuid::from_bytes(loc.0),
            object_offset: ObjectOffset {
                object_id,
                offset: loc.1,
                flags: loc.2,
            },
            length: loc.3,
            checksum: loc.4,
        })
    }

    pub fn delete_file(&self, file_id: Uuid) -> Result<()> {
        let write_tx = self.db.begin_write()?;
        {
            let file_table = write_tx.open_table(FILE_TABLE)?;
            let mut tombstone_table = write_tx.open_table(TOMBSTONE_TABLE)?;

            let file_raw = file_id.into_bytes();

            let mut range = file_table.range((file_raw, 0)..(file_raw, u32::MAX))?;
            if range.next().is_none() {
                return Err(Error::FileNotFound(file_id));
            }

            let now = Utc::now().timestamp();
            tombstone_table.insert(file_raw, now)?;
        }
        write_tx.commit()?;
        Ok(())
    }

    pub fn get_expired_tombstones(&self) -> Result<Vec<Uuid>> {
        let read_tx = self.db.begin_read()?;
        let tombstone_table = read_tx.open_table(TOMBSTONE_TABLE)?;

        let now = Utc::now().timestamp();
        let mut expired = Vec::new();

        for entry in tombstone_table.iter()? {
            let (file_id_bytes, deleted_at_guard) = entry?;
            let deleted_at = deleted_at_guard.value();

            if now - deleted_at >= self.retention_policy {
                expired.push(Uuid::from_bytes(file_id_bytes.value()));
            }
        }

        Ok(expired)
    }

    pub fn cleanup_tombstoned_file(&self, file_id: Uuid) -> Result<()> {
        let write_tx = self.db.begin_write()?;
        {
            let mut object_table = write_tx.open_table(OBJECT_TABLE)?;
            let mut file_table = write_tx.open_table(FILE_TABLE)?;
            let mut tombstone_table = write_tx.open_table(TOMBSTONE_TABLE)?;

            let file_raw = file_id.into_bytes();

            let range = file_table.range((file_raw, 0)..(file_raw, u32::MAX))?;
            let keys: Vec<(RawUuid, u32)> = range
                .map(|item| item.map(|(k, _)| k.value()).map_err(redb::Error::from))
                .collect::<std::result::Result<Vec<_>, redb::Error>>()?;

            for key in keys {
                if let Some(object_id_guard) = file_table.remove(key)? {
                    let object_id = object_id_guard.value();
                    object_table.remove(object_id)?;
                }
            }

            tombstone_table.remove(file_id.into_bytes())?;
        }
        write_tx.commit()?;
        Ok(())
    }

    pub fn check_liveliness(
        &self,
        file_id: Uuid,
        segment_id: Uuid,
        object_id: Uuid,
        offset: u64,
    ) -> Result<bool> {
        let read_tx = self.db.begin_read()?;
        let object_table = read_tx.open_table(OBJECT_TABLE)?;
        let tombstone_table = read_tx.open_table(TOMBSTONE_TABLE)?;

        if let Some(deleted_at_guard) = tombstone_table.get(file_id.into_bytes())? {
            let deleted_at = deleted_at_guard.value();
            let age = Utc::now().timestamp() - deleted_at;

            if age >= self.retention_policy {
                return Ok(false);
            }
        }

        if let Some(guard) = object_table.get(object_id.into_bytes())? {
            let (stored_segment_id, stored_offset, _, _, _) = guard.value();
            Ok(stored_segment_id == segment_id.into_bytes() && stored_offset == offset)
        } else {
            Ok(false)
        }
    }
}
