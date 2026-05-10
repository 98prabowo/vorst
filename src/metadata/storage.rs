use std::sync::Arc;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use uuid::Uuid;

use crate::{
    blob::{ObjectLocation, ObjectOffset},
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

pub struct MetadataStorage {
    db: Arc<Database>,
}

impl MetadataStorage {
    pub fn open(path: &str) -> Result<Self> {
        let db = Database::create(path)?;

        let write_tx = db.begin_write()?;
        {
            write_tx.open_table(OBJECT_TABLE)?;
            write_tx.open_table(FILE_TABLE)?;
        }
        write_tx.commit()?;

        Ok(Self { db: Arc::new(db) })
    }

    pub fn record_ingestion(
        &self,
        file_id: Uuid,
        chunk_idx: u32,
        location: ObjectLocation,
    ) -> Result<()> {
        let write_tx = self.db.begin_write()?;
        {
            let mut obj_table = write_tx.open_table(OBJECT_TABLE)?;
            let mut file_table = write_tx.open_table(FILE_TABLE)?;

            let obj_raw = location.object_offset.object_id.into_bytes();
            let seg_raw = location.segment_id.into_bytes();
            let file_raw = file_id.into_bytes();
            let offset = location.object_offset.offset;
            let flags = location.object_offset.flags;
            let length = location.length;
            let checksum = location.checksum;

            obj_table.insert(obj_raw, (seg_raw, offset, flags, length, checksum))?;
            file_table.insert((file_raw, chunk_idx), obj_raw)?;
        }
        write_tx.commit()?;

        Ok(())
    }

    pub fn record_multi_chunk_ingestion(
        &self,
        file_id: Uuid,
        chunks: Vec<ObjectLocation>,
    ) -> Result<()> {
        let write_tx = self.db.begin_write()?;
        {
            let mut obj_table = write_tx.open_table(OBJECT_TABLE)?;
            let mut file_table = write_tx.open_table(FILE_TABLE)?;

            for (idx, location) in chunks.iter().enumerate() {
                let obj_raw = location.object_offset.object_id.into_bytes();
                let seg_raw = location.segment_id.into_bytes();
                let file_raw = file_id.into_bytes();
                let offset = location.object_offset.offset;
                let flags = location.object_offset.flags;
                let length = location.length;
                let checksum = location.checksum;

                obj_table.insert(obj_raw, (seg_raw, offset, flags, length, checksum))?;
                file_table.insert((file_raw, idx as u32), obj_raw)?;
            }
        }
        write_tx.commit()?;

        Ok(())
    }

    pub fn update_locations(&self, updates: Vec<(Uuid, ObjectLocation)>) -> Result<()> {
        let write_tx = self.db.begin_write()?;
        {
            let mut obj_table = write_tx.open_table(OBJECT_TABLE)?;
            for (id, location) in updates {
                let obj_raw = id.into_bytes();
                let seg_raw = location.segment_id.into_bytes();
                let offset = location.object_offset.offset;
                let flags = location.object_offset.flags;
                let length = location.length;
                let checksum = location.checksum;
                obj_table.insert(obj_raw, (seg_raw, offset, flags, length, checksum))?;
            }
        }
        write_tx.commit()?;

        Ok(())
    }

    pub fn delete_file(&self, file_id: Uuid) -> Result<Vec<Uuid>> {
        let write_tx = self.db.begin_write()?;
        let mut deleted_objects = Vec::new();
        {
            let mut obj_table = write_tx.open_table(OBJECT_TABLE)?;
            let mut file_table = write_tx.open_table(FILE_TABLE)?;

            let file_raw = file_id.into_bytes();
            let range = file_table.range((file_raw, 0)..(file_raw, u32::MAX))?;

            let keys: Vec<(RawUuid, u32)> = range.map(|r| r.unwrap().0.value()).collect();

            for key in keys {
                if let Some(obj_id_guard) = file_table.remove(key)? {
                    let obj_id = obj_id_guard.value();
                    obj_table.remove(obj_id)?;
                    deleted_objects.push(Uuid::from_bytes(obj_id));
                }
            }
        }

        if deleted_objects.is_empty() {
            return Err(Error::FileNotFound(file_id));
        }

        write_tx.commit()?;
        Ok(deleted_objects)
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
        let table = read_tx.open_table(OBJECT_TABLE)?;

        let guard = table
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

    pub fn check_liveliness(&self, object_id: Uuid, segment_id: Uuid, offset: u64) -> Result<bool> {
        let read_tx = self.db.begin_read()?;
        let table = read_tx.open_table(OBJECT_TABLE)?;

        if let Some(guard) = table.get(object_id.into_bytes())? {
            let (curr_seg, curr_offset, _, _, _) = guard.value();
            Ok(curr_seg == segment_id.into_bytes() && curr_offset == offset)
        } else {
            Ok(false)
        }
    }
}
