use std::io::{self, Write, Seek, SeekFrom};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use std::sync::Arc;
use crate::error::Error;

pub const PAGE_SIZE: usize = 4096;
const PAGE_HEADER_SIZE: usize = 64;
const SLOT_SIZE: usize = 8;

/// Layout of a page in memory and on disk
/// +----------------+----------------+----------------+----------------+
/// |    Header     |  Slot Array    |  Free Space   |     Data      |
/// +----------------+----------------+----------------+----------------+
/// |     64B       |    Dynamic     |    Dynamic    |    Dynamic    |
/// 
/// Header (64 bytes):
/// - page_id: u64 (8 bytes)
/// - prev_page: u64 (8 bytes)
/// - next_page: u64 (8 bytes)
/// - free_space_offset: u16 (2 bytes)
/// - slot_count: u16 (2 bytes)
/// - checksum: u32 (4 bytes)
/// - flags: u8 (1 byte)
/// - page_type: u8 (1 byte)
/// - reserved: [u8; 30] (30 bytes)

#[derive(Debug)]
pub struct Page {
    id: PageId,
    data: Vec<u8>,
    dirty: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PageId {
    pub file_id: u64,
    pub page_num: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct Slot {
    offset: u16,
    length: u16,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum PageType {
    Data = 0,
    Index = 1,
    Overflow = 2,
    Free = 3,
}

impl Page {
    /// Create a new page with the given ID
    pub fn new(id: PageId, data: Vec<u8>) -> Self {
        let mut page = Self {
            id,
            data: vec![0; PAGE_SIZE],
            dirty: false,
        };

        // Initialize header
        page.set_page_id(id.page_num);
        page.set_prev_page(0);
        page.set_next_page(0);
        page.set_free_space_offset(PAGE_HEADER_SIZE as u16);
        page.set_slot_count(0);
        page.set_page_type(PageType::Data);
        page.set_flags(0);

        // Copy initial data if provided
        if !data.is_empty() {
            let len = data.len().min(PAGE_SIZE - PAGE_HEADER_SIZE);
            page.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + len]
                .copy_from_slice(&data[..len]);
            page.set_free_space_offset((PAGE_HEADER_SIZE + len) as u16);
        }

        page.update_checksum();
        page
    }

    /// Read a value from the given offset
    pub fn read_at(&self, offset: usize, len: usize) -> Result<&[u8], Error> {
        if offset + len > self.data.len() {
            return Err(Error::Storage("Read beyond page bounds".into()));
        }
        Ok(&self.data[offset..offset + len])
    }

    /// Write a value at the given offset
    pub fn write_at(&mut self, offset: usize, data: &[u8]) -> Result<(), Error> {
        if offset + data.len() > self.data.len() {
            return Err(Error::Storage("Write beyond page bounds".into()));
        }
        self.data[offset..offset + data.len()].copy_from_slice(data);
        self.dirty = true;
        Ok(())
    }

    /// Insert a new record, returns the slot ID
    pub fn insert_record(&mut self, data: &[u8]) -> Result<u16, Error> {
        let required_space = data.len() + SLOT_SIZE;
        let free_space = self.get_free_space();
        
        if required_space > free_space {
            return Err(Error::Storage("Insufficient space in page".into()));
        }

        // Get current positions
        let free_space_offset = self.get_free_space_offset();
        let slot_count = self.get_slot_count();

        // Create new slot
        let slot = Slot {
            offset: free_space_offset,
            length: data.len() as u16,
        };

        // Write data
        self.write_at(free_space_offset as usize, data)?;
        
        // Add slot entry
        self.write_slot(slot_count, slot)?;
        
        // Update header
        self.set_free_space_offset(free_space_offset + data.len() as u16);
        self.set_slot_count(slot_count + 1);
        self.update_checksum();
        
        Ok(slot_count)
    }

    /// Read a record by its slot ID
    pub fn read_record(&self, slot_id: u16) -> Result<&[u8], Error> {
        let slot = self.read_slot(slot_id)?;
        self.read_at(slot.offset as usize, slot.length as usize)
    }

    /// Update a record by its slot ID
    pub fn update_record(&mut self, slot_id: u16, data: &[u8]) -> Result<(), Error> {
        let slot = self.read_slot(slot_id)?;
        
        if data.len() as u16 <= slot.length {
            // Update in place if new data fits
            self.write_at(slot.offset as usize, data)?;
            
            // Mark any leftover space as free
            if data.len() as u16 < slot.length {
                // TODO: Implement free space management
            }
        } else {
            // Need to relocate record
            let new_slot = self.insert_record(data)?;
            self.delete_record(slot_id)?;
            // Update slot ID references
            // TODO: Implement slot ID reference updating
        }
        
        self.update_checksum();
        Ok(())
    }

    /// Delete a record by its slot ID
    pub fn delete_record(&mut self, slot_id: u16) -> Result<(), Error> {
        let slot = self.read_slot(slot_id)?;
        
        // Mark slot as deleted
        let deleted_slot = Slot {
            offset: 0,
            length: 0,
        };
        self.write_slot(slot_id, deleted_slot)?;
        
        // Add space to free list
        // TODO: Implement free space management
        
        self.update_checksum();
        Ok(())
    }

    /// Get the amount of free space available
    pub fn get_free_space(&self) -> usize {
        let free_space_offset = self.get_free_space_offset() as usize;
        let slot_array_size = self.get_slot_count() as usize * SLOT_SIZE;
        PAGE_SIZE - free_space_offset - slot_array_size
    }

    /// Compact the page by removing deleted records and consolidating free space
    pub fn compact(&mut self) -> Result<(), Error> {
        let mut new_data = vec![0; PAGE_SIZE];
        let mut new_offset = PAGE_HEADER_SIZE;
        let slot_count = self.get_slot_count();
        
        // Copy header
        new_data[..PAGE_HEADER_SIZE].copy_from_slice(&self.data[..PAGE_HEADER_SIZE]);
        
        // Relocate valid records
        for slot_id in 0..slot_count {
            let slot = self.read_slot(slot_id)?;
            if slot.length > 0 {  // Not deleted
                let data = self.read_at(slot.offset as usize, slot.length as usize)?;
                new_data[new_offset..new_offset + data.len()].copy_from_slice(data);
                
                // Update slot
                let new_slot = Slot {
                    offset: new_offset as u16,
                    length: slot.length,
                };
                self.write_slot(slot_id, new_slot)?;
                
                new_offset += data.len();
            }
        }
        
        // Update page data and header
        self.data = new_data;
        self.set_free_space_offset(new_offset as u16);
        self.update_checksum();
        
        Ok(())
    }

    /// Flush page to disk
    pub async fn flush<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<(), Error> {
        writer.write_all(&self.data).await?;
        writer.flush().await?;
        Ok(())
    }

    // Helper methods for header access

    fn get_page_id(&self) -> u64 {
        u64::from_le_bytes(self.data[0..8].try_into().unwrap())
    }

    fn set_page_id(&mut self, id: u64) {
        self.data[0..8].copy_from_slice(&id.to_le_bytes());
        self.dirty = true;
    }

    fn get_prev_page(&self) -> u64 {
        u64::from_le_bytes(self.data[8..16].try_into().unwrap())
    }

    fn set_prev_page(&mut self, id: u64) {
        self.data[8..16].copy_from_slice(&id.to_le_bytes());
        self.dirty = true;
    }

    fn get_next_page(&self) -> u64 {
        u64::from_le_bytes(self.data[16..24].try_into().unwrap())
    }

    fn set_next_page(&mut self, id: u64) {
        self.data[16..24].copy_from_slice(&id.to_le_bytes());
        self.dirty = true;
    }

    fn get_free_space_offset(&self) -> u16 {
        u16::from_le_bytes(self.data[24..26].try_into().unwrap())
    }

    fn set_free_space_offset(&mut self, offset: u16) {
        self.data[24..26].copy_from_slice(&offset.to_le_bytes());
        self.dirty = true;
    }

    fn get_slot_count(&self) -> u16 {
        u16::from_le_bytes(self.data[26..28].try_into().unwrap())
    }

    fn set_slot_count(&mut self, count: u16) {
        self.data[26..28].copy_from_slice(&count.to_le_bytes());
        self.dirty = true;
    }

    fn get_checksum(&self) -> u32 {
        u32::from_le_bytes(self.data[28..32].try_into().unwrap())
    }

    fn set_checksum(&mut self, checksum: u32) {
        self.data[28..32].copy_from_slice(&checksum.to_le_bytes());
        self.dirty = true;
    }

    fn get_flags(&self) -> u8 {
        self.data[32]
    }

    fn set_flags(&mut self, flags: u8) {
        self.data[32] = flags;
        self.dirty = true;
    }

    fn get_page_type(&self) -> PageType {
        match self.data[33] {
            0 => PageType::Data,
            1 => PageType::Index,
            2 => PageType::Overflow,
            3 => PageType::Free,
            _ => PageType::Data,
        }
    }

    fn set_page_type(&mut self, page_type: PageType) {
        self.data[33] = page_type as u8;
        self.dirty = true;
    }

    // Slot array management

    fn read_slot(&self, slot_id: u16) -> Result<Slot, Error> {
        if slot_id >= self.get_slot_count() {
            return Err(Error::Storage("Invalid slot ID".into()));
        }
        
        let offset = PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE;
        Ok(Slot {
            offset: u16::from_le_bytes(self.data[offset..offset + 2].try_into().unwrap()),
            length: u16::from_le_bytes(self.data[offset + 2..offset + 4].try_into().unwrap()),
        })
    }

    fn write_slot(&mut self, slot_id: u16, slot: Slot) -> Result<(), Error> {
        if slot_id > self.get_slot_count() {
            return Err(Error::Storage("Invalid slot ID".into()));
        }
        
        let offset = PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE;
        self.data[offset..offset + 2].copy_from_slice(&slot.offset.to_le_bytes());
        self.data[offset + 2..offset + 4].copy_from_slice(&slot.length.to_le_bytes());
        self.dirty = true;
        Ok(())
    }

    fn update_checksum(&mut self) {
        // Simple checksum: XOR all 4-byte chunks
        let mut checksum = 0u32;
        for chunk in self.data.chunks(4) {
            let chunk_bytes = if chunk.len() == 4 {
                chunk.try_into().unwrap()
            } else {
                let mut padded = [0u8; 4];
                padded[..chunk.len()].copy_from_slice(chunk);
                padded
            };
            checksum ^= u32::from_le_bytes(chunk_bytes);
        }
        self.set_checksum(checksum);
    }

    pub fn verify_checksum(&self) -> bool {
        let stored = self.get_checksum();
        let mut calculated = 0u32;
        
        // Zero out checksum field for calculation
        let mut data = self.data.clone();
        data[28..32].copy_from_slice(&[0; 4]);
        
        for chunk in data.chunks(4) {
            let chunk_bytes = if chunk.len() == 4 {
                chunk.try_into().unwrap()
            } else {
                let mut padded = [0u8; 4];
                padded[..chunk.len()].copy_from_slice(chunk);
                padded
            };
            calculated ^= u32::from_le_bytes(chunk_bytes);
        }
        
        stored == calculated
    }
}
