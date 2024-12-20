use memmap2::MmapMut;
use std::fs::{File, OpenOptions};
use std::path::Path;

const PAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub struct Page {
    id: u64,
    data: MmapMut,
    dirty: bool,
}

impl Page {
    pub fn new(file: &File, id: u64) -> std::io::Result<Self> {
        let data = unsafe { MmapMut::map_mut(file)? };
        Ok(Self {
            id,
            data,
            dirty: false,
        })
    }

    pub fn write_at(&mut self, offset: usize, bytes: &[u8]) -> std::io::Result<()> {
        if offset + bytes.len() > PAGE_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Write would exceed page size",
            ));
        }
        self.data[offset..offset + bytes.len()].copy_from_slice(bytes);
        self.dirty = true;
        Ok(())
    }

    pub fn read_at(&self, offset: usize, len: usize) -> std::io::Result<&[u8]> {
        if offset + len > PAGE_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Read would exceed page size",
            ));
        }
        Ok(&self.data[offset..offset + len])
    }
}