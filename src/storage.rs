use std::fs::{File, OpenOptions};
use std::io::{Read, Result, Seek, SeekFrom, Write};

pub const PAGE_SIZE: usize = 4096;

pub type PageId = u32;

// A Page is just a raw array of bytes.
// We derive Clone and Copy because it's just data.
#[derive(Debug, Clone, Copy)]
pub struct Page {
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new() -> Self {
        Self {
            data: [0; PAGE_SIZE],
        }
    }
}

pub struct DiskManager {
    file: File,
    next_page_id: PageId,
}

impl DiskManager {
    /// Opens or creates a database file
    pub fn open(path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        // Calculate next_page_id from file length
        let file_len = file.metadata()?.len();
        let next_page_id = (file_len / PAGE_SIZE as u64) as PageId;

        Ok(Self { file, next_page_id })
    }

    /// Reads a page from disk into memory
    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        let mut page = Page::new();
        self.file.read_exact(&mut page.data)?;

        Ok(page)
    }

    /// Writes a page from memory to disk
    pub fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<()> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page.data)?;
        self.file.flush()?;

        Ok(())
    }

    /// Allocates a new page and returns its ID
    pub fn allocate_page(&mut self) -> PageId {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        page_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn with_test_file<F>(name: &str, f: F)
    where
        F: FnOnce(&str),
    {
        let path = format!("test_{}.db", name);
        let _ = fs::remove_file(&path);
        f(&path);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_write_and_read_page() {
        with_test_file("write_read", |path| {
            let mut dm = DiskManager::open(path).unwrap();

            let page_id = dm.allocate_page();
            let mut page = Page::new();
            page.data[0] = 42;
            page.data[1] = 123;
            page.data[4095] = 255;

            dm.write_page(page_id, &page).unwrap();

            let read_page = dm.read_page(page_id).unwrap();

            assert_eq!(read_page.data[0], 42);
            assert_eq!(read_page.data[1], 123);
            assert_eq!(read_page.data[4095], 255);
        });
    }

    #[test]
    fn test_multiple_pages() {
        with_test_file("multiple", |path| {
            let mut dm = DiskManager::open(path).unwrap();

            let page_id_0 = dm.allocate_page();
            let page_id_1 = dm.allocate_page();

            let mut page0 = Page::new();
            page0.data[0] = 11;

            let mut page1 = Page::new();
            page1.data[0] = 22;

            dm.write_page(page_id_0, &page0).unwrap();
            dm.write_page(page_id_1, &page1).unwrap();

            let read0 = dm.read_page(page_id_0).unwrap();
            let read1 = dm.read_page(page_id_1).unwrap();

            assert_eq!(read0.data[0], 11);
            assert_eq!(read1.data[0], 22);
        });
    }

    #[test]
    fn test_persistence_across_reopen() {
        with_test_file("persist", |path| {
            // First session: write data
            {
                let mut dm = DiskManager::open(path).unwrap();
                let page_id = dm.allocate_page();

                let mut page = Page::new();
                page.data[0] = 99;

                dm.write_page(page_id, &page).unwrap();
                assert_eq!(dm.next_page_id, 1);
            }

            // Second session: read data back
            {
                let mut dm = DiskManager::open(path).unwrap();
                assert_eq!(dm.next_page_id, 1);

                let page = dm.read_page(0).unwrap();
                assert_eq!(page.data[0], 99);
            }
        });
    }

    #[test]
    fn test_allocate_page_increments() {
        with_test_file("allocate", |path| {
            let mut dm = DiskManager::open(path).unwrap();

            assert_eq!(dm.allocate_page(), 0);
            assert_eq!(dm.allocate_page(), 1);
            assert_eq!(dm.allocate_page(), 2);
            assert_eq!(dm.next_page_id, 3);
        });
    }

    #[test]
    fn test_overwrite_page() {
        with_test_file("overwrite", |path| {
            let mut dm = DiskManager::open(path).unwrap();

            let page_id = dm.allocate_page();

            let mut page = Page::new();
            page.data[0] = 1;
            dm.write_page(page_id, &page).unwrap();

            page.data[0] = 2;
            dm.write_page(page_id, &page).unwrap();

            let read_page = dm.read_page(page_id).unwrap();
            assert_eq!(read_page.data[0], 2);
        });
    }
}
