use crate::storage::{DiskManager, Page, PageId};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Result;
use std::rc::Rc;

pub struct BufferPool {
    disk: DiskManager,
    pages: HashMap<PageId, Rc<RefCell<Page>>>,
    capacity: usize,
}

impl BufferPool {
    pub fn new(disk: DiskManager, capacity: usize) -> Self {
        Self {
            disk,
            pages: HashMap::new(),
            capacity,
        }
    }

    /// Fetch a page from the buffer pool, reading from disk if not cached
    pub fn fetch_page(&mut self, page_id: PageId) -> Result<Rc<RefCell<Page>>> {
        // Cache hit
        if let Some(page) = self.pages.get(&page_id) {
            return Ok(Rc::clone(page));
        }

        // Cache miss - need to load from disk
        self.evict_if_needed();

        let page = self.disk.read_page(page_id)?;
        let page_rc = Rc::new(RefCell::new(page));

        self.pages.insert(page_id, Rc::clone(&page_rc));

        Ok(page_rc)
    }

    /// Create a new page in the buffer pool
    pub fn create_page(&mut self) -> Result<(PageId, Rc<RefCell<Page>>)> {
        self.evict_if_needed();

        let page_id = self.disk.allocate_page();
        let page = Page::new();
        let page_rc = Rc::new(RefCell::new(page));

        self.pages.insert(page_id, Rc::clone(&page_rc));

        Ok((page_id, page_rc))
    }

    /// Evict a page if we're at capacity
    fn evict_if_needed(&mut self) {
        if self.pages.len() >= self.capacity {
            // Simple eviction: remove the first page we find
            // A real database would use LRU or Clock algorithm
            if let Some(&page_id) = self.pages.keys().next() {
                self.evict_page(page_id);
            }
        }
    }

    /// Evict a specific page, writing it to disk first
    fn evict_page(&mut self, page_id: PageId) {
        if let Some(page_rc) = self.pages.remove(&page_id) {
            // Write to disk before evicting
            // In a real DB, we'd check if it's dirty first
            let page = page_rc.borrow();
            let _ = self.disk.write_page(page_id, &page);
        }
    }

    /// Flush all pages to disk
    pub fn flush_all(&mut self) -> Result<()> {
        for (&page_id, page_rc) in &self.pages {
            let page = page_rc.borrow();
            self.disk.write_page(page_id, &page)?;
        }
        Ok(())
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
        let path = format!("test_buffer_{}.db", name);
        let _ = fs::remove_file(&path);
        f(&path);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_create_and_fetch_page() {
        with_test_file("create_fetch", |path| {
            let disk = DiskManager::open(path).unwrap();
            let mut pool = BufferPool::new(disk, 10);

            // Create a new page
            let (page_id, page_rc) = pool.create_page().unwrap();
            assert_eq!(page_id, 0);

            // Modify the page
            {
                let mut page = page_rc.borrow_mut();
                page.data[0] = 42;
                page.data[100] = 99;
            }

            // Fetch the same page - should get cached version
            let fetched = pool.fetch_page(page_id).unwrap();
            {
                let page = fetched.borrow();
                assert_eq!(page.data[0], 42);
                assert_eq!(page.data[100], 99);
            }
        });
    }

    #[test]
    fn test_cache_hit() {
        with_test_file("cache_hit", |path| {
            let disk = DiskManager::open(path).unwrap();
            let mut pool = BufferPool::new(disk, 10);

            let (page_id, page_rc) = pool.create_page().unwrap();

            // Modify via first reference
            {
                let mut page = page_rc.borrow_mut();
                page.data[0] = 123;
            }

            // Fetch again - should be same Rc (cache hit)
            let fetched = pool.fetch_page(page_id).unwrap();

            // Both Rcs point to the same data
            assert!(Rc::ptr_eq(&page_rc, &fetched));

            // Modification should be visible
            assert_eq!(fetched.borrow().data[0], 123);
        });
    }

    #[test]
    fn test_flush_and_reload() {
        with_test_file("flush_reload", |path| {
            // First session: create and modify a page
            {
                let disk = DiskManager::open(path).unwrap();
                let mut pool = BufferPool::new(disk, 10);

                let (page_id, page_rc) = pool.create_page().unwrap();
                assert_eq!(page_id, 0);

                {
                    let mut page = page_rc.borrow_mut();
                    page.data[0] = 77;
                    page.data[4095] = 88;
                }

                pool.flush_all().unwrap();
            }

            // Second session: reload from disk
            {
                let disk = DiskManager::open(path).unwrap();
                let mut pool = BufferPool::new(disk, 10);

                let page_rc = pool.fetch_page(0).unwrap();
                let page = page_rc.borrow();

                assert_eq!(page.data[0], 77);
                assert_eq!(page.data[4095], 88);
            }
        });
    }

    #[test]
    fn test_eviction() {
        with_test_file("eviction", |path| {
            let disk = DiskManager::open(path).unwrap();
            let mut pool = BufferPool::new(disk, 3); // Only 3 pages capacity

            // Create 3 pages (fills the pool)
            let (id0, rc0) = pool.create_page().unwrap();
            let (_id1, _rc1) = pool.create_page().unwrap();
            let (_id2, _rc2) = pool.create_page().unwrap();

            // Modify first page
            {
                let mut page = rc0.borrow_mut();
                page.data[0] = 111;
            }

            assert_eq!(pool.pages.len(), 3);

            // Create 4th page - should trigger eviction
            let (_id3, _rc3) = pool.create_page().unwrap();

            // Pool should still be at capacity
            assert!(pool.pages.len() <= 3);

            // Flush to ensure evicted page was written
            pool.flush_all().unwrap();

            // Verify we can still fetch page 0 (either from cache or disk)
            let fetched = pool.fetch_page(id0).unwrap();
            assert_eq!(fetched.borrow().data[0], 111);
        });
    }

    #[test]
    fn test_multiple_pages() {
        with_test_file("multiple", |path| {
            let disk = DiskManager::open(path).unwrap();
            let mut pool = BufferPool::new(disk, 10);

            // Create several pages with different data
            for i in 0..5 {
                let (page_id, page_rc) = pool.create_page().unwrap();
                assert_eq!(page_id, i);

                let mut page = page_rc.borrow_mut();
                page.data[0] = i as u8;
            }

            // Verify each page has correct data
            for i in 0..5 {
                let page_rc = pool.fetch_page(i).unwrap();
                let page = page_rc.borrow();
                assert_eq!(page.data[0], i as u8);
            }
        });
    }



    #[test]
    fn test_shared_references() {
        with_test_file("shared_refs", |path| {
            let disk = DiskManager::open(path).unwrap();
            let mut pool = BufferPool::new(disk, 10);

            let (page_id, rc1) = pool.create_page().unwrap();
            let rc2 = pool.fetch_page(page_id).unwrap();
            let rc3 = pool.fetch_page(page_id).unwrap();

            // All references point to the same page
            assert!(Rc::ptr_eq(&rc1, &rc2));
            assert!(Rc::ptr_eq(&rc2, &rc3));

            // Modification through one is visible through others
            rc1.borrow_mut().data[0] = 55;
            assert_eq!(rc2.borrow().data[0], 55);
            assert_eq!(rc3.borrow().data[0], 55);
        });
    }
}
