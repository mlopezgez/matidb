use crate::storage::{PAGE_SIZE, Page};

// Header layout:
// [0..2]: num_slots (u16)
// [2..4]: free_space_pointer (u16)
// [4..8]: next_page_id (u32) - u32::MAX means no next page

const HEADER_SIZE: usize = 8;
const SLOT_SIZE: usize = 4; // offset (u16) + length (u16)

pub const NO_NEXT_PAGE: u32 = u32::MAX;

fn write_u16(data: &mut [u8], offset: usize, value: u16) {
    let bytes = value.to_le_bytes();
    data[offset] = bytes[0];
    data[offset + 1] = bytes[1];
}

fn read_u16(data: &[u8], offset: usize) -> u16 {
    let bytes = [data[offset], data[offset + 1]];
    u16::from_le_bytes(bytes)
}

fn write_u32(data: &mut [u8], offset: usize, value: u32) {
    let bytes = value.to_le_bytes();
    data[offset] = bytes[0];
    data[offset + 1] = bytes[1];
    data[offset + 2] = bytes[2];
    data[offset + 3] = bytes[3];
}

fn read_u32(data: &[u8], offset: usize) -> u32 {
    let bytes = [
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ];
    u32::from_le_bytes(bytes)
}

pub struct SlottedPage<'a> {
    pub page: &'a mut Page,
}

impl<'a> SlottedPage<'a> {
    pub fn new(page: &'a mut Page) -> Self {
        Self { page }
    }

    /// Initialize a fresh page
    pub fn init(&mut self) {
        self.set_num_slots(0);
        self.set_free_space_pointer(PAGE_SIZE as u16);
        self.set_next_page_id(NO_NEXT_PAGE);
    }

    /// Returns the number of slots currently in use
    pub fn num_slots(&self) -> u16 {
        read_u16(&self.page.data, 0)
    }

    fn set_num_slots(&mut self, num: u16) {
        write_u16(&mut self.page.data, 0, num);
    }

    /// Points to where the data area starts (grows downward from end)
    fn free_space_pointer(&self) -> u16 {
        read_u16(&self.page.data, 2)
    }

    fn set_free_space_pointer(&mut self, ptr: u16) {
        write_u16(&mut self.page.data, 2, ptr);
    }

    /// Get the next page ID in the linked list
    pub fn next_page_id(&self) -> u32 {
        read_u32(&self.page.data, 4)
    }

    /// Set the next page ID in the linked list
    pub fn set_next_page_id(&mut self, page_id: u32) {
        write_u32(&mut self.page.data, 4, page_id);
    }

    /// Calculate where the slot array ends
    fn slots_end(&self) -> usize {
        HEADER_SIZE + (self.num_slots() as usize * SLOT_SIZE)
    }

    /// Calculate available free space
    pub fn free_space(&self) -> usize {
        let data_start = self.free_space_pointer() as usize;
        let slots_end = self.slots_end();

        if data_start > slots_end {
            data_start - slots_end
        } else {
            0
        }
    }

    /// Add a tuple to the page
    pub fn add_tuple(&mut self, tuple_data: &[u8]) -> Result<u16, String> {
        let tuple_len = tuple_data.len();
        let required_space = SLOT_SIZE + tuple_len;

        if self.free_space() < required_space {
            return Err("Page full".to_string());
        }

        // Calculate where to write the tuple (grows down from end)
        let new_data_offset = self.free_space_pointer() as usize - tuple_len;

        // Write the tuple data
        self.page.data[new_data_offset..new_data_offset + tuple_len].copy_from_slice(tuple_data);

        // Write the slot entry
        let slot_id = self.num_slots();
        let slot_offset = HEADER_SIZE + (slot_id as usize * SLOT_SIZE);

        write_u16(&mut self.page.data, slot_offset, new_data_offset as u16);
        write_u16(&mut self.page.data, slot_offset + 2, tuple_len as u16);

        // Update header
        self.set_num_slots(slot_id + 1);
        self.set_free_space_pointer(new_data_offset as u16);

        Ok(slot_id)
    }

    /// Get a tuple by slot ID
    pub fn get_tuple(&self, slot_id: u16) -> Option<Vec<u8>> {
        if slot_id >= self.num_slots() {
            return None;
        }

        let slot_offset = HEADER_SIZE + (slot_id as usize * SLOT_SIZE);
        let data_offset = read_u16(&self.page.data, slot_offset) as usize;
        let data_length = read_u16(&self.page.data, slot_offset + 2) as usize;

        Some(self.page.data[data_offset..data_offset + data_length].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_get_tuple() {
        let mut page = Page::new();
        let mut slotted = SlottedPage::new(&mut page);
        slotted.init();

        let data = vec![1, 2, 3, 4, 5];
        let slot_id = slotted.add_tuple(&data).unwrap();

        assert_eq!(slot_id, 0);

        let retrieved = slotted.get_tuple(0).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_multiple_tuples() {
        let mut page = Page::new();
        let mut slotted = SlottedPage::new(&mut page);
        slotted.init();

        let data1 = vec![1, 2, 3];
        let data2 = vec![4, 5, 6, 7];
        let data3 = vec![8, 9];

        let slot1 = slotted.add_tuple(&data1).unwrap();
        let slot2 = slotted.add_tuple(&data2).unwrap();
        let slot3 = slotted.add_tuple(&data3).unwrap();

        assert_eq!(slot1, 0);
        assert_eq!(slot2, 1);
        assert_eq!(slot3, 2);

        assert_eq!(slotted.get_tuple(0).unwrap(), data1);
        assert_eq!(slotted.get_tuple(1).unwrap(), data2);
        assert_eq!(slotted.get_tuple(2).unwrap(), data3);
    }

    #[test]
    fn test_get_invalid_slot() {
        let mut page = Page::new();
        let mut slotted = SlottedPage::new(&mut page);
        slotted.init();

        assert!(slotted.get_tuple(0).is_none());
        assert!(slotted.get_tuple(99).is_none());
    }

    #[test]
    fn test_page_full() {
        let mut page = Page::new();
        let mut slotted = SlottedPage::new(&mut page);
        slotted.init();

        let huge_data = vec![0u8; PAGE_SIZE];
        let result = slotted.add_tuple(&huge_data);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Page full");
    }

    #[test]
    fn test_fill_page_gradually() {
        let mut page = Page::new();
        let mut slotted = SlottedPage::new(&mut page);
        slotted.init();

        let tuple_data = vec![0u8; 100];
        let mut count = 0;

        while slotted.add_tuple(&tuple_data).is_ok() {
            count += 1;
            if count > 100 {
                break;
            }
        }

        assert!(count > 0);
        assert_eq!(slotted.num_slots(), count);

        for i in 0..count {
            assert_eq!(slotted.get_tuple(i).unwrap(), tuple_data);
        }
    }

    #[test]
    fn test_variable_length_tuples() {
        let mut page = Page::new();
        let mut slotted = SlottedPage::new(&mut page);
        slotted.init();

        let short = vec![1u8; 10];
        let medium = vec![2u8; 100];
        let long = vec![3u8; 500];

        slotted.add_tuple(&short).unwrap();
        slotted.add_tuple(&medium).unwrap();
        slotted.add_tuple(&long).unwrap();

        assert_eq!(slotted.get_tuple(0).unwrap(), short);
        assert_eq!(slotted.get_tuple(1).unwrap(), medium);
        assert_eq!(slotted.get_tuple(2).unwrap(), long);
    }

    #[test]
    fn test_next_page_id() {
        let mut page = Page::new();
        let mut slotted = SlottedPage::new(&mut page);
        slotted.init();

        // Initially should be NO_NEXT_PAGE
        assert_eq!(slotted.next_page_id(), NO_NEXT_PAGE);

        // Set a next page
        slotted.set_next_page_id(42);
        assert_eq!(slotted.next_page_id(), 42);

        // Set back to no next page
        slotted.set_next_page_id(NO_NEXT_PAGE);
        assert_eq!(slotted.next_page_id(), NO_NEXT_PAGE);
    }



    #[test]
    fn test_with_row_serialization() {
        use crate::database::{Row, Value};

        let mut page = Page::new();
        let mut slotted = SlottedPage::new(&mut page);
        slotted.init();

        let row = Row {
            values: vec![
                Value::Long(42),
                Value::Text("Alice".to_string()),
                Value::Bool(true),
            ],
        };

        let bytes = row.to_bytes();
        let slot_id = slotted.add_tuple(&bytes).unwrap();

        let retrieved_bytes = slotted.get_tuple(slot_id).unwrap();
        let restored_row = Row::from_bytes(&retrieved_bytes).unwrap();

        assert_eq!(restored_row.values.len(), 3);
        assert!(matches!(restored_row.values[0], Value::Long(42)));
        assert!(matches!(&restored_row.values[1], Value::Text(s) if s == "Alice"));
        assert!(matches!(restored_row.values[2], Value::Bool(true)));
    }
}
