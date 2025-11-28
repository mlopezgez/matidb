use crate::buffer::BufferPool;
use crate::storage::{DiskManager, PageId};
use sqlparser::ast::{ColumnDef, DataType};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::rc::Rc;

#[derive(Debug, Clone)]
pub enum Value {
    Long(i64),
    Text(String),
    Bool(bool),
}

#[derive(Debug)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    /// Serialize a Row to bytes (self-describing with type tags)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        for value in &self.values {
            match value {
                Value::Long(n) => {
                    bytes.push(0);
                    bytes.extend_from_slice(&n.to_le_bytes());
                }
                Value::Text(s) => {
                    bytes.push(1);
                    let str_bytes = s.as_bytes();
                    bytes.extend_from_slice(&(str_bytes.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(str_bytes);
                }
                Value::Bool(b) => {
                    bytes.push(2);
                    bytes.push(if *b { 1 } else { 0 });
                }
            }
        }

        bytes
    }

    /// Deserialize bytes back to a Row (self-describing format with type tags)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let mut values = Vec::new();
        let mut offset = 0;

        while offset < bytes.len() {
            let type_tag = bytes[offset];
            offset += 1;

            match type_tag {
                0 => {
                    if offset + 8 > bytes.len() {
                        return Err("Unexpected end of data for Long".to_string());
                    }
                    let n = i64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    values.push(Value::Long(n));
                }
                1 => {
                    if offset + 4 > bytes.len() {
                        return Err("Unexpected end of data for Text length".to_string());
                    }
                    let len =
                        u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;

                    if offset + len > bytes.len() {
                        return Err("Unexpected end of data for Text content".to_string());
                    }
                    let s = String::from_utf8(bytes[offset..offset + len].to_vec())
                        .map_err(|e| e.to_string())?;
                    offset += len;
                    values.push(Value::Text(s));
                }
                2 => {
                    if offset >= bytes.len() {
                        return Err("Unexpected end of data for Bool".to_string());
                    }
                    let b = bytes[offset] != 0;
                    offset += 1;
                    values.push(Value::Bool(b));
                }
                _ => {
                    return Err(format!("Unknown type tag: {}", type_tag));
                }
            }
        }

        Ok(Row { values })
    }
}

/// Metadata for a table stored in the catalog
#[derive(Clone)]
pub struct TableMetadata {
    pub schema: Vec<ColumnDef>,
    pub first_page_id: PageId,
    pub last_page_id: PageId, // Optimization: track last page for faster inserts
}

pub struct Database {
    pub buffer_pool: Rc<RefCell<BufferPool>>,
    pub tables: HashMap<String, TableMetadata>,
    catalog_path: String,
}

impl Drop for Database {
    fn drop(&mut self) {
        // Attempt to flush pages when database goes out of scope.
        // We ignore errors here because we can't do much about them during drop.
        let _ = self.buffer_pool.borrow_mut().flush_all();
    }
}

impl Database {
    pub fn new(db_path: &str) -> std::io::Result<Self> {
        let disk = DiskManager::open(db_path)?;
        let buffer_pool = BufferPool::new(disk, 100); // 100 page capacity
        let catalog_path = format!("{}.catalog", db_path);

        let mut db = Self {
            buffer_pool: Rc::new(RefCell::new(buffer_pool)),
            tables: HashMap::new(),
            catalog_path: catalog_path.clone(),
        };

        // Load catalog if it exists
        if let Err(e) = db.load_catalog() {
            eprintln!("Warning: Could not load catalog: {}", e);
        }

        Ok(db)
    }

    #[cfg(test)]
    pub fn with_buffer_pool(buffer_pool: Rc<RefCell<BufferPool>>) -> Self {
        Self {
            buffer_pool,
            tables: HashMap::new(),
            catalog_path: String::new(), // Tests don't use catalog persistence
        }
    }

    /// Save the catalog to disk
    pub fn save_catalog(&self) -> std::io::Result<()> {
        if self.catalog_path.is_empty() {
            return Ok(()); // Skip for test databases
        }

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.catalog_path)?;

        for (table_name, metadata) in &self.tables {
            // Format: table_name|first_page_id|last_page_id|column_count
            writeln!(
                file,
                "{}|{}|{}|{}",
                table_name,
                metadata.first_page_id,
                metadata.last_page_id,
                metadata.schema.len()
            )?;

            // Write each column: name|type
            for col in &metadata.schema {
                let type_str = Self::datatype_to_string(&col.data_type);
                writeln!(file, "  {}|{}", col.name, type_str)?;
            }
        }

        file.flush()?;
        Ok(())
    }

    /// Load the catalog from disk
    fn load_catalog(&mut self) -> std::io::Result<()> {
        if self.catalog_path.is_empty() {
            return Ok(()); // Skip for test databases
        }

        let file = match File::open(&self.catalog_path) {
            Ok(f) => f,
            Err(_) => return Ok(()), // Catalog doesn't exist yet, that's ok
        };

        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        while let Some(Ok(line)) = lines.next() {
            if line.trim().is_empty() {
                continue;
            }

            // Parse table metadata line
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() != 4 {
                continue;
            }

            let table_name = parts[0].to_string();
            let first_page_id: PageId = parts[1].parse().unwrap_or(0);
            let last_page_id: PageId = parts[2].parse().unwrap_or(0);
            let column_count: usize = parts[3].parse().unwrap_or(0);

            // Parse columns
            let mut schema = Vec::new();
            for _ in 0..column_count {
                if let Some(Ok(col_line)) = lines.next() {
                    let col_line = col_line.trim();
                    let col_parts: Vec<&str> = col_line.split('|').collect();
                    if col_parts.len() == 2 {
                        let col_name = sqlparser::ast::Ident::new(col_parts[0]);
                        let data_type = Self::string_to_datatype(col_parts[1]);
                        schema.push(ColumnDef {
                            name: col_name,
                            data_type,
                            options: vec![],
                        });
                    }
                }
            }

            self.tables.insert(
                table_name,
                TableMetadata {
                    schema,
                    first_page_id,
                    last_page_id,
                },
            );
        }

        Ok(())
    }

    fn datatype_to_string(dt: &DataType) -> String {
        match dt {
            DataType::Int(_) | DataType::Integer(_) => "INT".to_string(),
            DataType::BigInt(_) => "BIGINT".to_string(),
            DataType::SmallInt(_) => "SMALLINT".to_string(),
            DataType::Text => "TEXT".to_string(),
            DataType::Varchar(_) => "VARCHAR".to_string(),
            DataType::Char(_) => "CHAR".to_string(),
            DataType::String(_) => "STRING".to_string(),
            DataType::Boolean => "BOOLEAN".to_string(),
            _ => "TEXT".to_string(), // Default fallback
        }
    }

    fn string_to_datatype(s: &str) -> DataType {
        match s.to_uppercase().as_str() {
            "INT" => DataType::Int(None),
            "INTEGER" => DataType::Integer(None),
            "BIGINT" => DataType::BigInt(None),
            "SMALLINT" => DataType::SmallInt(None),
            "TEXT" => DataType::Text,
            "VARCHAR" => DataType::Varchar(None),
            "CHAR" => DataType::Char(None),
            "STRING" => DataType::String(None),
            "BOOLEAN" => DataType::Boolean,
            _ => DataType::Text, // Default fallback
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_long() {
        let row = Row {
            values: vec![Value::Long(42)],
        };

        let bytes = row.to_bytes();
        let restored = Row::from_bytes(&bytes).unwrap();

        assert!(matches!(restored.values[0], Value::Long(42)));
    }

    #[test]
    fn test_serialize_text() {
        let row = Row {
            values: vec![Value::Text("hello".to_string())],
        };

        let bytes = row.to_bytes();
        let restored = Row::from_bytes(&bytes).unwrap();

        assert!(matches!(&restored.values[0], Value::Text(s) if s == "hello"));
    }

    #[test]
    fn test_serialize_bool() {
        let row = Row {
            values: vec![Value::Bool(true), Value::Bool(false)],
        };

        let bytes = row.to_bytes();
        let restored = Row::from_bytes(&bytes).unwrap();

        assert!(matches!(restored.values[0], Value::Bool(true)));
        assert!(matches!(restored.values[1], Value::Bool(false)));
    }

    #[test]
    fn test_serialize_mixed() {
        let row = Row {
            values: vec![
                Value::Long(123),
                Value::Text("Alice".to_string()),
                Value::Bool(true),
            ],
        };

        let bytes = row.to_bytes();
        let restored = Row::from_bytes(&bytes).unwrap();

        assert_eq!(restored.values.len(), 3);
        assert!(matches!(restored.values[0], Value::Long(123)));
        assert!(matches!(&restored.values[1], Value::Text(s) if s == "Alice"));
        assert!(matches!(restored.values[2], Value::Bool(true)));
    }

    #[test]
    fn test_serialize_empty_string() {
        let row = Row {
            values: vec![Value::Text("".to_string())],
        };

        let bytes = row.to_bytes();
        let restored = Row::from_bytes(&bytes).unwrap();

        assert!(matches!(&restored.values[0], Value::Text(s) if s.is_empty()));
    }

    #[test]
    fn test_serialize_large_number() {
        let row = Row {
            values: vec![Value::Long(i64::MAX), Value::Long(i64::MIN)],
        };

        let bytes = row.to_bytes();
        let restored = Row::from_bytes(&bytes).unwrap();

        assert!(matches!(restored.values[0], Value::Long(n) if n == i64::MAX));
        assert!(matches!(restored.values[1], Value::Long(n) if n == i64::MIN));
    }

    #[test]
    fn test_from_bytes_invalid_tag() {
        let bytes = vec![99, 0, 0, 0, 0];
        let result = Row::from_bytes(&bytes);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown type tag"));
    }

    #[test]
    fn test_from_bytes_truncated_long() {
        let bytes = vec![0, 1, 2, 3];
        let result = Row::from_bytes(&bytes);

        assert!(result.is_err());
    }

    #[test]
    fn test_from_bytes_truncated_text() {
        let bytes = vec![1, 10, 0, 0, 0, 65, 66];
        let result = Row::from_bytes(&bytes);

        assert!(result.is_err());
    }
}
