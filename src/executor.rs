use crate::database::{Database, Row, TableMetadata, Value};
use crate::slotted_page::{NO_NEXT_PAGE, SlottedPage};
use crate::storage::PageId;
use sqlparser::ast::{ColumnDef, Expr, ObjectName, SetExpr, Statement, Values};

pub fn execute(db: &mut Database, stmt: Statement) -> Result<String, String> {
    match stmt {
        Statement::CreateTable(create_table) => {
            handle_create_table(db, create_table.name, create_table.columns)
        }
        Statement::Insert(insert) => handle_insert(db, insert.table, insert.source),
        Statement::Query(query) => handle_query(db, *query),
        _ => Err("Unsupported statement".to_string()),
    }
}

fn handle_create_table(
    db: &mut Database,
    name: ObjectName,
    columns: Vec<ColumnDef>,
) -> Result<String, String> {
    let table_name = name.to_string();

    if db.tables.contains_key(&table_name) {
        return Err(format!("Table '{}' already exists", table_name));
    }

    // Create the first page for this table
    let (page_id, page_rc) = db
        .buffer_pool
        .borrow_mut()
        .create_page()
        .map_err(|e| e.to_string())?;

    // Initialize the page as a slotted page
    {
        let mut page = page_rc.borrow_mut();
        let mut slotted = SlottedPage::new(&mut page);
        slotted.init();
    }

    let metadata = TableMetadata {
        schema: columns,
        first_page_id: page_id,
        last_page_id: page_id,
    };

    db.tables.insert(table_name.clone(), metadata);
    db.buffer_pool.borrow_mut().flush_all().map_err(|e| e.to_string())?;
    db.save_catalog().map_err(|e| e.to_string())?;

    Ok(format!("Table '{}' created", table_name))
}

fn handle_insert(
    db: &mut Database,
    table: sqlparser::ast::TableObject,
    source: Option<Box<sqlparser::ast::Query>>,
) -> Result<String, String> {
    let table_name_str = table.to_string();

    // Get table metadata (clone to avoid borrow issues)
    let metadata = db
        .tables
        .get(&table_name_str)
        .ok_or_else(|| format!("Table '{}' does not exist", table_name_str))?
        .clone();

    let source = source.ok_or("INSERT requires VALUES")?;

    let rows = match *source.body {
        SetExpr::Values(Values { rows, .. }) => rows,
        _ => return Err("Only INSERT ... VALUES is supported".to_string()),
    };

    let mut inserted_count = 0;
    let mut last_page_id = metadata.last_page_id;

    for row_exprs in rows {
        let values: Vec<Value> = row_exprs
            .into_iter()
            .map(expr_to_value)
            .collect::<Result<Vec<_>, _>>()?;

        let row = Row { values };
        let bytes = row.to_bytes();

        // Find a page with space and insert
        last_page_id = insert_tuple(db, metadata.first_page_id, last_page_id, &bytes)?;
        inserted_count += 1;
    }

    // Update last_page_id in metadata
    if let Some(meta) = db.tables.get_mut(&table_name_str) {
        meta.last_page_id = last_page_id;
    }

    db.buffer_pool.borrow_mut().flush_all().map_err(|e| e.to_string())?;

    // Save catalog to disk (last_page_id may have changed)
    db.save_catalog().map_err(|e| e.to_string())?;

    Ok(format!("Inserted {} row(s)", inserted_count))
}

fn insert_tuple(
    db: &mut Database,
    _first_page_id: PageId,
    last_page_id: PageId,
    bytes: &[u8],
) -> Result<PageId, String> {
    // Start from the last known page (optimization)
    let mut current_page_id = last_page_id;

    loop {
        let page_rc = db
            .buffer_pool
            .borrow_mut()
            .fetch_page(current_page_id)
            .map_err(|e| e.to_string())?;

        let mut page = page_rc.borrow_mut();
        let mut slotted = SlottedPage::new(&mut page);

        // Try to add the tuple
        match slotted.add_tuple(bytes) {
            Ok(_) => {
                return Ok(current_page_id);
            }
            Err(_) => {
                // Page is full, check for next page
                let next = slotted.next_page_id();

                if next == NO_NEXT_PAGE {
                    // Need to allocate a new page
                    drop(page); // Release borrow before creating new page

                    let (new_page_id, new_page_rc) = db
                        .buffer_pool
                        .borrow_mut()
                        .create_page()
                        .map_err(|e| e.to_string())?;

                    // Initialize the new page
                    {
                        let mut new_page = new_page_rc.borrow_mut();
                        let mut new_slotted = SlottedPage::new(&mut new_page);
                        new_slotted.init();

                        // Add the tuple to the new page
                        new_slotted
                            .add_tuple(bytes)
                            .map_err(|e| format!("Tuple too large for page: {}", e))?;
                    }

                    // Link the old page to the new page
                    {
                        let page_rc = db
                            .buffer_pool
                            .borrow_mut()
                            .fetch_page(current_page_id)
                            .map_err(|e| e.to_string())?;

                        let mut page = page_rc.borrow_mut();
                        let mut slotted = SlottedPage::new(&mut page);
                        slotted.set_next_page_id(new_page_id);
                    }

                    return Ok(new_page_id);
                } else {
                    // Move to the next page
                    current_page_id = next;
                }
            }
        }
    }
}

fn handle_query(db: &Database, query: sqlparser::ast::Query) -> Result<String, String> {
    let select = match *query.body {
        SetExpr::Select(select) => select,
        _ => return Err("Only SELECT is supported".to_string()),
    };

    if select.from.is_empty() {
        return Err("SELECT requires a FROM clause".to_string());
    }

    let table_name = match &select.from[0].relation {
        sqlparser::ast::TableFactor::Table { name, .. } => name.to_string(),
        _ => return Err("Only simple table references are supported".to_string()),
    };

    let metadata = db
        .tables
        .get(&table_name)
        .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

    let is_select_star = select.projection.len() == 1
        && matches!(
            select.projection[0],
            sqlparser::ast::SelectItem::Wildcard(_)
        );

    if !is_select_star {
        return Err("Only SELECT * is supported for now".to_string());
    }

    // Scan all pages in the linked list
    let mut output = String::new();

    // Print column headers if schema exists
    if !metadata.schema.is_empty() {
        let headers: Vec<String> = metadata.schema.iter().map(|c| c.name.to_string()).collect();
        output.push_str(&headers.join("\t"));
        output.push('\n');
        output.push_str(&"-".repeat(headers.len() * 10));
        output.push('\n');
    }

    let mut row_count = 0;
    let mut current_page_id = metadata.first_page_id;

    loop {
        let page_rc = db
            .buffer_pool
            .borrow_mut()
            .fetch_page(current_page_id)
            .map_err(|e| e.to_string())?;

        let page = page_rc.borrow();
        // Need to create a mutable copy for SlottedPage
        let mut page_copy = *page;
        let slotted = SlottedPage::new(&mut page_copy);

        // Read all tuples from this page
        for slot_id in 0..slotted.num_slots() {
            if let Some(bytes) = slotted.get_tuple(slot_id) {
                match Row::from_bytes(&bytes) {
                    Ok(row) => {
                        let formatted: Vec<String> =
                            row.values.iter().map(format_value).collect();
                        output.push_str(&formatted.join("\t"));
                        output.push('\n');
                        row_count += 1;
                    }
                    Err(e) => {
                        return Err(format!("Failed to deserialize row: {}", e));
                    }
                }
            }
        }

        // Move to next page
        let next = slotted.next_page_id();
        if next == NO_NEXT_PAGE {
            break;
        }
        current_page_id = next;
    }

    output.push_str(&format!("({} rows)", row_count));

    Ok(output)
}

fn expr_to_value(expr: Expr) -> Result<Value, String> {
    match expr {
        Expr::Value(v) => match v.value {
            sqlparser::ast::Value::Number(n, _) => n
                .parse::<i64>()
                .map(Value::Long)
                .map_err(|_| format!("Invalid number: {}", n)),
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => Ok(Value::Text(s)),
            sqlparser::ast::Value::Boolean(b) => Ok(Value::Bool(b)),
            sqlparser::ast::Value::Null => Err("NULL not supported yet".to_string()),
            _ => Err(format!("Unsupported value type: {:?}", v)),
        },
        _ => Err(format!("Unsupported expression: {:?}", expr)),
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Long(n) => n.to_string(),
        Value::Text(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPool;
    use crate::storage::DiskManager;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::cell::RefCell;
    use std::fs;
    use std::rc::Rc;

    fn with_test_db<F>(name: &str, f: F)
    where
        F: FnOnce(&mut Database),
    {
        let path = format!("test_executor_{}.db", name);
        let _ = fs::remove_file(&path);

        let disk = DiskManager::open(&path).unwrap();
        let buffer_pool = Rc::new(RefCell::new(BufferPool::new(disk, 100)));
        let mut db = Database::with_buffer_pool(buffer_pool);

        f(&mut db);

        let _ = fs::remove_file(&path);
    }

    fn parse_and_execute(db: &mut Database, sql: &str) -> Result<String, String> {
        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
        execute(db, stmts.into_iter().next().unwrap())
    }

    #[test]
    fn test_create_table() {
        with_test_db("create", |db| {
            let result = parse_and_execute(db, "CREATE TABLE users (id INT, name TEXT)");

            assert!(result.is_ok());
            assert_eq!(result.unwrap(), "Table 'users' created");
            assert!(db.tables.contains_key("users"));
        });
    }

    #[test]
    fn test_create_table_already_exists() {
        with_test_db("create_exists", |db| {
            parse_and_execute(db, "CREATE TABLE users (id INT)").unwrap();

            let result = parse_and_execute(db, "CREATE TABLE users (id INT)");

            assert!(result.is_err());
            assert_eq!(result.unwrap_err(), "Table 'users' already exists");
        });
    }

    #[test]
    fn test_insert_single_row() {
        with_test_db("insert_single", |db| {
            parse_and_execute(db, "CREATE TABLE users (id INT, name TEXT)").unwrap();

            let result = parse_and_execute(db, "INSERT INTO users VALUES (1, 'Alice')");

            assert!(result.is_ok());
            assert_eq!(result.unwrap(), "Inserted 1 row(s)");
        });
    }

    #[test]
    fn test_insert_multiple_rows() {
        with_test_db("insert_multiple", |db| {
            parse_and_execute(db, "CREATE TABLE users (id INT, name TEXT)").unwrap();

            let result = parse_and_execute(db, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");

            assert!(result.is_ok());
            assert_eq!(result.unwrap(), "Inserted 2 row(s)");
        });
    }

    #[test]
    fn test_insert_into_nonexistent_table() {
        with_test_db("insert_nonexistent", |db| {
            let result = parse_and_execute(db, "INSERT INTO users VALUES (1, 'Alice')");

            assert!(result.is_err());
            assert_eq!(result.unwrap_err(), "Table 'users' does not exist");
        });
    }

    #[test]
    fn test_select_empty_table() {
        with_test_db("select_empty", |db| {
            parse_and_execute(db, "CREATE TABLE users (id INT, name TEXT)").unwrap();

            let result = parse_and_execute(db, "SELECT * FROM users");

            assert!(result.is_ok());
            assert!(result.unwrap().contains("(0 rows)"));
        });
    }

    #[test]
    fn test_select_with_data() {
        with_test_db("select_data", |db| {
            parse_and_execute(db, "CREATE TABLE users (id INT, name TEXT)").unwrap();
            parse_and_execute(db, "INSERT INTO users VALUES (1, 'Alice')").unwrap();
            parse_and_execute(db, "INSERT INTO users VALUES (2, 'Bob')").unwrap();

            let result = parse_and_execute(db, "SELECT * FROM users").unwrap();

            assert!(result.contains("Alice"));
            assert!(result.contains("Bob"));
            assert!(result.contains("(2 rows)"));
        });
    }

    #[test]
    fn test_insert_many_rows_multiple_pages() {
        with_test_db("insert_many", |db| {
            parse_and_execute(db, "CREATE TABLE users (id INT, name TEXT)").unwrap();

            // Insert 500 rows - should span multiple pages
            for i in 0..500 {
                let sql = format!("INSERT INTO users VALUES ({}, 'User{}')", i, i);
                parse_and_execute(db, &sql).unwrap();
            }

            let result = parse_and_execute(db, "SELECT * FROM users").unwrap();

            assert!(result.contains("(500 rows)"));
            assert!(result.contains("User0"));
            assert!(result.contains("User499"));
        });
    }

    #[test]
    fn test_insert_5000_rows() {
        with_test_db("insert_5000", |db| {
            parse_and_execute(db, "CREATE TABLE users (id INT, name TEXT)").unwrap();

            // Build a batch insert
            let mut values = Vec::new();
            for i in 0..100 {
                values.push(format!("({}, 'User{}')", i, i));
            }

            // Insert in batches of 100
            for batch in 0..50 {
                let mut batch_values = Vec::new();
                for i in 0..100 {
                    let id = batch * 100 + i;
                    batch_values.push(format!("({}, 'User{}')", id, id));
                }
                let sql = format!("INSERT INTO users VALUES {}", batch_values.join(", "));
                parse_and_execute(db, &sql).unwrap();
            }

            let result = parse_and_execute(db, "SELECT * FROM users").unwrap();

            assert!(result.contains("(5000 rows)"));
        });
    }
}
