# MatiDB

A simple relational database implementation in Rust with SQL support and TCP client-server architecture.

## Features

- SQL support (CREATE TABLE, INSERT, SELECT)
- Persistent storage with buffer pool management
- Slotted page layout for efficient tuple storage
- TCP client-server architecture
- Interactive local mode and remote client mode
- Catalog persistence for table metadata

## Building

```bash
cargo build --release
```

This creates two binaries:
- `target/release/matidb` - Server and local interactive mode
- `target/release/matidb-client` - Remote client

## Running

### Interactive Mode (Local)

Run the database locally with an interactive prompt:

```bash
./target/release/matidb
```

Example session:
```
MatiDB v0.2.0 - Now with persistent storage!
Type 'exit' to quit, 'tables' to list tables

matidb > CREATE TABLE users (id BIGINT, name TEXT, active BOOLEAN)
Table 'users' created

matidb > INSERT INTO users VALUES (1, 'Alice', true)
Inserted 1 row

matidb > SELECT * FROM users
id      name    active
------------------------------
1       Alice   true
(1 rows)

matidb > exit
Goodbye!
```

### Server Mode

Start the database server:

```bash
./target/release/matidb --server [address] [database_file]
```

Default address: `127.0.0.1:5432`  
Default database file: `mati.db`

Example:
```bash
./target/release/matidb --server 127.0.0.1:5432 mydb.db
```

### Client Mode

Connect to a running server:

```bash
./target/release/matidb-client [address]
```

Default address: `127.0.0.1:5432`

Example:
```bash
./target/release/matidb-client 127.0.0.1:5432
```

## Supported SQL

### CREATE TABLE

```sql
CREATE TABLE table_name (
    column1 datatype,
    column2 datatype,
    ...
)
```

Supported data types:
- `BIGINT`, `INT`, `INTEGER`, `SMALLINT` - Integer types (stored as i64)
- `TEXT`, `VARCHAR`, `CHAR`, `STRING` - Text types
- `BOOLEAN` - Boolean type

Example:
```sql
CREATE TABLE employees (id BIGINT, name TEXT, active BOOLEAN)
```

### INSERT

```sql
INSERT INTO table_name VALUES (value1, value2, ...)
```

Multiple rows:
```sql
INSERT INTO table_name VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')
```

### SELECT

Currently supports `SELECT *` only:

```sql
SELECT * FROM table_name
```

## Special Commands

- `tables` - List all tables in the database
- `flush` - Flush all pages to disk
- `exit` or `quit` - Exit the client/server

## Storage Architecture

### File Structure

Each database consists of two files:
- `<name>.db` - Data pages (4KB each)
- `<name>.db.catalog` - Table metadata and schema

### Buffer Pool

The buffer pool caches pages in memory with automatic eviction when capacity is reached. Current capacity: 100 pages.

### Slotted Pages

Each page uses a slotted page layout:
- Header (8 bytes): slot count, free space pointer, next page ID
- Slot directory: grows from the beginning
- Tuple data: grows from the end

Pages are linked in a chain for tables that span multiple pages.

### Data Persistence

Data is persisted to disk:
- After each client disconnects (in server mode)
- When the `flush` command is executed
- When the server shuts down
- Automatically when pages are evicted from the buffer pool

## Protocol (Client-Server)

The TCP protocol is text-based and simple:

**Client Request:**
```
<SQL query>\n
```

**Server Response:**
```
OK\n
<result content>\n
END\n
```

or

```
ERROR\n
<error message>\n
END\n
```

## Testing

Run the test suite:

```bash
cargo test
```

Test the TCP functionality with the Python test script:

```bash
# Terminal 1: Start server
./target/release/matidb --server 127.0.0.1:5432 test.db

# Terminal 2: Run test
python3 test_tcp.py
```

## Architecture Notes

### Single-Threaded Server

The current server implementation is single-threaded and handles one client at a time. This is because the `BufferPool` uses `Rc<RefCell<>>` which is not thread-safe.

To make it multi-threaded, the buffer pool would need to be refactored to use `Arc<Mutex<>>` instead.

### Memory Management

- Uses `Rc<RefCell<>>` for shared page references within the buffer pool
- Automatically flushes dirty pages when evicted
- Prevents data loss through multiple persistence mechanisms

## Project Structure

```
src/
├── main.rs          - Entry point, server/interactive mode selection
├── lib.rs           - Library exports
├── database.rs      - Database and catalog management
├── executor.rs      - SQL execution engine
├── buffer.rs        - Buffer pool for page caching
├── storage.rs       - Disk manager for page I/O
├── slotted_page.rs  - Slotted page layout implementation
├── protocol.rs      - TCP protocol handling
├── server.rs        - TCP server implementation
└── bin/
    └── matidb-client.rs - TCP client implementation
```

## Development

### Adding New SQL Features

1. Parse the SQL in `executor.rs`
2. Add handler function for the new statement type
3. Update the `execute()` function to route to your handler

### Modifying Storage

The storage layer is modular:
- `DiskManager` handles raw page I/O
- `BufferPool` manages in-memory pages
- `SlottedPage` provides tuple-level operations

## Known Limitations

- Only supports `SELECT *` (no column projection)
- No WHERE clause support
- No JOIN operations
- No indexes
- No transactions
- Single-threaded server
- Fixed page size (4KB)
- No type checking on INSERT

## License

This is an educational project.
