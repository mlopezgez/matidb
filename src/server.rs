use crate::database::Database;
use crate::executor::execute;
use crate::protocol::{read_query, write_response, Response};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::io::{BufReader, BufWriter};
use std::net::{TcpListener, TcpStream};

pub struct Server {
    db: Database,
    listener: TcpListener,
}

impl Server {
    pub fn new(addr: &str, db_path: &str) -> std::io::Result<Self> {
        let db = Database::new(db_path)?;
        let listener = TcpListener::bind(addr)?;
        
        println!("MatiDB Server v0.2.0 listening on {}", addr);
        println!("Database file: {}", db_path);
        
        Ok(Self {
            db,
            listener,
        })
    }

    pub fn run(mut self) -> std::io::Result<()> {
        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = handle_client(stream, &mut self.db) {
                        eprintln!("Error handling client: {}", e);
                    }
                    
                    // Flush after each client disconnects to ensure data persistence
                    if let Err(e) = self.db.buffer_pool.borrow_mut().flush_all() {
                        eprintln!("Warning: Failed to flush buffer pool: {}", e);
                    }
                    
                    // Save catalog after each client session
                    if let Err(e) = self.db.save_catalog() {
                        eprintln!("Warning: Failed to save catalog: {}", e);
                    }
                }
                Err(e) => {
                    eprintln!("Connection failed: {}", e);
                }
            }
        }
        
        // Final flush when server stops
        println!("Server shutting down, flushing data...");
        if let Err(e) = self.db.buffer_pool.borrow_mut().flush_all() {
            eprintln!("Error flushing buffer pool: {}", e);
        }
        if let Err(e) = self.db.save_catalog() {
            eprintln!("Error saving catalog: {}", e);
        }
        
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // Ensure data is flushed even if server is killed
        println!("Flushing database before shutdown...");
        if let Err(e) = self.db.buffer_pool.borrow_mut().flush_all() {
            eprintln!("Error flushing buffer pool on drop: {}", e);
        }
        if let Err(e) = self.db.save_catalog() {
            eprintln!("Error saving catalog on drop: {}", e);
        }
    }
}

fn handle_client(stream: TcpStream, db: &mut Database) -> std::io::Result<()> {
    let peer_addr = stream.peer_addr()?;
    println!("Client connected: {}", peer_addr);

    let read_stream = stream.try_clone()?;
    let write_stream = stream;
    
    let mut reader = BufReader::new(read_stream);
    let mut writer = BufWriter::new(write_stream);
    let dialect = GenericDialect {};

    loop {
        // Read query from client
        let query = match read_query(&mut reader) {
            Ok(q) if q.is_empty() => {
                println!("Client {} disconnected", peer_addr);
                break;
            }
            Ok(q) => q,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    println!("Client {} disconnected", peer_addr);
                    break;
                }
                eprintln!("Error reading query: {}", e);
                break;
            }
        };

        println!("Client {}: {}", peer_addr, query);

        // Handle special commands
        let response = match query.to_lowercase().as_str() {
            "exit" | "quit" => {
                write_response(&mut writer, &Response::Ok("Goodbye".to_string()))?;
                println!("Client {} disconnected", peer_addr);
                break;
            }
            "tables" => {
                let msg = if db.tables.is_empty() {
                    "No tables".to_string()
                } else {
                    db.tables.keys().map(|k| k.as_str()).collect::<Vec<_>>().join("\n")
                };
                Response::Ok(msg)
            }
            "flush" => {
                match db.buffer_pool.borrow_mut().flush_all() {
                    Ok(_) => Response::Ok("All pages flushed to disk".to_string()),
                    Err(e) => Response::Error(format!("Failed to flush: {}", e)),
                }
            }
            _ => {
                // Parse and execute SQL
                match Parser::parse_sql(&dialect, &query) {
                    Ok(statements) => {
                        let mut results = Vec::new();
                        let mut has_error = false;
                        let mut error_msg = String::new();
                        
                        for stmt in statements {
                            match execute(db, stmt) {
                                Ok(msg) => results.push(msg),
                                Err(e) => {
                                    has_error = true;
                                    error_msg = e;
                                    break;
                                }
                            }
                        }
                        
                        if has_error {
                            Response::Error(error_msg)
                        } else {
                            Response::Ok(results.join("\n"))
                        }
                    }
                    Err(e) => Response::Error(format!("Parse error: {}", e)),
                }
            }
        };

        write_response(&mut writer, &response)?;
    }

    Ok(())
}
