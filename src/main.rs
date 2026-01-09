use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::env;

mod buffer;
mod database;
mod executor;
mod protocol;
mod server;
mod slotted_page;
mod storage;

use database::Database;
use executor::execute;
use server::Server;

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();

    // Check if we should run in server mode
    if args.len() > 1 && args[1] == "--server" {
        let addr = if args.len() > 2 {
            args[2].as_str()
        } else {
            "127.0.0.1:5432"
        };

        let db_path = if args.len() > 3 {
            args[3].as_str()
        } else {
            "mati.db"
        };

        let server = Server::new(addr, db_path)?;
        return server.run();
    }

    // Run in interactive mode
    run_interactive()
}

fn run_interactive() -> std::io::Result<()> {
    // Initialize database with file storage
    let mut db = Database::new("mati.db").expect("Failed to initialize database");

    let mut rl = DefaultEditor::new().map_err(std::io::Error::other)?;
    let dialect = GenericDialect {};

    println!("MatiDB v0.2.0 - Now with persistent storage!");
    println!("Type 'exit' to quit, 'tables' to list tables\n");

    loop {
        let readline = rl.readline("matidb > ");
        match readline {
            Ok(line) => {
                let sql = line.trim();

                if sql.is_empty() {
                    continue;
                }

                rl.add_history_entry(sql).map_err(std::io::Error::other)?;

                match sql.to_lowercase().as_str() {
                    "exit" | "quit" => {
                        // Flush all pages before exit
                        if let Err(e) = db.buffer_pool.borrow_mut().flush_all() {
                            eprintln!("Warning: Failed to flush pages: {}", e);
                        }
                        println!("Goodbye!");
                        break;
                    }
                    "tables" => {
                        if db.tables.is_empty() {
                            println!("No tables");
                        } else {
                            for name in db.tables.keys() {
                                println!("  {}", name);
                            }
                        }
                        continue;
                    }
                    "flush" => {
                        match db.buffer_pool.borrow_mut().flush_all() {
                            Ok(_) => println!("All pages flushed to disk"),
                            Err(e) => eprintln!("Error flushing: {}", e),
                        }
                        continue;
                    }
                    _ => {}
                }

                match Parser::parse_sql(&dialect, sql) {
                    Ok(statements) => {
                        for stmt in statements {
                            match execute(&mut db, stmt) {
                                Ok(msg) => println!("{}", msg),
                                Err(e) => eprintln!("Error: {}", e),
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Parse error: {}", e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
            }
            Err(ReadlineError::Eof) => {
                if let Err(e) = db.buffer_pool.borrow_mut().flush_all() {
                    eprintln!("Warning: Failed to flush pages: {}", e);
                }
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                eprintln!("Error: {}", err);
                break;
            }
        }
    }

    Ok(())
}
