use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::env;
use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;

// We need to include the protocol module
use matidb::protocol::Response;

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    
    let addr = if args.len() > 1 {
        args[1].as_str()
    } else {
        "127.0.0.1:5432"
    };
    
    println!("Connecting to MatiDB server at {}...", addr);
    
    let stream = TcpStream::connect(addr)?;
    println!("Connected!\n");
    
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);
    
    let mut rl = DefaultEditor::new().map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
    })?;
    
    println!("MatiDB Client v0.2.0");
    println!("Type 'exit' to quit\n");
    
    loop {
        let readline = rl.readline("matidb> ");
        match readline {
            Ok(line) => {
                let query = line.trim();
                
                if query.is_empty() {
                    continue;
                }
                
                rl.add_history_entry(query).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
                })?;
                
                // Send query to server
                writeln!(writer, "{}", query)?;
                writer.flush()?;
                
                // Read response
                match Response::from_reader(&mut reader) {
                    Ok(Response::Ok(msg)) => {
                        println!("{}", msg);
                        
                        // Exit if we sent exit/quit
                        if query.to_lowercase() == "exit" || query.to_lowercase() == "quit" {
                            break;
                        }
                    }
                    Ok(Response::Error(msg)) => {
                        eprintln!("Error: {}", msg);
                    }
                    Err(e) => {
                        eprintln!("Connection error: {}", e);
                        break;
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
            }
            Err(ReadlineError::Eof) => {
                println!("\nGoodbye!");
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
