use std::io::{BufRead, BufReader, Read, Write};

/// Protocol for client-server communication
/// Messages are simple newline-delimited text
///
/// Client sends: SQL command (one line)
/// Server responds: "OK\n<result>\nEND\n" or "ERROR\n<message>\nEND\n"

#[derive(Debug)]
pub enum Response {
    Ok(String),
    Error(String),
}

#[allow(dead_code)]
impl Response {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Response::Ok(msg) => format!("OK\n{}\nEND\n", msg).into_bytes(),
            Response::Error(msg) => format!("ERROR\n{}\nEND\n", msg).into_bytes(),
        }
    }

    pub fn from_reader<R: Read>(reader: &mut BufReader<R>) -> std::io::Result<Self> {
        let mut first_line = String::new();
        let bytes_read = reader.read_line(&mut first_line)?;

        if bytes_read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Connection closed",
            ));
        }

        let first_line = first_line.trim();

        let mut content = String::new();
        loop {
            let mut line = String::new();
            let bytes_read = reader.read_line(&mut line)?;

            if bytes_read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Connection closed before END marker",
                ));
            }

            if line.trim() == "END" {
                break;
            }

            content.push_str(&line);
        }

        // Remove trailing newline if present
        if content.ends_with('\n') {
            content.pop();
        }

        match first_line {
            "OK" => Ok(Response::Ok(content)),
            "ERROR" => Ok(Response::Error(content)),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid response: {}", first_line),
            )),
        }
    }
}

pub fn read_query<R: Read>(reader: &mut BufReader<R>) -> std::io::Result<String> {
    let mut query = String::new();
    reader.read_line(&mut query)?;
    Ok(query.trim().to_string())
}

pub fn write_response<W: Write>(writer: &mut W, response: &Response) -> std::io::Result<()> {
    writer.write_all(&response.to_bytes())?;
    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_ok_response_serialization() {
        let response = Response::Ok("Query executed successfully".to_string());
        let bytes = response.to_bytes();
        let expected = b"OK\nQuery executed successfully\nEND\n";
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_error_response_serialization() {
        let response = Response::Error("Table not found".to_string());
        let bytes = response.to_bytes();
        let expected = b"ERROR\nTable not found\nEND\n";
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_ok_response_deserialization() {
        let data = b"OK\nQuery executed successfully\nEND\n";
        let cursor = Cursor::new(data);
        let mut reader = BufReader::new(cursor);

        let response = Response::from_reader(&mut reader).unwrap();

        match response {
            Response::Ok(msg) => assert_eq!(msg, "Query executed successfully"),
            _ => panic!("Expected Ok response"),
        }
    }

    #[test]
    fn test_error_response_deserialization() {
        let data = b"ERROR\nTable not found\nEND\n";
        let cursor = Cursor::new(data);
        let mut reader = BufReader::new(cursor);

        let response = Response::from_reader(&mut reader).unwrap();

        match response {
            Response::Error(msg) => assert_eq!(msg, "Table not found"),
            _ => panic!("Expected Error response"),
        }
    }

    #[test]
    fn test_multiline_response() {
        let response = Response::Ok("Row 1\nRow 2\nRow 3".to_string());
        let bytes = response.to_bytes();

        let cursor = Cursor::new(bytes.clone());
        let mut reader = BufReader::new(cursor);

        let parsed = Response::from_reader(&mut reader).unwrap();

        match parsed {
            Response::Ok(msg) => assert_eq!(msg, "Row 1\nRow 2\nRow 3"),
            _ => panic!("Expected Ok response"),
        }
    }

    #[test]
    fn test_read_query() {
        let data = b"SELECT * FROM users\n";
        let cursor = Cursor::new(data);
        let mut reader = BufReader::new(cursor);

        let query = read_query(&mut reader).unwrap();
        assert_eq!(query, "SELECT * FROM users");
    }
}
