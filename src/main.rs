#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::{Write, Read};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap(); //9092 port is typically used for kafka
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");

                let mut buffer = [0u8; 1024];
                match stream.read(&mut buffer) {
                    Ok(bytes_read) => {
                        println!("Read {} bytes", bytes_read);
                        let received_data = &buffer[..bytes_read];
                        println!("Received bytes: {:?}", received_data);
                        
                        // Convert to hex for easier reading
                        let hex_string: String = received_data.iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<Vec<String>>()
                            .join(" ");
                        println!("Hex: {}", &hex_string[0 .. 11]);
                        println!("Hex: {}", &hex_string[24 .. 36]);
                    }
                    Err(e) => println!("Failed to read: {}", e),
                }
                
                // Send hardcoded "7" back to the client
                // stream.write_all(&[0, 0, 0, 0, 0, 0, 0, 7]).unwrap();
                // stream.flush().unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
