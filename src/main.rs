#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::{Write, Read};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap(); //9092 port is typically used for kafka
    
    // FIRST MATCH: "Did we get a connection?"
    for stream in listener.incoming() { // Iterator of Result<TcpStream, Error>
        match stream { // ← Matching the connection attempt
            Ok(mut stream) => { // ← We have a TcpStream!
                println!("accepted new connection");

                // SECOND MATCH: "Did we read data successfully?"
                let mut buffer = [0u8; 1024];
                match stream.read(&mut buffer) { 
                    Ok(bytes_read) => {
                        let received_data = &buffer[..bytes_read];
                        println!("Message bytes: {:?}", received_data);
        
                        // Extract correlation ID from bytes 8-11 (after length, api_key, api_version)
                        if bytes_read >= 12 {
                            // Correlation ID is at bytes 8-11 in the request
                            let correlation_id = [
                                received_data[8],   
                                received_data[9],
                                received_data[10],   
                                received_data[11],  
                            ];
                            
                            // Build proper APIVersions response matching expected format
                            let mut response = Vec::new();
                            
                            // Message length (will be calculated and prepended)
                            let response_body = [
                                // Correlation ID (4 bytes)
                                correlation_id[0], correlation_id[1], correlation_id[2], correlation_id[3],
                                // Error code (2 bytes) - 0 for success
                                0x00, 0x00,
                                // Array length + 1 (compact array format) - 3 entries + 1 = 4
                                0x04,
                                // --- First element (API Key 17) ---
                                0x00, 0x11,  // API key 17 (2 bytes)
                                0x00, 0x00,  // Min version 0 (2 bytes)
                                0x00, 0x04,  // Max version 4 (2 bytes)
                                0x00,        // Tag buffer
                                // --- Second element (API Key 18) ---
                                0x00, 0x12,  // API key 18 (2 bytes)
                                0x00, 0x00,  // Min version 0 (2 bytes)
                                0x00, 0x04,  // Max version 4 (2 bytes)
                                0x00,        // Tag buffer
                                // --- Third element (API Key 19) ---
                                0x00, 0x13,  // API key 19 (2 bytes)
                                0x00, 0x00,  // Min version 0 (2 bytes)
                                0x00, 0x04,  // Max version 4 (2 bytes)
                                0x00,        // Tag buffer
                                // Throttle time (4 bytes) - 0
                                0x00, 0x00, 0x00, 0x00,
                                // Final tag buffer
                                0x00,
                            ];
                            
                            // Calculate message length (response body length)
                            let message_length = response_body.len() as u32;
                            let length_bytes = message_length.to_be_bytes();
                            
                            // Prepend message length
                            response.extend_from_slice(&length_bytes);
                            response.extend_from_slice(&response_body);
                            
                            stream.write_all(&response).unwrap();
                        }
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
