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
        
                        // Extract request details from the message
                        if bytes_read >= 12 {
                            // Extract API key from bytes 4-5
                            let api_key = u16::from_be_bytes([received_data[4], received_data[5]]);
                            // Extract API version from bytes 6-7
                            let api_version = u16::from_be_bytes([received_data[6], received_data[7]]);
                            // Correlation ID is at bytes 8-11 in the request
                            let correlation_id = [
                                received_data[8],   
                                received_data[9],
                                received_data[10],   
                                received_data[11],  
                            ];
                            
                            // Build APIVersions response dynamically
                            let mut response = Vec::new();
                            let mut response_body = Vec::new();
                            
                            // Add correlation ID (4 bytes)
                            response_body.extend_from_slice(&correlation_id);
                            
                            // Add error code (2 bytes) - 0 for success
                            response_body.extend_from_slice(&[0x00, 0x00]);
                            
                            // Define supported API versions dynamically
                            let supported_apis = vec![
                                (17u16, 0u16, 4u16), // API Key 17: min version 0, max version 4
                                (18u16, 0u16, 4u16), // API Key 18: min version 0, max version 4
                                (19u16, 0u16, 4u16), // API Key 19: min version 0, max version 4
                            ];
                            
                            // Check if the requested API key and version are supported
                            let mut error_code = 35u16; // UNSUPPORTED_VERSION_ERROR by default
                            for (supported_key, min_ver, max_ver) in &supported_apis {
                                if api_key == *supported_key && api_version >= *min_ver && api_version <= *max_ver {
                                    error_code = 0; // Success
                                    break;
                                }
                            }
                            
                            // Add error code (2 bytes)
                            response_body.extend_from_slice(&error_code.to_be_bytes());
                            
                            // Add array length + 1 (compact array format)
                            response_body.push((supported_apis.len() + 1) as u8);
                            
                            // Add each API version entry dynamically
                            for (api_key, min_version, max_version) in supported_apis {
                                // API key (2 bytes, big endian)
                                response_body.extend_from_slice(&api_key.to_be_bytes());
                                // Min version (2 bytes, big endian)
                                response_body.extend_from_slice(&min_version.to_be_bytes());
                                // Max version (2 bytes, big endian)
                                response_body.extend_from_slice(&max_version.to_be_bytes());
                                // Tag buffer
                                response_body.push(0x00);
                            }
                            
                            // Add throttle time (4 bytes) - 0
                            response_body.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
                            
                            // Add final tag buffer
                            response_body.push(0x00);
                            
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
