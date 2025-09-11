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
                            let correlation_id = [
                                received_data[8],   // 6f
                                received_data[9],   // 7f  
                                received_data[10],  // c6
                                received_data[11],  // 61
                            ];
                            
                            // Build response with extracted correlation ID
                            let response = [
                                0x00, 0x00, 0x00, 0x00,  // message_size
                                correlation_id[0],        // correlation_id[0]
                                correlation_id[1],        // correlation_id[1] 
                                correlation_id[2],        // correlation_id[2]
                                correlation_id[3],        // correlation_id[3]
                                0x00, 0x23
                            ];
                            
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
