#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::Write;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap(); //9092 port is typically used for kafka
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                
                // Send hardcoded "7" back to the client
                stream.write_all(b"7").unwrap();
                stream.flush().unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
