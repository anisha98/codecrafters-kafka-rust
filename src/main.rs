use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");
    
    let listener = TcpListener::bind("127.0.0.1:9092").await.unwrap();
    
    // Use loop to continuously accept connections
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("accepted new connection from {:?}", addr);
                // Spawn async task for each connection (non-blocking)
                tokio::spawn(async move {
                    handle_connection(stream).await;
                });
            }
            Err(e) => {
                println!("error accepting connection: {}", e);
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream) {
    // Keep connection alive for multiple requests
    loop {
        match read_request(&mut stream).await {
            Ok((api_key, api_version, correlation_id)) => {
                println!("received request: api_key={}, api_version={}, correlation_id={}", 
                         api_key, api_version, correlation_id);
                send_response(&mut stream, api_key, api_version, correlation_id).await;
            }
            Err(_) => {
                println!("connection closed or error occurred");
                break;
            }
        }
    }
}

async fn read_request(stream: &mut TcpStream) -> Result<(i16, i16, i32), std::io::Error> {
    // Read message length
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let size = i32::from_be_bytes(size_buf);
    
    // Read message content
    let mut buf = vec![0u8; size as usize];
    stream.read_exact(&mut buf).await?;
    
    // Parse request header
    let api_key = i16::from_be_bytes([buf[0], buf[1]]);
    let api_version = i16::from_be_bytes([buf[2], buf[3]]);
    let correlation_id = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    
    Ok((api_key, api_version, correlation_id))
}

async fn send_response(stream: &mut TcpStream, api_key: i16, api_version: i16, correlation_id: i32) {
    // Build response
    let mut response = Vec::new();
    
    // Check if API and version are supported
    let error_code = if api_key == 18 && (0..=4).contains(&api_version) {
        0i16 // Success
    } else {
        35i16 // Unsupported version
    };
    
    // Add correlation ID
    response.extend_from_slice(&correlation_id.to_be_bytes());
    
    // Add error code
    response.extend_from_slice(&error_code.to_be_bytes());
    
    if error_code == 0 {
        // Add successful APIVersions response
        response.push(4); // Array length + 1 (3 APIs + 1)
        
        // API 17
        response.extend_from_slice(&17i16.to_be_bytes()); // API key
        response.extend_from_slice(&0i16.to_be_bytes());  // Min version
        response.extend_from_slice(&4i16.to_be_bytes());  // Max version
        response.push(0); // Tag buffer
        
        // API 18
        response.extend_from_slice(&18i16.to_be_bytes()); // API key
        response.extend_from_slice(&0i16.to_be_bytes());  // Min version
        response.extend_from_slice(&4i16.to_be_bytes());  // Max version
        response.push(0); // Tag buffer

        // API 75
        response.extend_from_slice(&75i16.to_be_bytes()); // API key
        response.extend_from_slice(&0i16.to_be_bytes());  // Min version
        response.extend_from_slice(&0i16.to_be_bytes());  // Max version
        response.push(0); // Tag buffer
        
        // Throttle time (4 bytes)
        response.extend_from_slice(&0i32.to_be_bytes());
        
        // Final tag buffer
        response.push(0);
    }
    
    // Send response with length prefix
    let response_length = response.len() as i32;
    let _ = stream.write_all(&response_length.to_be_bytes()).await;
    let _ = stream.write_all(&response).await;
    let _ = stream.flush().await;
}
