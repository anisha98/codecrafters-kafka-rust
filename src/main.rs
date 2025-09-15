// Import standard library modules for I/O operations and networking
use std::{
    io::{Cursor, Read, Write}, // Cursor for in-memory reading, Read/Write traits for I/O
    net::{TcpListener, TcpStream}, // TCP networking components
};
use thiserror::Error; // Derive macro for creating custom error types

// Import our custom readers module for parsing Kafka protocol data
mod readers;
use readers::*;

// ### KAFKA ERROR CODES ### //
// These constants define standard Kafka error codes as per the protocol specification
const UNKNOWN_SERVER_ERROR: i16 = -1; // Generic server error
const NONE: i16 = 0;                   // No error (success)
const CORRUPT_MESSAGE: i16 = 2;        // Message format is invalid
const UNSUPPORTED_VERSION: i16 = 35;   // Requested API version not supported
const INVALID_REQUEST: i16 = 42;       // Request format is invalid

// Custom error enum for handling different types of Kafka protocol errors
// Uses thiserror to automatically implement Display and Error traits
#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error), // Network/file I/O errors (auto-converted from std::io::Error)
    #[error("Invalid message length: {0}")]
    InvalidMessageLength(i32), // Message size is invalid (too large, negative, etc.)
    #[error("Unsupported API version: {0}")]
    UnsupportedApiVersion(i16), // Client requested an API version we don't support
    #[error("Invalid string data: {0}")]
    InvalidString(#[from] std::string::FromUtf8Error), // String contains invalid UTF-8 data
    #[error("Invalid string length: {0}")]
    InvalidStringLength(i16), // String length field is invalid (negative)
    #[error("Unsupported API key: {0}")]
    UnsupportedApiKey(i16), // Client requested an API we don't implement
}

// Implementation to convert our custom errors to Kafka protocol error codes
impl KafkaError {
    pub fn to_error_code(&self) -> i16 {
        match self {
            KafkaError::Io(_) => UNKNOWN_SERVER_ERROR,        // -1: Generic server error
            KafkaError::UnsupportedApiKey(_) => INVALID_REQUEST, // 42: Invalid request format
            KafkaError::InvalidMessageLength(_) => CORRUPT_MESSAGE, // 2: Message format error
            KafkaError::InvalidString(_) => CORRUPT_MESSAGE,   // 2: Message format error
            KafkaError::InvalidStringLength(_) => CORRUPT_MESSAGE, // 2: Message format error
            KafkaError::UnsupportedApiVersion(_) => UNSUPPORTED_VERSION, // 35: Version not supported
        }
    }
}

// ### PROTOCOL CONSTANTS ### //
// Static array defining which API keys our server supports and their version ranges
const API_VERS_INFO: &[ApiKeyVerInfo] = &[
    ApiKeyVerInfo { id: 17, min: 0, max: 4 }, // API key 17: versions 0-4 supported
    ApiKeyVerInfo { id: 18, min: 0, max: 4 }, // API key 18 (APIVersions): versions 0-4 supported
    ApiKeyVerInfo { id: 19, min: 0, max: 4 }, // API key 19: versions 0-4 supported
];
// Empty tag buffer used in Kafka's compact format for forward compatibility
const TAG_BUFFER: &[u8] = &[0];

// Structure representing the header of a Kafka request message
struct KafkaRequestHeader {
    api_key: i16,        // Which API is being called (e.g., 18 = APIVersions)
    api_ver: i16,        // Version of the API being requested
    correlation_id: i32, // Unique ID to match requests with responses
    _client_id: Option<String>, // Optional client identifier (prefixed with _)
}

// Implementation for parsing Kafka request headers from raw bytes
impl KafkaRequestHeader {
    pub fn parse(buffer: &[u8]) -> Result<Self, KafkaError> {
        let mut cursor = Cursor::new(buffer); // Create cursor for sequential reading
        let api_key = read_int16(&mut cursor)?; // Read 2-byte API key (big-endian)
        let api_ver = read_int16(&mut cursor)?; // Read 2-byte API version (big-endian)
        let correlation_id = read_int32(&mut cursor)?; // Read 4-byte correlation ID (big-endian)
        let _client_id = read_nullable_string(&mut cursor)?; // Read optional client ID string

        // Return parsed header structure
        Ok(KafkaRequestHeader {
            api_key,
            api_ver,
            correlation_id,
            _client_id,
        })
    }
}

// Enum representing different types of responses our server can send
enum KafkaResponse {
    ApiVersions(ApiVersionsResponse), // Successful APIVersions response with supported APIs
    Error(ErrorResponse),             // Error response with error code
}

// Structure for APIVersions response containing supported API information
struct ApiVersionsResponse {
    pub correlation_id: i32,                      // Echo back the request's correlation ID
    pub api_key_versions: &'static [ApiKeyVerInfo], // Reference to supported API versions
}

// Structure defining an API key and its supported version range
struct ApiKeyVerInfo {
    pub id: i16,  // API key identifier (e.g., 18 for APIVersions)
    pub min: i16, // Minimum supported version for this API
    pub max: i16, // Maximum supported version for this API
}

// Structure for error responses
struct ErrorResponse {
    pub correlation_id: i32, // Echo back the request's correlation ID
    pub error_code: i16,     // Kafka error code (e.g., 35 for unsupported version)
}

// Main function - entry point of the Kafka broker
fn main() {
    println!("Logs from your program will appear here!"); // Debug output for testing
    
    // Bind TCP listener to localhost:9092 (standard Kafka port)
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    // Accept incoming connections in a loop
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => { // Successfully accepted a connection
                println!("accepted new connection"); // Log the new connection
                // Handle the connection and log any errors that occur
                if let Err(e) = handle_connection(stream) {
                    eprintln!("Error handling connection: {}", e);
                }
            }
            Err(e) => { // Failed to accept connection
                println!("error: {}", e); // Log the error
            }
        }
    }
}

// Function to handle a single client connection
pub fn handle_connection(mut stream: TcpStream) -> Result<(), KafkaError> {
    // Step 1: Read the complete request from the client
    let request_buffer = read_request(&mut stream)?;
    
    // Step 2: Parse the request header to extract API info
    let request_header = match KafkaRequestHeader::parse(&request_buffer) {
        Ok(header) => header, // Successfully parsed header
        Err(e) => {
            // Failed to parse - log error and return (don't crash the server)
            eprintln!("Error parsing incoming request header: {:?}", e);
            return Ok(());
        }
    };

    // Step 3: Process the request and generate appropriate response
    let response = match process_request(&request_header, &request_buffer) {
        Ok(response) => response, // Successfully processed request
        Err(e) => KafkaResponse::Error(ErrorResponse { // Error occurred - create error response
            correlation_id: request_header.correlation_id, // Echo back correlation ID
            error_code: e.to_error_code(), // Convert our error to Kafka error code
        }),
    };

    // Step 4: Send the response back to the client
    send_response(&mut stream, &response)?;
    Ok(()) // Connection handled successfully
}

// Function to read a complete Kafka request from the TCP stream
fn read_request(stream: &mut TcpStream) -> Result<Vec<u8>, KafkaError> {
    // Step 1: Read the 4-byte message length prefix
    let mut size_buf = [0u8; 4]; // Buffer for message length
    stream.read_exact(&mut size_buf)?; // Read exactly 4 bytes
    let size = i32::from_be_bytes(size_buf); // Convert big-endian bytes to integer

    // Step 2: Validate message length
    if size <= 0 || size > 1_000_000 { // Check for reasonable size limits
        return Err(KafkaError::InvalidMessageLength(size));
    }

    // Step 3: Read the actual message content
    let mut buf = vec![0u8; size as usize]; // Allocate buffer for message
    stream.read_exact(&mut buf)?; // Read the complete message

    Ok(buf) // Return the message bytes
}

// Function to process a request and determine the appropriate response
fn process_request(
    request_header: &KafkaRequestHeader, // Parsed request header
    _request_buffer: &[u8], // Full request buffer (unused for APIVersions)
) -> Result<KafkaResponse, KafkaError> {
    // Match on the API key to determine which API is being called
    match request_header.api_key {
        18 => { // APIVersions API (key 18)
            // Check if the requested version is supported (0-4)
            if !(0..=4).contains(&request_header.api_ver) {
                Err(KafkaError::UnsupportedApiVersion(request_header.api_ver))
            } else {
                // Return successful APIVersions response with supported APIs
                Ok(KafkaResponse::ApiVersions(ApiVersionsResponse {
                    correlation_id: request_header.correlation_id, // Echo correlation ID
                    api_key_versions: API_VERS_INFO, // Our supported API list
                }))
            }
        }
        17 | 19 => { // Other supported API keys (17, 19)
            // Check version support for these APIs too
            if !(0..=4).contains(&request_header.api_ver) {
                Err(KafkaError::UnsupportedApiVersion(request_header.api_ver))
            } else {
                // For now, return APIVersions response for all supported APIs
                Ok(KafkaResponse::ApiVersions(ApiVersionsResponse {
                    correlation_id: request_header.correlation_id,
                    api_key_versions: API_VERS_INFO,
                }))
            }
        }
        _ => Err(KafkaError::UnsupportedApiKey(request_header.api_key)), // Unsupported API key
    }
}

// Function to serialize and send a response back to the client
fn send_response(stream: &mut TcpStream, response: &KafkaResponse) -> Result<(), KafkaError> {
    let mut res_buf = vec![]; // Buffer to build the response bytes

    // Build response based on response type
    match response {
        KafkaResponse::ApiVersions(api_versions) => {
            // Build APIVersions response according to Kafka protocol
            
            // Add correlation ID (4 bytes, big-endian) - echo back from request
            res_buf.extend_from_slice(&api_versions.correlation_id.to_be_bytes());
            
            // Add error code (2 bytes, big-endian) - NONE (0) for success
            res_buf.extend_from_slice(&NONE.to_be_bytes());
            
            // Add API keys array length in compact format (length + 1)
            // This tells the client how many API entries follow
            res_buf
                .extend_from_slice(&(api_versions.api_key_versions.len() as u8 + 1).to_be_bytes());
            
            // Add each supported API key with its version range
            for api_key in api_versions.api_key_versions {
                res_buf.extend_from_slice(&api_key.id.to_be_bytes());  // API key ID (2 bytes)
                res_buf.extend_from_slice(&api_key.min.to_be_bytes()); // Min version (2 bytes)
                res_buf.extend_from_slice(&api_key.max.to_be_bytes()); // Max version (2 bytes)
                res_buf.extend_from_slice(TAG_BUFFER); // Tag buffer for forward compatibility
            }

            // Add throttle time in milliseconds (4 bytes) - 0 means no throttling
            res_buf.extend_from_slice(&[0u8; 4]);
            
            // Add final tag buffer for protocol compliance
            res_buf.extend_from_slice(TAG_BUFFER);
        }

        KafkaResponse::Error(err_res) => {
            // Build error response - much simpler format
            
            // Add correlation ID (4 bytes, big-endian) - echo back from request
            res_buf.extend_from_slice(&err_res.correlation_id.to_be_bytes());
            
            // Add error code (2 bytes, big-endian) - the specific error that occurred
            res_buf.extend_from_slice(&err_res.error_code.to_be_bytes());
        }
    };

    // Send the complete response with length prefix
    write_response_with_len(stream, &res_buf)
}

// Function to write a response with the required 4-byte length prefix
fn write_response_with_len(
    stream: &mut TcpStream,     // TCP stream to write to
    response_buffer: &[u8],     // Response data to send
) -> Result<(), KafkaError> {
    // Calculate response size and convert to big-endian bytes
    let size = response_buffer.len() as i32;
    
    // Write the 4-byte length prefix first (Kafka protocol requirement)
    stream.write_all(&size.to_be_bytes())?;
    
    // Write the actual response data
    stream.write_all(response_buffer)?;

    // Flush the stream to ensure data is sent immediately
    match stream.flush() {
        Ok(_) => Ok(()), // Successfully sent response
        Err(e) => Err(KafkaError::Io(e)), // Convert IO error to our error type
    }
}
