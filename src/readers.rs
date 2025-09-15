// Import required modules for reading binary data
use std::io::{Cursor, Read}; // Cursor for in-memory reading, Read trait for reading operations
use crate::KafkaError; // Import our custom error type from main module

// Function to read a 16-bit signed integer from a cursor in big-endian format
// Used for reading API keys, API versions, error codes, etc.
pub fn read_int16(cursor: &mut Cursor<&[u8]>) -> Result<i16, KafkaError> {
    let mut buf = [0u8; 2]; // Create 2-byte buffer for the integer
    cursor.read_exact(&mut buf)?; // Read exactly 2 bytes from cursor
    Ok(i16::from_be_bytes(buf)) // Convert big-endian bytes to i16 and return
}

// Function to read a 32-bit signed integer from a cursor in big-endian format
// Used for reading correlation IDs, message lengths, throttle times, etc.
pub fn read_int32(cursor: &mut Cursor<&[u8]>) -> Result<i32, KafkaError> {
    let mut buf = [0u8; 4]; // Create 4-byte buffer for the integer
    cursor.read_exact(&mut buf)?; // Read exactly 4 bytes from cursor
    Ok(i32::from_be_bytes(buf)) // Convert big-endian bytes to i32 and return
}

// Function to read a nullable string from a cursor according to Kafka protocol
// Kafka strings are prefixed with a 16-bit length field
// A length of -1 indicates a null string
pub fn read_nullable_string(cursor: &mut Cursor<&[u8]>) -> Result<Option<String>, KafkaError> {
    // First, read the 16-bit length prefix
    let length = read_int16(cursor)?;
    
    // Check for null string indicator
    if length == -1 {
        return Ok(None); // Return None for null strings
    }
    
    // Validate string length - negative values (except -1) are invalid
    if length < 0 {
        return Err(KafkaError::InvalidStringLength(length));
    }
    
    // Allocate buffer for string data based on length
    let mut buf = vec![0u8; length as usize];
    
    // Read the string bytes from cursor
    cursor.read_exact(&mut buf)?;
    
    // Convert bytes to UTF-8 string (may fail if invalid UTF-8)
    let string = String::from_utf8(buf)?;
    
    // Return the parsed string wrapped in Some
    Ok(Some(string))
}
