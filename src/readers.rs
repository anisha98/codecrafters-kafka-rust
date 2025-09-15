use std::io::{Cursor, Read};
use crate::KafkaError;

pub fn read_int16(cursor: &mut Cursor<&[u8]>) -> Result<i16, KafkaError> {
    let mut buf = [0u8; 2];
    cursor.read_exact(&mut buf)?;
    Ok(i16::from_be_bytes(buf))
}

pub fn read_int32(cursor: &mut Cursor<&[u8]>) -> Result<i32, KafkaError> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

pub fn read_nullable_string(cursor: &mut Cursor<&[u8]>) -> Result<Option<String>, KafkaError> {
    let length = read_int16(cursor)?;
    
    if length == -1 {
        return Ok(None);
    }
    
    if length < 0 {
        return Err(KafkaError::InvalidStringLength(length));
    }
    
    let mut buf = vec![0u8; length as usize];
    cursor.read_exact(&mut buf)?;
    
    let string = String::from_utf8(buf)?;
    Ok(Some(string))
}
