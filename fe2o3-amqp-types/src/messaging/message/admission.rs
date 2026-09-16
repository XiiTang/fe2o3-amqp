//! Message section grammar is checked before a codec can overwrite duplicate sections.
use serde_amqp::admission::Scan;
use serde_amqp::Error;
fn invalid() -> Error {
    Error::InvalidValue
}
/// Validate message section order, uniqueness, descriptors and body families.
pub fn validate(bytes: &[u8]) -> Result<(), Error> {
    let mut scan = Scan::new(bytes);
    let mut previous = None;
    while !scan.remaining().is_empty() {
        let start = scan.remaining();
        scan.value(0).map_err(Error::from)?;
        let section = &start[..start.len() - scan.remaining().len()];
        let (code, body) = descriptor(section)?;
        if previous.is_some_and(|last| code < last || code == last && !matches!(code, 0x75 | 0x76))
            || previous == Some(0x75) && code == 0x76
            || matches!(previous, Some(0x75 | 0x76)) && code == 0x77
        {
            return Err(invalid());
        }
        let constructor = *body.first().ok_or_else(invalid)?;
        let correct_type = match code {
            0x70 | 0x73 | 0x76 => matches!(constructor, 0x45 | 0xc0 | 0xd0),
            0x71 | 0x72 | 0x74 | 0x78 => matches!(constructor, 0xc1 | 0xd1),
            0x75 => matches!(constructor, 0xa0 | 0xb0),
            0x77 => true,
            _ => false,
        };
        if !correct_type {
            return Err(invalid());
        }
        previous = Some(code);
    }
    Ok(())
}
fn descriptor(bytes: &[u8]) -> Result<(u64, &[u8]), Error> {
    if bytes.first() != Some(&0) {
        return Err(invalid());
    }
    let (code, offset) = match bytes.get(1) {
        Some(0x53) => (u64::from(*bytes.get(2).ok_or_else(invalid)?), 3),
        Some(0x80) => (
            u64::from_be_bytes(bytes.get(2..10).ok_or_else(invalid)?.try_into().unwrap()),
            10,
        ),
        Some(0xa3 | 0xb3) => {
            let width: usize = if bytes[1] == 0xa3 { 1 } else { 4 };
            let length = if width == 1 {
                usize::from(*bytes.get(2).ok_or_else(invalid)?)
            } else {
                u32::from_be_bytes(bytes.get(2..6).ok_or_else(invalid)?.try_into().unwrap())
                    as usize
            };
            let end = (2 + width).checked_add(length).ok_or_else(invalid)?;
            let name = bytes.get(2 + width..end).ok_or_else(invalid)?;
            let names: [&[u8]; 9] = [
                b"amqp:header:list",
                b"amqp:delivery-annotations:map",
                b"amqp:message-annotations:map",
                b"amqp:properties:list",
                b"amqp:application-properties:map",
                b"amqp:data:binary",
                b"amqp:amqp-sequence:list",
                b"amqp:amqp-value:*",
                b"amqp:footer:map",
            ];
            let index = names.iter().position(|n| *n == name).ok_or_else(invalid)?;
            (0x70 + index as u64, end)
        }
        _ => return Err(invalid()),
    };
    Ok((code, bytes.get(offset..).ok_or_else(invalid)?))
}

/// Find the section and byte position of the received prefix. Section boundaries
/// are parsed by encoded lengths, never by searching for descriptor-like payload bytes.
pub fn prefix_position(bytes: &[u8]) -> Result<(u32, u64), Error> {
    let mut offset = 0usize;
    let mut number = 0u32;
    while offset < bytes.len() {
        match serde_amqp::admission::value_extent(&bytes[offset..]) {
            Ok(length) => {
                descriptor(&bytes[offset..offset + length])?;
                offset += length;
                number = number.checked_add(1).ok_or_else(invalid)?;
            }
            Err(serde_amqp::admission::AdmissionError::Incomplete) => {
                return Ok((number, (bytes.len() - offset) as u64))
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok((number, 0))
}
/// Resolve an explicit resumption point into a retained prefix. Points beyond
/// the retained bytes or outside their encoded section are rejected.
pub fn prefix_offset(bytes: &[u8], number: u32, offset: u64) -> Result<usize, Error> {
    let mut start = 0usize;
    for _ in 0..number {
        let length = serde_amqp::admission::value_extent(bytes.get(start..).ok_or_else(invalid)?)
            .map_err(Error::from)?;
        descriptor(&bytes[start..start + length])?;
        start += length;
    }
    let offset = usize::try_from(offset).map_err(|_| invalid())?;
    let rest = bytes.get(start..).ok_or_else(invalid)?;
    if offset > rest.len() {
        return Err(invalid());
    }
    if let Ok(length) = serde_amqp::admission::value_extent(rest) {
        if offset > length {
            return Err(invalid());
        }
    }
    start.checked_add(offset).ok_or_else(invalid)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fragmented_payload_header_lookalikes_are_not_sections() {
        let bytes = [
            0, 0x53, 0x75, 0xa0, 6, 0, 0x53, 0x75, 0, 255, 7, 0, 0x53, 0x75, 0xa0, 1, 9,
        ];
        for n in 0..11 {
            assert_eq!(prefix_position(&bytes[..n]).unwrap(), (0, n as u64));
        }
        assert_eq!(prefix_position(&bytes[..11]).unwrap(), (1, 0));
        for n in 12..17 {
            assert_eq!(prefix_position(&bytes[..n]).unwrap(), (1, (n - 11) as u64));
        }
        assert_eq!(prefix_position(&bytes).unwrap(), (2, 0));
        assert_eq!(prefix_offset(&bytes, 1, 3).unwrap(), 14);
        assert!(prefix_offset(&bytes, 0, 12).is_err());
        assert!(prefix_offset(&bytes[..13], 1, 3).is_err());
        validate(&bytes).unwrap();
    }
}
