//! Structural admission owned by the AMQP codec, before allocation or typed visitation.
/// A truncated encoded value is distinct from malformed syntax for incremental input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionError {
    /// More encoded bytes are needed.
    Incomplete,
    /// The encoded structure is invalid.
    Invalid,
    /// The materialization budget was exceeded.
    Limit,
}
impl std::fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AMQP admission: {:?}", self)
    }
}
impl std::error::Error for AdmissionError {}
impl From<AdmissionError> for crate::Error {
    fn from(_: AdmissionError) -> Self {
        Self::InvalidValue
    }
}
fn malformed() -> AdmissionError {
    AdmissionError::Invalid
}
/// Bounds apply to one materialized value tree, not cumulative network traffic.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Maximum nesting depth.
    pub depth: usize,
    /// Maximum expanded nodes, including zero-width array elements.
    pub nodes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            depth: 64,
            nodes: 131072,
        }
    }
}
#[derive(Debug)]
/// An allocation-free cursor over encoded AMQP values.
pub struct Scan<'a> {
    bytes: &'a [u8],
    nodes: usize,
    limits: Limits,
}
impl<'a> Scan<'a> {
    /// Use the codec materialization limits.
    pub fn new(bytes: &'a [u8]) -> Self {
        Self::with_limits(bytes, Limits::default())
    }
    /// Use explicit limits for this value tree.
    pub fn with_limits(bytes: &'a [u8], limits: Limits) -> Self {
        Self {
            bytes,
            nodes: 0,
            limits,
        }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], AdmissionError> {
        if n > self.bytes.len() {
            return Err(AdmissionError::Incomplete);
        }
        let (a, b) = self.bytes.split_at(n);
        self.bytes = b;
        Ok(a)
    }
    fn number(&mut self, n: usize) -> Result<usize, AdmissionError> {
        let mut b = [0; 8];
        b[8 - n..].copy_from_slice(self.take(n)?);
        usize::try_from(u64::from_be_bytes(b)).map_err(|_| malformed())
    }
    /// Expanded nodes visited so far.
    pub fn nodes(&self) -> usize {
        self.nodes
    }
    /// Unconsumed bytes, including any following payload.
    pub fn remaining(&self) -> &'a [u8] {
        self.bytes
    }
    /// Validate every encoded value in the input.
    pub fn all(mut self) -> Result<(), AdmissionError> {
        while !self.bytes.is_empty() {
            self.value(0)?;
        }
        Ok(())
    }
    /// Validate one value at the given initial nesting depth.
    pub fn value(&mut self, depth: usize) -> Result<(), AdmissionError> {
        let (constructor, depth) = self.constructor(depth)?;
        self.payload(constructor, depth)
    }
    fn constructor(&mut self, depth: usize) -> Result<(u8, usize), AdmissionError> {
        if depth > self.limits.depth {
            return Err(AdmissionError::Limit);
        }
        let code = self.number(1)? as u8;
        if code == 0 {
            // A descriptor is an unsigned long or symbol, never arbitrary nested data.
            let descriptor = self.number(1)? as u8;
            if !matches!(descriptor, 0x44 | 0x53 | 0x80 | 0xa3 | 0xb3) {
                return Err(malformed());
            }
            self.payload(descriptor, depth + 1)?;
            self.constructor(depth + 1)
        } else {
            crate::format_code::EncodingCodes::try_from(code).map_err(|_| malformed())?;
            Ok((code, depth))
        }
    }
    fn payload(&mut self, code: u8, depth: usize) -> Result<(), AdmissionError> {
        self.nodes = self.nodes.checked_add(1).ok_or_else(malformed)?;
        if depth > self.limits.depth || self.nodes > self.limits.nodes {
            return Err(AdmissionError::Limit);
        }
        match code {
            0x40..=0x45 => {}
            0x50..=0x55 => {
                self.take(1)?;
            }
            0x56 => {
                if self.number(1)? > 1 {
                    return Err(malformed());
                }
            }
            0x60 | 0x61 => {
                self.take(2)?;
            }
            0x70..=0x72 | 0x74 => {
                self.take(4)?;
            }
            0x73 => {
                let c = self.number(4)? as u32;
                if char::from_u32(c).is_none() {
                    return Err(malformed());
                }
            }
            0x80..=0x84 => {
                self.take(8)?;
            }
            0x94 | 0x98 => {
                self.take(16)?;
            }
            0xa0 | 0xa1 | 0xa3 | 0xb0 | 0xb1 | 0xb3 => {
                let n = self.number(if code < 0xb0 { 1 } else { 4 })?;
                let b = self.take(n)?;
                if matches!(code, 0xa1 | 0xb1) && std::str::from_utf8(b).is_err() {
                    return Err(malformed());
                }
                if matches!(code, 0xa3 | 0xb3) && !b.is_ascii() {
                    return Err(malformed());
                }
            }
            0xc0 | 0xc1 | 0xd0 | 0xd1 | 0xe0 | 0xf0 => {
                let width = if matches!(code, 0xc0 | 0xc1 | 0xe0) {
                    1
                } else {
                    4
                };
                let size = self.number(width)?;
                let mut child = Self {
                    bytes: self.take(size)?,
                    nodes: self.nodes,
                    limits: self.limits,
                };
                // The enclosing compound is already present in full. A short
                // child is malformed, rather than a request for more wire bytes.
                let validate_child = (|| -> Result<(), AdmissionError> {
                    let count = child.number(width)?;
                    if count > self.limits.nodes.saturating_sub(child.nodes)
                        || matches!(code, 0xc1 | 0xd1) && count % 2 != 0
                    {
                        return Err(malformed());
                    }
                    if matches!(code, 0xe0 | 0xf0) {
                        let (element, element_depth) = child.constructor(depth + 1)?;
                        if element == 0x40 {
                            return Err(malformed());
                        }
                        for _ in 0..count {
                            child.payload(element, element_depth)?;
                        }
                    } else {
                        for _ in 0..count {
                            child.value(depth + 1)?;
                        }
                    }
                    if !child.bytes.is_empty() {
                        return Err(malformed());
                    }
                    Ok(())
                })();
                validate_child.map_err(|e| {
                    if e == AdmissionError::Incomplete {
                        AdmissionError::Invalid
                    } else {
                        e
                    }
                })?;
                self.nodes = child.nodes;
            }
            _ => return Err(malformed()),
        }
        Ok(())
    }
}

/// Validate an array element constructor without materializing a value.
pub fn validate_array_constructor(bytes: &[u8]) -> Result<(), AdmissionError> {
    let mut scan = Scan::new(bytes);
    let (code, _) = scan.constructor(0)?;
    if code == 0x40 || !scan.remaining().is_empty() {
        return Err(AdmissionError::Invalid);
    }
    // Reject unknown format codes even though no elements follow.
    crate::format_code::EncodingCodes::try_from(code).map_err(|_| AdmissionError::Invalid)?;
    Ok(())
}

/// Validate one value and return its exact encoded length without consuming a following payload.
pub fn value_extent(bytes: &[u8]) -> Result<usize, AdmissionError> {
    let mut scan = Scan::new(bytes);
    scan.value(0)?;
    Ok(bytes.len() - scan.remaining().len())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn count_depth_unicode_and_lengths_are_checked_before_materialization() {
        for b in [
            vec![0xd0, 255, 255, 255, 255],
            vec![0xc1, 2, 1, 0x40],
            vec![0xe0, 2, 5, 0x40],
            vec![0xc0, 3, 1, 0x40, 0x40],
            vec![0xa3, 1, 255],
        ] {
            assert!(Scan::new(&b).all().is_err());
        }
        assert_eq!(
            value_extent(&[0xb0, 255, 255, 255, 255]),
            Err(AdmissionError::Incomplete)
        );
        let mut b = vec![0x40];
        for _ in 0..70 {
            let mut next = vec![0xd0];
            next.extend_from_slice(&((b.len() + 4) as u32).to_be_bytes());
            next.extend_from_slice(&1u32.to_be_bytes());
            next.extend(b);
            b = next;
        }
        assert!(Scan::new(&b).all().is_err());
        // Zero-width true values are legal; the node budget, not encoded byte length, bounds expansion.
        assert!(Scan::new(&[0xe0, 2, 10, 0x41]).all().is_ok());
        assert!(Scan::new(&[0xe0, 2, 0, 0x70]).all().is_ok());
        assert!(
            Scan::with_limits(&[0xe0, 2, 10, 0x41], Limits { depth: 4, nodes: 8 })
                .all()
                .is_err()
        );
    }
}
