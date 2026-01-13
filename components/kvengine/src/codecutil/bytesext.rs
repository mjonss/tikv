// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::RangeBounds;

use anyhow::{Result, anyhow, bail};
use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;

/// Extension trait for `bytes::Bytes` to provide additional reading utilities.
pub trait BytesExt {
    /// Reads a specified number of bytes from the end.
    /// The bytes to read will be returned, and the original bytes will be
    /// shrunk.
    fn try_get_last(&mut self, len: usize) -> Result<Bytes>;

    /// Reads a specified number of bytes from the start.
    /// The bytes to read will be returned, and the original bytes will be
    /// advanced.
    fn try_get_first(&mut self, len: usize) -> Result<Bytes>;

    /// Reads a little-endian u16 from the end.
    /// Bytes are shrunk after reading.
    fn try_get_last_u16_le(&mut self) -> Result<u16>;

    /// Reads a little-endian u32 from the end.
    /// Bytes are shrunk after reading.
    fn try_get_last_u32_le(&mut self) -> Result<u32>;

    /// Reads a little-endian u64 from the end.
    /// Bytes are shrunk after reading.
    fn try_get_last_u64_le(&mut self) -> Result<u64>;

    /// Returns an error if the bytes address is not aligned to the specified
    /// alignment.
    fn check_aligned<T>(&self) -> Result<()>;

    /// Returns a slice of type `T` from the bytes.
    /// The bytes must be aligned to the size of `T` otherwise an error is
    /// returned.
    fn try_as_slice<T: bytemuck::AnyBitPattern>(&self) -> Result<&[T]>;

    /// The same as bytes::slice but returns a Result instead of panic when the
    /// slice is out of bounds.
    fn try_slice(&self, range: impl RangeBounds<usize>) -> Result<Bytes>;
}

impl BytesExt for bytes::Bytes {
    fn try_get_last(&mut self, len: usize) -> Result<Bytes> {
        if len > self.len() {
            bail!("Unexpected EOF, len={}, to_read={}", self.len(), len);
        }
        let new_offset = self.len() - len;
        Ok(self.split_off(new_offset))
    }

    fn try_get_first(&mut self, len: usize) -> Result<Bytes> {
        if len > self.len() {
            bail!("Unexpected EOF, len={}, to_read={}", self.len(), len);
        }
        Ok(self.split_to(len))
    }

    fn try_get_last_u16_le(&mut self) -> Result<u16> {
        let new_offset = self
            .len()
            .checked_sub(2)
            .ok_or_else(|| anyhow!("Unexpected EOF, len={}, to_read=2", self.len()))?;
        let value = LittleEndian::read_u16(&self[new_offset..]);
        self.truncate(new_offset);
        Ok(value)
    }

    fn try_get_last_u32_le(&mut self) -> Result<u32> {
        let new_offset = self
            .len()
            .checked_sub(4)
            .ok_or_else(|| anyhow!("Unexpected EOF, len={}, to_read=4", self.len()))?;
        let value = LittleEndian::read_u32(&self[new_offset..]);
        self.truncate(new_offset);
        Ok(value)
    }

    fn try_get_last_u64_le(&mut self) -> Result<u64> {
        let new_offset = self
            .len()
            .checked_sub(8)
            .ok_or_else(|| anyhow!("Unexpected EOF, len={}, to_read=8", self.len()))?;
        let value = LittleEndian::read_u64(&self[new_offset..]);
        self.truncate(new_offset);
        Ok(value)
    }

    fn check_aligned<T>(&self) -> Result<()> {
        let ptr = self.as_ref().as_ptr() as usize;
        let align = std::mem::align_of::<T>();
        if ptr % align != 0 {
            bail!(
                "Bytes are not aligned to {}, ptr={:p}, len={}",
                align,
                self.as_ref().as_ptr(),
                self.len()
            );
        }
        Ok(())
    }

    fn try_as_slice<T: bytemuck::AnyBitPattern>(&self) -> Result<&[T]> {
        bytemuck::try_cast_slice::<u8, T>(self.as_ref()).map_err(|e| {
            anyhow!(
                "Invalid bytes for type {}, len={}: {}",
                std::any::type_name::<T>(),
                self.len(),
                e
            )
        })
    }

    fn try_slice(&self, range: impl RangeBounds<usize>) -> Result<Bytes> {
        // https://docs.rs/bytes/1.10.1/src/bytes/bytes.rs.html#373-413
        let len = self.len();
        let begin = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n
                .checked_add(1)
                .ok_or_else(|| anyhow!("Range start out of bounds: {:?}", n))?,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&n) => n
                .checked_add(1)
                .ok_or_else(|| anyhow!("Range end out of bounds: {:?}", n))?,
            std::ops::Bound::Excluded(&n) => n,
            std::ops::Bound::Unbounded => len,
        };
        if begin > end {
            bail!(
                "Range start must not be greater than end: {:?} <= {:?}",
                begin,
                end
            );
        }
        if end > len {
            bail!("Range end out of bounds: {:?} <= {:?}", end, len);
        }
        Ok(self.slice(range))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_last() {
        // Success case
        let mut bytes = Bytes::from(vec![1, 2, 3, 4, 5]);
        let result = bytes.try_get_last(2).expect("should succeed");
        assert_eq!(result, Bytes::from(vec![4, 5]));
        assert_eq!(bytes, Bytes::from(vec![1, 2, 3]));

        // Error case
        let mut bytes = Bytes::from(vec![1, 2]);
        let result = bytes.try_get_last(5);
        result.unwrap_err();
    }

    #[test]
    fn test_get_first() {
        // Success case
        let mut bytes = Bytes::from(vec![1, 2, 3, 4, 5]);
        let result = bytes.try_get_first(2).expect("should succeed");
        assert_eq!(result, Bytes::from(vec![1, 2]));
        assert_eq!(bytes, Bytes::from(vec![3, 4, 5]));

        // Error case
        let mut bytes = Bytes::from(vec![1, 2]);
        let result = bytes.try_get_first(5);
        result.unwrap_err();
    }

    #[test]
    fn test_get_last_u16_le() {
        // Success case
        let mut bytes = Bytes::from(vec![1, 2, 3, 4]);
        let result = bytes.try_get_last_u16_le().expect("should succeed");
        assert_eq!(result, 0x0403); // little endian
        assert_eq!(bytes, Bytes::from(vec![1, 2]));

        // Error case
        let mut bytes = Bytes::from(vec![1]);
        let result = bytes.try_get_last_u16_le();
        result.unwrap_err();
    }

    #[test]
    fn test_get_last_u32_le() {
        // Success case
        let mut bytes = Bytes::from(vec![1, 2, 3, 4, 5, 6]);
        let result = bytes.try_get_last_u32_le().expect("should succeed");
        assert_eq!(result, 0x06050403); // little endian
        assert_eq!(bytes, Bytes::from(vec![1, 2]));

        // Error case
        let mut bytes = Bytes::from(vec![1, 2]);
        let result = bytes.try_get_last_u32_le();
        result.unwrap_err();
    }

    #[test]
    fn test_get_last_u64_le() {
        // Success case
        let mut bytes = Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let result = bytes.try_get_last_u64_le().expect("should succeed");
        assert_eq!(result, 0x0a09080706050403); // little endian
        assert_eq!(bytes, Bytes::from(vec![1, 2]));

        // Error case
        let mut bytes = Bytes::from(vec![1, 2, 3]);
        let result = bytes.try_get_last_u64_le();
        result.unwrap_err();
    }

    #[test]
    fn test_check_aligned() {
        // A standard byte allocation should be aligned
        let bytes = Bytes::from(vec![1, 2, 3, 4]);
        let result = bytes.check_aligned::<u32>();
        result.unwrap();

        bytes.slice(1..).check_aligned::<u32>().unwrap_err();
    }

    #[test]
    fn test_as_slice() {
        // Success case
        let bytes = Bytes::from(vec![1u8, 2, 3, 4]);
        let result = bytes.try_as_slice::<u8>().expect("should succeed");
        assert_eq!(result, &[1u8, 2, 3, 4]);

        // Error case - misaligned size
        let bytes = Bytes::from(vec![1u8, 2, 3]);
        let result = bytes.try_as_slice::<u32>();
        result.unwrap_err();
    }

    #[test]
    fn test_try_slice() {
        let bytes = Bytes::from(vec![1, 2, 3, 4, 5]);

        // Success case
        let result = bytes.try_slice(1..4).expect("should succeed");
        assert_eq!(result, Bytes::from(vec![2, 3, 4]));

        // Error case - out of bounds
        let result = bytes.try_slice(10..20);
        result.unwrap_err();

        // Error case - invalid range (start > end)
        #[allow(clippy::reversed_empty_ranges)]
        let result = bytes.try_slice(4..2);
        result.unwrap_err();
    }
}
