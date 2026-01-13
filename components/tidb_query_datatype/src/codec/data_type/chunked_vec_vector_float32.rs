// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryInto;

use super::{
    ChunkRef, ChunkedVec, UnsafeRefInto, VectorFloat32, VectorFloat32Ref, bit_vec::BitVec,
};
use crate::{
    codec::{Error, Result, datum},
    impl_chunked_vec_common,
};

const LEN_PREFIX_SIZE: usize = std::mem::size_of::<u32>();
const ELEMENT_SIZE: usize = std::mem::size_of::<f32>();

/// A vector storing `Option<VectorFloat32>` with a compact layout.
///
/// Inside `ChunkedVecVectorFloat32`, `bitmap` indicates if an element at given
/// index is null, and `data` stores actual data. VectorFloat32 data are stored
/// adjacent to each other in `data`. If element at a given index is null, then
/// it takes no space in `data`. Otherwise, a variable size VectorFloat32 data
/// is stored in `data`, and `var_offset` indicates the starting position of
/// each element.
#[derive(Debug, PartialEq, Clone)]
pub struct ChunkedVecVectorFloat32 {
    data: Vec<f32>, // Only contains the data part, without any length prefix
    bitmap: BitVec,
    length: usize,
    var_offset: Vec<usize>,
}

impl ChunkedVecVectorFloat32 {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<VectorFloat32Ref<'_>> {
        assert!(idx < self.len());
        if self.bitmap.get(idx) {
            let sliced_data = &self.data[self.var_offset[idx]..self.var_offset[idx + 1]];
            Some(VectorFloat32Ref::from_f32(sliced_data))
        } else {
            None
        }
    }

    /// Pushes a VectorFloat32 datum in TiDB datum format (flag + payload).
    ///
    /// Compared with decoding into `VectorFloat32` first, this avoids per-row
    /// allocations and an extra copy.
    #[inline]
    pub fn push_vector_float32_datum(&mut self, raw_datum: &[u8]) -> Result<()> {
        if raw_datum.is_empty() {
            return Err(Error::InvalidDataType(
                "Failed to decode datum flag".to_owned(),
            ));
        }
        match raw_datum[0] {
            datum::NIL_FLAG => {
                self.push_null();
                Ok(())
            }
            datum::VECTOR_FLOAT32_FLAG => {
                // Keep aligned with `VectorFloat32Decoder`: VectorFloat32 only
                // supports little endian encoding.
                if !cfg!(target_endian = "little") {
                    return Err(Error::InvalidDataType(
                        "Failed to decode datum payload as vectorFloat32".to_owned(),
                    ));
                }

                let payload = &raw_datum[1..];
                if payload.is_empty() {
                    // Compatibility with `read_vector_float32_ref`: treat empty
                    // payload as an empty vector.
                    self.bitmap.push(true);
                    self.var_offset.push(self.data.len());
                    self.length += 1;
                    return Ok(());
                }
                if payload.len() < LEN_PREFIX_SIZE {
                    return Err(Error::InvalidDataType(
                        "Failed to decode datum payload as vectorFloat32".to_owned(),
                    ));
                }

                let n =
                    u32::from_le_bytes(payload[0..LEN_PREFIX_SIZE].try_into().unwrap()) as usize;
                let data_bytes_len = n.checked_mul(ELEMENT_SIZE).ok_or_else(|| {
                    Error::InvalidDataType(
                        "Failed to decode datum payload as vectorFloat32".to_owned(),
                    )
                })?;
                if payload.len() < LEN_PREFIX_SIZE + data_bytes_len {
                    return Err(Error::InvalidDataType(
                        "Failed to decode datum payload as vectorFloat32".to_owned(),
                    ));
                }
                let data_bytes = &payload[LEN_PREFIX_SIZE..LEN_PREFIX_SIZE + data_bytes_len];

                self.bitmap.push(true);
                let old_len = self.data.len();
                self.data.reserve(n);
                // SAFETY: We reserve `n` extra `f32`s and copy bytes into the
                // spare capacity, then update the length. The newly added
                // elements are fully initialized by the copy.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        data_bytes.as_ptr(),
                        self.data.spare_capacity_mut().as_mut_ptr().cast::<u8>(),
                        data_bytes_len,
                    );
                    self.data.set_len(old_len + n);
                }
                self.var_offset.push(self.data.len());
                self.length += 1;
                Ok(())
            }
            flag => Err(Error::InvalidDataType(format!(
                "Unsupported datum flag {} for VectorFloat32 vector",
                flag
            ))),
        }
    }
}

impl ChunkedVec<VectorFloat32> for ChunkedVecVectorFloat32 {
    impl_chunked_vec_common! { VectorFloat32 }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            bitmap: BitVec::with_capacity(capacity),
            var_offset: vec![0],
            length: 0,
        }
    }

    #[inline]
    fn push_data(&mut self, value: VectorFloat32) {
        self.bitmap.push(true);
        self.data.extend_from_slice(value.as_ref().data());
        self.var_offset.push(self.data.len());
        self.length += 1;
    }

    #[inline]
    fn push_null(&mut self) {
        self.bitmap.push(false);
        self.var_offset.push(self.data.len());
        self.length += 1;
    }

    fn len(&self) -> usize {
        self.length
    }

    fn truncate(&mut self, len: usize) {
        if len < self.len() {
            self.data.truncate(self.var_offset[len]);
            self.bitmap.truncate(len);
            self.var_offset.truncate(len + 1);
            self.length = len;
        }
    }

    fn capacity(&self) -> usize {
        self.data.capacity().max(self.length)
    }

    fn append(&mut self, other: &mut Self) {
        self.data.append(&mut other.data);
        self.bitmap.append(&mut other.bitmap);
        let var_offset_last = *self.var_offset.last().unwrap();
        for i in 1..other.var_offset.len() {
            self.var_offset.push(other.var_offset[i] + var_offset_last);
        }
        self.length += other.length;
        other.var_offset = vec![0];
        other.length = 0;
    }

    fn to_vec(&self) -> Vec<Option<VectorFloat32>> {
        let mut x = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            x.push(self.get(i).map(|x| x.to_owned()));
        }
        x
    }
}

impl<'a> ChunkRef<'a, VectorFloat32Ref<'a>> for &'a ChunkedVecVectorFloat32 {
    #[inline]
    fn get_option_ref(self, idx: usize) -> Option<VectorFloat32Ref<'a>> {
        self.get(idx)
    }

    fn get_bit_vec(self) -> &'a BitVec {
        &self.bitmap
    }

    #[inline]
    fn phantom_data(self) -> Option<VectorFloat32Ref<'a>> {
        None
    }
}

impl From<Vec<Option<VectorFloat32>>> for ChunkedVecVectorFloat32 {
    fn from(v: Vec<Option<VectorFloat32>>) -> ChunkedVecVectorFloat32 {
        ChunkedVecVectorFloat32::from_vec(v)
    }
}

impl UnsafeRefInto<&'static ChunkedVecVectorFloat32> for &ChunkedVecVectorFloat32 {
    unsafe fn unsafe_into(self) -> &'static ChunkedVecVectorFloat32 {
        std::mem::transmute(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_data_and_get() {
        let mut chunked_vec = ChunkedVecVectorFloat32::with_capacity(5);

        chunked_vec.push_data(VectorFloat32::copy_from_f32(&[1.1, 2.2, 3.3]));
        chunked_vec.push_data(VectorFloat32::copy_from_f32(&[4.4, 5.5]));
        // push a null value
        chunked_vec.push_null();
        chunked_vec.push_data(VectorFloat32::copy_from_f32(&[6.6, 7.7, 8.8, 9.9]));

        assert_eq!(chunked_vec.len(), 4);

        let vector1 = chunked_vec.get(0).unwrap().to_string();
        assert_eq!(vector1, "[1.1,2.2,3.3]");

        let vector2 = chunked_vec.get(1).unwrap().to_string();
        assert_eq!(vector2, "[4.4,5.5]");

        // check if null value is right
        assert!(chunked_vec.get(2).is_none());

        let vector3 = chunked_vec.get(3).unwrap().to_string();
        assert_eq!(vector3, "[6.6,7.7,8.8,9.9]");
    }
}
