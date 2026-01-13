// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

/// Returns the next aligned offset based on the given offset and alignment.
/// For example:
/// - offset = 0, align = 8 -> returns 0
/// - offset = 5, align = 8 -> returns 8
/// - offset = 7, align = 8 -> returns 8
/// - offset = 8, align = 8 -> returns 8
/// - offset = 9, align = 8 -> returns 16
pub fn next_aligned_offset(offset: usize, align: usize) -> usize {
    if align == 0 {
        panic!("Alignment must be greater than zero");
    }
    offset.div_ceil(align) * align
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_aligned_offset_examples() {
        // Test cases from the documentation examples
        assert_eq!(next_aligned_offset(0, 8), 0);
        assert_eq!(next_aligned_offset(5, 8), 8);
        assert_eq!(next_aligned_offset(7, 8), 8);
        assert_eq!(next_aligned_offset(8, 8), 8);
        assert_eq!(next_aligned_offset(9, 8), 16);
    }

    #[test]
    fn test_next_aligned_offset_different_alignments() {
        // Test with alignment of 1 (should return the same offset)
        assert_eq!(next_aligned_offset(0, 1), 0);
        assert_eq!(next_aligned_offset(5, 1), 5);
        assert_eq!(next_aligned_offset(100, 1), 100);

        // Test with alignment of 4
        assert_eq!(next_aligned_offset(0, 4), 0);
        assert_eq!(next_aligned_offset(1, 4), 4);
        assert_eq!(next_aligned_offset(3, 4), 4);
        assert_eq!(next_aligned_offset(4, 4), 4);
        assert_eq!(next_aligned_offset(5, 4), 8);

        // Test with alignment of 16
        assert_eq!(next_aligned_offset(0, 16), 0);
        assert_eq!(next_aligned_offset(1, 16), 16);
        assert_eq!(next_aligned_offset(15, 16), 16);
        assert_eq!(next_aligned_offset(16, 16), 16);
        assert_eq!(next_aligned_offset(17, 16), 32);
    }

    #[test]
    #[should_panic(expected = "Alignment must be greater than zero")]
    fn test_next_aligned_offset_zero_alignment_panics() {
        next_aligned_offset(10, 0);
    }

    #[test]
    fn test_next_aligned_offset_large_values() {
        // Test with larger values to ensure no overflow issues
        assert_eq!(next_aligned_offset(1000, 64), 1024);
        assert_eq!(next_aligned_offset(1024, 64), 1024);
        assert_eq!(next_aligned_offset(1025, 64), 1088);
    }
}
