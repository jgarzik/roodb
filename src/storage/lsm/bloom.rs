//! Bloom filter for SSTable point-lookup acceleration
//!
//! Uses double-hashing (h1 + i*h2) to generate k hash functions.
//! Parameters: 10 bits/key, 3 hash functions → ~1.7% false positive rate.

/// Number of hash functions
const NUM_HASHES: u32 = 3;

/// Bits per key
const BITS_PER_KEY: usize = 10;

/// Bloom filter
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: u32,
    num_hashes: u32,
}

impl BloomFilter {
    /// Create a new bloom filter sized for `num_keys` keys
    pub fn new(num_keys: usize) -> Self {
        let num_bits = (num_keys * BITS_PER_KEY).max(64) as u32;
        let num_bytes = num_bits.div_ceil(8) as usize;
        BloomFilter {
            bits: vec![0u8; num_bytes],
            num_bits,
            num_hashes: NUM_HASHES,
        }
    }

    /// Add a key to the bloom filter
    pub fn add(&mut self, key: &[u8]) {
        let (h1, h2) = double_hash(key);
        for i in 0..self.num_hashes {
            let bit = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % self.num_bits as u64;
            self.bits[bit as usize / 8] |= 1 << (bit % 8);
        }
    }

    /// Check if a key might be present (false positives possible)
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = double_hash(key);
        for i in 0..self.num_hashes {
            let bit = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % self.num_bits as u64;
            if self.bits[bit as usize / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize to bytes: [num_hashes: 4B][num_bits: 4B][bit_data...]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.bits.len());
        out.extend_from_slice(&self.num_hashes.to_le_bytes());
        out.extend_from_slice(&self.num_bits.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let num_hashes = u32::from_le_bytes(data[0..4].try_into().ok()?);
        let num_bits = u32::from_le_bytes(data[4..8].try_into().ok()?);
        let expected_bytes = num_bits.div_ceil(8) as usize;
        if data.len() < 8 + expected_bytes {
            return None;
        }
        Some(BloomFilter {
            bits: data[8..8 + expected_bytes].to_vec(),
            num_bits,
            num_hashes,
        })
    }
}

/// Double hash using FNV-1a with two different seeds
fn double_hash(key: &[u8]) -> (u64, u64) {
    // FNV-1a hash
    let mut h1: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h1 ^= b as u64;
        h1 = h1.wrapping_mul(0x0100_0000_01b3);
    }

    // Second hash: FNV-1a with different offset
    let mut h2: u64 = 0x6c62_272e_07bb_0142;
    for &b in key {
        h2 ^= b as u64;
        h2 = h2.wrapping_mul(0x0100_0000_01b3);
    }

    (h1, h2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let mut bf = BloomFilter::new(100);
        bf.add(b"hello");
        bf.add(b"world");

        assert!(bf.may_contain(b"hello"));
        assert!(bf.may_contain(b"world"));
        // "missing" should almost certainly not match
        assert!(!bf.may_contain(b"missing"));
    }

    #[test]
    fn test_serialization() {
        let mut bf = BloomFilter::new(100);
        bf.add(b"key1");
        bf.add(b"key2");

        let bytes = bf.to_bytes();
        let bf2 = BloomFilter::from_bytes(&bytes).unwrap();

        assert!(bf2.may_contain(b"key1"));
        assert!(bf2.may_contain(b"key2"));
        assert!(!bf2.may_contain(b"key3"));
    }

    #[test]
    fn test_false_positive_rate() {
        let n = 10_000;
        let mut bf = BloomFilter::new(n);
        for i in 0..n {
            bf.add(format!("key-{}", i).as_bytes());
        }

        // Check all inserted keys are found
        for i in 0..n {
            assert!(bf.may_contain(format!("key-{}", i).as_bytes()));
        }

        // Measure false positive rate on keys not inserted
        let mut false_positives = 0;
        let test_count = 10_000;
        for i in n..n + test_count {
            if bf.may_contain(format!("other-{}", i).as_bytes()) {
                false_positives += 1;
            }
        }

        let fpr = false_positives as f64 / test_count as f64;
        // Expected ~1.7% FPR with 10 bits/key and 3 hashes
        assert!(fpr < 0.05, "FPR too high: {:.2}%", fpr * 100.0);
    }
}
