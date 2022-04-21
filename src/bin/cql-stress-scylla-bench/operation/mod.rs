pub mod read;
pub mod write;

use anyhow::Result;
use rand::RngCore;
use sha2::{Digest, Sha256};

const GENERATED_DATA_HEADER_SIZE: usize = 24;
const GENERATED_DATA_MIN_SIZE: usize = GENERATED_DATA_HEADER_SIZE + 33;

pub(self) fn generate_row_data(pk: i64, ck: i64, size: usize) -> Vec<u8> {
    if size == 0 {
        Vec::new()
    } else if size < GENERATED_DATA_HEADER_SIZE {
        let mut buf = Vec::with_capacity(std::cmp::max(1 + 8, size));
        buf.push(size as u8);
        buf.extend((pk ^ ck).to_le_bytes());
        buf.resize(size, 0u8);
        buf
    } else {
        let mut buf = Vec::with_capacity(std::cmp::max(GENERATED_DATA_MIN_SIZE, size));
        buf.extend((size as u64).to_le_bytes());
        buf.extend(pk.to_le_bytes());
        buf.extend(ck.to_le_bytes());
        if size < GENERATED_DATA_MIN_SIZE {
            buf.resize(size, 0u8);
        } else if size >= GENERATED_DATA_MIN_SIZE {
            // Make place for the payload
            buf.resize(size - 32, 0u8);

            // Generate random payload
            let payload = &mut buf[GENERATED_DATA_HEADER_SIZE..size - 32];
            rand::thread_rng().fill_bytes(payload);

            // Hash it with SHA256
            let mut hasher = Sha256::new();
            hasher.update(payload);
            let hash = hasher.finalize();

            // Put the hash at the end
            buf.extend(&hash[..]);
        }
        buf
    }
}

pub(self) fn validate_row_data(pk: i64, ck: i64, data: &[u8]) -> Result<()> {
    let size = data.len();
    let original_data = data;

    // TODO: Is this correct?
    if size == 0 {
        return Ok(());
    }

    let (encoded_size, data) = if size < GENERATED_DATA_HEADER_SIZE {
        (data[0] as usize, &data[1..])
    } else {
        (
            u64::from_le_bytes(data[..8].try_into().unwrap()) as usize,
            &data[8..],
        )
    };

    anyhow::ensure!(
        size == encoded_size,
        "Actual size of value ({}) doesn't match size stored in value ({})",
        size,
        encoded_size,
    );

    // There is no random payload for sizes < GENERATED_DATA_MIN_SIZE
    if size < GENERATED_DATA_MIN_SIZE {
        // TODO: Probably we could the check without an allocation
        let expected_data = generate_row_data(pk, ck, size);
        anyhow::ensure!(
            original_data == expected_data,
            "Actual value doesn't match expected value; expected: {:?}, actual: {:?}",
            expected_data,
            original_data,
        );
        return Ok(());
    }

    let stored_pk = i64::from_le_bytes(data[..8].try_into().unwrap());
    anyhow::ensure!(
        stored_pk == pk,
        "Actual pk ({}) doesn't match pk stored in value ({})",
        pk,
        stored_pk,
    );

    let stored_ck = i64::from_le_bytes(data[8..16].try_into().unwrap());
    anyhow::ensure!(
        stored_ck == ck,
        "Actual ck ({}) doesn't match ck stored in value ({})",
        ck,
        stored_ck,
    );

    let payload = &data[16..data.len() - 32];
    let mut hasher = Sha256::new();
    hasher.update(payload);
    let hash = hasher.finalize();

    let stored_checksum = &data[data.len() - 32..];
    anyhow::ensure!(
        stored_checksum == &hash[..],
        "Corrupt checksum or data: calculated checksum ({:?} doesn't match stored checksum ({:?}) over data: {:?}",
        &hash[..],
        stored_checksum,
        payload,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_validate_data() {
        let pk = 123;
        let ck = 456;
        for size in 1..=100 {
            let mut data = generate_row_data(pk, ck, size);
            assert_eq!(data.len(), size);

            // Check that the data is valid
            validate_row_data(pk, ck, &data).unwrap();

            // Corrupt each single byte and check that validation detects it
            for i in 0..size {
                data[i] = !data[i];
                let res = validate_row_data(pk, ck, &data);
                data[i] = !data[i];
                assert!(
                    res.is_err(),
                    "Validation succeeded for corrupted data; size: {}, flipped byte idx: {}, data: {:?}",
                    size,
                    i,
                    &data,
                );
            }
        }
    }
}
