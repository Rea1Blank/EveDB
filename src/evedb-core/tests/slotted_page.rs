// SPDX-License-Identifier: AGPL-3.0-only

//! Storage format fixtures, boundary conditions, and malformed-page checks.

use evedb_core::storage::{MAX_RECORD_SIZE, PAGE_SIZE, PageError, SlottedPage};

#[test]
fn records_survive_encoding_and_further_inserts() {
    let mut page = SlottedPage::new(42);
    let records: [&[u8]; 4] = [
        b"hello",
        b"",
        &[0, 255, 0, 128],
        "EveDB: \u{1f4be}".as_bytes(),
    ];
    for (index, record) in records.iter().enumerate() {
        assert_eq!(page.insert(record), Ok(index as u16));
    }
    let mut decoded = SlottedPage::from_bytes(page.as_bytes()).unwrap();
    assert_eq!(decoded.page_id(), 42);
    assert_eq!(decoded.record_count(), 4);
    assert_eq!(decoded.insert(b"next"), Ok(4));
    for (index, record) in records.iter().enumerate() {
        assert_eq!(decoded.get(index as u16), Some(*record));
    }
    assert_eq!(decoded.get(4), Some(b"next".as_slice()));
    assert_eq!(decoded.get(5), None);
    assert_eq!(decoded.get(u16::MAX), None);
}

#[test]
fn encoding_matches_a_handwritten_format_fixture() {
    // Header: EVPG, version 2, size 8192, id 0x0102030405060708,
    // one slot, lower 44, upper 8189, LSN zero, CRC32C 0x95d4a757.
    let mut expected = [0; PAGE_SIZE];
    expected[..24].copy_from_slice(&[
        b'E', b'V', b'P', b'G', 2, 0, 0, 32, 8, 7, 6, 5, 4, 3, 2, 1, 1, 0, 44, 0, 253, 31, 0, 0,
    ]);
    expected[32..36].copy_from_slice(&[87, 167, 212, 149]);
    expected[40..44].copy_from_slice(&[253, 31, 3, 0]);
    expected[8189..].copy_from_slice(b"abc");
    let mut page = SlottedPage::new(0x0102_0304_0506_0708);
    page.insert(b"abc").unwrap();
    assert_eq!(page.as_bytes(), &expected);
    let decoded = SlottedPage::from_bytes(&expected).unwrap();
    assert_eq!(decoded.get(0), Some(b"abc".as_slice()));
}

#[test]
fn exact_fit_and_failed_insert_leave_existing_records_intact() {
    let mut page = SlottedPage::new(0);
    let record = vec![0xa5; MAX_RECORD_SIZE];
    assert_eq!(page.insert(&record), Ok(0));
    assert_eq!(page.free_space(), 0);
    let before = *page.as_bytes();
    assert_eq!(page.insert(b""), Err(PageError::Full));
    assert_eq!(page.as_bytes(), &before);
    let decoded = SlottedPage::from_bytes(&before).unwrap();
    assert_eq!(decoded.get(0), Some(record.as_slice()));
}

#[test]
fn full_error_preserves_space_for_a_smaller_record() {
    let mut page = SlottedPage::new(1);
    page.insert(&vec![7; MAX_RECORD_SIZE - 8]).unwrap();
    let before = *page.as_bytes();
    assert_eq!(page.insert(b"12345"), Err(PageError::Full));
    assert_eq!(page.as_bytes(), &before);
    assert_eq!(page.insert(b"1234"), Ok(1));
    assert_eq!(page.free_space(), 0);
    let decoded = SlottedPage::from_bytes(page.as_bytes()).unwrap();
    assert_eq!(decoded.get(1), Some(b"1234".as_slice()));
}

#[test]
fn oversized_record_is_rejected_without_mutation() {
    let mut page = SlottedPage::new(u64::MAX);
    page.insert(b"existing").unwrap();
    let before = *page.as_bytes();
    assert_eq!(
        page.insert(&vec![0; MAX_RECORD_SIZE + 1]),
        Err(PageError::RecordTooLarge)
    );
    assert_eq!(page.as_bytes(), &before);
}

#[test]
fn empty_records_exhaust_directory_space_without_overflow() {
    let mut page = SlottedPage::new(0);
    let mut count = 0;
    while page.free_space() >= 4 {
        assert_eq!(page.insert(b""), Ok(count));
        count += 1;
    }
    assert_eq!(count, 2038);
    assert_eq!(page.insert(b""), Err(PageError::Full));
    let decoded = SlottedPage::from_bytes(page.as_bytes()).unwrap();
    for slot in 0..count {
        assert_eq!(decoded.get(slot), Some(b"".as_slice()));
    }
}

#[test]
fn malformed_headers_are_rejected() {
    for size in [0, 23, PAGE_SIZE - 1, PAGE_SIZE + 1] {
        assert!(matches!(
            SlottedPage::from_bytes(&vec![0; size]),
            Err(PageError::InvalidSize)
        ));
    }
    let page = SlottedPage::new(0);
    for offset in [0, 6, 22] {
        let mut bytes = *page.as_bytes();
        bytes[offset] ^= 1;
        assert!(matches!(
            SlottedPage::from_bytes(&bytes),
            Err(PageError::InvalidHeader)
        ));
    }
    let mut bytes = *page.as_bytes();
    bytes[4..6].copy_from_slice(&3_u16.to_le_bytes());
    assert!(matches!(
        SlottedPage::from_bytes(&bytes),
        Err(PageError::UnsupportedVersion(3))
    ));
}

#[test]
fn invalid_boundaries_gaps_and_overlaps_are_rejected() {
    let mut page = SlottedPage::new(0);
    page.insert(b"first").unwrap();
    page.insert(b"second").unwrap();
    for (offset, value) in [
        (16, u16::MAX), // Oversized slot count.
        (18, 24),       // Directory does not cover all slots.
        (20, 0),        // Free space overlaps the directory.
        (20, u16::MAX), // Free space extends outside the page.
        (40, 10),       // Record points into the header.
        (40, 8186),     // Gap between first record and page end.
        (42, u16::MAX), // Record extends outside the page.
        (44, 8182),     // Second record overlaps the first.
        (46, 5),        // Gap between records.
    ] {
        let mut bytes = *page.as_bytes();
        bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
        assert!(
            matches!(
                SlottedPage::from_bytes(&bytes),
                Err(PageError::InvalidLayout)
            ),
            "accepted invalid field at offset {offset}"
        );
    }
}

#[test]
fn replace_remove_and_compaction_preserve_other_slots_and_lsn() {
    let mut page = SlottedPage::new(9);
    page.set_lsn(71);
    page.insert(b"first").unwrap();
    page.insert(b"second").unwrap();
    page.insert(b"").unwrap();
    page.replace(0, b"a longer first record").unwrap();
    assert!(page.remove(1));
    assert!(!page.remove(1));
    assert_eq!(page.get(2), Some(b"".as_slice()));
    let before = *page.as_bytes();
    assert_eq!(
        page.replace(0, &vec![0; MAX_RECORD_SIZE]),
        Err(PageError::Full)
    );
    assert_eq!(page.as_bytes(), &before);
    let decoded = SlottedPage::from_bytes(&before).unwrap();
    assert_eq!(decoded.lsn(), 71);
    assert_eq!(decoded.get(0), Some(b"a longer first record".as_slice()));
    assert_eq!(decoded.get(1), None);
}

#[test]
fn payload_and_lsn_corruption_are_detected() {
    let mut page = SlottedPage::new(0);
    page.insert(b"payload").unwrap();
    for offset in [24, 32, PAGE_SIZE - 1] {
        let mut bytes = *page.as_bytes();
        bytes[offset] ^= 1;
        assert!(matches!(
            SlottedPage::from_bytes(&bytes),
            Err(PageError::ChecksumMismatch)
        ));
    }
}

#[test]
fn empty_page_requires_an_empty_payload_region() {
    let page = SlottedPage::new(7);
    let decoded = SlottedPage::from_bytes(page.as_bytes()).unwrap();
    assert_eq!(decoded.record_count(), 0);
    assert_eq!(decoded.get(0), None);
    let mut bytes = *page.as_bytes();
    bytes[20..22].copy_from_slice(&8191_u16.to_le_bytes());
    assert!(matches!(
        SlottedPage::from_bytes(&bytes),
        Err(PageError::InvalidLayout)
    ));
}
