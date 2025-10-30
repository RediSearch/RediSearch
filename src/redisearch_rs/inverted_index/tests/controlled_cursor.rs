use std::io::{IoSlice, Seek, SeekFrom, Write};

use inverted_index::controlled_cursor::ControlledCursor;

#[test]
fn test_write_seek_and_vectored_write_with_padding() {
    // Start with an empty vec
    let mut buffer = Vec::new();

    assert_eq!(buffer.capacity(), 0, "Initial capacity should be 0");

    // Step 1: Write 4 bytes
    let bytes_written = {
        let mut cursor = ControlledCursor::new(&mut buffer);
        let initial_data = b"test";
        cursor
            .write(initial_data)
            .expect("Failed to write initial data")
    };

    // Verify the write succeeded
    assert_eq!(bytes_written, 4, "Should have written 4 bytes");
    assert_eq!(buffer.len(), 4, "Buffer length should be 4");
    assert_eq!(&buffer[..], b"test", "Buffer should contain 'test'");

    // Verify capacity grew correctly (should use reserve_exact, so capacity == length)
    assert_eq!(
        buffer.capacity(),
        4,
        "Capacity should be 4 after writing 4 bytes",
    );

    // Step 2: Do a vectored write of 6 bytes total (two 3-byte slices) after end of capacity
    let bytes_written = {
        let mut cursor = ControlledCursor::new(&mut buffer);
        cursor
            .seek(SeekFrom::Start(9)) // Position at 9 for the write
            .expect("Failed to seek from current position");

        let vec_data1 = b"abc";
        let vec_data2 = b"xyz";
        let bufs = [IoSlice::new(vec_data1), IoSlice::new(vec_data2)];

        cursor
            .write_vectored(&bufs)
            .expect("Failed to write vectored data")
    };

    // Verify vectored write succeeded
    assert_eq!(
        bytes_written, 6,
        "Should have written 6 bytes in vectored write"
    );

    // Verify buffer length is now 15 (position 9 + 6 bytes written)
    assert_eq!(buffer.len(), 15, "Buffer length should be 15");

    // Verify capacity grew correctly
    // The growth follows this sequence (see `reserve_and_pad` logic):
    // 0, 1, 2, 3, 4, 5, 7, 9, 11, 14, 17, ...
    //
    // To accommodate 15 bytes, capacity should be at least 17.
    assert_eq!(buffer.capacity(), 17, "Capacity should grow correctly",);

    // Step 3: Verify the contents
    // Bytes 0-3: "test"
    assert_eq!(&buffer[0..4], b"test", "First 4 bytes should be 'test'");

    // Bytes 4-8: should be zero-padded (5 bytes of padding)
    assert_eq!(&buffer[4..9], &[0u8; 5], "Bytes 4-8 should be zero-padded");

    // Bytes 9-14: "abcxyz"
    assert_eq!(
        &buffer[9..15],
        b"abcxyz",
        "Bytes 9-14 should contain 'abcxyz'"
    );

    // Complete verification of entire buffer
    let expected = b"test\0\0\0\0\0abcxyz";
    assert_eq!(
        &buffer[..],
        expected,
        "Buffer contents don't match expected. Got {:?}, expected {:?}",
        buffer,
        expected
    );
}

#[test]
fn test_seek_and_overwrite() {
    // Test seeking backwards and overwriting existing data
    let mut buffer = Vec::new();

    {
        let mut cursor = ControlledCursor::new(&mut buffer);

        // Write initial data
        cursor.write(b"XXXXXXXXXX").expect("Write failed");
    }
    assert_eq!(&buffer[..], b"XXXXXXXXXX");

    {
        let mut cursor = ControlledCursor::new(&mut buffer);

        // Seek back to position 2
        cursor.seek(SeekFrom::Start(2)).expect("Seek failed");

        // Overwrite with new data
        cursor.write(b"test").expect("Write failed");
    }

    // Verify the overwrite
    assert_eq!(
        &buffer[..],
        b"XXtestXXXX",
        "Overwrite should replace middle bytes"
    );
}
