/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::alloc::{Layout, alloc};
use std::io::{Read, Write};
use std::ptr::{NonNull, copy_nonoverlapping};

use crate::{Buffer, BufferReader, BufferWriter};

#[test]
fn buffer_creation() {
    unsafe {
        let capacity = 100;
        let buffer_ptr = create_test_buffer(capacity);
        let buffer = buffer_ptr.as_ref();

        assert_eq!(buffer.capacity(), capacity);
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.remaining_capacity(), capacity);
        assert!(buffer.is_empty());

        free_test_buffer(buffer_ptr);
    }
}

#[test]
fn buffer_as_slice() {
    unsafe {
        let capacity = 100;
        let mut buffer_ptr = create_test_buffer(capacity);
        let buffer = buffer_ptr.as_mut();

        // Fill buffer with some data
        let test_data = [1, 2, 3, 4, 5];
        copy_nonoverlapping(test_data.as_ptr(), buffer.data.as_ptr(), test_data.len());
        buffer.len = test_data.len();

        // Check slice access
        let slice = buffer.as_slice();
        assert_eq!(slice, &test_data);

        free_test_buffer(buffer_ptr);
    }
}

#[test]
fn buffer_as_mut_slice() {
    unsafe {
        let capacity = 100;
        let mut buffer_ptr = create_test_buffer(capacity);
        let buffer = buffer_ptr.as_mut();

        // Initialize memory before setting length
        for i in 0..5 {
            *buffer.data.as_ptr().add(i) = 0; // Zero-initialize
        }
        // Now it's safe to set the length
        buffer.len = 5;

        // Modify via mut slice
        let mut_slice = buffer.as_mut_slice();
        for (i, item) in mut_slice.iter_mut().enumerate() {
            *item = (i + 1) as u8;
        }

        // Verify changes
        let expected = [1, 2, 3, 4, 5];
        assert_eq!(buffer.as_slice(), &expected);

        free_test_buffer(buffer_ptr);
    }
}

#[test]
fn buffer_advance() {
    unsafe {
        let mut buffer_ptr = create_test_buffer(100);
        let buffer = buffer_ptr.as_mut();

        assert_eq!(buffer.len(), 0);

        // Initialize first 10 bytes before advancing
        for i in 0..10 {
            *buffer.data.as_ptr().add(i) = i as u8;
        }
        buffer.advance(10);
        assert_eq!(buffer.len(), 10);
        assert_eq!(buffer.remaining_capacity(), 90);

        // Initialize next 20 bytes before advancing
        for i in 0..20 {
            *buffer.data.as_ptr().add(10 + i) = i as u8;
        }
        buffer.advance(20);
        assert_eq!(buffer.len(), 30);
        assert_eq!(buffer.remaining_capacity(), 70);

        free_test_buffer(buffer_ptr);
    }
}

#[test]
fn buffer_advance_overflow() {
    use std::panic;

    unsafe {
        let capacity = 100;
        let mut buffer_ptr = create_test_buffer(capacity);

        // Use catch_unwind to handle the panic and clean up memory
        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let buffer = buffer_ptr.as_mut();
            buffer.advance(capacity + 1);
        }));

        // Clean up the buffer
        free_test_buffer(buffer_ptr);

        // Assert that the panic occurred with the expected message
        match result {
            Err(panic_payload) => {
                match (
                    panic_payload.downcast_ref::<&str>(),
                    panic_payload.downcast_ref::<String>(),
                ) {
                    (Some(message), _) => {
                        assert!(
                            message.contains("n <= self.remaining_capacity()"),
                            "Expected panic message to contain 'n <= self.remaining_capacity()',\
                            got: {}",
                            message
                        );
                    }
                    (None, Some(message)) => {
                        assert!(
                            message.contains("n <= self.remaining_capacity()"),
                            "Expected panic message to contain 'n <= self.remaining_capacity()',\
                            got: {}",
                            message
                        );
                    }
                    (None, None) => {
                        panic!("Expected a string panic message");
                    }
                }
            }
            Ok(_) => panic!("Expected buffer.advance to panic, but it didn't"),
        }
    }
}

#[test]
fn buffer_reader() {
    unsafe {
        let mut buffer_ptr = create_test_buffer(100);
        let buffer = buffer_ptr.as_mut();

        // Fill buffer with test data
        let test_data = b"Hello, world!";
        copy_nonoverlapping(test_data.as_ptr(), buffer.data.as_ptr(), test_data.len());
        buffer.len = test_data.len();

        // Create reader
        let mut reader = BufferReader {
            buf: buffer_ptr,
            pos: 0,
        };

        // Read data
        let mut dest = [0u8; 5];
        assert_eq!(reader.read(&mut dest).unwrap(), 5);
        assert_eq!(dest, b"Hello"[..]);
        assert_eq!(reader.pos, 5);

        // Read more data
        let mut dest = [0u8; 8];
        assert_eq!(reader.read(&mut dest).unwrap(), 8);
        assert_eq!(dest, b", world!"[..]);
        assert_eq!(reader.pos, 13);

        // Try to read more than available (should just give us 0)
        let mut dest = [0u8; 1];
        assert_eq!(reader.read(&mut dest).unwrap(), 0);

        free_test_buffer(buffer_ptr);
    }
}

#[test]
fn buffer_writer() {
    unsafe {
        let mut buffer_ptr = create_test_buffer(100);
        let buffer = buffer_ptr.as_mut();

        // Create writer
        let mut writer = BufferWriter {
            buf: buffer_ptr,
            cursor: buffer.data,
        };

        // Write data
        let test_data = b"Hello";
        assert_eq!(writer.write(test_data).unwrap(), 5);
        let buffer = writer.buf.as_ref();
        assert_eq!(buffer.len, 5);

        // Write more data
        let test_data = b", world!";
        assert_eq!(writer.write(test_data).unwrap(), 8);
        let buffer = writer.buf.as_ref();
        assert_eq!(buffer.len, 13);

        // Check the written data
        let expected = b"Hello, world!";
        assert_eq!(buffer.as_slice(), expected);

        free_test_buffer(buffer_ptr);
    }
}

#[test]
fn buffer_writer_grow() {
    unsafe {
        let initial_capacity = 10;
        let mut buffer_ptr = create_test_buffer(initial_capacity);
        let buffer = buffer_ptr.as_mut();

        // Create writer
        let mut writer = BufferWriter {
            buf: buffer_ptr,
            cursor: buffer.data,
        };

        // Write data that fits within initial capacity
        let test_data = b"HelloWorld";
        assert_eq!(writer.write(test_data).unwrap(), 10);
        let buffer = writer.buf.as_ref();
        assert_eq!(buffer.len, 10);

        // Write more data that will require growing the buffer
        let test_data = b"MoreData";
        assert_eq!(writer.write(test_data).unwrap(), 8);

        // Buffer should have grown
        let buffer = writer.buf.as_ref();
        assert!(buffer.capacity > initial_capacity);
        assert_eq!(buffer.len, 18);

        // Check the written data
        let expected = b"HelloWorldMoreData";
        assert_eq!(&buffer.as_slice()[..18], expected);

        // Verify that the cursor has been updated to point within the new allocation and is
        // positioned at the end of the written data.
        let cursor_ptr = writer.cursor.as_ptr();
        assert_eq!(cursor_ptr as usize - buffer.data.as_ptr() as usize, 18);
        // Make sure cursor is pointing to a location within the new allocation.
        assert!(cursor_ptr >= buffer.data.as_ptr());
        assert!(cursor_ptr <= buffer.data.as_ptr().add(buffer.capacity));

        free_test_buffer(buffer_ptr);
    }
}

#[test]
fn buffer_grow_edge_cases() {
    unsafe {
        // Test growing a buffer that's at capacity
        let initial_capacity = 10;
        let mut buffer_ptr = create_test_buffer(initial_capacity);
        let buffer = buffer_ptr.as_mut();

        // Fill buffer to capacity
        for i in 0..initial_capacity {
            *buffer.data.as_ptr().add(i) = (i % 255) as u8;
        }
        buffer.len = initial_capacity;

        // Create writer at exact end of buffer
        let mut writer = BufferWriter {
            buf: buffer_ptr,
            cursor: NonNull::new(buffer.data.as_ptr().add(initial_capacity)).unwrap(),
        };

        // Write 1 more byte - should trigger grow
        let test_data = b"!";
        assert_eq!(writer.write(test_data).unwrap(), 1);

        // Buffer should have grown
        let buffer = writer.buf.as_ref();
        assert!(buffer.capacity > initial_capacity);
        assert_eq!(buffer.len, initial_capacity + 1);

        // Verify all data is preserved
        for i in 0..initial_capacity {
            assert_eq!(buffer.as_slice()[i], (i % 255) as u8);
        }
        assert_eq!(buffer.as_slice()[initial_capacity], b'!');

        // Test growing by large amount
        let large_data = vec![b'x'; 100];
        assert_eq!(writer.write(&large_data).unwrap(), 100);

        // Buffer capacity should accommodate all data
        let buffer = writer.buf.as_ref();
        assert!(buffer.capacity >= initial_capacity + 1 + 100);
        assert_eq!(buffer.len, initial_capacity + 1 + 100);

        // Verify the large data was written correctly
        for i in 0..100 {
            assert_eq!(buffer.as_slice()[initial_capacity + 1 + i], b'x');
        }

        // Verify that the cursor has been updated to point within the new allocation and is
        // positioned at the end of the written data.
        let cursor_ptr = writer.cursor.as_ptr();
        assert_eq!(
            cursor_ptr as usize - buffer.data.as_ptr() as usize,
            initial_capacity + 1 + 100
        );
        // Make sure cursor is pointing to a location within the new allocation.
        assert!(cursor_ptr >= buffer.data.as_ptr());
        assert!(cursor_ptr <= buffer.data.as_ptr().add(buffer.capacity));

        free_test_buffer(buffer_ptr);
    }
}

// Helper function to create a new buffer for testing
unsafe fn create_test_buffer(capacity: usize) -> NonNull<Buffer> {
    let layout = Layout::array::<u8>(capacity).unwrap();
    let data_ptr = unsafe { alloc(layout) };
    let data = NonNull::new(data_ptr).unwrap();

    let buffer = Box::new(Buffer {
        data,
        capacity,
        len: 0,
    });

    NonNull::new(Box::into_raw(buffer)).unwrap()
}

// Helper function to clean up buffer after tests
unsafe fn free_test_buffer(buffer: NonNull<Buffer>) {
    let buffer_box = unsafe { Box::from_raw(buffer.as_ptr()) };
    let layout = Layout::array::<u8>(buffer_box.capacity()).unwrap();
    unsafe { std::alloc::dealloc(buffer_box.data.as_ptr(), layout) };
}
