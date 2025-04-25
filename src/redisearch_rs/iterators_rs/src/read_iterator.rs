//! This depends on open work on the inverted_index
//! 
//! 1. We need an IndexReader 
//! 2. IndexReader needs the InvertedIndex, Decoder and Encoder, etc.
//! 3. Many open points: Plan with Zeenix
//!
//! Probably will stay [crate::mock_iterator::MockIterator] for now.