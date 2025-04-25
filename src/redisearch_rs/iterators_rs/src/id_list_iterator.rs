//! # Notes
//! 
//! Easily portable as the only type that needs porting is a list of doc ids, which could be a vec on the rust side.
//! Besides that we can leave the payload as a void pointer for now.
//!
//! --> Good for starters
//!
//! ## Dependencies
//!
//! For each iterator both the base c-struct (indexIterator) and the impl struct (here) 
//! a virtual result neither needs the RSYieladableMetric type
//!
//! ### indexIterator dependencies
//!
//! - RSIndexResult is RSResultType_Virtual --> only doc id is needed on the RSIndexResult
//! - RLookupKey (not used in this iterator)
//!
//! Check Assumption:
//!
//! 1. RSYieldableMetric field 'metric' of RSIndexResult is only used in MetricIterator and HybridIterator
//! 
