//! A concurrent B-tree implementation
//!
//! This implementation relies on a QSBR (quiescent-state based reclamation) system to free removed nodes and entries.
//! Your code MUST call `qsbr_reclaimer().register_thread()` before interacting with the btree, and
//! MUST periodically call and `qsbr_reclaimer().mark_quiescent()` while holding no possibly-deleted
//! references or it will leak memory. Calling `mark_quiescent` while holding possibly-deleted references will cause UB.
//!
//! The tree also allows for long-lived concurrent access to keys and values, so those may not be mutated in place except with additional synchronization,
//! and must be dropped via the tree's methods or manually via QSBR.
//!
//! The tree is intended to be used with variable sized keys -- nodes only store pointers to keys and values. Keys are reference counted when
//! they're used for split keys, not cloned. The tree uses "hybrid locks" for scalability when the tree is large, and the tree provides
//! several parallel bulk operations that should be very fast with large amounts of data.

#[macro_use]
mod debug;

mod array_types;
mod bulk_operations;
mod coalescing;
mod cursor;
mod hybrid_latch;
mod internal_node;
mod iter;
mod leaf_node;
mod node;
mod pointers;
mod reference;
mod root_node;
mod search;
mod search_dequeue;
mod splitting;
mod sync;
mod tree;
mod util;
pub use cursor::{Cursor, CursorMut};
pub use iter::{BackwardBTreeIterator, ForwardBTreeIterator};
pub use qsbr::{qsbr_reclaimer, MemoryReclaimer};
pub use reference::{Entry, Ref};
pub use tree::{
    BTree, BTreeKey, BTreeValue, InsertOrModifyIfResult, RemoveOrModifyDecision,
    RemoveOrModifyIfResult,
};
