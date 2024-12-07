use std::fmt::Debug;
use std::marker::PhantomData;

use crate::internal_node::InternalNode;
use crate::leaf_node::LeafNode;

impl<K, V> Debug for NodePtr<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ node: {:?} }}", self.node)
    }
}

impl<K, V> From<&InternalNode<K, V>> for NodePtr<K, V> {
    fn from(node: &InternalNode<K, V>) -> Self {
        NodePtr::from_internal(node as *const InternalNode<K, V> as *mut InternalNode<K, V>)
    }
}

impl<K, V> From<&LeafNode<K, V>> for NodePtr<K, V> {
    fn from(node: &LeafNode<K, V>) -> Self {
        NodePtr::from_leaf(node as *const LeafNode<K, V> as *mut LeafNode<K, V>)
    }
}

impl<K, V> From<&mut LeafNode<K, V>> for NodePtr<K, V> {
    fn from(node: &mut LeafNode<K, V>) -> Self {
        NodePtr::from_leaf(node)
    }
}

impl<K, V> From<&mut InternalNode<K, V>> for NodePtr<K, V> {
    fn from(node: &mut InternalNode<K, V>) -> Self {
        NodePtr::from_internal(node)
    }
}

impl<K, V> From<Box<InternalNode<K, V>>> for NodePtr<K, V> {
    fn from(node: Box<InternalNode<K, V>>) -> Self {
        NodePtr::from_internal(Box::into_raw(node))
    }
}
impl<K, V> From<*mut InternalNode<K, V>> for NodePtr<K, V> {
    fn from(node: *mut InternalNode<K, V>) -> Self {
        NodePtr::from_internal(node)
    }
}

impl<K, V> From<*mut LeafNode<K, V>> for NodePtr<K, V> {
    fn from(node: *mut LeafNode<K, V>) -> Self {
        NodePtr::from_leaf(node)
    }
}

pub struct NodePtr<K, V> {
    node: *mut (),
    phantom_keys: PhantomData<K>,
    phantom_values: PhantomData<V>,
}

impl<K, V> PartialEq for NodePtr<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node
    }
}

impl<K, V> Copy for NodePtr<K, V> {}

impl<K, V> Clone for NodePtr<K, V> {
    fn clone(&self) -> Self {
        NodePtr {
            node: self.node,
            phantom_keys: PhantomData::default(),
            phantom_values: PhantomData::default(),
        }
    }
}

impl<K, V> NodePtr<K, V> {
    pub fn from_internal(node: *mut InternalNode<K, V>) -> NodePtr<K, V> {
        let ptr = node as *mut ();
        NodePtr {
            node: ptr,
            phantom_keys: PhantomData::default(),
            phantom_values: PhantomData::default(),
        }
    }

    pub fn from_leaf(node: *mut LeafNode<K, V>) -> NodePtr<K, V> {
        let constptr = node as *mut ();
        NodePtr {
            node: constptr,
            phantom_keys: PhantomData::default(),
            phantom_values: PhantomData::default(),
        }
    }

    pub fn is_null(&self) -> bool {
        self.node.is_null()
    }

    pub unsafe fn as_internal_node_ptr(&self) -> *mut InternalNode<K, V> {
        self.node as *mut InternalNode<K, V>
    }

    pub unsafe fn as_leaf_node_ptr(&self) -> *mut LeafNode<K, V> {
        self.node as *mut LeafNode<K, V>
    }
}

pub struct NodeRef<K, V> {
    height: usize,
    opaque_node_ptr: NodePtr<K, V>,
}

impl<K: PartialOrd + Debug + Clone, V: Debug> NodeRef<K, V> {
    pub fn new(node_ptr: NodePtr<K, V>, height: usize) -> NodeRef<K, V> {
        NodeRef {
            height,
            opaque_node_ptr: node_ptr,
        }
    }

    pub fn is_internal(&self) -> bool {
        self.height > 0
    }

    pub fn is_leaf(&self) -> bool {
        self.height == 0
    }

    pub fn height(self) -> usize {
        self.height
    }

    pub fn as_internal_node(&self) -> *mut InternalNode<K, V> {
        debug_assert!(self.height > 0);
        unsafe { self.opaque_node_ptr.as_internal_node_ptr() }
    }

    pub fn as_leaf_node(&self) -> *mut LeafNode<K, V> {
        debug_assert!(self.height == 0);
        unsafe { self.opaque_node_ptr.as_leaf_node_ptr() }
    }

    pub fn set_parent(&self, parent: *mut InternalNode<K, V>) {
        if self.is_internal() {
            unsafe {
                (*self.as_internal_node()).parent = parent;
            }
        } else {
            unsafe {
                (*self.as_leaf_node()).parent = parent;
            }
        }
    }

    pub fn check_invariants(&self, height: usize) {
        if self.is_internal() {
            unsafe {
                (*self.as_internal_node()).check_invariants(height);
            }
        } else {
            unsafe {
                (*self.as_leaf_node()).check_invariants();
            }
        }
    }

    pub fn print_node(&self) {
        if self.is_internal() {
            unsafe {
                (*self.as_internal_node()).print_node(self.height);
            }
        } else {
            unsafe {
                (*self.as_leaf_node()).print_node();
            }
        }
    }
}
