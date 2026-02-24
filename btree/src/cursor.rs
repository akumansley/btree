use std::borrow::Borrow;
use std::ops::Deref;

use crate::pointers::node_ref::{marker, SharedNodeRef};
use crate::reference::Entry;
use crate::search::get_leaf_exclusively_using_optimistic_search_with_fallback;
use crate::search::get_leaf_shared_using_optimistic_search_with_fallback;
use crate::search::{
    get_first_leaf_exclusively_using_optimistic_search,
    get_first_leaf_exclusively_using_shared_search, get_first_leaf_shared_using_optimistic_search,
    get_first_leaf_shared_using_shared_search, get_last_leaf_exclusively_using_optimistic_search,
    get_last_leaf_exclusively_using_shared_search, get_last_leaf_shared_using_optimistic_search,
    get_last_leaf_shared_using_shared_search,
};
use crate::splitting::EntryLocation;
use crate::tree::{
    BTree, BTreeKey, BTreeValue, GetOrInsertResult, InsertOrModifyIfResult, ModificationType,
    ModifyDecision,
};
use crate::util::UnwrapEither;
use thin::{QsArc, QsOwned, QsShared, QsWeak};

pub struct Cursor<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    pub tree: &'a BTree<K, V>,
    pub current_leaf: Option<SharedNodeRef<K, V, marker::LockedShared, marker::Leaf>>,
    pub current_index: usize,
    pub current_leaf_num_keys: Option<u16>,
}

impl<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Cursor<'a, K, V> {
    fn set_current_leaf(&mut self, leaf: SharedNodeRef<K, V, marker::LockedShared, marker::Leaf>) {
        self.current_leaf = Some(leaf);
        self.current_leaf_num_keys = Some(leaf.num_keys_relaxed().try_into().unwrap());
    }

    fn release_current_leaf(&mut self) {
        self.current_leaf.take().unwrap().unlock_shared();
        self.current_leaf_num_keys = None;
    }

    fn current_leaf_num_keys(&self) -> usize {
        self.current_leaf_num_keys.unwrap() as usize
    }

    pub fn seek_to_start(&mut self) {
        if let Some(leaf) = self.current_leaf.take() {
            leaf.unlock_shared();
        }
        let leaf = match get_first_leaf_shared_using_optimistic_search(self.tree.root.as_node_ref())
        {
            Ok(leaf) => leaf,
            Err(_) => get_first_leaf_shared_using_shared_search(self.tree.root.as_node_ref()),
        };
        self.set_current_leaf(leaf);
        self.current_index = 0;
    }

    pub fn seek_to_end(&mut self) {
        if let Some(leaf) = self.current_leaf.take() {
            leaf.unlock_shared();
        }
        let leaf = match get_last_leaf_shared_using_optimistic_search(self.tree.root.as_node_ref())
        {
            Ok(leaf) => leaf,
            Err(_) => get_last_leaf_shared_using_shared_search(self.tree.root.as_node_ref()),
        };
        self.set_current_leaf(leaf);
        self.current_index = self.current_leaf_num_keys().saturating_sub(1);
    }

    pub fn seek<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        // optimistically try the current leaf, but quickly give up and search from the top
        if let Some(leaf) = self.current_leaf.as_ref() {
            let index = leaf.binary_search_key(key).unwrap_either();
            if index < leaf.num_keys() {
                let leaf_key = leaf.storage.get_key(index);
                if leaf_key.deref().borrow() == key {
                    self.current_index = index;
                    return true;
                }
            }
            self.release_current_leaf();
        }
        self.seek_from_top(key)
    }

    fn seek_from_top<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            if self.current_leaf.is_some() {
                self.current_leaf.take().unwrap().unlock_shared();
            }
            let leaf = get_leaf_shared_using_optimistic_search_with_fallback(
                self.tree.root.as_node_ref(),
                key,
            );
            let result = leaf.binary_search_key(key);
            let index = result.unwrap_either();

            // If insertion point is past the end of this leaf, advance to next leaf
            // to maintain "points at element" semantics
            if result.is_err() && index >= leaf.num_keys() {
                if let Some(next_leaf_ref) = leaf.next_leaf() {
                    match next_leaf_ref.try_lock_shared() {
                        Ok(next_leaf) => {
                            leaf.unlock_shared();
                            self.set_current_leaf(next_leaf);
                            self.current_index = 0;
                            return false;
                        }
                        Err(_) => {
                            // Couldn't get lock, retry from top
                            leaf.unlock_shared();
                            continue;
                        }
                    }
                }
                // No next leaf - we're past the end of the tree
            }

            self.set_current_leaf(leaf);
            self.current_index = index;
            return result.is_ok();
        }
    }

    #[inline(always)]
    pub fn current(&self) -> Option<Entry<K, V>> {
        if let Some(leaf) = &self.current_leaf {
            if self.current_index < self.current_leaf_num_keys() {
                return Some(Entry::new(
                    leaf.storage.get_key(self.current_index),
                    leaf.storage.get_value(self.current_index),
                ));
            }
        }
        None
    }

    pub fn move_next_slow(&mut self) -> bool {
        loop {
            if self.current_leaf.is_none() {
                return false;
            }
            let leaf = self.current_leaf.unwrap();
            if self.current_index < self.current_leaf_num_keys().saturating_sub(1) {
                self.current_index += 1;
                return true;
            }

            // Move to next leaf
            let maybe_next_leaf = leaf.next_leaf();
            if maybe_next_leaf.is_none() {
                self.release_current_leaf();
                return false;
            }

            let next_leaf = match maybe_next_leaf.unwrap().try_lock_shared() {
                Ok(leaf) => leaf,
                Err(_) => {
                    // we didn't attain the lock, so restart from the top
                    // to the current key
                    let key = leaf.storage.get_key(self.current_index);
                    self.seek_from_top(&key);
                    continue;
                }
            };
            self.release_current_leaf();
            self.set_current_leaf(next_leaf);
            self.current_index = 0;
            return true;
        }
    }

    #[inline(always)]
    pub fn move_next(&mut self) -> bool {
        if self.current_leaf.is_some()
            && self.current_index < self.current_leaf_num_keys().saturating_sub(1)
        {
            self.current_index += 1;
            return true;
        } else {
            return self.move_next_slow();
        }
    }

    pub fn move_prev(&mut self) -> bool {
        loop {
            if self.current_leaf.is_none() {
                return false;
            }
            let leaf = self.current_leaf.unwrap();
            if self.current_index > 0 {
                self.current_index -= 1;
                return true;
            }

            // Move to previous leaf
            let maybe_prev_leaf = leaf.prev_leaf();
            if maybe_prev_leaf.is_none() {
                self.release_current_leaf();
                return false;
            }

            let prev_leaf = match maybe_prev_leaf.unwrap().try_lock_shared() {
                Ok(leaf) => leaf,
                Err(_) => {
                    // we didn't attain the lock, so restart from the top
                    // to the current key
                    let key = leaf.storage.get_key(self.current_index);
                    self.release_current_leaf();
                    self.seek(&key);
                    continue;
                }
            };
            self.release_current_leaf();
            self.set_current_leaf(prev_leaf);
            self.current_index = self.current_leaf_num_keys();
        }
    }
}

impl<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Drop for Cursor<'a, K, V> {
    fn drop(&mut self) {
        if let Some(leaf) = self.current_leaf.take() {
            leaf.unlock_shared();
        }
    }
}

pub struct CursorMut<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    tree: &'a BTree<K, V>,
    current_leaf: Option<SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>>,
    current_index: usize,
}

impl<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> CursorMut<'a, K, V> {
    pub(crate) fn new(tree: &'a BTree<K, V>) -> Self {
        Self {
            tree,
            current_leaf: None,
            current_index: 0,
        }
    }
    pub(crate) fn new_from_location(
        tree: &'a BTree<K, V>,
        entry_location: EntryLocation<K, V>,
    ) -> Self {
        Self::new_from_leaf_and_index(tree, entry_location.leaf, entry_location.index)
    }
    pub(crate) fn new_from_leaf_and_index(
        tree: &'a BTree<K, V>,
        leaf: SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
        index: usize,
    ) -> Self {
        CursorMut {
            tree,
            current_leaf: Some(leaf),
            current_index: index,
        }
    }
    pub fn seek_to_start(&mut self) {
        if let Some(leaf) = self.current_leaf.take() {
            leaf.unlock_exclusive();
        }
        let leaf = match get_first_leaf_exclusively_using_optimistic_search(
            self.tree.root.as_node_ref(),
        ) {
            Ok(leaf) => leaf,
            Err(_) => get_first_leaf_exclusively_using_shared_search(self.tree.root.as_node_ref()),
        };
        self.current_leaf = Some(leaf);
        self.current_index = 0;
    }

    pub fn seek_to_end(&mut self) {
        if let Some(leaf) = self.current_leaf.take() {
            leaf.unlock_exclusive();
        }
        let leaf =
            match get_last_leaf_exclusively_using_optimistic_search(self.tree.root.as_node_ref()) {
                Ok(leaf) => leaf,
                Err(_) => {
                    get_last_leaf_exclusively_using_shared_search(self.tree.root.as_node_ref())
                }
            };
        self.current_index = leaf.num_keys().saturating_sub(1);
        self.current_leaf = Some(leaf);
    }

    pub fn seek<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        // optimistically try the current leaf, but quickly give up and search from the top
        if let Some(leaf) = self.current_leaf.as_ref() {
            let index = leaf.binary_search_key(key).unwrap_either();
            if index < leaf.num_keys() {
                let leaf_key = leaf.storage.get_key(index);
                if leaf_key.deref().borrow() == key {
                    self.current_index = index;
                    return true;
                }
            }
            self.current_leaf.take().unwrap().unlock_exclusive();
        }
        self.seek_from_top(key)
    }

    pub fn update_value(&mut self, value: QsOwned<V>) {
        let leaf = self.current_leaf.as_mut().unwrap();
        leaf.update(self.current_index, value);
    }

    pub fn modify_value<E>(&mut self, modify_fn: impl FnOnce(QsOwned<V>) -> Result<QsOwned<V>, (QsOwned<V>, E)>) -> Result<(), E> {
        let leaf = self.current_leaf.as_mut().unwrap();
        let index = self.current_index;
        leaf.modify_value(index, modify_fn)
    }

    /// update_fn is called with the new value and the existing old value
    pub fn insert_or_modify_if<F, E>(
        &mut self,
        key: QsArc<K>,
        new_value: QsOwned<V>,
        predicate: impl Fn(QsShared<V>) -> ModifyDecision<E>,
        modify_fn: F,
    ) -> InsertOrModifyIfResult<E>
    where
        F: Fn(QsOwned<V>, QsOwned<V>) -> Result<QsOwned<V>, (QsOwned<V>, E)>,
    {
        // Search directly for the insertion leaf without using seek,
        // since seek has "points at element" semantics which may advance
        // to the next leaf when the key would be at the end of a leaf.
        if self.current_leaf.is_some() {
            self.current_leaf.take().unwrap().unlock_exclusive();
        }
        let leaf = get_leaf_exclusively_using_optimistic_search_with_fallback(
            self.tree.root.as_node_ref(),
            &key,
        );
        let search_result = leaf.binary_search_key(&key);
        self.current_leaf = Some(leaf);
        self.current_index = search_result.unwrap_either();

        let leaf = self.current_leaf.as_mut().unwrap();
        if let Ok(index) = search_result {
            let existing_value = leaf.storage.get_value(index);
            match predicate(existing_value) {
                ModifyDecision::Modify => {
                    match leaf.modify_value(index, |old_value| modify_fn(old_value, new_value)) {
                        Ok(()) => return InsertOrModifyIfResult::Modified,
                        Err(e) => return InsertOrModifyIfResult::Error(e),
                    }
                }
                ModifyDecision::DoNothing => return InsertOrModifyIfResult::DidNothing,
                ModifyDecision::Error(e) => return InsertOrModifyIfResult::Error(e),
            }
        } else if leaf.has_capacity_for_modification(ModificationType::Insertion) {
            let index = search_result.unwrap_err();
            leaf.insert_new_value_at_index(key, new_value, index);
            self.tree
                .root
                .len
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return InsertOrModifyIfResult::Inserted; // New key inserted
        } else {
            // we need to split the leaf, so give up the lock
            self.current_leaf.take().unwrap().unlock_exclusive();

            // someone may have inserted while we were searching, so use get_or_insert
            // entry_location is the resulting locked leaf that the cursor will hold
            let (EntryLocation { leaf, index }, result) =
                self.tree.get_or_insert_pessimistic(key, new_value);
            self.current_leaf = Some(leaf);
            self.current_index = index;

            match result {
                GetOrInsertResult::Inserted => {
                    self.tree
                        .root
                        .len
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    InsertOrModifyIfResult::Inserted
                }
                GetOrInsertResult::GotReturningExistingAndProposed(
                    existing_value,
                    proposed_value,
                ) => {
                    match predicate(existing_value) {
                        ModifyDecision::Modify => {
                            match self.current_leaf
                                .as_mut()
                                .unwrap()
                                .modify_value(self.current_index, |old_value| {
                                    modify_fn(old_value, proposed_value)
                                }) {
                                Ok(()) => return InsertOrModifyIfResult::Modified,
                                Err(e) => return InsertOrModifyIfResult::Error(e),
                            }
                        }
                        ModifyDecision::DoNothing => return InsertOrModifyIfResult::DidNothing,
                        ModifyDecision::Error(e) => return InsertOrModifyIfResult::Error(e),
                    }
                }
            }
        }
    }

    fn seek_from_top<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            if self.current_leaf.is_some() {
                self.current_leaf.take().unwrap().unlock_exclusive();
            }
            let leaf = get_leaf_exclusively_using_optimistic_search_with_fallback(
                self.tree.root.as_node_ref(),
                key,
            );
            let result = leaf.binary_search_key(key);
            let index = result.unwrap_either();

            // If insertion point is past the end of this leaf, advance to next leaf
            // to maintain "points at element" semantics
            if result.is_err() && index >= leaf.num_keys() {
                if let Some(next_leaf_ref) = leaf.next_leaf() {
                    match next_leaf_ref.try_lock_exclusive() {
                        Ok(next_leaf) => {
                            leaf.unlock_exclusive();
                            self.current_leaf = Some(next_leaf);
                            self.current_index = 0;
                            return false;
                        }
                        Err(_) => {
                            // Couldn't get lock, retry from top
                            leaf.unlock_exclusive();
                            continue;
                        }
                    }
                }
                // No next leaf - we're past the end of the tree
            }

            self.current_leaf = Some(leaf);
            self.current_index = index;
            return result.is_ok();
        }
    }

    pub fn current(&self) -> Option<Entry<K, V>> {
        if let Some(leaf) = &self.current_leaf {
            if self.current_index < leaf.num_keys() {
                return Some(Entry::new(
                    leaf.storage.get_key(self.current_index),
                    leaf.storage.get_value(self.current_index),
                ));
            }
        }
        None
    }

    pub fn move_next(&mut self) -> bool {
        loop {
            if let None = self.current_leaf {
                return false;
            }
            let leaf = self.current_leaf.unwrap();
            if self.current_index < leaf.num_keys().saturating_sub(1) {
                self.current_index += 1;
                return true;
            }

            // Move to next leaf
            let maybe_next_leaf = leaf.next_leaf();
            if maybe_next_leaf.is_none() {
                self.current_leaf.take().unwrap().unlock_exclusive();
                return false;
            }

            let next_leaf = match maybe_next_leaf.unwrap().try_lock_exclusive() {
                Ok(leaf) => leaf,
                Err(_) => {
                    // we didn't attain the lock, so restart from the top
                    // to the current key
                    let key = leaf.storage.get_key(self.current_index);
                    self.seek_from_top(&key);
                    continue;
                }
            };
            self.current_leaf.take().unwrap().unlock_exclusive();
            self.current_leaf = Some(next_leaf);
            self.current_index = 0;
            return true;
        }
    }

    pub fn move_prev(&mut self) -> bool {
        loop {
            if let Some(leaf) = self.current_leaf {
                if self.current_index > 0 {
                    self.current_index -= 1;
                    return true;
                }

                // Move to previous leaf
                let maybe_prev_leaf = leaf.prev_leaf();
                if maybe_prev_leaf.is_none() {
                    self.current_leaf.take().unwrap().unlock_exclusive();
                    return false;
                }

                let prev_leaf = match maybe_prev_leaf.unwrap().try_lock_exclusive() {
                    Ok(leaf) => leaf,
                    Err(_) => {
                        // we didn't attain the lock, so restart from the top
                        // to the current key
                        let key = leaf.storage.get_key(self.current_index);
                        self.seek_from_top(&key);
                        continue;
                    }
                };
                self.current_leaf.take().unwrap().unlock_exclusive();
                self.current_index = prev_leaf.num_keys().saturating_sub(1);
                self.current_leaf = Some(prev_leaf);
                return true;
            }
        }
    }
}

impl<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Drop for CursorMut<'a, K, V> {
    fn drop(&mut self) {
        if let Some(leaf) = self.current_leaf.take() {
            leaf.unlock_exclusive();
        }
    }
}

pub struct NonLockingCursor<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    pub tree: &'a BTree<K, V>,
    unlocked_leaf: Option<SharedNodeRef<K, V, marker::Unlocked, marker::Leaf>>,
    current_index: usize,
    remembered_key: Option<QsWeak<K>>,
}

impl<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> NonLockingCursor<'a, K, V> {
    pub(crate) fn new(tree: &'a BTree<K, V>) -> Self {
        Self {
            tree,
            unlocked_leaf: None,
            current_index: 0,
            remembered_key: None,
        }
    }

    fn clear_position(&mut self) {
        self.unlocked_leaf = None;
        self.current_index = 0;
        self.remembered_key = None;
    }

    fn remember_and_unlock(
        &mut self,
        leaf: SharedNodeRef<K, V, marker::LockedShared, marker::Leaf>,
        index: usize,
    ) {
        self.current_index = index;
        if index < leaf.num_keys() {
            self.remembered_key = Some(leaf.storage.get_key(index));
        } else {
            self.remembered_key = None;
        }
        self.unlocked_leaf = Some(leaf.unlock_shared());
    }

    fn lock_at_remembered_position(
        &mut self,
    ) -> Option<SharedNodeRef<K, V, marker::LockedShared, marker::Leaf>> {
        let unlocked_leaf = self.unlocked_leaf?;

        let remembered_key = match self.remembered_key {
            Some(key) => key,
            None => {
                // Past end of leaf (no key at current position).
                // Try to re-lock and position at end.
                if let Ok(locked_leaf) = unlocked_leaf.try_lock_shared() {
                    if !locked_leaf.header().is_retired() {
                        self.current_index = locked_leaf.num_keys();
                        return Some(locked_leaf);
                    }
                    locked_leaf.unlock_shared();
                }
                // Leaf is retired or can't lock - position is lost
                return None;
            }
        };

        // Fast path: try to re-lock the stored leaf
        if let Ok(locked_leaf) = unlocked_leaf.try_lock_shared() {
            if !locked_leaf.header().is_retired() && locked_leaf.num_keys() > 0 {
                self.current_index =
                    locked_leaf.binary_search_key(&*remembered_key).unwrap_either();
                return Some(locked_leaf);
            }
            locked_leaf.unlock_shared();
        }

        // Slow path: search from root
        let leaf = get_leaf_shared_using_optimistic_search_with_fallback(
            self.tree.root.as_node_ref(),
            &*remembered_key,
        );
        self.current_index = leaf.binary_search_key(&*remembered_key).unwrap_either();
        Some(leaf)
    }

    pub fn seek_to_start(&mut self) {
        let leaf =
            match get_first_leaf_shared_using_optimistic_search(self.tree.root.as_node_ref()) {
                Ok(leaf) => leaf,
                Err(_) => get_first_leaf_shared_using_shared_search(self.tree.root.as_node_ref()),
            };
        self.remember_and_unlock(leaf, 0);
    }

    pub fn seek_to_end(&mut self) {
        let leaf =
            match get_last_leaf_shared_using_optimistic_search(self.tree.root.as_node_ref()) {
                Ok(leaf) => leaf,
                Err(_) => get_last_leaf_shared_using_shared_search(self.tree.root.as_node_ref()),
            };
        let index = leaf.num_keys().saturating_sub(1);
        self.remember_and_unlock(leaf, index);
    }

    pub fn seek<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        // Optimistically try the current leaf
        if let Some(unlocked_leaf) = self.unlocked_leaf {
            if let Ok(locked_leaf) = unlocked_leaf.try_lock_shared() {
                if !locked_leaf.header().is_retired() {
                    let index = locked_leaf.binary_search_key(key).unwrap_either();
                    if index < locked_leaf.num_keys() {
                        let leaf_key = locked_leaf.storage.get_key(index);
                        if leaf_key.deref().borrow() == key {
                            self.remember_and_unlock(locked_leaf, index);
                            return true;
                        }
                    }
                }
                locked_leaf.unlock_shared();
            }
        }
        self.seek_from_top(key)
    }

    fn seek_from_top<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            let leaf = get_leaf_shared_using_optimistic_search_with_fallback(
                self.tree.root.as_node_ref(),
                key,
            );
            let result = leaf.binary_search_key(key);
            let index = result.unwrap_either();

            // If insertion point is past the end of this leaf, advance to next leaf
            // to maintain "points at element" semantics
            if result.is_err() && index >= leaf.num_keys() {
                if let Some(next_leaf_ref) = leaf.next_leaf() {
                    match next_leaf_ref.try_lock_shared() {
                        Ok(next_leaf) => {
                            leaf.unlock_shared();
                            self.remember_and_unlock(next_leaf, 0);
                            return false;
                        }
                        Err(_) => {
                            // Couldn't get lock, retry from top
                            leaf.unlock_shared();
                            continue;
                        }
                    }
                }
                // No next leaf - we're past the end of the tree
            }

            self.remember_and_unlock(leaf, index);
            return result.is_ok();
        }
    }

    pub fn current(&mut self) -> Option<Entry<K, V>> {
        let leaf = self.lock_at_remembered_position()?;
        if self.current_index < leaf.num_keys() {
            let entry = Entry::new(
                leaf.storage.get_key(self.current_index),
                leaf.storage.get_value(self.current_index),
            );
            self.remember_and_unlock(leaf, self.current_index);
            Some(entry)
        } else {
            // Past end - preserve position so move_prev() can still work
            self.remember_and_unlock(leaf, self.current_index);
            None
        }
    }

    pub fn move_next(&mut self) -> bool {
        loop {
            let leaf = match self.lock_at_remembered_position() {
                Some(leaf) => leaf,
                None => return false,
            };

            if self.current_index < leaf.num_keys().saturating_sub(1) {
                let new_index = self.current_index + 1;
                self.remember_and_unlock(leaf, new_index);
                return true;
            }

            // Move to next leaf
            let maybe_next = leaf.next_leaf();
            if maybe_next.is_none() {
                leaf.unlock_shared();
                self.clear_position();
                return false;
            }

            match maybe_next.unwrap().try_lock_shared() {
                Ok(next_leaf) => {
                    if next_leaf.header().is_retired() || next_leaf.num_keys() == 0 {
                        // Next leaf is retired or empty, re-seek from root
                        next_leaf.unlock_shared();
                        let key = self.remembered_key.unwrap();
                        leaf.unlock_shared();
                        self.seek_from_top(&*key);
                        continue;
                    }
                    leaf.unlock_shared();
                    self.remember_and_unlock(next_leaf, 0);
                    return true;
                }
                Err(_) => {
                    // Couldn't lock next leaf, re-seek from root
                    let key = self.remembered_key.unwrap();
                    leaf.unlock_shared();
                    self.seek_from_top(&*key);
                    continue;
                }
            }
        }
    }

    pub fn move_prev(&mut self) -> bool {
        loop {
            let leaf = match self.lock_at_remembered_position() {
                Some(leaf) => leaf,
                None => return false,
            };

            if self.current_index > 0 {
                let new_index = self.current_index - 1;
                self.remember_and_unlock(leaf, new_index);
                return true;
            }

            // Move to previous leaf
            let maybe_prev = leaf.prev_leaf();
            if maybe_prev.is_none() {
                leaf.unlock_shared();
                self.clear_position();
                return false;
            }

            match maybe_prev.unwrap().try_lock_shared() {
                Ok(prev_leaf) => {
                    if prev_leaf.header().is_retired() || prev_leaf.num_keys() == 0 {
                        // Previous leaf is retired or empty, re-seek from root
                        prev_leaf.unlock_shared();
                        let key = self.remembered_key.unwrap();
                        leaf.unlock_shared();
                        self.seek_from_top(&*key);
                        continue;
                    }
                    leaf.unlock_shared();
                    let last_index = prev_leaf.num_keys().saturating_sub(1);
                    self.remember_and_unlock(prev_leaf, last_index);
                    return true;
                }
                Err(_) => {
                    // Couldn't lock prev leaf, re-seek from root
                    let key = self.remembered_key.unwrap();
                    leaf.unlock_shared();
                    self.seek_from_top(&*key);
                    continue;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use btree_macros::qsbr_test;
    use thin::QsArc;

    use crate::array_types::ORDER;
    use crate::qsbr_reclaimer;
    use std::sync::Barrier;

    use super::*;

    #[qsbr_test]
    fn test_cursor() {
        let tree = BTree::<usize, String>::new();

        // Insert some test data
        for i in 0..10 {
            tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
        }

        // Test forward traversal
        let mut cursor = tree.cursor();
        cursor.seek_to_start();
        for i in 0..10 {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_next();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test backward traversal
        cursor.seek_to_end();
        for i in (0..10).rev() {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_prev();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test mixed traversal
        cursor.seek_to_start();
        assert_eq!(*cursor.current().unwrap().value(), "value0");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap().value(), "value1");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap().value(), "value2");
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap().value(), "value1");
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap().value(), "value0");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap().value(), "value1");

        // Test seeking
        cursor.seek(&5);
        assert_eq!(*cursor.current().unwrap().value(), "value5");
        cursor.seek(&7);
        assert_eq!(*cursor.current().unwrap().value(), "value7");
    }

    #[qsbr_test]
    fn test_cursor_leaf_boundaries() {
        let tree = BTree::<usize, String>::new();

        // Insert enough elements to force leaf splits
        // Using ORDER * 2 ensures we have at least 2 leaves
        let n = ORDER * 2;
        for i in 0..n {
            tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
            tree.check_invariants();
        }

        // Test forward traversal across leaves
        let mut cursor = tree.cursor();
        cursor.seek_to_start();
        for i in 0..n {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_next();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test backward traversal across leaves
        cursor.seek_to_end();
        for i in (0..n).rev() {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_prev();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test seeking across leaves
        cursor.seek(&(ORDER - 1)); // Should be near end of first leaf
        assert_eq!(
            *cursor.current().unwrap().value(),
            format!("value{}", ORDER - 1)
        );
        cursor.move_next();
        assert_eq!(
            *cursor.current().unwrap().value(),
            format!("value{}", ORDER)
        ); // Should cross to next leaf

        cursor.seek(&ORDER); // Should be at start of second leaf
        cursor.move_prev();
        assert_eq!(
            *cursor.current().unwrap().value(),
            format!("value{}", ORDER - 1)
        ); // Should cross back to first leaf
    }

    #[qsbr_test]
    fn test_cursor_mut() {
        let tree = BTree::<usize, String>::new();

        // Insert some test data
        for i in 0..10 {
            tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
        }

        // Test forward traversal
        let mut cursor = tree.cursor_mut();
        cursor.seek_to_start();
        for i in 0..10 {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_next();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test backward traversal
        cursor.seek_to_end();
        for i in (0..10).rev() {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_prev();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test mixed traversal
        cursor.seek_to_start();
        assert_eq!(*cursor.current().unwrap().value(), "value0");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap().value(), "value1");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap().value(), "value2");
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap().value(), "value1");
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap().value(), "value0");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap().value(), "value1");

        // Test seeking
        cursor.seek(&5);
        assert_eq!(*cursor.current().unwrap().value(), "value5");
        cursor.seek(&7);
        assert_eq!(*cursor.current().unwrap().value(), "value7");

        drop(cursor);
        drop(tree);
    }

    #[qsbr_test]
    fn test_cursor_mut_leaf_boundaries() {
        let tree = BTree::<usize, String>::new();

        // Insert enough elements to force leaf splits
        // Using ORDER * 2 ensures we have at least 2 leaves
        let n = ORDER * 2;
        for i in 0..n {
            tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
            tree.check_invariants();
        }

        // Test forward traversal across leaves
        let mut cursor = tree.cursor_mut();
        cursor.seek_to_start();
        for i in 0..n {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_next();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test backward traversal across leaves
        cursor.seek_to_end();
        for i in (0..n).rev() {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_prev();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test seeking across leaves
        cursor.seek(&(ORDER - 1)); // Should be near end of first leaf
        assert_eq!(
            *cursor.current().unwrap().value(),
            format!("value{}", ORDER - 1)
        );
        cursor.move_next();
        assert_eq!(
            *cursor.current().unwrap().value(),
            format!("value{}", ORDER)
        ); // Should cross to next leaf

        cursor.seek(&ORDER); // Should be at start of second leaf
        cursor.move_prev();
        assert_eq!(
            *cursor.current().unwrap().value(),
            format!("value{}", ORDER - 1)
        ); // Should cross back to first leaf

        drop(cursor);
        drop(tree);
    }

    #[qsbr_test]
    fn test_interaction_between_mut_cursor_and_shared_cursor() {
        let tree = BTree::<usize, String>::new();

        // Insert enough elements to ensure multiple leaves
        let n = ORDER * 3; // Use 3 times ORDER to ensure multiple leaves
        for i in 0..n {
            tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
            tree.check_invariants();
        }

        let barrier = Barrier::new(2);

        std::thread::scope(|s| {
            // First thread starts at end with mut cursor and moves backwards
            let tree_ref = &tree;
            let barrier_ref = &barrier;
            s.spawn(move || {
                let _guard = qsbr_reclaimer().guard();
                {
                    let mut cursor_mut = tree_ref.cursor_mut();
                    cursor_mut.seek_to_end();

                    // Wait for both cursors to be ready
                    barrier_ref.wait();

                    // Move backwards and verify values
                    let mut expected = n - 1;
                    loop {
                        assert_eq!(
                            *cursor_mut.current().unwrap().value(),
                            format!("value{}", expected)
                        );
                        if !cursor_mut.move_prev() {
                            break;
                        }
                        expected -= 1;
                        std::thread::yield_now();
                    }
                    assert_eq!(expected, 0);
                }
            });

            // Second thread starts at beginning with shared cursor and moves forwards
            let tree_ref = &tree;
            let barrier_ref = &barrier;
            s.spawn(move || {
                let _guard = qsbr_reclaimer().guard();
                {
                    let mut cursor_shared = tree_ref.cursor();
                    cursor_shared.seek_to_start();

                    // Wait for both cursors to be ready
                    barrier_ref.wait();

                    // Move forward and verify values
                    let mut expected = 0;
                    loop {
                        assert_eq!(
                            *cursor_shared.current().unwrap().value(),
                            format!("value{}", expected)
                        );
                        if !cursor_shared.move_next() {
                            break;
                        }
                        expected += 1;
                        std::thread::yield_now();
                    }
                    assert_eq!(expected, n - 1);
                }
            });
        });
    }

    #[qsbr_test]
    fn test_cursor_mut_insert_or_modify_if_with_forced_splits() {
        let tree = BTree::<usize, usize>::new();
        let num_threads = 16;
        let operations_per_thread = 500;

        let barrier = Barrier::new(num_threads);

        std::thread::scope(|s| {
            for thread_id in 0..num_threads {
                let tree_ref = &tree;
                let barrier_ref = &barrier;
                s.spawn(move || {
                    let _guard = qsbr_reclaimer().guard();

                    barrier_ref.wait();

                    for op in 0..operations_per_thread {
                        // Use sequential keys to force splits more aggressively
                        let key = thread_id * operations_per_thread + op;

                        if op % 5 == 0 && op > 0 {
                            // Remove a previous key
                            let remove_key = thread_id * operations_per_thread + (op - 1);
                            tree_ref.remove(&remove_key);
                        } else {
                            // Use cursor's insert_or_modify_if
                            let mut cursor = tree_ref.cursor_mut();
                            cursor.insert_or_modify_if::<_, ()>(
                                QsArc::new(key),
                                QsOwned::new(key),
                                |_| ModifyDecision::Modify,
                                |old_val, new_val| Ok(QsOwned::new(*old_val + *new_val)),
                            );
                            drop(cursor);
                        }

                        // Yield frequently to increase contention during splits
                        if op % 3 == 0 {
                            std::thread::yield_now();
                        }
                    }
                });
            }
        });

        tree.check_invariants();
    }

    #[qsbr_test]
    fn test_cursor_mut_insert_or_modify_if_with_get_with_on_empty_tree() {
        let tree = BTree::<usize, usize>::new();
        let num_threads = 8;
        let barrier = Barrier::new(num_threads);

        std::thread::scope(|s| {
            // Half the threads do insert_or_modify_if
            for thread_id in 0..num_threads / 2 {
                let tree_ref = &tree;
                let barrier_ref = &barrier;
                s.spawn(move || {
                    let _guard = qsbr_reclaimer().guard();
                    barrier_ref.wait();

                    for op in 0..100 {
                        let key = thread_id * 100 + op;
                        let mut cursor = tree_ref.cursor_mut();
                        cursor.insert_or_modify_if::<_, ()>(
                            QsArc::new(key),
                            QsOwned::new(key),
                            |_| ModifyDecision::Modify,
                            |old_val, new_val| Ok(QsOwned::new(*old_val + *new_val)),
                        );
                        drop(cursor);
                        std::thread::yield_now();
                    }
                });
            }

            // Other half do get_with
            for _ in num_threads / 2..num_threads {
                let tree_ref = &tree;
                let barrier_ref = &barrier;
                s.spawn(move || {
                    let _guard = qsbr_reclaimer().guard();
                    barrier_ref.wait();

                    for key in 0..400 {
                        let _ = tree_ref.get_with(&key, |v| *v);
                        std::thread::yield_now();
                    }
                });
            }
        });

        tree.check_invariants();
    }

    #[qsbr_test]
    fn test_cursor_move_next_empty_tree() {
        let tree = BTree::<usize, usize>::new();
        let mut cursor = tree.cursor();
        cursor.seek_to_start();
        assert!(!cursor.move_next());
    }

    #[qsbr_test]
    fn test_cursor_mut_seek_to_end_empty_tree() {
        let tree = BTree::<usize, usize>::new();
        let mut cursor = tree.cursor_mut();
        cursor.seek_to_end();
        assert!(cursor.current().is_none());
    }

    #[qsbr_test]
    fn test_cursor_move_next_prefix_scan() {
        let tree = BTree::<str, usize>::new();
        tree.insert(QsArc::new_from_str("prefix key1"), QsOwned::new(1));
        tree.insert(QsArc::new_from_str("prefix key2"), QsOwned::new(2));
        tree.insert(QsArc::new_from_str("prefix key3"), QsOwned::new(3));
        let mut cursor = tree.cursor();

        // "prefiy" > "prefix key3", so no greater key exists -> points at None
        assert!(!cursor.seek("prefiy"));
        assert!(cursor.current().is_none());
        assert!(cursor.move_prev());
        assert!(cursor.current().unwrap().key() == "prefix key3");

        // "prefix" < "prefix key1", so points at next greater key "prefix key1"
        assert!(!cursor.seek("prefix"));
        assert!(cursor.current().unwrap().key() == "prefix key1");
        assert!(cursor.move_next());
        assert!(cursor.current().unwrap().key() == "prefix key2");
        assert!(cursor.move_next());
        assert!(cursor.current().unwrap().key() == "prefix key3");
        assert!(!cursor.move_next());

        // Exact match
        assert!(cursor.seek("prefix key2"));
        assert!(cursor.current().unwrap().key() == "prefix key2");
    }

    #[qsbr_test]
    fn test_cursor_seek_between_leaves() {
        let tree = BTree::<usize, usize>::new();

        // Insert enough elements to force multiple leaves (ORDER = 64)
        // Use keys 0, 10, 20, 30, ... to leave gaps for seeking between
        for i in 0..(ORDER * 2) {
            tree.insert(QsArc::new(i * 10), QsOwned::new(i));
        }

        let mut cursor = tree.cursor();

        // Seek to a key that exists
        assert!(cursor.seek(&100));
        assert_eq!(*cursor.current().unwrap().key(), 100);

        // Seek to a key between two existing keys within a leaf
        // 105 should point at 110 (next greater key)
        assert!(!cursor.seek(&105));
        assert_eq!(*cursor.current().unwrap().key(), 110);

        // Seek to a key that would be between leaves
        // Find the boundary by looking for where one leaf ends and another begins
        // With ORDER=64 keys per leaf, after splitting we'd have ~32-64 keys per leaf
        // Let's seek to a value just after what should be the last key in first leaf
        cursor.seek_to_start();
        let mut prev_key = *cursor.current().unwrap().key();
        for _ in 0..ORDER {
            if !cursor.move_next() {
                break;
            }
            prev_key = *cursor.current().unwrap().key();
        }
        // prev_key is now around the ORDER-th element
        // Seek to prev_key + 5 (a gap), should point at prev_key + 10
        let gap_key = prev_key + 5;
        let expected_next = prev_key + 10;
        assert!(!cursor.seek(&gap_key));
        assert!(
            cursor.current().is_some(),
            "seek to {} should point at next greater key {}, not None",
            gap_key,
            expected_next
        );
        assert_eq!(*cursor.current().unwrap().key(), expected_next);

        // Seek past the end
        let max_key = (ORDER * 2 - 1) * 10;
        assert!(!cursor.seek(&(max_key + 5)));
        assert!(cursor.current().is_none());
    }

    #[qsbr_test]
    fn test_cursor_seek_at_leaf_boundary() {
        let tree = BTree::<usize, usize>::new();

        // Insert enough elements to force multiple leaves
        for i in 0..(ORDER * 2) {
            tree.insert(QsArc::new(i * 10), QsOwned::new(i));
        }

        // Find the actual leaf boundary by traversing and detecting when we cross leaves
        let mut cursor = tree.cursor();
        cursor.seek_to_start();

        // Collect all keys and find where leaf boundaries are
        let mut keys = Vec::new();
        loop {
            keys.push(*cursor.current().unwrap().key());
            if !cursor.move_next() {
                break;
            }
        }

        // Now seek to keys just before each key to test "points at element" semantics
        for &key in &keys {
            if key > 0 {
                // Seek to key - 5 (which doesn't exist), should point at key
                assert!(!cursor.seek(&(key - 5)));
                assert!(
                    cursor.current().is_some(),
                    "seek to {} should point at {}, not None",
                    key - 5,
                    key
                );
                assert_eq!(
                    *cursor.current().unwrap().key(),
                    key,
                    "seek to {} should point at {}",
                    key - 5,
                    key
                );
            }
        }

        // Also test move_prev after seek past end of a leaf
        // Find a key near the middle of the tree
        let mid_key = keys[keys.len() / 2];
        cursor.seek(&mid_key);
        assert!(cursor.move_prev());
        assert_eq!(*cursor.current().unwrap().key(), keys[keys.len() / 2 - 1]);
    }

    #[qsbr_test]
    fn test_non_locking_cursor() {
        let tree = BTree::<usize, String>::new();

        // Insert some test data
        for i in 0..10 {
            tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
        }

        // Test forward traversal
        let mut cursor = tree.non_locking_cursor();
        cursor.seek_to_start();
        for i in 0..10 {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_next();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test backward traversal
        cursor.seek_to_end();
        for i in (0..10).rev() {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_prev();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test mixed traversal
        cursor.seek_to_start();
        assert_eq!(*cursor.current().unwrap().value(), "value0");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap().value(), "value1");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap().value(), "value2");
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap().value(), "value1");
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap().value(), "value0");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap().value(), "value1");

        // Test seeking
        cursor.seek(&5);
        assert_eq!(*cursor.current().unwrap().value(), "value5");
        cursor.seek(&7);
        assert_eq!(*cursor.current().unwrap().value(), "value7");
    }

    #[qsbr_test]
    fn test_non_locking_cursor_leaf_boundaries() {
        let tree = BTree::<usize, String>::new();

        // Insert enough elements to force leaf splits
        let n = ORDER * 2;
        for i in 0..n {
            tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
            tree.check_invariants();
        }

        // Test forward traversal across leaves
        let mut cursor = tree.non_locking_cursor();
        cursor.seek_to_start();
        for i in 0..n {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_next();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test backward traversal across leaves
        cursor.seek_to_end();
        for i in (0..n).rev() {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_prev();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test seeking across leaves
        cursor.seek(&(ORDER - 1));
        assert_eq!(
            *cursor.current().unwrap().value(),
            format!("value{}", ORDER - 1)
        );
        cursor.move_next();
        assert_eq!(
            *cursor.current().unwrap().value(),
            format!("value{}", ORDER)
        );

        cursor.seek(&ORDER);
        cursor.move_prev();
        assert_eq!(
            *cursor.current().unwrap().value(),
            format!("value{}", ORDER - 1)
        );
    }

    #[qsbr_test]
    fn test_non_locking_cursor_prefix_scan() {
        let tree = BTree::<str, usize>::new();
        tree.insert(QsArc::new_from_str("prefix key1"), QsOwned::new(1));
        tree.insert(QsArc::new_from_str("prefix key2"), QsOwned::new(2));
        tree.insert(QsArc::new_from_str("prefix key3"), QsOwned::new(3));
        let mut cursor = tree.non_locking_cursor();

        // "prefiy" > "prefix key3", so no greater key exists -> points at None
        assert!(!cursor.seek("prefiy"));
        assert!(cursor.current().is_none());
        assert!(cursor.move_prev());
        assert!(cursor.current().unwrap().key() == "prefix key3");

        // "prefix" < "prefix key1", so points at next greater key "prefix key1"
        assert!(!cursor.seek("prefix"));
        assert!(cursor.current().unwrap().key() == "prefix key1");
        assert!(cursor.move_next());
        assert!(cursor.current().unwrap().key() == "prefix key2");
        assert!(cursor.move_next());
        assert!(cursor.current().unwrap().key() == "prefix key3");
        assert!(!cursor.move_next());

        // Exact match
        assert!(cursor.seek("prefix key2"));
        assert!(cursor.current().unwrap().key() == "prefix key2");
    }

    #[qsbr_test]
    fn test_non_locking_cursor_seek_between_leaves() {
        let tree = BTree::<usize, usize>::new();

        // Insert enough elements to force multiple leaves
        // Use keys 0, 10, 20, 30, ... to leave gaps for seeking between
        for i in 0..(ORDER * 2) {
            tree.insert(QsArc::new(i * 10), QsOwned::new(i));
        }

        let mut cursor = tree.non_locking_cursor();

        // Seek to a key that exists
        assert!(cursor.seek(&100));
        assert_eq!(*cursor.current().unwrap().key(), 100);

        // Seek to a key between two existing keys within a leaf
        // 105 should point at 110 (next greater key)
        assert!(!cursor.seek(&105));
        assert_eq!(*cursor.current().unwrap().key(), 110);

        // Seek to a key that would be between leaves
        cursor.seek_to_start();
        let mut prev_key = *cursor.current().unwrap().key();
        for _ in 0..ORDER {
            if !cursor.move_next() {
                break;
            }
            prev_key = *cursor.current().unwrap().key();
        }
        // prev_key is now around the ORDER-th element
        // Seek to prev_key + 5 (a gap), should point at prev_key + 10
        let gap_key = prev_key + 5;
        let expected_next = prev_key + 10;
        assert!(!cursor.seek(&gap_key));
        assert!(
            cursor.current().is_some(),
            "seek to {} should point at next greater key {}, not None",
            gap_key,
            expected_next
        );
        assert_eq!(*cursor.current().unwrap().key(), expected_next);

        // Seek past the end
        let max_key = (ORDER * 2 - 1) * 10;
        assert!(!cursor.seek(&(max_key + 5)));
        assert!(cursor.current().is_none());
    }

    #[qsbr_test]
    fn test_interaction_between_mut_cursor_and_non_locking_cursor() {
        let tree = BTree::<usize, String>::new();

        // Insert enough elements to ensure multiple leaves
        let n = ORDER * 3;
        for i in 0..n {
            tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
            tree.check_invariants();
        }

        let barrier = Barrier::new(2);

        std::thread::scope(|s| {
            // First thread starts at end with mut cursor and moves backwards
            let tree_ref = &tree;
            let barrier_ref = &barrier;
            s.spawn(move || {
                let _guard = qsbr_reclaimer().guard();
                {
                    let mut cursor_mut = tree_ref.cursor_mut();
                    cursor_mut.seek_to_end();

                    // Wait for both cursors to be ready
                    barrier_ref.wait();

                    // Move backwards and verify values
                    let mut expected = n - 1;
                    loop {
                        assert_eq!(
                            *cursor_mut.current().unwrap().value(),
                            format!("value{}", expected)
                        );
                        if !cursor_mut.move_prev() {
                            break;
                        }
                        expected -= 1;
                        std::thread::yield_now();
                    }
                    assert_eq!(expected, 0);
                }
            });

            // Second thread starts at beginning with non-locking cursor and moves forwards
            let tree_ref = &tree;
            let barrier_ref = &barrier;
            s.spawn(move || {
                let _guard = qsbr_reclaimer().guard();
                {
                    let mut cursor = tree_ref.non_locking_cursor();
                    cursor.seek_to_start();

                    // Wait for both cursors to be ready
                    barrier_ref.wait();

                    // Move forward and verify values
                    let mut expected = 0;
                    loop {
                        assert_eq!(
                            *cursor.current().unwrap().value(),
                            format!("value{}", expected)
                        );
                        if !cursor.move_next() {
                            break;
                        }
                        expected += 1;
                        std::thread::yield_now();
                    }
                    assert_eq!(expected, n - 1);
                }
            });
        });
    }
}
