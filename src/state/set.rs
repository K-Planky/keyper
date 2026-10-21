//! A dense member vector keeps SMEMBERS O(cardinality), even after removals.
//! std HashSet iteration is O(capacity), which may reflect a much larger past
//! set. An index plus swap-remove gives O(1) average insert/remove/contains and
//! O(length) enumeration, at the cost of storing each member's bytes twice.

use std::collections::HashMap;

#[derive(Default)]
pub(super) struct Set {
    pub(super) members: Vec<Vec<u8>>,
    positions: HashMap<Vec<u8>, usize>,
}

impl Set {
    pub(super) fn insert(&mut self, member: Vec<u8>) -> bool {
        use std::collections::hash_map::Entry;
        match self.positions.entry(member) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                self.members.push(entry.key().clone());
                entry.insert(self.members.len() - 1);
                true
            }
        }
    }

    pub(super) fn remove(&mut self, member: &[u8]) -> bool {
        let Some(index) = self.positions.remove(member) else {
            return false;
        };
        self.members.swap_remove(index);
        if let Some(moved) = self.members.get(index) {
            // The swapped-in member already has an index; update it in place.
            *self.positions.get_mut(moved.as_slice()).unwrap() = index;
        }
        true
    }

    pub(super) fn contains(&self, member: &[u8]) -> bool {
        self.positions.contains_key(member)
    }
}
