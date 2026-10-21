//! Dense field/value pairs plus a lookup index. Enumerating a std HashMap is
//! O(capacity), not O(current length) after deletions; dense pairs preserve the
//! HGETALL/HVALS bound. Fields are stored twice, values once. Swap-removal makes
//! order unspecified and keeps average O(1) field operations.

use std::collections::HashMap;

#[derive(Default)]
pub(super) struct Hash {
    pub(super) pairs: Vec<(Vec<u8>, Vec<u8>)>,
    positions: HashMap<Vec<u8>, usize>,
}

impl Hash {
    pub(super) fn set(&mut self, field: Vec<u8>, value: Vec<u8>) -> bool {
        use std::collections::hash_map::Entry;
        match self.positions.entry(field) {
            Entry::Occupied(entry) => {
                self.pairs[*entry.get()].1 = value;
                false
            }
            Entry::Vacant(entry) => {
                self.pairs.push((entry.key().clone(), value));
                entry.insert(self.pairs.len() - 1);
                true
            }
        }
    }

    pub(super) fn get(&self, field: &[u8]) -> Option<&Vec<u8>> {
        self.positions.get(field).map(|index| &self.pairs[*index].1)
    }

    pub(super) fn remove(&mut self, field: &[u8]) -> bool {
        let Some(index) = self.positions.remove(field) else {
            return false;
        };
        self.pairs.swap_remove(index);
        if let Some((moved, _)) = self.pairs.get(index) {
            // The swapped-in field already has an index; update it in place.
            *self.positions.get_mut(moved.as_slice()).unwrap() = index;
        }
        true
    }
}
