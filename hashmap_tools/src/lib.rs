use std::collections::hash_map;

use consume_on_drop::{Consume, ConsumeOnDrop};

pub struct OccupiedEntry<'a, K, V>(ConsumeOnDrop<OccupiedEntryInner<'a, K, V>>);

impl<'a, K, V> OccupiedEntry<'a, K, V> {
    pub fn key(&self) -> &K {
        match &*self.0 {
            OccupiedEntryInner::Vacant { entry, .. } => entry.key(),
            OccupiedEntryInner::Occupied(entry) => entry.key(),
        }
    }
    pub fn get(&self) -> &V {
        match &*self.0 {
            OccupiedEntryInner::Vacant { value, .. } => value,
            OccupiedEntryInner::Occupied(entry) => entry.get(),
        }
    }
    pub fn get_mut(&mut self) -> &mut V {
        match &mut *self.0 {
            OccupiedEntryInner::Vacant { value, .. } => value,
            OccupiedEntryInner::Occupied(entry) => entry.get_mut(),
        }
    }
    pub fn into_mut(self) -> &'a mut V {
        ConsumeOnDrop::into_inner(self.0).into_mut()
    }
    pub fn remove(self) -> V {
        match ConsumeOnDrop::into_inner(self.0) {
            OccupiedEntryInner::Vacant { value, .. } => value,
            OccupiedEntryInner::Occupied(entry) => entry.remove(),
        }
    }
}

enum OccupiedEntryInner<'a, K, V> {
    Vacant {
        entry: hash_map::VacantEntry<'a, K, V>,
        value: V,
    },
    Occupied(hash_map::OccupiedEntry<'a, K, V>),
}

impl<'a, K, V> OccupiedEntryInner<'a, K, V> {
    pub fn into_mut(self) -> &'a mut V {
        match self {
            OccupiedEntryInner::Vacant { entry, value } => entry.insert(value),
            OccupiedEntryInner::Occupied(entry) => entry.into_mut(),
        }
    }
}

impl<K, V> Consume for OccupiedEntryInner<'_, K, V> {
    fn consume(self) {
        self.into_mut();
    }
}

pub fn or_insert_with_key<K, V>(
    entry: hash_map::Entry<K, V>,
    f: impl FnOnce(&K) -> V,
) -> OccupiedEntry<K, V> {
    match entry {
        hash_map::Entry::Vacant(entry) => {
            OccupiedEntry(ConsumeOnDrop::new(OccupiedEntryInner::Vacant {
                value: f(entry.key()),
                entry,
            }))
        }
        hash_map::Entry::Occupied(entry) => {
            OccupiedEntry(ConsumeOnDrop::new(OccupiedEntryInner::Occupied(entry)))
        }
    }
}

pub fn or_insert_with<K, V>(
    entry: hash_map::Entry<K, V>,
    f: impl FnOnce() -> V,
) -> OccupiedEntry<K, V> {
    or_insert_with_key(entry, |_| f())
}

pub fn or_insert<K, V>(entry: hash_map::Entry<K, V>, value: V) -> OccupiedEntry<K, V> {
    or_insert_with(entry, || value)
}

pub fn or_default<K, V: Default>(entry: hash_map::Entry<K, V>) -> OccupiedEntry<K, V> {
    or_insert_with(entry, V::default)
}
