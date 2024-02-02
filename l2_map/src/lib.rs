use std::{
    collections::{hash_map, HashMap},
    hash::Hash,
    iter, mem,
};

use arrayvec::ArrayVec;
use derive_where::derive_where;
use index_list::{Index, IndexList};
use itertools::Either;

#[derive_where(Default)]
pub struct L2Map<K1: Clone + Eq + Hash, K2: Clone + Eq + Hash, V, const LIM: usize = 2> {
    map: HashMap<(K1, K2), Index>,
    ranges: HashMap<K1, RangeEntry<(K2, V), LIM>>,
    values: IndexList<(K2, V)>,
}

#[derive_where(Default)]
enum RangeEntry<T, const LIM: usize> {
    #[derive_where(default)]
    Inline(ArrayVec<T, LIM>),
    External(ExternalEntry),
}

#[derive(Clone, Copy)]
struct ExternalEntry {
    i: Index,
    j: Index,
}
impl ExternalEntry {
    fn iter<T>(self, values: &IndexList<T>) -> impl Iterator<Item = &T> {
        let Self { mut i, j } = self;
        iter::from_fn(move || {
            if i.is_none() {
                return None;
            }
            let result = values.get(i).unwrap();
            if i == j {
                i = Index::new();
            } else {
                i = values.next_index(i);
            }
            Some(result)
        })
    }
}

impl<K1: Clone + Eq + Hash, K2: Clone + Eq + Hash, V, const LIM: usize> L2Map<K1, K2, V, LIM> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&mut self, k1: K1, k2: K2, v: V) -> Option<V> {
        let mut e = hashmap_tools::or_default(self.ranges.entry(k1));
        let external = match e.get_mut() {
            RangeEntry::Inline(inline) => {
                for (current_k, current_v) in inline.iter_mut() {
                    if current_k == &k2 {
                        return Some(mem::replace(current_v, v));
                    }
                }
                let (k2, v) = match inline.try_push((k2, v)) {
                    Ok(()) => return None,
                    Err(cap_err) => cap_err.element(),
                };
                let mut iter = mem::take(inline).into_iter().chain(iter::once((k2, v)));
                let k1 = e.key();
                let (k2, v) = iter.next().unwrap();
                let mut j = self.values.insert_last((k2.clone(), v));
                self.map.insert((k1.clone(), k2), j);
                let i = j;
                for (k2, v) in iter {
                    j = self.values.insert_after(j, (k2.clone(), v));
                    self.map.insert((k1.clone(), k2), j);
                }
                ExternalEntry { i, j }
            }
            &mut RangeEntry::External(mut external) => {
                match self.map.entry((e.key().clone(), k2)) {
                    hash_map::Entry::Occupied(e) => {
                        let list_entry = self.values.get_mut(*e.get()).unwrap();
                        return Some(mem::replace(&mut list_entry.1, v));
                    }
                    hash_map::Entry::Vacant(e) => {
                        let (_k1, k2) = e.key();
                        external.j = self.values.insert_after(external.j, (k2.clone(), v));
                        e.insert(external.j);
                        external
                    }
                }
            }
        };
        *e.into_mut() = RangeEntry::External(external);
        None
    }
    pub fn get_iter<'a>(&'a self, k: &'a K1) -> impl Iterator<Item = &'a (K2, V)> {
        self.ranges.get(k).into_iter().flat_map(|e| match e {
            RangeEntry::Inline(inline) => Either::Left(inline.iter()),
            RangeEntry::External(external) => Either::Right(external.iter(&self.values)),
        })
    }
    pub fn get(&self, k1: &K1, k2: &K2) -> Option<&V> {
        let range = self.ranges.get(k1)?;
        match range {
            RangeEntry::Inline(inline) => {
                for (x, v) in inline.iter() {
                    if x == k2 {
                        return Some(v);
                    }
                }
                None
            }
            RangeEntry::External(_) => {
                let i = self.map.get(&(k1.clone(), k2.clone()))?;
                let (found_k2, v) = self.values.get(*i).unwrap();
                assert!(found_k2 == k2);
                Some(v)
            }
        }
    }
    pub fn get_mut(&mut self, k1: &K1, k2: &K2) -> Option<&mut V> {
        let range = self.ranges.get_mut(k1)?;
        match range {
            RangeEntry::Inline(inline) => {
                for (x, v) in inline.iter_mut() {
                    if x == k2 {
                        return Some(v);
                    }
                }
                None
            }
            RangeEntry::External(_) => {
                let i = self.map.get(&(k1.clone(), k2.clone()))?;
                let (found_k2, v) = self.values.get_mut(*i).unwrap();
                assert!(found_k2 == k2);
                Some(v)
            }
        }
    }
    pub fn remove(&mut self, k1: &K1, k2: &K2) -> Option<V> {
        let range = self.ranges.get_mut(k1)?;
        match range {
            RangeEntry::Inline(inline) => {
                for (i, (x, _v)) in inline.iter().enumerate() {
                    if x == k2 {
                        let (_, v) = inline.remove(i);
                        return Some(v);
                    }
                }
                None
            }
            RangeEntry::External(external) => {
                let i = self.map.remove(&(k1.clone(), k2.clone()))?;
                if external.i == external.j {
                    assert!(i == external.i);
                    self.ranges.remove(k1);
                } else if i == external.i {
                    external.i = self.values.next_index(i);
                } else if i == external.j {
                    external.j = self.values.prev_index(i);
                }
                let (removed_k2, v) = self.values.remove(i).unwrap();
                assert!(removed_k2 == *k2);
                Some(v)
            }
        }
    }
}
