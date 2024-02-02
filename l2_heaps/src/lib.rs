use std::{collections::HashMap, hash::Hash, iter, mem};

use arrayvec::ArrayVec;
use derive_where::derive_where;
use index_list::{Index, IndexList};

use self::external_heap::{ExternalHeap, HeapEntry};

mod external_heap;

#[derive_where(Default)]
pub struct L2Heaps<K1: Clone + Eq + Hash, K2: Clone + Ord + Hash, V, const LIM: usize = 2> {
    map: HashMap<(K1, K2), Index>,
    ranges: HashMap<K1, RangeEntry<(K2, V), LIM>>,
    values: IndexList<HeapEntry<K2, V>>,
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

impl<K1: Clone + Eq + Hash, K2: Clone + Ord + Hash, V, const LIM: usize> L2Heaps<K1, K2, V, LIM> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&mut self, k1: K1, k2: K2, v: V) -> Option<V> {
        let mut e = hashmap_tools::or_default(self.ranges.entry(k1));
        let external = match e.get_mut() {
            RangeEntry::Inline(inline) => {
                for (x, old_v) in inline.iter_mut() {
                    if x == &k2 {
                        return Some(mem::replace(old_v, v));
                    }
                }
                let k2v = match inline.try_push((k2, v)) {
                    Ok(()) => return None,
                    Err(cap_err) => cap_err.element(),
                };
                let iter = mem::take(inline).into_iter().chain(iter::once(k2v));
                let k1 = e.key();
                let mut heap = ExternalHeap::new(k1, &mut self.map, &mut self.values);
                for (k2, v) in iter {
                    heap.insert(k2, v);
                }
                heap.into_external().unwrap()
            }
            &mut RangeEntry::External(external) => {
                let mut heap =
                    ExternalHeap::from_external(e.key(), external, &mut self.map, &mut self.values);
                if let Some(old_v) = heap.insert(k2, v) {
                    return Some(old_v);
                }
                heap.into_external().unwrap()
            }
        };
        *e.into_mut() = RangeEntry::External(external);
        None
    }
    pub fn get(&self, k1: &K1, k2: &K2) -> Option<&V> {
        match self.ranges.get(k1)? {
            RangeEntry::Inline(inline) => inline.iter().find(|(x, _)| x == k2).map(|(_, v)| v),
            RangeEntry::External(_external) => {
                let &ind = self.map.get(&(k1.clone(), k2.clone()))?;
                Some(&self.values.get(ind).unwrap().value)
            }
        }
    }
    pub fn get_mut(&mut self, k1: &K1, k2: &K2) -> Option<&mut V> {
        match self.ranges.get_mut(k1)? {
            RangeEntry::Inline(inline) => inline.iter_mut().find(|(x, _)| x == k2).map(|(_, v)| v),
            RangeEntry::External(_external) => {
                let &ind = self.map.get(&(k1.clone(), k2.clone()))?;
                Some(&mut self.values.get_mut(ind).unwrap().value)
            }
        }
    }
    pub fn get_max(&self, k1: &K1) -> Option<(&K2, &V)> {
        match self.ranges.get(k1)? {
            RangeEntry::Inline(inline) => {
                let (k2, v) = inline
                    .iter()
                    .max_by(|(kl, _vl), (kr, _vr)| kl.cmp(kr))
                    .unwrap();
                Some((k2, v))
            }
            RangeEntry::External(external) => Some(self.values.get(external.i).unwrap().as_refs()),
        }
    }
    pub fn remove(&mut self, k1: &K1, k2: &K2) -> Option<V> {
        match self.ranges.get_mut(k1)? {
            RangeEntry::Inline(inline) => {
                for (i, (x, _)) in inline.iter().enumerate() {
                    if x == k2 {
                        let (_, removed) = inline.remove(i);
                        if inline.is_empty() {
                            self.ranges.remove(k1);
                        }
                        return Some(removed);
                    }
                }
                None
            }
            RangeEntry::External(external) => {
                let mut heap =
                    ExternalHeap::from_external(k1, *external, &mut self.map, &mut self.values);
                let removed = heap.remove(k2)?;
                match heap.into_external() {
                    Some(new_external) => *external = new_external,
                    None => {
                        self.ranges.remove(k1).unwrap();
                    }
                }
                Some(removed)
            }
        }
    }
}
