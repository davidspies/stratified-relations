use std::{collections::HashMap, hash::Hash, mem, ptr};

use index_list::{Index, IndexList};

use crate::ExternalEntry;

pub(crate) struct ExternalHeap<'a, K1: Clone + Eq + Hash, K2: Clone + Ord + Hash, V> {
    k1: &'a K1,
    external: Option<ExternalEntry>,
    map: &'a mut HashMap<(K1, K2), Index>,
    values: &'a mut IndexList<HeapEntry<K2, V>>,
}
impl<'a, K1: Clone + Eq + Hash, K2: Clone + Ord + Hash, V> ExternalHeap<'a, K1, K2, V> {
    pub(crate) fn new(
        k1: &'a K1,
        map: &'a mut HashMap<(K1, K2), Index>,
        values: &'a mut IndexList<HeapEntry<K2, V>>,
    ) -> Self {
        Self {
            k1,
            external: None,
            map,
            values,
        }
    }

    pub(crate) fn from_external(
        k1: &'a K1,
        external: ExternalEntry,
        map: &'a mut HashMap<(K1, K2), Index>,
        values: &'a mut IndexList<HeapEntry<K2, V>>,
    ) -> Self {
        Self {
            k1,
            external: Some(external),
            map,
            values,
        }
    }

    pub(crate) fn insert(&mut self, k2: K2, v: V) -> Option<V> {
        if let Some(&ind) = self.map.get(&(self.k1.clone(), k2.clone())) {
            let entry = self.values.get_mut(ind).unwrap();
            return Some(mem::replace(&mut entry.value, v));
        }
        let mut ind = self.push_back_entry(k2.clone(), v);
        while let Some(parent) = self.bubble_up(ind) {
            ind = parent;
        }
        self.map.insert((self.k1.clone(), k2), ind);
        None
    }

    fn push_back_entry(&mut self, k2: K2, v: V) -> Index {
        let Some(external) = &mut self.external else {
            let i = self.values.insert_last(HeapEntry::initial(k2, v));
            self.external = Some(ExternalEntry { i, j: i });
            return i;
        };
        let prev_last_ind = external.j;
        let new_last_ind = self
            .values
            .insert_after(prev_last_ind, HeapEntry::initial(k2, v));
        external.j = new_last_ind;
        let prev_parent_ind = self.values.get(prev_last_ind).unwrap().parent;
        let (parent_ind, parent_child) = if prev_parent_ind.is_none() {
            assert_eq!(prev_last_ind, external.i);
            let new_parent = self.values.get_mut(prev_last_ind).unwrap();
            (prev_last_ind, &mut new_parent.left_child)
        } else {
            let prev_parent = self.values.get_mut(prev_parent_ind).unwrap();
            if prev_parent.right_child.is_none() {
                (prev_parent_ind, &mut prev_parent.right_child)
            } else {
                let new_parent_ind = self.values.next_index(prev_parent_ind);
                let new_parent = self.values.get_mut(new_parent_ind).unwrap();
                (new_parent_ind, &mut new_parent.left_child)
            }
        };
        *parent_child = new_last_ind;
        self.values.get_mut(new_last_ind).unwrap().parent = parent_ind;
        new_last_ind
    }

    pub(crate) fn remove(&mut self, k2: &K2) -> Option<V> {
        let mut ind = self.map.remove(&(self.k1.clone(), k2.clone()))?;
        let ExternalEntry { i, j } = self.external.as_mut().unwrap();
        let last_ind = *j;
        if *i == last_ind {
            self.external = None;
        } else {
            *j = self.values.prev_index(last_ind);
        }
        let mut removed = self.values.remove(last_ind).unwrap();
        let parent_ind = removed.parent;
        if !parent_ind.is_none() {
            let parent = self.values.get_mut(parent_ind).unwrap();
            if parent.left_child == last_ind {
                parent.left_child = Index::new();
            } else {
                assert_eq!(parent.right_child, last_ind);
                parent.right_child = Index::new();
            }
        }
        if ind == last_ind {
            return Some(removed.value);
        }
        let place = self.values.get_mut(ind).unwrap();
        mem::swap(&mut place.k2, &mut removed.k2);
        mem::swap(&mut place.value, &mut removed.value);
        while let Some(parent) = self.bubble_up(ind) {
            ind = parent;
        }
        while let Some(child) = self.sink_down(ind) {
            ind = child;
        }
        let k2 = self.values.get(ind).unwrap().k2.clone();
        self.map.insert((self.k1.clone(), k2), ind);
        Some(removed.value)
    }

    pub(crate) fn into_external(self) -> Option<ExternalEntry> {
        self.external
    }

    fn bubble_up(&mut self, ind: Index) -> Option<Index> {
        let entry = self.values.get(ind).unwrap();
        let parent_ind = entry.parent;
        if parent_ind.is_none() {
            return None;
        }
        let parent = self.values.get(parent_ind).unwrap();
        if entry.k2 <= parent.k2 {
            return None;
        }
        self.map.insert((self.k1.clone(), parent.k2.clone()), ind);
        self.swap_entry_values(ind, parent_ind);
        Some(parent_ind)
    }

    fn sink_down(&mut self, ind: Index) -> Option<Index> {
        let entry = self.values.get(ind).unwrap();
        let left_child_ind = entry.left_child;
        let right_child_ind = entry.right_child;
        if left_child_ind.is_none() {
            return None;
        }
        let (chosen_child_ind, chosen_child) = if right_child_ind.is_none() {
            (left_child_ind, self.values.get(left_child_ind).unwrap())
        } else {
            let left_child = self.values.get(left_child_ind).unwrap();
            let right_child = self.values.get(right_child_ind).unwrap();
            if left_child.k2 >= right_child.k2 {
                (left_child_ind, left_child)
            } else {
                (right_child_ind, right_child)
            }
        };
        if entry.k2 >= chosen_child.k2 {
            return None;
        }
        self.map
            .insert((self.k1.clone(), chosen_child.k2.clone()), ind);
        self.swap_entry_values(ind, chosen_child_ind);
        Some(chosen_child_ind)
    }

    fn swap_entry_values(&mut self, l_ind: Index, r_ind: Index) {
        let l = self.values.get_mut(l_ind).unwrap();
        let (l_ptr_k2, l_ptr_value) = (&mut l.k2 as *mut K2, &mut l.value as *mut V);
        let r = self.values.get_mut(r_ind).unwrap();
        let (r_ptr_k2, r_ptr_value) = (&mut r.k2 as *mut K2, &mut r.value as *mut V);
        unsafe {
            ptr::swap(l_ptr_k2, r_ptr_k2);
            ptr::swap(l_ptr_value, r_ptr_value)
        }
    }
}

pub(crate) struct HeapEntry<K2, V> {
    parent: Index,
    left_child: Index,
    right_child: Index,
    k2: K2,
    pub(crate) value: V,
}

impl<K2, V> HeapEntry<K2, V> {
    pub(crate) fn initial(k2: K2, value: V) -> Self {
        Self {
            parent: Index::new(),
            left_child: Index::new(),
            right_child: Index::new(),
            k2,
            value,
        }
    }

    pub(crate) fn as_refs(&self) -> (&K2, &V) {
        (&self.k2, &self.value)
    }
}
