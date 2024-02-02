use std::{collections::HashMap, hash::Hash, mem};

use arrayvec::ArrayVec;

use crate::op::{CommitId, RelationalOp};

pub(crate) struct TopNs<
    K: Clone + Eq + Hash,
    V: Clone + Ord + Hash,
    Op: RelationalOp<T = (K, V)>,
    const N: usize = 1,
> {
    relation: Op,
    tops: HashMap<K, ArrayVec<(V, i64), N>>,
    heaps: l2_heaps::L2Heaps<K, V, i64>,
}

impl<K: Clone + Eq + Hash, V: Clone + Ord + Hash, Op: RelationalOp<T = (K, V)>, const N: usize>
    TopNs<K, V, Op, N>
{
    pub fn new(relation: Op) -> Self {
        Self {
            relation,
            tops: HashMap::new(),
            heaps: l2_heaps::L2Heaps::new(),
        }
    }
}

impl<K: Clone + Eq + Hash, V: Clone + Ord + Hash, Op: RelationalOp<T = (K, V)>, const N: usize>
    RelationalOp for TopNs<K, V, Op, N>
{
    type T = (K, ArrayVec<V, N>);
    type Unconsolidated = Self;

    fn for_each(&mut self, commit_id: CommitId, mut f: impl FnMut((K, ArrayVec<V, N>), i64)) {
        self.relation.for_each(commit_id, |(k, v), count| {
            let mut entry = hashmap_tools::or_default(self.tops.entry(k.clone()));
            let vec = entry.get_mut();
            for (i, (x, cur_count)) in vec.iter_mut().enumerate() {
                if *x == v {
                    *cur_count += count;
                    if *cur_count == 0 {
                        f((k.clone(), output(vec)), -1);
                        vec.remove(i);
                        if let Some((new_back, &nb_count)) = self.heaps.get_max(&k) {
                            let new_back = new_back.clone();
                            self.heaps.remove(&k, &new_back);
                            vec.push((new_back, nb_count));
                        }
                        if vec.is_empty() {
                            entry.remove();
                        } else {
                            f((k, output(vec)), 1);
                        }
                    }
                    return;
                }
            }
            if vec.len() < N {
                if !vec.is_empty() {
                    f((k.clone(), output(vec)), -1);
                }
                vec.push((v.clone(), count));
                vec.sort_by(|(a, _), (b, _)| b.cmp(a));
                f((k, output(vec)), 1);
            } else {
                let mut to_insert = (v, count);
                let cur_back = vec.last().unwrap();
                if cur_back.0 < to_insert.0 {
                    f((k.clone(), output(vec)), -1);
                    let cur_back = vec.last_mut().unwrap();
                    mem::swap(cur_back, &mut to_insert);
                    vec.sort_by(|(a, _), (b, _)| b.cmp(a));
                    f((k.clone(), output(vec)), 1);
                }
                let (v, count) = to_insert;
                match self.heaps.get_mut(&k, &v) {
                    Some(current_count) => {
                        *current_count += count;
                        if *current_count == 0 {
                            self.heaps.remove(&k, &v);
                        }
                    }
                    None => {
                        self.heaps.insert(k.clone(), v.clone(), count);
                    }
                }
            }
        })
    }
    fn unconsolidate(self) -> Self::Unconsolidated {
        self
    }
}

fn output<V: Clone, const N: usize>(vec: &ArrayVec<(V, i64), N>) -> ArrayVec<V, N> {
    ArrayVec::from_iter(vec.iter().map(|(v, _)| v.clone()))
}
