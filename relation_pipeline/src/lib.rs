use std::hash::{Hash, Hasher};
use std::{cmp::Reverse, collections::hash_map::DefaultHasher, convert::identity, iter, rc::Rc};

use arrayvec::ArrayVec;
use either::Either;

use self::ops::Dynamic;

pub use self::{
    context::{CreationContext, ExecutionContext},
    input::Input,
    op::RelationalOp,
    ops::Save,
    output::Output,
    relation::Relation,
};

mod context;
mod input;
mod op;
mod output;
mod relation;

pub mod ops;

pub type InputRelation<T> = Relation<T, self::ops::InputOp<T>>;

impl<T, Op: RelationalOp<T = T>> Relation<T, Op> {
    pub fn cartesian_product<U: Clone + Eq + Hash>(
        self,
        other: Relation<U, impl RelationalOp<T = U>>,
    ) -> Relation<(T, U), impl RelationalOp<T = (T, U)>>
    where
        T: Clone + Eq + Hash,
    {
        self.map_h_(|t| ((), t))
            .join(other.map_h_(|u| ((), u)))
            .map_h_(|((), (t, u))| (t, u))
    }
    pub fn union_(
        self,
        other: Relation<T, impl RelationalOp<T = T>>,
    ) -> Relation<T, impl RelationalOp<T = T>> {
        assert!(Rc::ptr_eq(
            &self.current_commit_id,
            &other.current_commit_id
        ));
        Relation::new(
            ops::Concat::new(self.relation, other.relation),
            self.current_commit_id,
        )
    }
    pub fn union<'a>(
        self,
        other: Relation<T, impl RelationalOp<T = T> + 'a>,
    ) -> Relation<T, impl RelationalOp<T = T> + 'a>
    where
        T: Clone + Eq + Hash + 'a,
        Op: 'a,
    {
        self.union_(other).consolidate().distinct_h()
    }
    pub fn counts(self) -> Relation<(T, i64), impl RelationalOp<T = (T, i64)>>
    where
        T: Clone + Eq + Hash,
    {
        Relation::new(ops::Counts::new(self.relation), self.current_commit_id)
    }
    pub fn distinct(self) -> Relation<T, impl RelationalOp<T = T>>
    where
        T: Clone + Eq + Hash,
    {
        Relation::new(ops::Distinct::new(self.relation), self.current_commit_id)
    }
    pub fn distinct_h(self) -> Relation<T, impl RelationalOp<T = T>>
    where
        T: Clone + Eq + Hash,
    {
        Relation::new(ops::Distinct::new(self.relation.op), self.current_commit_id)
    }
    pub fn dynamic<'a>(self) -> Relation<T, Dynamic<'a, T>>
    where
        Op: 'a,
    {
        Relation::new(Dynamic::new(self.relation.op), self.current_commit_id)
    }
    pub fn filter(self, mut f: impl FnMut(&T) -> bool) -> Relation<T, impl RelationalOp<T = T>> {
        self.flat_map_(move |t| f(&t).then_some(t))
    }
    pub fn flat_map_h_<U, R: IntoIterator<Item = U>>(
        self,
        f: impl FnMut(T) -> R,
    ) -> Relation<U, impl RelationalOp<T = U>> {
        Relation::new(
            ops::FlatMap::new(self.relation.op, f),
            self.current_commit_id,
        )
    }
    pub fn flat_map_h<'a, U: Clone + Eq + Hash + 'a, R: IntoIterator<Item = U> + 'a>(
        self,
        f: impl FnMut(T) -> R + 'a,
    ) -> Relation<U, impl RelationalOp<T = U> + 'a>
    where
        T: 'a,
        Op: 'a,
    {
        self.flat_map_h_(f).consolidate().distinct_h()
    }
    pub fn flat_map_<U, R: IntoIterator<Item = U>>(
        self,
        f: impl FnMut(T) -> R,
    ) -> Relation<U, impl RelationalOp<T = U>> {
        Relation::new(ops::FlatMap::new(self.relation, f), self.current_commit_id)
    }
    pub fn flat_map<'a, U: Clone + Eq + Hash + 'a, R: IntoIterator<Item = U> + 'a>(
        self,
        f: impl FnMut(T) -> R + 'a,
    ) -> Relation<U, impl RelationalOp<T = U> + 'a>
    where
        T: 'a,
        Op: 'a,
    {
        self.flat_map_(f).consolidate().distinct_h()
    }
    pub fn flatten_h_<U>(self) -> Relation<U, impl RelationalOp<T = U>>
    where
        T: IntoIterator<Item = U>,
    {
        self.flat_map_h_(identity)
    }
    pub fn flatten_h<'a, U: Clone + Eq + Hash + 'a>(
        self,
    ) -> Relation<U, impl RelationalOp<T = U> + 'a>
    where
        T: IntoIterator<Item = U> + 'a,
        Op: 'a,
    {
        self.flat_map_h(identity)
    }
    pub fn flatten_<U>(self) -> Relation<U, impl RelationalOp<T = U>>
    where
        T: IntoIterator<Item = U>,
    {
        self.flat_map_(identity)
    }
    pub fn flatten<'a, U: Clone + Eq + Hash + 'a>(
        self,
    ) -> Relation<U, impl RelationalOp<T = U> + 'a>
    where
        T: IntoIterator<Item = U> + 'a,
        Op: 'a,
    {
        self.flat_map(identity)
    }
    pub fn global_max(self) -> Relation<T, impl RelationalOp<T = T>>
    where
        T: Clone + Ord + Hash,
    {
        self.map_h_(|t| ((), t)).maxes().map_h_(|((), t)| t)
    }
    pub fn global_min(self) -> Relation<T, impl RelationalOp<T = T>>
    where
        T: Clone + Ord + Hash,
    {
        self.map_h_(|t| ((), t)).mins().map_h_(|((), t)| t)
    }
    pub fn intersection(
        self,
        other: Relation<T, impl RelationalOp<T = T>>,
    ) -> Relation<T, impl RelationalOp<T = T>>
    where
        T: Clone + Eq + Hash,
    {
        self.map_h_(|t| (t, ()))
            .join(other.map_h_(|t| (t, ())))
            .map_h_(|(t, ((), ()))| t)
    }
    pub fn map_<U>(self, mut f: impl FnMut(T) -> U) -> Relation<U, impl RelationalOp<T = U>> {
        self.flat_map_(move |t| iter::once(f(t)))
    }
    pub fn map<'a, U: Clone + Eq + Hash + 'a>(
        self,
        mut f: impl FnMut(T) -> U + 'a,
    ) -> Relation<U, impl RelationalOp<T = U> + 'a>
    where
        T: 'a,
        Op: 'a,
    {
        self.flat_map(move |t| iter::once(f(t)))
    }
    pub fn map_h_<U>(self, mut f: impl FnMut(T) -> U) -> Relation<U, impl RelationalOp<T = U>> {
        self.flat_map_h_(move |t| iter::once(f(t)))
    }
    pub fn map_h<'a, U: Clone + Eq + Hash + 'a>(
        self,
        mut f: impl FnMut(T) -> U + 'a,
    ) -> Relation<U, impl RelationalOp<T = U> + 'a>
    where
        T: 'a,
        Op: 'a,
    {
        self.flat_map_h(move |t| iter::once(f(t)))
    }
    pub fn set_minus(
        self,
        other: Relation<T, impl RelationalOp<T = T>>,
    ) -> Relation<T, impl RelationalOp<T = T>>
    where
        T: Clone + Eq + Hash,
    {
        self.map_h_(|t| (t, ())).antijoin(other).map_h_(|(t, ())| t)
    }
    pub fn save(self) -> Save<T, Op>
    where
        T: Clone + Eq + Hash,
    {
        Save::new(self.relation.op, self.current_commit_id)
    }
    pub fn collect(self) -> Save<T>
    where
        T: Clone + Eq + Hash,
        Op: 'static,
    {
        self.dynamic().save()
    }
}

impl<K, V, Op: RelationalOp<T = (K, V)>> Relation<(K, V), Op> {
    pub fn antijoin(
        self,
        other: Relation<K, impl RelationalOp<T = K>>,
    ) -> Relation<(K, V), impl RelationalOp<T = (K, V)>>
    where
        K: Clone + Eq + Hash,
        V: Clone + Eq + Hash,
    {
        assert!(Rc::ptr_eq(
            &self.current_commit_id,
            &other.current_commit_id
        ));
        Relation::new(
            ops::Antijoin::new(self.relation, other.relation),
            self.current_commit_id,
        )
    }
    #[allow(clippy::type_complexity)]
    pub fn join<V2>(
        self,
        other: Relation<(K, V2), impl RelationalOp<T = (K, V2)>>,
    ) -> Relation<(K, (V, V2)), impl RelationalOp<T = (K, (V, V2))>>
    where
        K: Clone + Eq + Hash,
        V: Clone + Eq + Hash,
        V2: Clone + Eq + Hash,
    {
        assert!(Rc::ptr_eq(
            &self.current_commit_id,
            &other.current_commit_id
        ));
        Relation::new(
            ops::Join::new(self.relation, other.relation),
            self.current_commit_id,
        )
    }
    pub fn join_values_<V2>(
        self,
        other: Relation<(K, V2), impl RelationalOp<T = (K, V2)>>,
    ) -> Relation<(V, V2), impl RelationalOp<T = (V, V2)>>
    where
        K: Clone + Eq + Hash,
        V: Clone + Eq + Hash,
        V2: Clone + Eq + Hash,
    {
        self.join(other).snds_()
    }
    pub fn join_values<'a, V2>(
        self,
        other: Relation<(K, V2), impl RelationalOp<T = (K, V2)> + 'a>,
    ) -> Relation<(V, V2), impl RelationalOp<T = (V, V2)> + 'a>
    where
        K: Clone + Eq + Hash + 'a,
        V: Clone + Eq + Hash + 'a,
        V2: Clone + Eq + Hash + 'a,
        Op: 'a,
    {
        self.join(other).snds()
    }
    pub fn semijoin(
        self,
        other: Relation<K, impl RelationalOp<T = K>>,
    ) -> Relation<(K, V), impl RelationalOp<T = (K, V)>>
    where
        K: Clone + Eq + Hash,
        V: Clone + Eq + Hash,
    {
        self.join(other.map_h_(|k| (k, ())))
            .map_h_(|(k, (v, ()))| (k, v))
    }
    pub fn fsts_(self) -> Relation<K, impl RelationalOp<T = K>> {
        self.map_h_(|(k, _)| k)
    }
    pub fn fsts<'a>(self) -> Relation<K, impl RelationalOp<T = K> + 'a>
    where
        K: Clone + Eq + Hash + 'a,
        V: 'a,
        Op: 'a,
    {
        self.fsts_().consolidate().distinct_h()
    }
    pub fn snds_(self) -> Relation<V, impl RelationalOp<T = V>> {
        self.map_h_(|(_, v)| v)
    }
    pub fn snds<'a>(self) -> Relation<V, impl RelationalOp<T = V> + 'a>
    where
        K: 'a,
        V: Clone + Eq + Hash + 'a,
        Op: 'a,
    {
        self.snds_().consolidate().distinct_h()
    }
    pub fn top_ns<const N: usize>(
        self,
    ) -> Relation<(K, ArrayVec<V, N>), impl RelationalOp<T = (K, ArrayVec<V, N>)>>
    where
        K: Clone + Eq + Hash,
        V: Clone + Ord + Hash,
    {
        Relation::new(ops::TopNs::new(self.relation), self.current_commit_id)
    }
    pub fn random_ns<const N: usize>(
        self,
        seed: u64,
    ) -> Relation<(K, ArrayVec<V, N>), impl RelationalOp<T = (K, ArrayVec<V, N>)>>
    where
        K: Clone + Eq + Hash,
        V: Clone + Ord + Hash,
    {
        self.map_h_(move |x| {
            let mut hasher = DefaultHasher::new();
            hasher.write_u64(seed);
            x.hash(&mut hasher);
            let (k, v) = x;
            (k, (hasher.finish(), v))
        })
        .top_ns::<N>()
        .map_h_(|(k, arr)| (k, arr.into_iter().map(|(_, v)| v).collect()))
    }
    pub fn maxes(self) -> Relation<(K, V), impl RelationalOp<T = (K, V)>>
    where
        K: Clone + Eq + Hash,
        V: Clone + Ord + Hash,
    {
        self.top_ns::<1>()
            .map_h_(|(k, v)| (k, v.into_iter().next().unwrap()))
    }
    pub fn mins(self) -> Relation<(K, V), impl RelationalOp<T = (K, V)>>
    where
        K: Clone + Eq + Hash,
        V: Clone + Ord + Hash,
    {
        self.map_h_(|(k, v)| (k, Reverse(v)))
            .maxes()
            .map_h_(|(k, Reverse(v))| (k, v))
    }
    pub fn split_(
        self,
    ) -> (
        Relation<K, impl RelationalOp<T = K>>,
        Relation<V, impl RelationalOp<T = V>>,
    ) {
        let (left, right) = ops::split(self.relation);
        (
            Relation::new(left, Rc::clone(&self.current_commit_id)),
            Relation::new(right, self.current_commit_id),
        )
    }
    pub fn split<'a>(
        self,
    ) -> (
        Relation<K, impl RelationalOp<T = K> + 'a>,
        Relation<V, impl RelationalOp<T = V> + 'a>,
    )
    where
        K: Clone + Eq + Hash + 'a,
        V: Clone + Eq + Hash + 'a,
        Op: 'a,
    {
        let (l, r) = self.split_();
        (l.consolidate().distinct_h(), r.consolidate().distinct_h())
    }
    pub fn swaps(self) -> Relation<(V, K), impl RelationalOp<T = (V, K)>> {
        self.map_h_(|(k, v)| (v, k))
    }
}

impl<L, R, Op: RelationalOp<T = Either<L, R>>> Relation<Either<L, R>, Op> {
    pub fn partition(
        self,
    ) -> (
        Relation<L, impl RelationalOp<T = L>>,
        Relation<R, impl RelationalOp<T = R>>,
    ) {
        let (l, r) = self
            .map_h_(|x| match x {
                Either::Left(l) => (Some(l), None),
                Either::Right(r) => (None, Some(r)),
            })
            .split_();
        (l.flatten_h_(), r.flatten_h_())
    }
}
