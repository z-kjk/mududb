use std::ops::Bound;

use mudu::common::result::RS;

use crate::index::btree::btree_index::BTreeIndex;
use crate::index::index_key::compare_context::CompareContext;
use crate::index::index_key::key_tuple::KeyTuple;

pub trait OrderedIndex<V> {
    fn new(context: CompareContext) -> Self
    where
        Self: Sized;

    fn len(&self) -> RS<usize>;

    fn is_empty(&self) -> RS<bool>;

    fn clear(&mut self) -> RS<()>
    where
        V: Clone;

    fn contains_key(&self, key: &KeyTuple) -> RS<bool>;

    fn get(&self, key: &KeyTuple) -> RS<Option<&V>>;

    fn get_key_value(&self, key: &KeyTuple) -> RS<Option<(&KeyTuple, &V)>>;

    fn first_key_value(&self) -> RS<Option<(&KeyTuple, &V)>>;

    fn last_key_value(&self) -> RS<Option<(&KeyTuple, &V)>>;

    fn insert(&mut self, key: KeyTuple, value: V) -> RS<Option<V>>
    where
        V: Clone;

    fn remove(&mut self, key: &KeyTuple) -> RS<Option<V>>
    where
        V: Clone;

    fn pop_first(&mut self) -> RS<Option<(KeyTuple, V)>>
    where
        V: Clone;

    fn pop_last(&mut self) -> RS<Option<(KeyTuple, V)>>
    where
        V: Clone;

    fn range(&self, bounds: (Bound<&KeyTuple>, Bound<&KeyTuple>)) -> RS<Vec<(&KeyTuple, &V)>>;
}

impl<V> OrderedIndex<V> for BTreeIndex<V> {
    fn new(context: CompareContext) -> Self
    where
        Self: Sized,
    {
        BTreeIndex::new(context)
    }

    fn len(&self) -> RS<usize> {
        BTreeIndex::len(self)
    }

    fn is_empty(&self) -> RS<bool> {
        BTreeIndex::is_empty(self)
    }

    fn clear(&mut self) -> RS<()>
    where
        V: Clone,
    {
        BTreeIndex::clear(self)
    }

    fn contains_key(&self, key: &KeyTuple) -> RS<bool> {
        BTreeIndex::contains_key(self, key)
    }

    fn get(&self, key: &KeyTuple) -> RS<Option<&V>> {
        BTreeIndex::get(self, key)
    }

    fn get_key_value(&self, key: &KeyTuple) -> RS<Option<(&KeyTuple, &V)>> {
        BTreeIndex::get_key_value(self, key)
    }

    fn first_key_value(&self) -> RS<Option<(&KeyTuple, &V)>> {
        BTreeIndex::first_key_value(self)
    }

    fn last_key_value(&self) -> RS<Option<(&KeyTuple, &V)>> {
        BTreeIndex::last_key_value(self)
    }

    fn insert(&mut self, key: KeyTuple, value: V) -> RS<Option<V>>
    where
        V: Clone,
    {
        BTreeIndex::insert(self, key, value)
    }

    fn remove(&mut self, key: &KeyTuple) -> RS<Option<V>>
    where
        V: Clone,
    {
        BTreeIndex::remove(self, key)
    }

    fn pop_first(&mut self) -> RS<Option<(KeyTuple, V)>>
    where
        V: Clone,
    {
        BTreeIndex::pop_first(self)
    }

    fn pop_last(&mut self) -> RS<Option<(KeyTuple, V)>>
    where
        V: Clone,
    {
        BTreeIndex::pop_last(self)
    }

    fn range(&self, bounds: (Bound<&KeyTuple>, Bound<&KeyTuple>)) -> RS<Vec<(&KeyTuple, &V)>> {
        BTreeIndex::range(self, bounds)
    }
}
