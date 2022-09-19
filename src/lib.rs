pub mod base_log;
pub mod derived_log;

use std::iter::DoubleEndedIterator;

pub trait BaseLog {
    type Event;
    type Iterator<'a>: DoubleEndedIterator<Item = &'a Self::Event>
    where
        Self::Event: 'a,
        Self: 'a;
    fn iter(&self, min_seq_exclusive: u64, max_seq_inclusive: u64) -> Self::Iterator<'_>;
}

pub trait DerivedLog {
    type Derived;
    fn get_at_seq(&self, seq: u64) -> Self::Derived;
}
