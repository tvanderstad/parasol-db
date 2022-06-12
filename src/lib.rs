#![feature(generic_associated_types)]

pub mod base_log;
pub mod derived_log;

use std::iter::Iterator;

pub trait BaseLog {
    type Event;
    type Iterator<'a>: Iterator<Item = &'a Self::Event>
    where
        Self::Event: 'a,
        Self: 'a;
    fn iter<'a>(&'a self, min_seq_exclusive: u64, max_seq_inclusive: u64) -> Self::Iterator<'a>; // range scan (todo: backwards range scan)
    fn write(&mut self, event: Self::Event) -> u64; // returns sequence number for the write (todo: separate)
}

pub trait DerivedLog {
    type Derived;
    fn get_at_seq(&self, seq: u64) -> Self::Derived; // what is the value of the index at this seq?
}
