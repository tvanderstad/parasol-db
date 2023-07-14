#![feature(never_type)]
#![feature(associated_type_defaults)]

pub mod database;
pub mod dest_log;
pub mod source_log;

use std::iter::DoubleEndedIterator;

type Seq = u64;

pub trait View {
    type Event;
    type Iterator<'iter>: DoubleEndedIterator<Item = (Seq, &'iter Self::Event)>
    where
        Self: 'iter;
    fn scan(&self, start_inclusive: Seq, end_exclusive: Seq) -> Self::Iterator<'_>;
    fn next_seq(&self) -> Seq;
}

pub trait Table: View {
    fn write<Iter: IntoIterator<Item = Self::Event>>(&mut self, events: Iter);
}

pub trait Index: Sync {
    fn update(&mut self, seq: Seq);
    fn current_seq(&self) -> Seq;
}
