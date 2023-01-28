pub mod dest_log;
pub mod scheduler;
pub mod source_log;

use std::iter::DoubleEndedIterator;

pub trait SourceLog {
    type Event;
    type Iterator<'iter>: DoubleEndedIterator<Item = &'iter Self::Event>
    where
        Self: 'iter;
    fn scan(&self, min_seq_exclusive: u64, max_seq_inclusive: u64) -> Self::Iterator<'_>;
    fn current_seq(&self) -> u64;
}

pub trait WritableSourceLog: SourceLog {
    fn write<Iter: IntoIterator<Item = Self::Event>>(&mut self, events: Iter);
}

pub trait DestLog {
    fn update(&mut self, seq: u64);
    fn current_seq(&self) -> u64;
}
