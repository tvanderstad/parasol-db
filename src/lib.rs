pub mod source_log;
pub mod sink_log;
pub mod scheduler;

use std::iter::DoubleEndedIterator;

pub trait SourceLog {
    type Event<'a>
    where
        Self: 'a;
    type Iterator<'a>: DoubleEndedIterator<Item = Self::Event<'a>>
    where
        Self: 'a;
    fn scan(&self, min_seq_exclusive: u64, max_seq_inclusive: u64) -> Self::Iterator<'_>;
}

pub trait SinkLog {
    type AtSeq;
    fn seq(&self, seq: u64) -> Self::AtSeq;
}

pub trait StatefulSinkLog : SinkLog {
    fn update(&mut self, seq: u64);
    fn current_seq(&self) -> u64;
}