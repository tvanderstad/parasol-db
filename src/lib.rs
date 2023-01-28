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

pub trait DestLog: Sized {
    fn update(&mut self, seq: u64);
    fn current_seq(&self) -> u64;
}

pub trait Scheduler: WritableSourceLog {}

pub mod log_list {
    use crate::{DestLog, SourceLog};

    pub trait DestListNode: Sized {
        fn update_all(&mut self, seq: u64);
        fn append<Next: DestLog>(self, next: Next) -> IntermediateDestListNode<Self, Next> {
            IntermediateDestListNode {
                prev: self,
                curr: next,
            }
        }
    }

    impl DestListNode for () {
        fn update_all(&mut self, _: u64) {}
    }

    pub struct IntermediateDestListNode<Prev: DestListNode, Curr: DestLog> {
        prev: Prev,
        curr: Curr,
    }

    impl<Prev: DestListNode, Curr: DestLog> DestListNode for IntermediateDestListNode<Prev, Curr> {
        fn update_all(&mut self, seq: u64) {
            self.prev.update_all(seq);
            self.curr.update(seq);
        }
    }

    pub trait SourceListNode: Sized {
        fn update_all(&mut self, seq: u64);
        fn append<Next: SourceLog>(self, next: Next) -> IntermediateSourceListNode<Self, Next> {
            IntermediateSourceListNode {
                prev: self,
                curr: next,
            }
        }
    }

    impl SourceListNode for () {
        fn update_all(&mut self, _: u64) {}
    }

    pub struct IntermediateSourceListNode<Prev: SourceListNode, Curr: SourceLog> {
        prev: Prev,
        curr: Curr,
    }
}
