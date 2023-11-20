#![feature(never_type)]
#![feature(associated_type_defaults)]

pub mod index;
pub mod table;
pub mod view;

use std::iter::DoubleEndedIterator;

type Seq = u64;

pub trait View {
    type Event;
    type Iterator<'iter>: DoubleEndedIterator<Item = (Seq, &'iter Self::Event)>
    where
        Self: 'iter;
    fn scan(&self, start_exclusive: Seq, end_inclusive: Seq) -> Self::Iterator<'_>;
    fn current_seq(&self) -> Seq;
}

// if you are a view, a reference to you is a view as well
impl<V: View> View for &V {
    type Event = V::Event;
    type Iterator<'iter> = V::Iterator<'iter> where Self: 'iter;

    fn scan(&self, start_exclusive: Seq, end_inclusive: Seq) -> Self::Iterator<'_> {
        (*self).scan(start_exclusive, end_inclusive)
    }

    fn current_seq(&self) -> Seq {
        (*self).current_seq()
    }
}

pub trait Table: View {
    fn write<Iter: IntoIterator<Item = Self::Event>>(&mut self, events: Iter);
}

pub trait Index: Sync {
    fn update(&mut self, seq: Seq);
    fn current_seq(&self) -> Seq;
}
