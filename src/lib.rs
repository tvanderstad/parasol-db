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

    /// Scan the view for events between the given sequences. Returns an double-ended iterator over the events. No work
    /// is done until the iterator is consumed.
    fn scan(&self, start_exclusive: Seq, end_inclusive: Seq) -> Self::Iterator<'_>;

    /// Returns the current sequence number of the view. All new events will have a sequence number greater than this.
    fn get_current_seq(&self) -> Seq;
}

// if you are a view, a reference to you is a view as well
impl<V: View> View for &V {
    type Event = V::Event;
    type Iterator<'iter> = V::Iterator<'iter> where Self: 'iter;

    fn scan(&self, start_exclusive: Seq, end_inclusive: Seq) -> Self::Iterator<'_> {
        (*self).scan(start_exclusive, end_inclusive)
    }

    fn get_current_seq(&self) -> Seq {
        (*self).get_current_seq()
    }
}

pub trait Table: View {
    /// Write the given events to the table. Returns the sequence numbers assigned, in order.
    fn append<Iter: IntoIterator<Item = Self::Event>>(&mut self, events: Iter) -> Vec<Seq>;

    /// Sets the current sequence number of the table unless its sequence number is already greater.
    fn set_current_seq(&mut self, seq: Seq);
}

pub trait Index {
    type Source: View;

    /// Incorporates all changes up to and including the given sequence number into the index.
    fn update(&mut self, source: &Self::Source, seq: Seq);

    /// Returns the sequence number for which all changes up to and including it have been incorporated into the index.
    fn get_current_seq(&self) -> Seq;
}
