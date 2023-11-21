pub mod index;
pub mod table;
pub mod view;

use std::iter::DoubleEndedIterator;

pub type Seq = u64;

pub trait View {
    type Event;
    type Iterator: DoubleEndedIterator<Item = (Seq, Self::Event)>;

    /// Scan the view for events between the given sequences. Returns an double-ended iterator over the events. No work
    /// is done until the iterator is consumed.
    fn scan(&mut self, start_exclusive: Seq, end_inclusive: Seq) -> Self::Iterator;

    /// Returns the current sequence number of the view. All new events will have a sequence number greater than this.
    fn get_current_seq(&mut self) -> Seq;
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
    fn update(&mut self, source: &mut Self::Source, seq: Seq);

    /// Returns the sequence number for which all changes up to and including it have been incorporated into the index.
    fn get_current_seq(&self) -> Seq;
}
