use either::Either;

use crate::{Seq, View};

impl<Event, L: View<Event = Event>, R: View<Event = Event>> View for Either<L, R> {
    type Event = Event;
    type Iterator = EitherViewIterator<Event, L, R>;

    fn scan(&mut self, start_exclusive: Seq, end_inclusive: Seq) -> Self::Iterator {
        match self {
            Either::Left(left) => {
                EitherViewIterator::Left(left.scan(start_exclusive, end_inclusive))
            }
            Either::Right(right) => {
                EitherViewIterator::Right(right.scan(start_exclusive, end_inclusive))
            }
        }
    }

    fn get_current_seq(&mut self) -> Seq {
        match self {
            Either::Left(left) => left.get_current_seq(),
            Either::Right(right) => right.get_current_seq(),
        }
    }
}

#[derive(Clone)]
pub enum EitherViewIterator<Event, L: View<Event = Event>, R: View<Event = Event>> {
    Left(L::Iterator),
    Right(R::Iterator),
}

impl<Event, L: View<Event = Event>, R: View<Event = Event>> Iterator
    for EitherViewIterator<Event, L, R>
{
    type Item = (Seq, Event);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            EitherViewIterator::Left(left) => left.next().map(|(seq, event)| (seq, event)),
            EitherViewIterator::Right(right) => right.next().map(|(seq, event)| (seq, event)),
        }
    }
}

impl<Event, L: View<Event = Event>, R: View<Event = Event>> DoubleEndedIterator
    for EitherViewIterator<Event, L, R>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            EitherViewIterator::Left(left) => left.next_back().map(|(seq, event)| (seq, event)),
            EitherViewIterator::Right(right) => right.next_back().map(|(seq, event)| (seq, event)),
        }
    }
}
