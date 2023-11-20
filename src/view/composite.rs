use crate::{Seq, Table, View};

#[derive(Clone)]
pub struct CompositeView<V: View> {
    logs: Vec<V>,
    vector_clock: Vec<Seq>,
}

impl<V: View> CompositeView<V> {
    pub fn new(n: usize) -> Self {
        todo!()
    }
}

impl<V: View> View for CompositeView<V> {
    type Event = V::Event;
    type Iterator<'iter> = CompositeViewIterator<'iter, V> where V: 'iter;

    fn scan(&self, start: Seq, end: Seq) -> Self::Iterator<'_> {
        todo!()
    }

    fn current_seq(&self) -> Seq {
        todo!()
    }
}

pub struct CompositeViewIterator<'iter, V: View> {
    view: &'iter CompositeView<V>,
    reverse: bool,
}

impl<'iter, V: View> CompositeViewIterator<'iter, V> {
    fn new(
        view: &'iter CompositeView<V>, reverse: bool, min_seq_exclusive: Seq,
        max_seq_inclusive: Seq,
    ) -> Self {
        todo!()
    }

    fn next(&mut self) -> Option<(Seq, &'iter V::Event)> {
        todo!()
    }

    fn next_back(&mut self) -> Option<(Seq, &'iter V::Event)> {
        todo!()
    }
}

impl<'iter, V: View> Iterator for CompositeViewIterator<'iter, V> {
    type Item = (Seq, &'iter V::Event);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.reverse {
            CompositeViewIterator::<'iter, V>::next(self)
        } else {
            CompositeViewIterator::<'iter, V>::next_back(self)
        }
    }
}

impl<'iter, V: View> DoubleEndedIterator for CompositeViewIterator<'iter, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if !self.reverse {
            CompositeViewIterator::<'iter, V>::next_back(self)
        } else {
            CompositeViewIterator::<'iter, V>::next(self)
        }
    }
}
