use crate::{table::vec::VecTable, Seq, View};

#[derive(Clone)]
pub struct CompositeView<V: View> {
    views: Vec<V>,
    vector_clock: Vec<Seq>,
}

impl<Event: Clone> CompositeView<VecTable<Event>> {
    pub fn new(n: usize) -> Self {
        Self { views: vec![VecTable::new(); n], vector_clock: vec![0; n] }
    }
}

impl<V: View> View for CompositeView<V>
where
    for<'a> V::Iterator<'a>: Clone,
{
    type Event = V::Event;
    type Iterator<'iter> = CompositeViewIterator<'iter, V> where V: 'iter;

    fn scan(&self, start: Seq, end: Seq) -> Self::Iterator<'_> {
        CompositeViewIterator::new(self, start, end)
    }

    fn current_seq(&self) -> Seq {
        // current seq for the purposes of reading is the minimum of sequences in the vector clock.
        // the entry for a vector clock is only updated by a transmission from that node, which is a promise not to
        // assign lower sequence numbers to writes, so that the events before the minimum sequence number are immutable
        self.vector_clock.iter().min().copied().unwrap_or_default()
    }
}

pub struct CompositeViewIterator<'iter, V: View + 'iter> {
    iterators: Vec<V::Iterator<'iter>>,
}

impl<'iter, V: View> CompositeViewIterator<'iter, V>
where
    V::Iterator<'iter>: Clone,
{
    fn new(view: &'iter CompositeView<V>, start: Seq, end: Seq) -> Self {
        // iterate each constituent view
        Self {
            iterators: view
                .views
                .iter()
                .map(|view| view.scan(start, end))
                .collect(),
        }
    }
}

impl<'iter, V: View + 'iter> Iterator for CompositeViewIterator<'iter, V>
where
    V::Iterator<'iter>: Clone,
{
    type Item = (Seq, &'iter V::Event);

    fn next(&mut self) -> Option<Self::Item> {
        let min_seq_idx = {
            // clone iterators
            let mut iterators = self.iterators.iter().cloned().collect::<Vec<_>>();

            // which iterator has the next event with the lowest sequence number?
            let mut min_seq = Seq::MAX;
            let mut min_seq_idx = None;
            for (idx, iter) in iterators.iter_mut().enumerate() {
                if let Some((seq, _)) = iter.next() {
                    // if there are multiple, prefer the lowest node index (break ties by node id)
                    if seq < min_seq {
                        min_seq = seq;
                        min_seq_idx = Some(idx);
                    }
                }
            }

            min_seq_idx
        };

        // advance the iterator with the lowest sequence number and return the result if there is one
        min_seq_idx.and_then(|idx| self.iterators[idx].next())
    }
}

impl<'iter, V: View> DoubleEndedIterator for CompositeViewIterator<'iter, V>
where
    V::Iterator<'iter>: Clone,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        let max_seq_idx = {
            // clone iterators
            let mut iterators = self.iterators.iter().cloned().collect::<Vec<_>>();

            // which iterator has the next event with the highest sequence number?
            let mut max_seq = Seq::MIN;
            let mut max_seq_idx = None;
            for (idx, iter) in iterators.iter_mut().enumerate() {
                if let Some((seq, _)) = iter.next_back() {
                    // if there are multiple, prefer the highest node index (break ties by node id)
                    if seq >= max_seq {
                        max_seq = seq;
                        max_seq_idx = Some(idx);
                    }
                }
            }

            max_seq_idx
        };

        // advance the iterator with the highest sequence number and return the result if there is one
        max_seq_idx.and_then(|idx| self.iterators[idx].next_back())
    }
}

#[cfg(test)]
mod tests {
    use super::CompositeView;
    use crate::table::vec::VecTable;
    use crate::{Seq, View};

    #[test]
    fn iter_none() {
        let composite = CompositeView::<VecTable<i32>>::new(5);
        assert_eq!(composite.current_seq(), 0);
        assert_eq!(
            composite
                .scan(Seq::MIN, Seq::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            Vec::<&i32>::new()
        );
    }
}
