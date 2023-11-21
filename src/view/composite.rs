use crate::{Seq, View};

#[derive(Clone)]
pub struct CompositeView<V: View> {
    views: Vec<V>,
    vector_clock: Vec<Seq>,
}

impl<V: View> CompositeView<V> {
    pub fn new(views: Vec<V>) -> Self {
        let vector_clock = vec![0; views.len()];
        Self { views, vector_clock }
    }

    pub fn vector_clock_update(&mut self, node_id: usize, seq: Seq) {
        self.vector_clock[node_id] = seq;
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

    fn get_current_seq(&self) -> Seq {
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
    use crate::{Seq, Table, View};

    #[test]
    fn scan_none() {
        let composite = CompositeView::<VecTable<i32>>::new(vec![VecTable::new(); 5]);
        assert_eq!(composite.get_current_seq(), 0);
        assert_eq!(
            composite
                .scan(Seq::MIN, Seq::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            Vec::<&i32>::new()
        );
    }

    #[test]
    fn scan_one() {
        let mut composite = CompositeView::<VecTable<i32>>::new(vec![VecTable::new(); 5]);

        composite.views[0].append([12]);

        assert_eq!(composite.get_current_seq(), 0);
        assert_eq!(
            composite
                .scan(Seq::MIN, Seq::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&12]
        );
    }

    #[test]
    fn scan_multiple_one_node() {
        let mut composite = CompositeView::<VecTable<i32>>::new(vec![VecTable::new(); 5]);

        composite.views[0].append([12, 34, 56]);

        assert_eq!(composite.get_current_seq(), 0);
        assert_eq!(
            composite
                .scan(Seq::MIN, Seq::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&12, &34, &56]
        );
    }

    #[test]
    fn scan_multiple_multiple_nodes() {
        let mut composite = CompositeView::<VecTable<i32>>::new(vec![VecTable::new(); 5]);

        composite.views[0].append([12]);
        composite.views[1].append([34]);
        composite.views[2].append([56]);

        assert_eq!(composite.get_current_seq(), 0);
        assert_eq!(
            composite
                .scan(Seq::MIN, Seq::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&12, &34, &56]
        );
    }

    #[test]
    fn scan_multiple_each_multiple_nodes() {
        let mut composite = CompositeView::<VecTable<i32>>::new(vec![VecTable::new(); 5]);

        composite.views[0].append([12, 56]);
        composite.views[1].append([34, 90]);
        composite.views[2].append([78]);

        assert_eq!(composite.get_current_seq(), 0);
        assert_eq!(
            composite
                .scan(Seq::MIN, Seq::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&12, &34, &78, &56, &90] // ordered by (seq, node) pair
        );
    }

    #[test]
    fn scan_multiple_each_multiple_nodes_sparse_seqs() {
        let mut composite = CompositeView::<VecTable<i32>>::new(vec![VecTable::new(); 5]);

        // unrealistic/heavy-handed way to specify all sequence numbers
        composite.views[0].set_current_seq(0);
        composite.views[0].append([12]);
        composite.views[1].set_current_seq(1);
        composite.views[1].append([34]);
        composite.views[0].set_current_seq(2);
        composite.views[0].append([56]);
        composite.views[2].set_current_seq(3);
        composite.views[2].append([78]);
        composite.views[1].set_current_seq(4);
        composite.views[1].append([90]);

        assert_eq!(composite.get_current_seq(), 0);
        assert_eq!(
            composite
                .scan(Seq::MIN, Seq::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&12, &34, &56, &78, &90] // nodes don't matter in this case because seqs are unique
        );
    }
}
