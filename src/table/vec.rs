use crate::{Seq, Table, View};

#[derive(Clone)]
pub struct VecTable<Event> {
    pub seqs: Vec<Seq>,
    pub events: Vec<Event>,
}

impl<Event: Clone> VecTable<Event> {
    pub fn new() -> Self {
        VecTable { seqs: Vec::new(), events: Vec::new() }
    }
}

impl<Event: Clone> Default for VecTable<Event> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Event: Clone> View for VecTable<Event> {
    type Event = Event;
    type Iterator<'iter> = VecTableIterator<'iter, Event> where Event: 'iter;

    fn scan(&self, start: Seq, end: Seq) -> Self::Iterator<'_> {
        let reverse = start > end;
        let (min, max) = if reverse { (end, start) } else { (start, end) };
        VecTableIterator::new(self, reverse, min, max)
    }

    fn current_seq(&self) -> Seq {
        self.seqs.iter().max().copied().unwrap_or_default() as Seq
    }
}

impl<Event: Clone> Table for VecTable<Event> {
    fn write<Iter: IntoIterator<Item = Self::Event>>(&mut self, events: Iter) {
        for event in events.into_iter() {
            self.seqs.push(self.current_seq() + 1);
            self.events.push(event);
        }
    }
}

#[derive(Clone)]
pub struct VecTableIterator<'iter, Event> {
    table: &'iter VecTable<Event>,
    reverse: bool,
    min_idx_inclusive: usize,
    max_idx_exclusive: usize,
}

impl<'iter, Event> VecTableIterator<'iter, Event> {
    fn new(
        table: &'iter VecTable<Event>, reverse: bool, min_seq_exclusive: Seq,
        max_seq_inclusive: Seq,
    ) -> Self {
        // note: we swap inclusive/exclusive because we must be able to decrement max_idx to where it excludes everything
        // if we left it inclusive, that would require usize underflow
        let min_idx = match table.seqs.binary_search(&min_seq_exclusive) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        };
        let max_idx = match table.seqs.binary_search(&max_seq_inclusive) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        };
        Self { table, reverse, min_idx_inclusive: min_idx, max_idx_exclusive: max_idx }
    }

    fn next(&mut self) -> Option<(Seq, &'iter Event)> {
        if self.min_idx_inclusive == self.max_idx_exclusive {
            None
        } else {
            let result = &self.table.events[self.min_idx_inclusive];
            let current = self.table.seqs[self.min_idx_inclusive];
            self.min_idx_inclusive += 1;
            Some((current, result))
        }
    }

    fn next_back(&mut self) -> Option<(Seq, &'iter Event)> {
        if self.min_idx_inclusive == self.max_idx_exclusive {
            None
        } else {
            self.max_idx_exclusive -= 1; // decrementing before reference is what makes this exclusive
            let result = &self.table.events[self.max_idx_exclusive];
            let current = self.table.seqs[self.max_idx_exclusive];
            Some((current, result))
        }
    }
}

impl<'iter, Event> Iterator for VecTableIterator<'iter, Event> {
    type Item = (Seq, &'iter Event);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.reverse {
            VecTableIterator::<'iter, Event>::next(self)
        } else {
            VecTableIterator::<'iter, Event>::next_back(self)
        }
    }
}

impl<'iter, Event> DoubleEndedIterator for VecTableIterator<'iter, Event> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if !self.reverse {
            VecTableIterator::<'iter, Event>::next_back(self)
        } else {
            VecTableIterator::<'iter, Event>::next(self)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::VecTable;
    use crate::{Seq, Table, View};

    #[test]
    fn scan_none() {
        let table = VecTable::<i32>::new();
        assert_eq!(table.current_seq(), 0);
        assert_eq!(
            table
                .scan(Seq::MIN, Seq::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            Vec::<&i32>::new()
        );
    }

    #[test]
    fn scan_one() {
        let mut table = VecTable::<i32>::new();
        table.write([12]);
        assert_eq!(table.current_seq(), 1);
        assert_eq!(
            table
                .scan(Seq::MIN, Seq::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&12]
        );
    }

    #[test]
    fn scan_multiple() {
        let mut table = VecTable::<i32>::new();
        table.write([12, 34, 56, 78]);
        assert_eq!(table.current_seq(), 4);
        assert_eq!(
            table
                .scan(Seq::MIN, Seq::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&12, &34, &56, &78]
        );
    }

    #[test]
    fn scan_partial_one() {
        let mut table = VecTable::<i32>::new();
        table.write([12, 34, 56, 78]);
        assert_eq!(table.current_seq(), 4);
        assert_eq!(
            table
                .scan(1, 2)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&34]
        );
    }

    #[test]
    fn scan_partial_multiple() {
        let mut table = VecTable::<i32>::new();
        table.write([12, 34, 56, 78]);
        assert_eq!(table.current_seq(), 4);
        assert_eq!(
            table
                .scan(1, 3)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&34, &56]
        );
    }

    #[test]
    fn scan_none_rev() {
        let table = VecTable::<i32>::new();
        assert_eq!(
            table
                .scan(Seq::MIN, Seq::MAX)
                .rev()
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            Vec::<&i32>::new()
        );
    }

    #[test]
    fn scan_one_rev() {
        let mut table = VecTable::<i32>::new();
        table.write([12]);
        assert_eq!(table.current_seq(), 1);
        assert_eq!(
            table
                .scan(Seq::MIN, Seq::MAX)
                .rev()
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&12]
        );
    }

    #[test]
    fn scan_multiple_rev() {
        let mut table = VecTable::<i32>::new();
        table.write([12, 34, 56, 78]);
        assert_eq!(table.current_seq(), 4);
        assert_eq!(
            table
                .scan(Seq::MIN, Seq::MAX)
                .rev()
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&78, &56, &34, &12]
        );
    }

    #[test]
    fn scan_partial_one_rev() {
        let mut table = VecTable::<i32>::new();
        table.write([12, 34, 56, 78]);
        assert_eq!(table.current_seq(), 4);
        assert_eq!(
            table
                .scan(1, 2)
                .rev()
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&34]
        );
    }

    #[test]
    fn scan_partial_multiple_rev() {
        let mut table = VecTable::<i32>::new();
        table.write([12, 34, 56, 78]);
        assert_eq!(table.current_seq(), 4);
        assert_eq!(
            table
                .scan(1, 3)
                .rev()
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&56, &34]
        );
    }
}
