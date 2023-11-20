use crate::{Seq, Table, View};

#[derive(Clone)]
pub struct VectorLog<Event> {
    seqs: Vec<Seq>,
    events: Vec<Event>,
}

impl<Event: Clone> VectorLog<Event> {
    pub fn new() -> Self {
        VectorLog { seqs: Vec::new(), events: Vec::new() }
    }
}

impl<Event: Clone> Default for VectorLog<Event> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Event> View for VectorLog<Event> {
    type Event = Event;
    type Iterator<'iter> = VectorLogIterator<'iter, Event> where Event: 'iter;

    fn scan(&self, start: Seq, end: Seq) -> Self::Iterator<'_> {
        let reverse = start > end;
        let (min, max) = if reverse { (end, start) } else { (start, end) };
        VectorLogIterator::new(self, reverse, min, max)
    }

    fn current_seq(&self) -> Seq {
        self.seqs.iter().max().copied().unwrap_or_default() + 1 as Seq
    }
}

impl<Event> Table for VectorLog<Event> {
    fn write<Iter: IntoIterator<Item = Self::Event>>(&mut self, events: Iter) {
        for event in events.into_iter() {
            self.seqs.push(self.current_seq());
            self.events.push(event);
        }
    }
}

pub struct VectorLogIterator<'iter, Event> {
    log: &'iter VectorLog<Event>,
    reverse: bool,
    min_idx_inclusive: usize,
    max_idx_exclusive: usize,
}

impl<'iter, Event> VectorLogIterator<'iter, Event> {
    fn new(
        log: &'iter VectorLog<Event>, reverse: bool, min_seq_exclusive: Seq, max_seq_inclusive: Seq,
    ) -> Self {
        // note: we swap inclusive/exclusive because we must be able to decrement max_idx to where it excludes everything
        // if we left it inclusive, that would require usize underflow
        let min_idx = match log.seqs.binary_search(&min_seq_exclusive) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        };
        let max_idx = match log.seqs.binary_search(&max_seq_inclusive) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        };
        VectorLogIterator { log, reverse, min_idx_inclusive: min_idx, max_idx_exclusive: max_idx }
    }

    fn next(&mut self) -> Option<(Seq, &'iter Event)> {
        if self.min_idx_inclusive == self.max_idx_exclusive {
            None
        } else {
            let result = &self.log.events[self.min_idx_inclusive];
            let current = self.log.seqs[self.min_idx_inclusive];
            self.min_idx_inclusive += 1;
            Some((current, result))
        }
    }

    fn next_back(&mut self) -> Option<(Seq, &'iter Event)> {
        if self.min_idx_inclusive == self.max_idx_exclusive {
            None
        } else {
            self.max_idx_exclusive -= 1; // decrementing before reference is what makes this exclusive
            let result = &self.log.events[self.max_idx_exclusive];
            let current = self.log.seqs[self.max_idx_exclusive];
            Some((current, result))
        }
    }
}

impl<'iter, Event> Iterator for VectorLogIterator<'iter, Event> {
    type Item = (Seq, &'iter Event);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.reverse {
            VectorLogIterator::<'iter, Event>::next(self)
        } else {
            VectorLogIterator::<'iter, Event>::next_back(self)
        }
    }
}

impl<'iter, Event> DoubleEndedIterator for VectorLogIterator<'iter, Event> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if !self.reverse {
            VectorLogIterator::<'iter, Event>::next_back(self)
        } else {
            VectorLogIterator::<'iter, Event>::next(self)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::VectorLog;
    use crate::{Seq, Table, View};

    #[test]
    fn iter_none() {
        let log = VectorLog::<i32>::new();
        assert_eq!(log.current_seq(), 1);
        assert_eq!(
            log.scan(Seq::MIN, Seq::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            Vec::<&i32>::new()
        );
    }

    #[test]
    fn iter_one() {
        let mut log = VectorLog::<i32>::new();
        log.write([12]);
        assert_eq!(log.current_seq(), 2);
        assert_eq!(
            log.scan(Seq::MIN, Seq::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&12]
        );
    }

    #[test]
    fn iter_multiple() {
        let mut log = VectorLog::<i32>::new();
        log.write([12, 34, 56, 78]);
        assert_eq!(log.current_seq(), 5);
        assert_eq!(
            log.scan(Seq::MIN, Seq::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&12, &34, &56, &78]
        );
    }

    #[test]
    fn iter_partial_one() {
        let mut log = VectorLog::<i32>::new();
        log.write([12, 34, 56, 78]);
        assert_eq!(log.current_seq(), 5);
        assert_eq!(
            log.scan(1, 2)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&34]
        );
    }

    #[test]
    fn iter_partial_multiple() {
        let mut log = VectorLog::<i32>::new();
        log.write([12, 34, 56, 78]);
        assert_eq!(log.current_seq(), 5);
        assert_eq!(
            log.scan(1, 3)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&34, &56]
        );
    }

    #[test]
    fn iter_none_rev() {
        let log = VectorLog::<i32>::new();
        assert_eq!(
            log.scan(Seq::MIN, Seq::MAX)
                .rev()
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            Vec::<&i32>::new()
        );
    }

    #[test]
    fn iter_one_rev() {
        let mut log = VectorLog::<i32>::new();
        log.write([12]);
        assert_eq!(log.current_seq(), 2);
        assert_eq!(
            log.scan(Seq::MIN, Seq::MAX)
                .rev()
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&12]
        );
    }

    #[test]
    fn iter_multiple_rev() {
        let mut log = VectorLog::<i32>::new();
        log.write([12, 34, 56, 78]);
        assert_eq!(log.current_seq(), 5);
        assert_eq!(
            log.scan(Seq::MIN, Seq::MAX)
                .rev()
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&78, &56, &34, &12]
        );
    }

    #[test]
    fn iter_partial_one_rev() {
        let mut log = VectorLog::<i32>::new();
        log.write([12, 34, 56, 78]);
        assert_eq!(log.current_seq(), 5);
        assert_eq!(
            log.scan(1, 2)
                .rev()
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&34]
        );
    }

    #[test]
    fn iter_partial_multiple_rev() {
        let mut log = VectorLog::<i32>::new();
        log.write([12, 34, 56, 78]);
        assert_eq!(log.current_seq(), 5);
        assert_eq!(
            log.scan(1, 3)
                .rev()
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&56, &34]
        );
    }
}
