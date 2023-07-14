use crate::{Seq, Table, View};

#[derive(Clone)]
pub struct VectorLog<Event> {
    seqs: Vec<Seq>, // todo: support sparse segs (a few places assume these to be contiguous)
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

    fn next_seq(&self) -> Seq {
        self.seqs.len() as Seq // todo: use seqs array
    }
}

impl<Event> Table for VectorLog<Event> {
    fn write<Iter: IntoIterator<Item = Self::Event>>(&mut self, events: Iter) {
        for event in events.into_iter() {
            self.seqs.push(self.next_seq());
            self.events.push(event);
        }
    }
}

pub struct VectorLogIterator<'iter, Event> {
    log: &'iter VectorLog<Event>,
    reverse: bool,
    min_seq: Seq,
    max_seq: Seq,
}

impl<'iter, Event> VectorLogIterator<'iter, Event> {
    fn new(log: &'iter VectorLog<Event>, reverse: bool, min_seq: Seq, max_seq: Seq) -> Self {
        // todo: use seqs array
        Self { log, reverse, min_seq, max_seq: std::cmp::min(max_seq, log.seqs.len() as Seq) }
    }

    fn next(&mut self) -> Option<(Seq, &'iter Event)> {
        let min_seq = self.min_seq as usize; // todo: use seqs array
        if min_seq == self.log.seqs.len() || self.min_seq == self.max_seq {
            None
        } else {
            let result = &self.log.events[min_seq];
            let current = self.min_seq;
            self.min_seq += 1;
            Some((current, result))
        }
    }

    fn next_back(&mut self) -> Option<(Seq, &'iter Event)> {
        let max_seq = self.max_seq as usize; // todo: use seqs array
        if self.max_seq == 0 || self.min_seq == self.max_seq {
            None
        } else {
            let result = &self.log.events[max_seq - 1];
            let current = self.max_seq;
            self.max_seq -= 1;
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
        assert_eq!(log.next_seq(), 0);
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
        assert_eq!(log.next_seq(), 1);
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
        assert_eq!(log.next_seq(), 4);
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
        assert_eq!(log.next_seq(), 4);
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
        assert_eq!(log.next_seq(), 4);
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
        assert_eq!(log.next_seq(), 1);
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
        assert_eq!(log.next_seq(), 4);
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
        assert_eq!(log.next_seq(), 4);
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
        assert_eq!(log.next_seq(), 4);
        assert_eq!(
            log.scan(1, 3)
                .rev()
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&56, &34]
        );
    }
}
