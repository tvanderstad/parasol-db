use crate::{SourceLog, WritableSourceLog};

pub struct VectorLog<Event> {
    seqs: Vec<u64>,
    events: Vec<Event>,
}

impl<Event: Clone> VectorLog<Event> {
    pub fn new() -> Self {
        VectorLog {
            seqs: Vec::new(),
            events: Vec::new(),
        }
    }
}

impl<Event: Clone> Default for VectorLog<Event> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Event> SourceLog for VectorLog<Event> {
    type Event = Event;
    type Iterator<'iter> = VectorLogIterator<'iter, Event> where Event: 'iter;

    fn scan(&self, min_seq_exclusive: u64, max_seq_inclusive: u64) -> Self::Iterator<'_> {
        VectorLogIterator::new(self, min_seq_exclusive, max_seq_inclusive)
    }

    fn current_seq(&self) -> u64 {
        self.seqs.last().unwrap_or(&0).to_owned()
    }
}

impl<Event> WritableSourceLog for VectorLog<Event> {
    fn write<Iter: IntoIterator<Item = Self::Event>>(&mut self, events: Iter) {
        for event in events.into_iter() {
            let next_seq = self.seqs.last().unwrap_or(&0).to_owned() + 1;
            self.seqs.push(next_seq);
            self.events.push(event);
        }
    }
}

pub struct VectorLogIterator<'iter, Event> {
    log: &'iter VectorLog<Event>,
    next: usize,
    next_back: usize,
}

impl<'iter, Event> VectorLogIterator<'iter, Event> {
    fn new(log: &'iter VectorLog<Event>, min_seq_exclusive: u64, max_seq_inclusive: u64) -> Self {
        let next = match log.seqs.binary_search(&min_seq_exclusive) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        let next_back = match log.seqs.binary_search(&max_seq_inclusive) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        Self {
            log,
            next,
            next_back,
        }
    }
}

impl<'iter, Event> Iterator for VectorLogIterator<'iter, Event> {
    type Item = (u64, &'iter Event);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.log.seqs.len() || self.next >= self.next_back {
            None
        } else {
            let result = &self.log.events[self.next];
            let current = self.next as u64;
            self.next += 1;
            Some((current, result))
        }
    }
}

impl<'iter, Event> DoubleEndedIterator for VectorLogIterator<'iter, Event> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.next_back == 0 || self.next >= self.next_back {
            None
        } else {
            let result = &self.log.events[self.next_back - 1];
            let current = self.next_back as u64;
            self.next_back -= 1;
            Some((current, result))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::VectorLog;
    use crate::{SourceLog, WritableSourceLog};

    #[test]
    fn iter_none() {
        let log = VectorLog::<i32>::new();
        assert_eq!(log.current_seq(), 0);
        assert_eq!(
            log.scan(u64::MIN, u64::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            Vec::<&i32>::new()
        );
    }

    #[test]
    fn iter_one() {
        let mut log = VectorLog::<i32>::new();
        log.write([12]);
        assert_eq!(log.current_seq(), 1);
        assert_eq!(
            log.scan(u64::MIN, u64::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&12]
        );
    }

    #[test]
    fn iter_multiple() {
        let mut log = VectorLog::<i32>::new();
        log.write([12, 34, 56, 78]);
        assert_eq!(log.current_seq(), 4);
        assert_eq!(
            log.scan(u64::MIN, u64::MAX)
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&12, &34, &56, &78]
        );
    }

    #[test]
    fn iter_partial_one() {
        let mut log = VectorLog::<i32>::new();
        log.write([12, 34, 56, 78]);
        assert_eq!(log.current_seq(), 4);
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
        assert_eq!(log.current_seq(), 4);
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
            log.scan(u64::MIN, u64::MAX)
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
        assert_eq!(log.current_seq(), 1);
        assert_eq!(
            log.scan(u64::MIN, u64::MAX)
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
        assert_eq!(log.current_seq(), 4);
        assert_eq!(
            log.scan(u64::MIN, u64::MAX)
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
        assert_eq!(log.current_seq(), 4);
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
        assert_eq!(log.current_seq(), 4);
        assert_eq!(
            log.scan(1, 3)
                .rev()
                .map(|(_, event)| event)
                .collect::<Vec<&i32>>(),
            vec![&56, &34]
        );
    }
}
