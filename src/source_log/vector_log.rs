use crate::SourceLog;

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
    type Iterator<'a> = VectorLogIterator<'a, Event> where Event: 'a;

    fn scan(&self, min_seq_exclusive: u64, max_seq_inclusive: u64) -> Self::Iterator<'_> {
        VectorLogIterator::new(self, min_seq_exclusive, max_seq_inclusive)
    }

    fn current_seq(&self) -> u64 {
        self.seqs.last().unwrap_or(&0).to_owned()
    }
}

impl<Event> VectorLog<Event> {
    pub fn write(&mut self, event: Event) -> u64 {
        let next_seq = self.seqs.last().unwrap_or(&0).to_owned() + 1;
        self.seqs.push(next_seq);
        self.events.push(event);
        next_seq
    }
}

pub struct VectorLogIterator<'a, Event> {
    log: &'a VectorLog<Event>,
    next: usize,
    next_back: usize,
}

impl<'a, Event> VectorLogIterator<'a, Event> {
    fn new(log: &'a VectorLog<Event>, min_seq_exclusive: u64, max_seq_inclusive: u64) -> Self {
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

impl<'a, Event> Iterator for VectorLogIterator<'a, Event> {
    type Item = &'a Event;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.log.seqs.len() || self.next >= self.next_back {
            None
        } else {
            let result = Some(&self.log.events[self.next]);
            self.next += 1;
            result
        }
    }
}

impl<'a, Event> DoubleEndedIterator for VectorLogIterator<'a, Event> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.next_back == 0 || self.next >= self.next_back {
            None
        } else {
            let result = Some(&self.log.events[self.next_back - 1]);
            self.next_back -= 1;
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::VectorLog;
    use crate::SourceLog;

    #[test]
    fn iter_none() {
        let log = VectorLog::<i32>::new();
        assert_eq!(
            log.scan(u64::MIN, u64::MAX).collect::<Vec<&i32>>(),
            Vec::<&i32>::new()
        );
    }

    #[test]
    fn iter_one() {
        let mut log = VectorLog::<i32>::new();
        assert_eq!(log.write(12), 1);
        assert_eq!(
            log.scan(u64::MIN, u64::MAX).collect::<Vec<&i32>>(),
            vec![&12]
        );
    }

    #[test]
    fn iter_multiple() {
        let mut log = VectorLog::<i32>::new();
        assert_eq!(log.write(12), 1);
        assert_eq!(log.write(34), 2);
        assert_eq!(log.write(56), 3);
        assert_eq!(log.write(78), 4);
        assert_eq!(
            log.scan(u64::MIN, u64::MAX).collect::<Vec<&i32>>(),
            vec![&12, &34, &56, &78]
        );
    }

    #[test]
    fn iter_partial_one() {
        let mut log = VectorLog::<i32>::new();
        assert_eq!(log.write(12), 1);
        assert_eq!(log.write(34), 2);
        assert_eq!(log.write(56), 3);
        assert_eq!(log.write(78), 4);
        assert_eq!(log.scan(1, 2).collect::<Vec<&i32>>(), vec![&34]);
    }

    #[test]
    fn iter_partial_multiple() {
        let mut log = VectorLog::<i32>::new();
        assert_eq!(log.write(12), 1);
        assert_eq!(log.write(34), 2);
        assert_eq!(log.write(56), 3);
        assert_eq!(log.write(78), 4);
        assert_eq!(log.scan(1, 3).collect::<Vec<&i32>>(), vec![&34, &56]);
    }

    #[test]
    fn iter_none_rev() {
        let log = VectorLog::<i32>::new();
        assert_eq!(
            log.scan(u64::MIN, u64::MAX).rev().collect::<Vec<&i32>>(),
            Vec::<&i32>::new()
        );
    }

    #[test]
    fn iter_one_rev() {
        let mut log = VectorLog::<i32>::new();
        assert_eq!(log.write(12), 1);
        assert_eq!(
            log.scan(u64::MIN, u64::MAX).rev().collect::<Vec<&i32>>(),
            vec![&12]
        );
    }

    #[test]
    fn iter_multiple_rev() {
        let mut log = VectorLog::<i32>::new();
        assert_eq!(log.write(12), 1);
        assert_eq!(log.write(34), 2);
        assert_eq!(log.write(56), 3);
        assert_eq!(log.write(78), 4);
        assert_eq!(
            log.scan(u64::MIN, u64::MAX).rev().collect::<Vec<&i32>>(),
            vec![&78, &56, &34, &12]
        );
    }

    #[test]
    fn iter_partial_one_rev() {
        let mut log = VectorLog::<i32>::new();
        assert_eq!(log.write(12), 1);
        assert_eq!(log.write(34), 2);
        assert_eq!(log.write(56), 3);
        assert_eq!(log.write(78), 4);
        assert_eq!(log.scan(1, 2).rev().collect::<Vec<&i32>>(), vec![&34]);
    }

    #[test]
    fn iter_partial_multiple_rev() {
        let mut log = VectorLog::<i32>::new();
        assert_eq!(log.write(12), 1);
        assert_eq!(log.write(34), 2);
        assert_eq!(log.write(56), 3);
        assert_eq!(log.write(78), 4);
        assert_eq!(log.scan(1, 3).rev().collect::<Vec<&i32>>(), vec![&56, &34]);
    }
}
