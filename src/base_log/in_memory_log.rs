use crate::BaseLog;

pub struct InMemoryLog<Event> {
    seqs: Vec<u64>,
    events: Vec<Event>,
}

impl<Event: Clone> InMemoryLog<Event> {
    pub fn new() -> Self {
        InMemoryLog {
            seqs: Vec::new(),
            events: Vec::new(),
        }
    }
}

impl<Event> BaseLog for InMemoryLog<Event> {
    type Event = Event;
    type Iterator<'a> = InMemoryIterator<'a, Event> where Event: 'a;

    // todo: binary search
    fn iter<'a>(&'a self, min_seq_exclusive: u64, max_seq_inclusive: u64) -> Self::Iterator<'a> {
        InMemoryIterator::<'a, Event> {
            log: &self,
            min_seq_exclusive,
            max_seq_inclusive,
            i: 0,
        }
    }

    fn write(&mut self, event: Event) -> u64 {
        let next_seq = self.seqs.last().unwrap_or(&0).to_owned() + 1;
        self.seqs.push(next_seq);
        self.events.push(event);
        next_seq
    }
}

pub struct InMemoryIterator<'a, Event> {
    log: &'a InMemoryLog<Event>,
    min_seq_exclusive: u64,
    max_seq_inclusive: u64,
    i: usize,
}

impl<'a, Event> Iterator for InMemoryIterator<'a, Event> {
    type Item = &'a Event;

    fn next(&mut self) -> Option<Self::Item> {
        self.i += 1;
        while self.i - 1 < self.log.seqs.len()
            && self.min_seq_exclusive >= self.log.seqs[self.i - 1]
        {
            self.i += 1;
        }
        if self.i - 1 >= self.log.seqs.len() || self.max_seq_inclusive < self.log.seqs[self.i - 1] {
            None
        } else {
            Some(&self.log.events[self.i - 1])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryLog;
    use crate::BaseLog;

    #[test]
    fn iter_none() {
        let log = InMemoryLog::<i32>::new();
        assert_eq!(log.iter(0, 0).collect::<Vec<&i32>>(), Vec::<&i32>::new());
    }

    #[test]
    fn iter_one() {
        let mut log = InMemoryLog::<i32>::new();
        assert_eq!(log.write(12), 1);
        assert_eq!(log.iter(0, 1).collect::<Vec<&i32>>(), vec![&12]);
    }

    #[test]
    fn iter_multiple() {
        let mut log = InMemoryLog::<i32>::new();
        assert_eq!(log.write(12), 1);
        assert_eq!(log.write(34), 2);
        assert_eq!(log.write(56), 3);
        assert_eq!(log.write(78), 4);
        assert_eq!(
            log.iter(0, 4).collect::<Vec<&i32>>(),
            vec![&12, &34, &56, &78]
        );
    }

    #[test]
    fn iter_partial_one() {
        let mut log = InMemoryLog::<i32>::new();
        assert_eq!(log.write(12), 1);
        assert_eq!(log.write(34), 2);
        assert_eq!(log.write(56), 3);
        assert_eq!(log.write(78), 4);
        assert_eq!(log.iter(1, 2).collect::<Vec<&i32>>(), vec![&34]);
    }

    #[test]
    fn iter_partial_multiple() {
        let mut log = InMemoryLog::<i32>::new();
        assert_eq!(log.write(12), 1);
        assert_eq!(log.write(34), 2);
        assert_eq!(log.write(56), 3);
        assert_eq!(log.write(78), 4);
        assert_eq!(log.iter(1, 3).collect::<Vec<&i32>>(), vec![&34, &56]);
    }
}
