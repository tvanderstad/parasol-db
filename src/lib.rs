#![feature(generic_associated_types)]

use std::iter::Iterator;

#[derive(Debug)]
pub enum Error {}

pub type Result<T> = std::result::Result<T, Error>;

pub trait BaseLog<Event> {
    type Iterator<'a>: Iterator<Item = &'a Event>
    where
        Event: 'a,
        Self: 'a;
    fn get_seq(&self) -> u64;
    fn iter<'a>(&'a self, min_seq_exclusive: u64, max_seq_inclusive: u64) -> Self::Iterator<'a>;
    fn write(&mut self, event: Event);
}

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

impl<Event> BaseLog<Event> for InMemoryLog<Event> {
    type Iterator<'a> = InMemoryIterator<'a, Event> where Event: 'a;

    fn get_seq(&self) -> u64 {
        self.seqs.last().unwrap_or(&0).to_owned()
    }

    // todo: binary search
    fn iter<'a>(&'a self, min_seq_exclusive: u64, max_seq_inclusive: u64) -> Self::Iterator<'a> {
        InMemoryIterator::<'a, Event> {
            log: &self,
            min_seq_exclusive,
            max_seq_inclusive,
            i: 0,
        }
    }

    fn write(&mut self, event: Event) {
        self.seqs.push(self.get_seq() + 1);
        self.events.push(event)
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

trait DerivedLog<Derived> {
    fn get_at_seq(&self, seq: u64) -> &Derived; // what is the value of the index at this seq?
}

#[cfg(test)]
mod tests {
    use crate::{InMemoryLog, BaseLog};

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct KeyValueAssignment<K, V> {
        key: K,
        value: V,
    }

    #[test]
    fn iter() {
        let mut log = InMemoryLog::<KeyValueAssignment<String, String>>::new();
        let kva = KeyValueAssignment {
            key: String::from("key"),
            value: String::from("value"),
        };
        log.write(kva.clone());
        assert_eq!(
            log.iter(0, 1)
                .collect::<Vec<&KeyValueAssignment<String, String>>>(),
            vec![&kva]
        );
    }

    #[test]
    fn iter_partial() {
        let mut log = InMemoryLog::<KeyValueAssignment<String, String>>::new();
        let kva1 = KeyValueAssignment {
            key: String::from("key1"),
            value: String::from("value1"),
        };
        let kva2 = KeyValueAssignment {
            key: String::from("key2"),
            value: String::from("value2"),
        };
        let kva3 = KeyValueAssignment {
            key: String::from("key3"),
            value: String::from("value3"),
        };
        let kva4 = KeyValueAssignment {
            key: String::from("key4"),
            value: String::from("value4"),
        };
        log.write(kva1.clone());
        log.write(kva2.clone());
        log.write(kva3.clone());
        log.write(kva4.clone());
        assert_eq!(
            log.iter(1, 3)
                .collect::<Vec<&KeyValueAssignment<String, String>>>(),
            vec![&kva2, &kva3]
        );
    }
}
