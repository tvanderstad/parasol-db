#![feature(generic_associated_types)]

use std::iter::Iterator;

#[derive(Debug)]
pub enum Error {}

pub type Result<T> = std::result::Result<T, Error>;

pub trait Log<Event> {
    type Iterator<'a>: Iterator<Item = &'a Event>
    where
        Event: 'a,
        Self: 'a;
    fn get_seq(&self) -> u64;
    fn iter<'a>(&'a self, min_seq_exclusive: u64, max_seq_inclusive: u64) -> Self::Iterator<'a>;
    fn write(&mut self, event: Event);
}

pub trait Compactor<Event> {
    fn keep(&mut self, seq: u64, event: &Event) -> bool;
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

    // todo: don't clone
    pub fn compact<TCompactor: Compactor<Event>>(
        &mut self,
        compactor: &mut TCompactor,
    ) -> Result<()> {
        let mut new_seqs = Vec::new();
        let mut new_events = Vec::new();
        for i in 0..self.seqs.len() {
            if compactor.keep(self.seqs[i], &self.events[i]) {
                new_seqs.push(self.seqs[i]);
                new_events.push(self.events[i].clone());
            }
        }
        self.seqs = new_seqs;
        self.events = new_events;
        Ok(())
    }
}

impl<Event> Log<Event> for InMemoryLog<Event> {
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

#[cfg(test)]
mod tests {
    use crate::{Compactor, InMemoryLog, Log};
    use core::hash::Hash;
    use std::collections::HashSet;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct KeyValueAssignment<K, V> {
        key: K,
        value: V,
    }

    struct KeyValueAssignmentCompactor<K: Clone> {
        keys: HashSet<K>,
    }

    impl<K: Clone + Eq + Hash, V: Clone> Compactor<KeyValueAssignment<K, V>>
        for KeyValueAssignmentCompactor<K>
    {
        fn keep(&mut self, _seq: u64, event: &KeyValueAssignment<K, V>) -> bool {
            self.keys.insert(event.key.clone())
        }
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

    #[test]
    fn compact_empty() {
        let mut log = InMemoryLog::<KeyValueAssignment<String, String>>::new();
        let mut compactor = KeyValueAssignmentCompactor::<String> {
            keys: HashSet::new(),
        };
        log.compact(&mut compactor).unwrap();
        assert_eq!(compactor.keys.len(), 0);
    }

    #[test]
    fn compact_one() {
        let mut log = InMemoryLog::<KeyValueAssignment<String, String>>::new();
        log.write(KeyValueAssignment {
            key: String::from("key"),
            value: String::from("value"),
        });
        let mut compactor = KeyValueAssignmentCompactor::<String> {
            keys: HashSet::new(),
        };
        log.compact(&mut compactor).unwrap();
        assert_eq!(compactor.keys.len(), 1);
    }
}
