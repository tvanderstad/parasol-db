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
    fn iter<'a>(&'a self) -> Self::Iterator<'a>;
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

    fn iter<'a>(&'a self) -> Self::Iterator<'a> {
        InMemoryIterator::<'a, Event> {
            events: &self.events,
            i: 0,
        }
    }

    fn write(&mut self, event: Event) {
        self.seqs.push(self.seqs.len() as u64);
        self.events.push(event)
    }
}

pub struct InMemoryIterator<'a, Event> {
    events: &'a Vec<Event>,
    i: usize,
}

impl<'a, Event> Iterator for InMemoryIterator<'a, Event> {
    type Item = &'a Event;

    fn next(&mut self) -> Option<Self::Item> {
        self.i += 1;
        if self.i - 1 >= self.events.len() {
            None
        } else {
            Some(&self.events[self.i - 1])
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Compactor, InMemoryLog, Log};
    use core::hash::Hash;
    use std::collections::HashSet;

    #[derive(Clone)]
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
