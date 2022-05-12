#![feature(generic_associated_types)]

use std::iter::Iterator;

#[derive(Debug)]
enum Error {}

type Result<T> = std::result::Result<T, Error>;

trait Compactor<TEvent: Event> {
    fn keep(&mut self, seq: u64, event: &TEvent) -> bool;
}

pub trait Event : Clone {}

trait Log<TEvent: Event> {
    type Iterator<'a>: Iterator<Item=&'a TEvent> where TEvent: 'a, Self: 'a;
    fn get_seq(self) -> u64;
    fn iter<'a>(&'a self) -> Self::Iterator<'a>;
    fn compact<TCompactor: Compactor<TEvent>>(&mut self, compactor: &mut TCompactor) -> Result<()>;
}

pub struct InMemoryLog<TEvent: Event> {
    seqs: Vec<u64>,
    events: Vec<TEvent>,
}

impl <TEvent: Event> InMemoryLog<TEvent> {
    pub fn new() -> Self {
        InMemoryLog{ seqs: Vec::new(), events: Vec::new() }
    }
}

impl <TEvent: Event> Log<TEvent> for InMemoryLog<TEvent> {
    type Iterator<'a> = InMemoryIterator<'a, TEvent> where TEvent: 'a;

    fn get_seq(self) -> u64 {
        self.seqs.last().unwrap_or(&0).to_owned()
    }

    fn iter<'a>(&'a self) -> Self::Iterator<'a> {
        InMemoryIterator::<'a, TEvent>{ events: &self.events, i: 0 }
    }

    fn compact<TCompactor: Compactor<TEvent>>(&mut self, compactor: &mut TCompactor) -> Result<()> {
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

struct InMemoryIterator<'a, TEvent: Event> {
    events: &'a Vec<TEvent>,
    i: usize,
}

impl<'a, TEvent: Event> Iterator for InMemoryIterator<'a, TEvent> {
    type Item = &'a TEvent;

    fn next(&mut self) -> Option<Self::Item> {
        self.i += 1;
        if self.i - 1 >= self.events.len() {
            None
        } else {
            Some(&self.events[self.i-1])
        }
    }
}

#[cfg(test)]
mod tests {
    use core::hash::Hash;
    use std::collections::HashSet;
    use crate::{Event, Compactor, InMemoryLog, Log};

    #[derive(Clone)]
    struct KeyValueAssignment<K, V> {
        key: K,
        value: V,
    }

    impl<K: Clone, V: Clone> Event for KeyValueAssignment<K, V>{}

    struct KeyValueAssignmentCompactor<K: Clone> {
        keys: HashSet<K>,
    }

    impl<K: Clone + Eq + Hash, V: Clone> Compactor<KeyValueAssignment<K, V>> for KeyValueAssignmentCompactor<K> {
        fn keep(&mut self, _seq: u64, event: &KeyValueAssignment<K, V>) -> bool {
            self.keys.insert(event.key.clone())
        }
    }

    #[test]
    fn it_works() {
        let mut log = InMemoryLog::<KeyValueAssignment<String, String>>::new();
        let mut compactor = KeyValueAssignmentCompactor::<String>{ keys: HashSet::new() };
        log.compact(&mut compactor).unwrap();
    }
}
