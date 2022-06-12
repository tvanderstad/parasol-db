#![feature(generic_associated_types)]

use std::iter::Iterator;

#[derive(Debug)]
pub enum Error {}

pub type Result<T> = std::result::Result<T, Error>;

pub trait BaseLog {
    type Event;
    type Iterator<'a>: Iterator<Item = &'a Self::Event>
    where
        Self::Event: 'a,
        Self: 'a;
    fn iter<'a>(&'a self, min_seq_exclusive: u64, max_seq_inclusive: u64) -> Self::Iterator<'a>; // range scan (todo: backwards range scan)
    fn write(&mut self, event: Self::Event) -> u64; // returns sequence number for the write
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

trait DerivedLog {
    type Derived;
    fn get_at_seq(&self, seq: u64) -> Self::Derived; // what is the value of the index at this seq?
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{BaseLog, DerivedLog, InMemoryLog};

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct KeyValueAssignment<K, V> {
        key: K,
        value: V,
    }

    struct HashMapLog<'a, Base: BaseLog> {
        base: &'a Base,
    }

    impl<'a, Base, K, V> DerivedLog for HashMapLog<'a, Base>
    where
        Base: BaseLog<Event = KeyValueAssignment<K, V>>,
        K: std::cmp::Eq + std::hash::Hash + std::clone::Clone,
        V: std::clone::Clone,
    {
        type Derived = HashMap<K, V>;
        fn get_at_seq(&self, seq: u64) -> HashMap<K, V> {
            let mut result = HashMap::<K, V>::new();
            for key_value_assignment in self.base.iter(0, seq) {
                match key_value_assignment {
                    KeyValueAssignment { key, value } => {
                        result.insert(key.clone(), value.clone());
                    }
                }
            }
            result
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
    fn derived() {
        let (log, seq) = {
            // todo: shared and mut references at the same time using unsafe
            let mut log = InMemoryLog::<KeyValueAssignment<String, String>>::new();
            log.write(KeyValueAssignment {
                key: String::from("key1"),
                value: String::from("value1"),
            });
            log.write(KeyValueAssignment {
                key: String::from("key2"),
                value: String::from("value2"),
            });
            log.write(KeyValueAssignment {
                key: String::from("key3"),
                value: String::from("value3"),
            });
            let seq = log.write(KeyValueAssignment {
                key: String::from("key4"),
                value: String::from("value4"),
            });
            (log, seq)
        };
        let hash_map_log = HashMapLog { base: &log };
        let hash_map = hash_map_log.get_at_seq(seq);
        assert_eq!(
            hash_map,
            HashMap::from_iter(
                vec![
                    (String::from("key1"), String::from("value1")),
                    (String::from("key2"), String::from("value2")),
                    (String::from("key3"), String::from("value3")),
                    (String::from("key4"), String::from("value4")),
                ]
                .into_iter()
            )
        );
    }
}
