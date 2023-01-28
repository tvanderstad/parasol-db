use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::{DestLog, SourceLog};

pub struct HashMapLog<Source, ToAssignment, Key, Value>
where
    Source: SourceLog,
    ToAssignment: Fn(&Source::Event) -> Option<(Key, Value)>,
    Key: Clone + Eq + Hash,
    Value: Clone,
{
    source: Arc<Mutex<Source>>,
    current_seq: u64,
    to_assignment: ToAssignment,
    map: HashMap<Key, Value>,
}

impl<Source, ToAssignment, Key, Value> DestLog for HashMapLog<Source, ToAssignment, Key, Value>
where
    Source: SourceLog,
    ToAssignment: Fn(&Source::Event) -> Option<(Key, Value)>,
    Key: Clone + Eq + Hash,
    Value: Clone,
{
    fn update(&mut self, seq: u64) {
        for event in self.source.lock().unwrap().scan(self.current_seq, seq) {
            if let Some((key, value)) = (self.to_assignment)(event) {
                self.map.insert(key, value);
            }
        }

        self.current_seq = seq;
    }

    fn current_seq(&self) -> u64 {
        self.current_seq
    }
}

impl<Source, ToAssignment, Key, Value> HashMapLog<Source, ToAssignment, Key, Value>
where
    Source: SourceLog,
    ToAssignment: Fn(&Source::Event) -> Option<(Key, Value)>,
    Key: Clone + Eq + Hash,
    Value: Clone,
{
    pub fn new(source: Arc<Mutex<Source>>, to_assignment: ToAssignment) -> Self {
        Self {
            source,
            current_seq: Default::default(),
            to_assignment,
            map: Default::default(),
        }
    }

    pub fn get_all(&self, seq: u64) -> HashMap<Key, Value> {
        let mut result = self.map.clone();
        let mut keys_mutated_since_seq = HashSet::new();
        for event in self.source.lock().unwrap().scan(seq, self.current_seq) {
            if let Some((key, _)) = (self.to_assignment)(event) {
                keys_mutated_since_seq.insert(key);
            }
        }

        // for each key mutated since seq, overwrite with the most recent assignment before seq
        for event in self.source.lock().unwrap().scan(0, seq).rev() {
            if keys_mutated_since_seq.is_empty() {
                break;
            }
            if let Some((key, value)) = (self.to_assignment)(event) {
                if keys_mutated_since_seq.remove(&key) {
                    result.insert(key, value);
                }
            }
        }

        // remaining mutated events are deleted
        for key in keys_mutated_since_seq {
            result.remove(&key);
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use crate::dest_log::hash_map_log::HashMapLog;
    use crate::{DestLog, SourceLog, WritableSourceLog};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::source_log::vector_log::VectorLog;

    fn tuple_to_assignment<Kvp: Clone>(kvp: &Kvp) -> Option<Kvp> {
        Some(kvp.clone())
    }

    #[test]
    fn get_at_seq_none() {
        let log = VectorLog::<(&str, &str)>::new();
        let log = Arc::new(Mutex::new(log));
        let hash_map_log = HashMapLog::new(log.clone(), tuple_to_assignment);

        let hash_map = hash_map_log.get_all(4);
        assert_eq!(hash_map, HashMap::from_iter(vec![].into_iter()));
    }

    #[test]
    fn get_at_seq_one() {
        let log = VectorLog::<(&str, &str)>::new();
        let log = Arc::new(Mutex::new(log));
        let mut hash_map_log = HashMapLog::new(log.clone(), tuple_to_assignment);

        let log_current_seq = {
            let mut log = log.lock().unwrap();
            log.write([("key1", "value1")]);
            log.current_seq()
        };
        hash_map_log.update(log_current_seq);
        assert_eq!(log_current_seq, 1);
        assert_eq!(hash_map_log.current_seq(), 1);

        let hash_map = hash_map_log.get_all(4);
        assert_eq!(
            hash_map,
            HashMap::from_iter(vec![("key1", "value1"),].into_iter())
        );
    }

    #[test]
    fn get_at_seq_all() {
        let log = VectorLog::<(&str, &str)>::new();
        let log = Arc::new(Mutex::new(log));
        let mut hash_map_log = HashMapLog::new(log.clone(), tuple_to_assignment);

        let log_current_seq = {
            let mut log = log.lock().unwrap();
            log.write([
                ("key1", "value1"),
                ("key2", "value2"),
                ("key3", "value3"),
                ("key4", "value4"),
            ]);
            log.current_seq()
        };
        hash_map_log.update(log_current_seq);
        assert_eq!(log_current_seq, 4);
        assert_eq!(hash_map_log.current_seq(), 4);

        let hash_map = hash_map_log.get_all(4);
        assert_eq!(
            hash_map,
            HashMap::from_iter(
                vec![
                    ("key1", "value1"),
                    ("key2", "value2"),
                    ("key3", "value3"),
                    ("key4", "value4"),
                ]
                .into_iter()
            )
        );
    }

    #[test]
    fn get_at_seq_partial() {
        let log = VectorLog::<(&str, &str)>::new();
        let log = Arc::new(Mutex::new(log));
        let mut hash_map_log = HashMapLog::new(log.clone(), tuple_to_assignment);

        let log_current_seq = {
            let mut log = log.lock().unwrap();
            log.write([
                ("key1", "value1"),
                ("key2", "value2"),
                ("key3", "value3"),
                ("key4", "value4"),
            ]);
            log.current_seq()
        };
        hash_map_log.update(log_current_seq);
        assert_eq!(log_current_seq, 4);
        assert_eq!(hash_map_log.current_seq(), 4);

        let hash_map = hash_map_log.get_all(3);
        assert_eq!(
            hash_map,
            HashMap::from_iter(
                vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3"),].into_iter()
            )
        );
    }

    #[test]
    fn get_at_seq_partial_overwrite() {
        let log = VectorLog::<(&str, &str)>::new();
        let log = Arc::new(Mutex::new(log));
        let mut hash_map_log = HashMapLog::new(log.clone(), tuple_to_assignment);

        let log_current_seq = {
            let mut log = log.lock().unwrap();
            log.write([
                ("key1", "value1"),
                ("key2", "value2"),
                ("key3", "value3"),
                ("key2", "VALUE2"),
            ]);
            log.current_seq()
        };
        hash_map_log.update(log_current_seq);
        assert_eq!(log_current_seq, 4);
        assert_eq!(hash_map_log.current_seq(), 4);

        let hash_map = hash_map_log.get_all(3);
        assert_eq!(
            hash_map,
            HashMap::from_iter(
                vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3"),].into_iter()
            )
        );
    }
}
