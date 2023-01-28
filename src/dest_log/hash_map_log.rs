use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use crate::{DestLog, SourceLog};

pub struct HashMapLog<'a, Source, Key, Value, ToAssignment>
where
    Source: SourceLog,
    Key: Clone + Eq + Hash,
    Value: Clone,
    ToAssignment: Fn(&'a Source::Event) -> Option<(Key, Value)>, // todo: support delete, clear, etc
{
    source: &'a Source,
    to_assignment: ToAssignment,
    current_seq: u64,
    map: HashMap<Key, Value>,
}

impl<'a, Source, Key, Value, ToAssignment> DestLog
    for HashMapLog<'a, Source, Key, Value, ToAssignment>
where
    Source: SourceLog,
    Key: Clone + Eq + Hash,
    Value: Clone,
    ToAssignment: Fn(&'a Source::Event) -> Option<(Key, Value)>,
{
    fn update(&mut self, seq: u64) {
        for event in self.source.scan(self.current_seq, seq) {
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

impl<'a, Source, Key, Value, ToAssignment> HashMapLog<'a, Source, Key, Value, ToAssignment>
where
    Source: SourceLog,
    Key: Clone + Eq + Hash,
    Value: Clone,
    ToAssignment: Fn(&'a Source::Event) -> Option<(Key, Value)>,
{
    fn new(source: &'a Source, to_assignment: ToAssignment) -> Self {
        Self {
            source,
            to_assignment,
            current_seq: 0,
            map: HashMap::new(),
        }
    }

    fn get_all(&self, seq: u64) -> HashMap<Key, Value> {
        let mut result = self.map.clone();
        let mut keys_mutated_since_seq = HashSet::new();
        for event in self.source.scan(seq, self.current_seq) {
            if let Some((key, _)) = (self.to_assignment)(event) {
                keys_mutated_since_seq.insert(key);
            }
        }

        // for each key mutated since seq, overwrite with the most recent assignment before seq
        for event in self.source.scan(0, seq).rev() {
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
    use crate::{DestLog, SourceLog};
    use std::collections::HashMap;

    use crate::source_log::vector_log::VectorLog;

    fn tuple_to_assignment<Kvp: Clone>(kvp: &Kvp) -> Option<Kvp> {
        Some(kvp.clone())
    }

    #[test]
    fn get_at_seq_none() {
        let log = VectorLog::<(&str, &str)>::new();
        let hash_map_log = HashMapLog::new(&log, tuple_to_assignment);
        let hash_map = hash_map_log.get_all(4);
        assert_eq!(hash_map, HashMap::from_iter(vec![].into_iter()));
    }

    #[test]
    fn get_at_seq_one() {
        let mut log = VectorLog::<(&str, &str)>::new();
        assert_eq!(log.write(("key1", "value1")), 1);
        let mut hash_map_log = HashMapLog::new(&log, tuple_to_assignment);
        hash_map_log.update(log.current_seq());
        let hash_map = hash_map_log.get_all(4);
        assert_eq!(
            hash_map,
            HashMap::from_iter(vec![("key1", "value1"),].into_iter())
        );
    }

    #[test]
    fn get_at_seq_all() {
        let mut log = VectorLog::<(&str, &str)>::new();
        assert_eq!(log.write(("key1", "value1")), 1);
        assert_eq!(log.write(("key2", "value2")), 2);
        assert_eq!(log.write(("key3", "value3")), 3);
        assert_eq!(log.write(("key4", "value4")), 4);
        let mut hash_map_log = HashMapLog::new(&log, tuple_to_assignment);
        hash_map_log.update(log.current_seq());
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
        let mut log = VectorLog::<(&str, &str)>::new();
        assert_eq!(log.write(("key1", "value1")), 1);
        assert_eq!(log.write(("key2", "value2")), 2);
        assert_eq!(log.write(("key3", "value3")), 3);
        assert_eq!(log.write(("key4", "value4")), 4);
        let mut hash_map_log = HashMapLog::new(&log, tuple_to_assignment);
        hash_map_log.update(log.current_seq());
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
        let mut log = VectorLog::<(&str, &str)>::new();
        assert_eq!(log.write(("key1", "value1")), 1);
        assert_eq!(log.write(("key2", "value2")), 2);
        assert_eq!(log.write(("key3", "value3")), 3);
        assert_eq!(log.write(("key2", "VALUE2")), 4);
        let mut hash_map_log = HashMapLog::new(&log, tuple_to_assignment);
        hash_map_log.update(log.current_seq());
        let hash_map = hash_map_log.get_all(3);
        assert_eq!(
            hash_map,
            HashMap::from_iter(
                vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3"),].into_iter()
            )
        );
    }
}
