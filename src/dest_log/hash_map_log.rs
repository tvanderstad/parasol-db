use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::{DestLog, SourceLog};

pub enum HashMapUpdate<Key, Value>
where
    Key: Clone + Eq + Hash,
    Value: Clone,
{
    Insert { key: Key, value: Value },
    Remove { key: Key },
    Clear,
}

pub struct HashMapLog<Source, ToAssignment, Key, Value>
where
    Source: SourceLog,
    ToAssignment: Fn(&Source::Event) -> Vec<HashMapUpdate<Key, Value>>,
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
    ToAssignment: Fn(&Source::Event) -> Vec<HashMapUpdate<Key, Value>>,
    Key: Clone + Eq + Hash,
    Value: Clone,
{
    fn update(&mut self, seq: u64) {
        for (_, event) in self.source.lock().unwrap().scan(self.current_seq, seq) {
            for update in (self.to_assignment)(event) {
                match update {
                    HashMapUpdate::Insert { key, value } => {
                        self.map.insert(key, value);
                    }
                    HashMapUpdate::Remove { key } => {
                        self.map.remove(&key);
                    }
                    HashMapUpdate::Clear => {
                        self.map.clear();
                    }
                }
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
    ToAssignment: Fn(&Source::Event) -> Vec<HashMapUpdate<Key, Value>>,
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

    /// Returns a HashMap representing the state of the log at `seq`.
    pub fn get_all(&self, seq: u64) -> HashMap<Key, Value> {
        let mut result = self.map.clone();

        if seq >= self.current_seq {
            // read ahead of current sequence: apply un-applied updates to clone of current state
            for (_, event) in self.source.lock().unwrap().scan(self.current_seq, seq) {
                for update in (self.to_assignment)(event) {
                    match update {
                        HashMapUpdate::Insert { key, value } => {
                            result.insert(key, value);
                        }
                        HashMapUpdate::Remove { key } => {
                            result.remove(&key);
                        }
                        HashMapUpdate::Clear => {
                            result.clear();
                        }
                    }
                }
            }
        } else {
            // read behind current sequence: rewind updates from current state
            let mut keys_mutated_since_seq = HashSet::new();
            let mut cleared = false;

            // which keys have changed since the state we're reading at?
            for (_, event) in self.source.lock().unwrap().scan(seq, self.current_seq) {
                for update in (self.to_assignment)(event) {
                    match update {
                        HashMapUpdate::Insert { key, .. } | HashMapUpdate::Remove { key } => {
                            keys_mutated_since_seq.insert(key);
                        }
                        HashMapUpdate::Clear => {
                            cleared = true;
                            break;
                        }
                    }
                }
            }

            let mut most_recent_clear_seq = 0;
            for (event_seq, event) in self.source.lock().unwrap().scan(0, seq).rev() {
                for update in (self.to_assignment)(event) {
                    match update {
                        // if the map was cleared, we need to rebuild from the most recent clear before read seq
                        HashMapUpdate::Clear => {
                            // remaining keys not inserted until after read seq since prior clear
                            for key in keys_mutated_since_seq {
                                result.remove(&key);
                            }
                            if most_recent_clear_seq != 0 {
                                most_recent_clear_seq = event_seq;
                            }
                            break;
                        }
                        // otherwise, go find the most recent assignment for each changed key
                        HashMapUpdate::Insert { key, value } => {
                            if keys_mutated_since_seq.remove(&key) {
                                result.insert(key, value);
                            }
                        }
                        HashMapUpdate::Remove { key } => {}
                    }
                }

                if cleared {}

                if keys_mutated_since_seq.is_empty() {
                    break;
                }
            }

            // remaining mutated events are deleted
            for key in keys_mutated_since_seq {
                result.remove(&key);
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use crate::dest_log::hash_map_log::{HashMapLog, HashMapUpdate};
    use crate::{DestLog, SourceLog, WritableSourceLog};
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::sync::{Arc, Mutex};

    use crate::source_log::vector_log::VectorLog;

    fn tuple_to_insert<Key: Clone + Eq + Hash, Value: Clone>(
        kvp: &(Key, Value),
    ) -> Vec<HashMapUpdate<Key, Value>> {
        let (key, value) = kvp.clone();
        vec![HashMapUpdate::Insert { key, value }]
    }

    #[test]
    fn get_at_seq_none() {
        let log = VectorLog::<(&str, &str)>::new();
        let log = Arc::new(Mutex::new(log));
        let hash_map_log = HashMapLog::new(log.clone(), tuple_to_insert);

        let hash_map = hash_map_log.get_all(4);
        assert_eq!(hash_map, HashMap::from_iter(vec![].into_iter()));
    }

    #[test]
    fn get_at_seq_one() {
        let log = VectorLog::<(&str, &str)>::new();
        let log = Arc::new(Mutex::new(log));
        let mut hash_map_log = HashMapLog::new(log.clone(), tuple_to_insert);

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
        let mut hash_map_log = HashMapLog::new(log.clone(), tuple_to_insert);

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
        let mut hash_map_log = HashMapLog::new(log.clone(), tuple_to_insert);

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
        let mut hash_map_log = HashMapLog::new(log.clone(), tuple_to_insert);

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
