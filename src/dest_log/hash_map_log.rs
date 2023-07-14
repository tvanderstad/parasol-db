use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use crate::{Index, Seq, View};

#[derive(Clone)]
pub enum HashMapUpdate<Key, Value>
where
    Key: Clone + Eq + Hash,
    Value: Clone,
{
    Insert { key: Key, value: Value },
    Remove { key: Key },
    Clear,
}

pub struct HashMapIndex<'s, Source, Key, Value>
where
    Source: View,
    Key: Clone + Eq + Hash,
    Value: Clone,
{
    source: &'s Source,
    current_seq: Seq,
    to_assignment: fn(&'s Source::Event) -> Vec<HashMapUpdate<Key, Value>>,
    map: HashMap<Key, Value>,
}

impl<'s, Source, Key, Value> Index for HashMapIndex<'s, Source, Key, Value>
where
    Source: View,
    Key: Clone + Eq + Hash,
    Value: Clone,
    Self: Sync,
{
    fn update(&mut self, seq: Seq) {
        for (_, event) in self.source.scan(self.current_seq, seq) {
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

    fn current_seq(&self) -> Seq {
        self.current_seq
    }
}

impl<'s, Source, Key, Value> HashMapIndex<'s, Source, Key, Value>
where
    Source: View,
    Key: Clone + Eq + Hash,
    Value: Clone,
{
    pub fn new(
        source: &'s Source, to_assignment: fn(&Source::Event) -> Vec<HashMapUpdate<Key, Value>>,
    ) -> Self {
        Self { source, current_seq: Default::default(), to_assignment, map: Default::default() }
    }

    /// Returns the value associated with a single key at `seq`.
    pub fn get(&self, seq: Seq, key: &Key) -> Option<Value> {
        if seq >= self.current_seq {
            // read backwards from read seq to current seq
            for (_, event) in self.source.scan(self.current_seq, seq).rev() {
                for update in (self.to_assignment)(event).into_iter().rev() {
                    match update {
                        HashMapUpdate::Insert { key: update_key, value } => {
                            if key == &update_key {
                                // most recent modification to key was insertion of this value
                                return Some(value);
                            }
                        }
                        HashMapUpdate::Remove { key: update_key } => {
                            if key == &update_key {
                                // most recent modification to key was removal
                                return None;
                            }
                        }
                        HashMapUpdate::Clear => {
                            // most recent modification to key was clear
                            return None;
                        }
                    }
                }
            }

            // if none of the operations ahead of seq pertain to key, return the value in the map
            self.map.get(key).cloned()
        } else {
            // read backwards from current seq to read seq to find most recent modification (if any) since current seq
            let mut modified = false;
            for (_, event) in self.source.scan(seq, self.current_seq).rev() {
                for update in (self.to_assignment)(event).into_iter().rev() {
                    match update {
                        HashMapUpdate::Insert { key: update_key, .. } => {
                            if key == &update_key {
                                // overwritten since current seq
                                modified = true;
                                break;
                            }
                        }
                        HashMapUpdate::Remove { key: update_key } => {
                            if key == &update_key {
                                // removed since current seq
                                modified = true;
                                break;
                            }
                        }
                        HashMapUpdate::Clear => {
                            // cleared since current seq
                            modified = true;
                            break;
                        }
                    }
                }
            }

            if modified {
                // if it's been modified, read backwards from seq until we find its most recent modification
                for (_, event) in self.source.scan(0, seq).rev() {
                    for update in (self.to_assignment)(event).into_iter().rev() {
                        match update {
                            HashMapUpdate::Insert { key: update_key, value } => {
                                if key == &update_key {
                                    // most recent modification is insertion
                                    return Some(value);
                                }
                            }
                            HashMapUpdate::Remove { key: update_key } => {
                                if key == &update_key {
                                    // most recent modification is removal
                                    return None;
                                }
                            }
                            HashMapUpdate::Clear => {
                                // most recent modification is clear
                                return None;
                            }
                        }
                    }
                }

                // this key was not modified before seq (worst case performance)
                None
            } else {
                // if it hasn't been modified, return the current value
                self.map.get(key).cloned()
            }
        }
    }

    /// Returns the full map at `seq`.
    pub fn get_all(&self, seq: Seq) -> HashMap<Key, Value> {
        if seq >= self.current_seq {
            // read ahead of current sequence: apply un-applied updates to clone of current state
            let mut result = self.map.clone();
            for (_, event) in self.source.scan(self.current_seq, seq) {
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
            result
        } else {
            // read behind current sequence: rewind updates from current state
            let mut modified_keys = HashSet::new();
            let mut cleared = false;

            // determine which keys have changed since the state we're reading at
            // if the map was cleared, that means all keys have been modified, even ones not in the current map
            for (_, event) in self.source.scan(seq, self.current_seq) {
                for update in (self.to_assignment)(event) {
                    match update {
                        HashMapUpdate::Insert { key, .. } | HashMapUpdate::Remove { key } => {
                            modified_keys.insert(key);
                        }
                        HashMapUpdate::Clear => {
                            cleared = true;
                            break;
                        }
                    }
                }
            }

            if cleared {
                // if the state was cleared since seq, rebuild it from the most recent clear before seq
                let mut removed_keys = HashSet::new();
                let mut result = HashMap::new();
                for (_, event) in self.source.scan(0, seq).rev() {
                    for update in (self.to_assignment)(event).into_iter().rev() {
                        match update {
                            HashMapUpdate::Clear => {
                                // this is the most recent clear, the one we needed to rebuild from
                                break;
                            }
                            HashMapUpdate::Insert { key, value } => {
                                // only the most recent insert counts, and only if it wasn't removed after
                                if !result.contains_key(&key) && !removed_keys.contains(&key) {
                                    result.insert(key, value);
                                }
                            }
                            HashMapUpdate::Remove { key } => {
                                // note removed keys so they're not inserted if the removal happened after the insertion
                                removed_keys.insert(key);
                            }
                        }
                    }
                }
                result
            } else {
                // otherwise, look back from seq for the most recent modification to each modified key
                let mut result = self.map.clone();
                for (_, event) in self.source.scan(0, seq).rev() {
                    for update in (self.to_assignment)(event).into_iter().rev() {
                        match update {
                            HashMapUpdate::Clear => {
                                // remaining keys not inserted between this clear and seq
                                for key in &modified_keys {
                                    result.remove(key);
                                }
                            }
                            HashMapUpdate::Insert { key, value } => {
                                // only the most recent insert counts, and only if it wasn't removed more recently
                                if modified_keys.remove(&key) {
                                    result.insert(key, value);
                                }
                            }
                            HashMapUpdate::Remove { key } => {
                                // note removed keys so they're not inserted if the removal happened after the insertion
                                modified_keys.remove(&key);
                            }
                        }
                    }

                    // once we find all modified keys, we're done
                    if modified_keys.is_empty() {
                        return result;
                    }
                }

                // remaining keys not inserted between 0 and seq
                for key in &modified_keys {
                    result.remove(key);
                }

                // at least one key modified after seq was not modified before seq (worst case performance)
                result
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dest_log::hash_map_log::{HashMapIndex, HashMapUpdate};
    use crate::{Index, Table, View};
    use std::collections::HashMap;
    use std::hash::Hash;

    use crate::source_log::vector_log::VectorLog;

    fn tuple_to_insert<Key: Clone + Eq + Hash, Value: Clone>(
        kvp: &(Key, Value),
    ) -> Vec<HashMapUpdate<Key, Value>> {
        let (key, value) = kvp.clone();
        vec![HashMapUpdate::Insert { key, value }]
    }

    #[test]
    fn get_all() {
        let log = VectorLog::<(&str, &str)>::new();
        let mut hash_map_log = HashMapIndex::new(&log, tuple_to_insert);

        let log_current_seq = {
            let mut log = log;
            log.write([
                ("key1", "value1"),
                ("key2", "value2"),
                ("key3", "value3"),
                ("key4", "value4"),
            ]);
            log.next_seq()
        };
        hash_map_log.update(log_current_seq);
        assert_eq!(log_current_seq, 4);
        assert_eq!(hash_map_log.current_seq(), 4);

        assert_eq!(hash_map_log.get_all(0), HashMap::from_iter(vec![].into_iter()));
        assert_eq!(
            hash_map_log.get_all(1),
            HashMap::from_iter(vec![("key1", "value1")].into_iter())
        );
        assert_eq!(
            hash_map_log.get_all(2),
            HashMap::from_iter(vec![("key1", "value1"), ("key2", "value2")].into_iter())
        );
        assert_eq!(
            hash_map_log.get_all(3),
            HashMap::from_iter(
                vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")].into_iter()
            )
        );
        assert_eq!(
            hash_map_log.get_all(4),
            HashMap::from_iter(
                vec![
                    ("key1", "value1"),
                    ("key2", "value2"),
                    ("key3", "value3"),
                    ("key4", "value4")
                ]
                .into_iter()
            )
        );
    }

    #[test]
    fn get_all_overwrite() {
        let log = VectorLog::<(&str, &str)>::new();
        let mut hash_map_log = HashMapIndex::new(&log, tuple_to_insert);

        let log_current_seq = {
            let mut log = log;
            log.write([
                ("key1", "value1"),
                ("key2", "value2"),
                ("key3", "value3"),
                ("key2", "VALUE2"),
            ]);
            log.next_seq()
        };
        hash_map_log.update(log_current_seq);
        assert_eq!(log_current_seq, 4);
        assert_eq!(hash_map_log.current_seq(), 4);

        assert_eq!(hash_map_log.get_all(0), HashMap::from_iter(vec![].into_iter()));
        assert_eq!(
            hash_map_log.get_all(1),
            HashMap::from_iter(vec![("key1", "value1")].into_iter())
        );
        assert_eq!(
            hash_map_log.get_all(2),
            HashMap::from_iter(vec![("key1", "value1"), ("key2", "value2")].into_iter())
        );
        assert_eq!(
            hash_map_log.get_all(3),
            HashMap::from_iter(
                vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")].into_iter()
            )
        );
        assert_eq!(
            hash_map_log.get_all(4),
            HashMap::from_iter(
                vec![("key1", "value1"), ("key2", "VALUE2"), ("key3", "value3")].into_iter()
            )
        );
    }

    #[test]
    fn get_all_clear() {
        let log = VectorLog::<HashMapUpdate<&str, &str>>::new();
        let mut hash_map_log = HashMapIndex::new(&log, |assignment| vec![assignment.clone()]);

        let log_current_seq = {
            let mut log = log;
            log.write([
                HashMapUpdate::Insert { key: "key1", value: "value1" },
                HashMapUpdate::Insert { key: "key2", value: "value2" },
                HashMapUpdate::Clear,
                HashMapUpdate::Insert { key: "key3", value: "value3" },
            ]);
            log.next_seq()
        };
        hash_map_log.update(log_current_seq);
        assert_eq!(log_current_seq, 4);
        assert_eq!(hash_map_log.current_seq(), 4);

        assert_eq!(hash_map_log.get_all(0), HashMap::from_iter(vec![].into_iter()));
        assert_eq!(
            hash_map_log.get_all(1),
            HashMap::from_iter(vec![("key1", "value1")].into_iter())
        );
        assert_eq!(
            hash_map_log.get_all(2),
            HashMap::from_iter(vec![("key1", "value1"), ("key2", "value2")].into_iter())
        );
        assert_eq!(hash_map_log.get_all(3), HashMap::from_iter(vec![].into_iter()));
        assert_eq!(
            hash_map_log.get_all(4),
            HashMap::from_iter(vec![("key3", "value3")].into_iter())
        );
    }

    #[test]
    fn get_all_clear_multiple_modifications() {
        let log = VectorLog::<HashMapUpdate<&str, &str>>::new();
        let mut hash_map_log = HashMapIndex::new(&log, |assignment| vec![assignment.clone()]);

        let log_current_seq = {
            let mut log = log;
            log.write([
                HashMapUpdate::Insert { key: "key1", value: "value1" },
                HashMapUpdate::Clear,
                HashMapUpdate::Insert { key: "key1", value: "value1" },
                HashMapUpdate::Insert { key: "key1", value: "VALUE1" },
            ]);
            log.next_seq()
        };
        hash_map_log.update(log_current_seq);
        assert_eq!(log_current_seq, 4);
        assert_eq!(hash_map_log.current_seq(), 4);

        assert_eq!(hash_map_log.get_all(0), HashMap::from_iter(vec![].into_iter()));
        assert_eq!(
            hash_map_log.get_all(1),
            HashMap::from_iter(vec![("key1", "value1")].into_iter())
        );
        assert_eq!(hash_map_log.get_all(2), HashMap::from_iter(vec![].into_iter()));
        assert_eq!(
            hash_map_log.get_all(3),
            HashMap::from_iter(vec![("key1", "value1")].into_iter())
        );
        assert_eq!(
            hash_map_log.get_all(4),
            HashMap::from_iter(vec![("key1", "VALUE1")].into_iter())
        );
    }
}
