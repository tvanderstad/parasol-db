use std::collections::HashMap;

use crate::{SourceLog, SinkLog};

trait KeyValueAssignment {
    type Key;
    type Value;
    fn get(&self) -> (Self::Key, Self::Value);
}

impl<K: Clone, V: Clone> KeyValueAssignment for (K, V) {
    type Key = K;
    type Value = V;
    fn get(&self) -> (Self::Key, Self::Value) {
        let (key, value) = self;
        (key.clone(), value.clone())
    }
}

struct HashMapLog<'a, Source: SourceLog> {
    source: &'a Source,
}

impl<'a, Source, Assignment: KeyValueAssignment + 'a> SinkLog for HashMapLog<'a, Source>
where
    Source: SourceLog<Event<'a> = &'a Assignment>,
    Assignment::Key: Eq + std::hash::Hash,
{
    type AtSeq = HashMap<Assignment::Key, Assignment::Value>;
    fn seq(&self, seq: u64) -> HashMap<Assignment::Key, Assignment::Value> {
        let mut result = HashMap::<Assignment::Key, Assignment::Value>::new();
        for key_value_assignment in self.source.scan(0, seq) {
            let (key, value) = key_value_assignment.get();
            result.insert(key, value);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::source_log::vector_log::VectorLog;
    use crate::SinkLog;

    use super::HashMapLog;

    #[test]
    fn get_at_seq_none() {
        let log = VectorLog::<(&str, &str)>::new();
        let hash_map_log = HashMapLog { source: &log };
        let hash_map = hash_map_log.seq(4);
        assert_eq!(hash_map, HashMap::from_iter(vec![].into_iter()));
    }

    #[test]
    fn get_at_seq_one() {
        let mut log = VectorLog::<(&str, &str)>::new();
        assert_eq!(log.write(("key1", "value1")), 1);
        let hash_map_log = HashMapLog { source: &log };
        let hash_map = hash_map_log.seq(4);
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
        let hash_map_log = HashMapLog { source: &log };
        let hash_map = hash_map_log.seq(4);
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
        let hash_map_log = HashMapLog { source: &log };
        let hash_map = hash_map_log.seq(3);
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
        let hash_map_log = HashMapLog { source: &log };
        let hash_map = hash_map_log.seq(3);
        assert_eq!(
            hash_map,
            HashMap::from_iter(
                vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3"),].into_iter()
            )
        );
    }
}
