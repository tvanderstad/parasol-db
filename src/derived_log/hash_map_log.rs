use std::collections::HashMap;

use crate::{BaseLog, DerivedLog};

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

struct HashMapLog<'a, Base: BaseLog> {
    base: &'a Base,
}

impl<'a, Base, Assignment: KeyValueAssignment + 'a> DerivedLog for HashMapLog<'a, Base>
where
    Base: BaseLog<Event<'a> = &'a Assignment>,
    Assignment::Key: std::cmp::Eq + std::hash::Hash,
{
    type Derived = HashMap<Assignment::Key, Assignment::Value>;
    fn get_at_seq(&self, seq: u64) -> HashMap<Assignment::Key, Assignment::Value> {
        let mut result = HashMap::<Assignment::Key, Assignment::Value>::new();
        for key_value_assignment in self.base.scan(0, seq) {
            let (key, value) = key_value_assignment.get();
            result.insert(key, value);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::base_log::vector_log::VectorLog;
    use crate::DerivedLog;

    use super::HashMapLog;

    #[test]
    fn get_at_seq_none() {
        let log = VectorLog::<(&str, &str)>::new();
        let hash_map_log = HashMapLog { base: &log };
        let hash_map = hash_map_log.get_at_seq(4);
        assert_eq!(hash_map, HashMap::from_iter(vec![].into_iter()));
    }

    #[test]
    fn get_at_seq_one() {
        let mut log = VectorLog::<(&str, &str)>::new();
        assert_eq!(log.write(("key1", "value1")), 1);
        let hash_map_log = HashMapLog { base: &log };
        let hash_map = hash_map_log.get_at_seq(4);
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
        let hash_map_log = HashMapLog { base: &log };
        let hash_map = hash_map_log.get_at_seq(4);
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
        let hash_map_log = HashMapLog { base: &log };
        let hash_map = hash_map_log.get_at_seq(3);
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
        let hash_map_log = HashMapLog { base: &log };
        let hash_map = hash_map_log.get_at_seq(3);
        assert_eq!(
            hash_map,
            HashMap::from_iter(
                vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3"),].into_iter()
            )
        );
    }
}
