use std::collections::HashMap;

use crate::{BaseLog, DerivedLog};

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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::base_log::in_memory_log::InMemoryLog;
    use crate::{BaseLog, DerivedLog};

    use super::HashMapLog;
    use super::KeyValueAssignment;

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
