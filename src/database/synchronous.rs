use crate::{DestLog, WritableSourceLog};
use std::sync::{Arc, Mutex};

pub struct SynchronousDatabase<Base>
where
    Base: WritableSourceLog,
{
    base: Arc<Mutex<Base>>,
    derived: Vec<Arc<Mutex<dyn DestLog>>>,
}

impl<Base> SynchronousDatabase<Base>
where
    Base: WritableSourceLog,
{
    fn write<Iter: IntoIterator<Item = Base::Event>>(&mut self, events: Iter) {
        let log_current_seq = {
            let mut base = self.base.lock().unwrap();
            base.write(events);
            base.current_seq()
        };
        for derived in &mut self.derived {
            let mut derived = derived.lock().unwrap();
            derived.update(log_current_seq);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::database::synchronous::SynchronousDatabase;
    use crate::dest_log::hash_map_log::{HashMapLog, HashMapUpdate};
    use crate::source_log::vector_log::VectorLog;
    use crate::{DestLog, SourceLog};
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::sync::{Arc, Mutex};

    fn tuple_to_insert<Key: Clone + Eq + Hash, Value: Clone>(
        kvp: &(Key, Value),
    ) -> Vec<HashMapUpdate<Key, Value>> {
        let (key, value) = kvp.clone();
        vec![HashMapUpdate::Insert { key, value }]
    }

    #[test]
    fn no_dests() {
        let log = VectorLog::<(&str, &str)>::new();
        let log = Arc::new(Mutex::new(log));
        let mut scheduler = SynchronousDatabase {
            base: log.clone(),
            derived: Default::default(),
        };
        scheduler.write([
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
            ("key4", "value4"),
        ]);

        {
            let log = log.lock().unwrap();
            assert_eq!(log.current_seq(), 4);
        }
    }

    #[test]
    fn one_dest() {
        let log = VectorLog::<(&str, &str)>::new();
        let log = Arc::new(Mutex::new(log));
        let hash_map_log = HashMapLog::new(log.clone(), tuple_to_insert);
        let hash_map_log = Arc::new(Mutex::new(hash_map_log));

        let mut scheduler = SynchronousDatabase {
            base: log.clone(),
            derived: vec![hash_map_log.clone()],
        };
        scheduler.write([
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
            ("key4", "value4"),
        ]);

        {
            let log = log.lock().unwrap();
            assert_eq!(log.current_seq(), 4);
        }
        {
            let hash_map_log = hash_map_log.lock().unwrap();
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
    }
}
