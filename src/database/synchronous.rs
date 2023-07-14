use crate::{Index, Table, View};

// SynchronousDatabase must be a proc macro; write should write to all writeables, update to updatables, current_seq to min of current seqs

pub struct SynchronousDatabase<Base: Table, Index: crate::Index> {
    base: Base,
    dests: Index,
}

pub trait SynchronousDatabase {
    type Base: Table;
    type Dest: Index;

    fn base(&mut self) -> &mut Self::Base;
    fn dests(&mut self) -> Vec<&mut Self::Dest>;

    fn write<Iter: IntoIterator<Item = <Self::Base as View>::Event>>(&mut self, events: Iter) {
        let log_current_seq = {
            self.base().write(events);
            self.base().next_seq()
        };
        for derived in self.dests() {
            derived.update(log_current_seq);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::database::synchronous::SynchronousDatabase;
    use crate::dest_log::hash_map_log::{HashMapIndex, HashMapUpdate};
    use crate::source_log::vector_log::VectorLog;
    use crate::{Index, Table, View};
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::sync::{Arc, Mutex};

    fn tuple_to_insert<Key: Clone + Eq + Hash, Value: Clone>(
        kvp: &(Key, Value),
    ) -> Vec<HashMapUpdate<Key, Value>> {
        let (key, value) = kvp.clone();
        vec![HashMapUpdate::Insert { key, value }]
    }

    struct NoDestsDb<Base: Table> {
        base: Base,
    }

    impl<Base: Table> SynchronousDatabase for NoDestsDb<Base> {
        type Base = Base;
        type Dest = !;

        fn base(&mut self) -> &mut Self::Base {
            &mut self.base
        }

        fn dests(&mut self) -> Vec<&mut Self::Dest> {
            Default::default()
        }
    }

    #[test]
    fn no_dests() {
        let base = VectorLog::<(&str, &str)>::new();
        let mut db = NoDestsDb { base };

        db.write([("key1", "value1"), ("key2", "value2"), ("key3", "value3"), ("key4", "value4")]);

        assert_eq!(db.base.next_seq(), 4);
    }

    struct HmDb<'base, Base: Table, Dest: View> {
        base: Base,
        dest: Dest,
    }

    impl<'base, Base: Table, Key: Clone + Eq + Hash, Value: Clone> SynchronousDatabase
        for HmDb<'base, Base, Key, Value>
    {
        type Base = Base;
        type Dest = HashMapIndex;

        fn base(&mut self) -> &mut Self::Base {
            &mut self.base
        }

        fn dests(&mut self) -> Vec<&mut Self::Dest> {
            Default::default()
        }
    }

    #[test]
    fn one_dest() {
        let base = VectorLog::<(&str, &str)>::new();
        let mut db: SynchronousDatabase<_, Dest<'_, VectorLog<(&str, &str)>>> =
            SynchronousDatabase { base, derived: vec![] };
        let hash_map_log = HashMapIndex::new(&db.base, tuple_to_insert);

        db.derived.push(Dest::HashMap(hash_map_log));

        db.write([("key1", "value1"), ("key2", "value2"), ("key3", "value3"), ("key4", "value4")]);

        // assert_eq!(scheduler.base.next_seq(), 4);
        // assert_eq!(scheduler.derived[0].current_seq(), 4);
        //
        // assert_eq!(
        //     hash_map_log.get_all(4),
        //     HashMap::from_iter(
        //         vec![
        //             ("key1", "value1"),
        //             ("key2", "value2"),
        //             ("key3", "value3"),
        //             ("key4", "value4"),
        //         ]
        //         .into_iter()
        //     )
        // );
    }

    enum Dest<'s, Base: View> {
        HashMap(HashMapIndex<'s, Base, &'static str, &'static str>),
    }
}
