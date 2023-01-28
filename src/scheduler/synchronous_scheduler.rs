use crate::log_list::DestListNode;
use crate::{SourceLog, WritableSourceLog};

pub struct SynchronousScheduler<Base: WritableSourceLog, DestList: DestListNode> {
    base: Base,
    dest_list: DestList,
}

impl<Base: WritableSourceLog, SdlList: DestListNode> SourceLog
    for SynchronousScheduler<Base, SdlList>
{
    type Event = Base::Event;
    type Iterator<'iter> = Base::Iterator<'iter> where Self: 'iter;

    fn scan(&self, min_seq_exclusive: u64, max_seq_inclusive: u64) -> Self::Iterator<'_> {
        self.base.scan(min_seq_exclusive, max_seq_inclusive)
    }

    fn current_seq(&self) -> u64 {
        self.base.current_seq()
    }
}

impl<Base: WritableSourceLog, SdlList: DestListNode> WritableSourceLog
    for SynchronousScheduler<Base, SdlList>
{
    fn write<Iter: IntoIterator<Item = Self::Event>>(&mut self, events: Iter) {
        self.base.write(events);
        self.dest_list.update_all(self.base.current_seq());
    }
}

#[cfg(test)]
mod tests {
    // use crate::dest_log::hash_map_log::HashMapLog;
    use crate::scheduler::synchronous_scheduler::SynchronousScheduler;
    use crate::source_log::vector_log::VectorLog;
    use crate::WritableSourceLog;

    // fn tuple_to_assignment<Kvp: Clone>(kvp: &Kvp) -> Option<Kvp> {
    //     Some(kvp.clone())
    // }

    #[test]
    fn no_dests() {
        let log = VectorLog::<(&str, &str)>::new();
        let mut scheduler = SynchronousScheduler {
            base: log,
            dest_list: (),
        };
        scheduler.write([
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
            ("key4", "value4"),
        ]);
    }
}
