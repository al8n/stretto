use crossbeam::sync::WaitGroup;
use std::time::Duration;

#[derive(Debug, Copy, Clone)]
#[repr(u8)]
enum ItemFlag {
    New,
    Delete,
    Update,
}

pub(crate) struct Item<T> {
    flag: ItemFlag,
    key: u64,
    conflict: u64,
    value: Option<T>,
    cost: i64,
    expiration: Duration,
    wg: WaitGroup,
}
