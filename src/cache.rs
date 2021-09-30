use crossbeam::sync::WaitGroup;
use std::time::Duration;

#[derive(Debug, Copy, Clone)]
#[repr(u8)]
enum ItemFlag {
    New,
    Delete,
    Update,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct ItemMeta {
    key: u64,
    conflict: u64,
    cost: i64,
}

impl ItemMeta {
    pub(crate) fn new(k: u64, cost: i64, conflict: u64) -> Self {
        Self {
            key: k,
            conflict,
            cost
        }
    }
}

pub(crate) struct Item<T> {
    flag: ItemFlag,
    meta: ItemMeta,
    value: Option<T>,
    expiration: Duration,
    wg: WaitGroup,
}
