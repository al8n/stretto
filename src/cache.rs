use crate::ttl::Time;
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
            cost,
        }
    }
}

pub(crate) struct Item<V> {
    flag: ItemFlag,
    meta: ItemMeta,
    pub value: Option<V>,
    expiration: Time,
    wg: WaitGroup,
}

impl<V> Item<V> {
    pub fn get_key(&self) -> u64 {
        self.meta.key
    }

    pub fn get_conflict(&self) -> u64 {
        self.meta.conflict
    }

    pub fn get_expiration(&self) -> Time {
        self.expiration
    }
}
