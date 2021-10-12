#[cfg(test)]
mod test;
mod wg;

cfg_not_async!(
    mod sync_impl;
    pub use sync_impl::Cache;
    use sync_impl::CacheProcessor;
);

cfg_async!(
    mod async_impl;
    pub use async_impl::Cache;
    use async_impl::CacheProcessor;
);

use crate::{
    cache::wg::WaitGroup,
    metrics::MetricType,
    store::UpdateResult,
    ttl::Time,
    utils::{ValueRef, ValueRefMut},
    CacheCallback, CacheError, Coster, DefaultCacheCallback, DefaultCoster, DefaultUpdateValidator,
    Item as CrateItem, KeyBuilder, UpdateValidator,
};
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::time::Duration;

// TODO: find the optimal value for this
const DEFAULT_INSERT_BUF_SIZE: usize = 32 * 1024;
// const DEFAULT_BUFFER_ITEMS: usize = 64;
const DEFAULT_CLEANUP_DURATION: Duration = Duration::from_millis(500);

pub(crate) enum Item<V> {
    New {
        key: u64,
        conflict: u64,
        cost: i64,
        value: V,
        expiration: Time,
    },
    Update {
        key: u64,
        cost: i64,
        external_cost: i64,
    },
    Delete {
        key: u64,
        conflict: u64,
    },
    Wait(WaitGroup),
}

impl<V> Item<V> {
    fn new(key: u64, conflict: u64, cost: i64, val: V, exp: Time) -> Self {
        Self::New {
            key,
            conflict,
            cost,
            value: val,
            expiration: exp,
        }
    }

    fn update(key: u64, cost: i64, external_cost: i64) -> Self {
        Self::Update {
            key,
            cost,
            external_cost,
        }
    }

    fn delete(key: u64, conflict: u64) -> Self {
        Self::Delete { key, conflict }
    }

    fn is_update(&self) -> bool {
        match self {
            Item::Update { .. } => true,
            _ => false,
        }
    }
}

pub struct CacheBuilder<
    K: Hash + Eq,
    V: Send + Sync + 'static,
    KH: KeyBuilder<K>,
    C: Coster<V>,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
    PS: BuildHasher + Clone + 'static,
    ES: BuildHasher + Clone + 'static,
    SS: BuildHasher + Clone + 'static,
> {
    /// metrics determines whether cache statistics are kept during the cache's
    /// lifetime. There *is* some overhead to keeping statistics, so you should
    /// only set this flag to true when testing or throughput performance isn't a
    /// major factor.
    metrics: bool,

    /// ignore_internal_cost set to true indicates to the cache that the cost of
    /// internally storing the value should be ignored. This is useful when the
    /// cost passed to set is not using bytes as units. Keep in mind that setting
    /// this to true will increase the memory usage.
    ignore_internal_cost: bool,

    num_counters: usize,

    max_cost: i64,

    // buffer_items determines the size of Get buffers.
    //
    // Unless you have a rare use case, using `64` as the BufferItems value
    // results in good performance.
    // buffer_items: usize,
    /// `insert_buffer_size` determines the size of insert buffers.
    ///
    /// Default is 32 * 1024 (**TODO:** need to figure out the optimal size.).
    insert_buffer_size: usize,

    /// `cleanup_duration` is the duration for internal store to cleanup expired entry.
    ///
    /// Default is 500ms.
    cleanup_duration: Duration,

    /// key_to_hash is used to customize the key hashing algorithm.
    /// Each key will be hashed using the provided function. If keyToHash value
    /// is not set, the default keyToHash function is used.
    key_to_hash: KH,

    /// cost evaluates a value and outputs a corresponding cost. This function
    /// is ran after insert is called for a new item or an item update with a cost
    /// param of 0.
    coster: Option<C>,

    /// update_validator is called when a value already exists in cache and is being updated.
    update_validator: Option<U>,

    callback: Option<CB>,

    policy_hasher: Option<PS>,

    expiration_hasher: Option<ES>,

    store_hasher: Option<SS>,

    marker_k: PhantomData<fn(K)>,
    marker_v: PhantomData<fn(V)>,
}

impl<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<K>>
    CacheBuilder<
        K,
        V,
        KH,
        DefaultCoster<V>,
        DefaultUpdateValidator<V>,
        DefaultCacheCallback<V>,
        RandomState,
        RandomState,
        RandomState,
    >
{
    pub fn new(num_counters: usize, max_cost: i64, index: KH) -> Self {
        Self {
            num_counters,
            max_cost,
            // buffer_items: DEFAULT_BUFFER_ITEMS,
            insert_buffer_size: DEFAULT_INSERT_BUF_SIZE,
            metrics: false,
            callback: Some(DefaultCacheCallback::default()),
            policy_hasher: Some(RandomState::default()),
            expiration_hasher: Some(RandomState::default()),
            key_to_hash: index,
            update_validator: Some(DefaultUpdateValidator::default()),
            coster: Some(DefaultCoster::default()),
            ignore_internal_cost: false,
            cleanup_duration: DEFAULT_CLEANUP_DURATION,
            marker_k: Default::default(),
            marker_v: Default::default(),
            store_hasher: Some(RandomState::default()),
        }
    }
}

impl<K, V, KH, C, U, CB, PS, ES, SS> CacheBuilder<K, V, KH, C, U, CB, PS, ES, SS>
where
    K: Hash + Eq,
    V: Send + Sync + 'static,
    KH: KeyBuilder<K>,
    C: Coster<V>,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
    PS: BuildHasher + Clone + 'static,
    ES: BuildHasher + Clone + 'static,
    SS: BuildHasher + Clone + 'static,
{
    pub fn set_num_counters(self, num_counters: usize) -> Self {
        Self {
            num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
            store_hasher: self.store_hasher,
        }
    }

    pub fn set_max_cost(self, max_cost: i64) -> Self {
        Self {
            num_counters: self.num_counters,
            max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_buffer_size(self, sz: usize) -> Self {
        Self {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: sz,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_metrics(self, val: bool) -> Self {
        Self {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: val,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_ignore_internal_cost(self, val: bool) -> Self {
        Self {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: val,
            cleanup_duration: self.cleanup_duration,
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_cleanup_duration(self, d: Duration) -> Self {
        Self {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: d,
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_indexer<NKH: KeyBuilder<K>>(
        self,
        index: NKH,
    ) -> CacheBuilder<K, V, NKH, C, U, CB, PS, ES, SS> {
        CacheBuilder {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: index,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_coster<NC: Coster<V>>(
        self,
        coster: NC,
    ) -> CacheBuilder<K, V, KH, NC, U, CB, PS, ES, SS> {
        CacheBuilder {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: Some(coster),
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_update_validator<NU: UpdateValidator<V>>(
        self,
        uv: NU,
    ) -> CacheBuilder<K, V, KH, C, NU, CB, PS, ES, SS> {
        CacheBuilder {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: Some(uv),
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_callback<NCB: CacheCallback<V>>(
        self,
        cb: NCB,
    ) -> CacheBuilder<K, V, KH, C, U, NCB, PS, ES, SS> {
        CacheBuilder {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: Some(cb),
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_policy_hasher<NPS: BuildHasher + Clone + 'static>(
        self,
        hasher: NPS,
    ) -> CacheBuilder<K, V, KH, C, U, CB, NPS, ES, SS> {
        CacheBuilder {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            policy_hasher: Some(hasher),
            expiration_hasher: self.expiration_hasher,
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_expiration_hasher<NES: BuildHasher + Clone + 'static>(
        self,
        hasher: NES,
    ) -> CacheBuilder<K, V, KH, C, U, CB, PS, NES, SS> {
        CacheBuilder {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            policy_hasher: self.policy_hasher,
            expiration_hasher: Some(hasher),
            store_hasher: self.store_hasher,
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }

    pub fn set_store_hasher<NSS: BuildHasher + Clone + 'static>(
        self,
        hasher: NSS,
    ) -> CacheBuilder<K, V, KH, C, U, CB, PS, ES, NSS> {
        CacheBuilder {
            num_counters: self.num_counters,
            max_cost: self.max_cost,
            insert_buffer_size: self.insert_buffer_size,
            metrics: self.metrics,
            callback: self.callback,
            key_to_hash: self.key_to_hash,
            update_validator: self.update_validator,
            coster: self.coster,
            ignore_internal_cost: self.ignore_internal_cost,
            cleanup_duration: self.cleanup_duration,
            policy_hasher: self.policy_hasher,
            expiration_hasher: self.expiration_hasher,
            store_hasher: Some(hasher),
            marker_k: self.marker_k,
            marker_v: self.marker_v,
        }
    }
}

impl<K: Hash + Eq, V: Send + Sync + 'static, KH: KeyBuilder<K>> Cache<K, V, KH> {
    pub fn new(num_counters: usize, max_cost: i64, index: KH) -> Result<Self, CacheError> {
        CacheBuilder::new(num_counters, max_cost, index).finalize()
    }

    pub fn builder(
        num_counters: usize,
        max_cost: i64,
        index: KH,
    ) -> CacheBuilder<
        K,
        V,
        KH,
        DefaultCoster<V>,
        DefaultUpdateValidator<V>,
        DefaultCacheCallback<V>,
        RandomState,
        RandomState,
        RandomState,
    > {
        CacheBuilder::new(num_counters, max_cost, index)
    }
}

impl<K, V, KH, C, U, CB, PS, ES, SS> Cache<K, V, KH, C, U, CB, PS, ES, SS>
where
    K: Hash + Eq,
    V: Send + Sync + 'static,
    KH: KeyBuilder<K>,
    C: Coster<V>,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
    PS: BuildHasher + Clone + 'static,
    ES: BuildHasher + Clone + 'static,
    SS: BuildHasher + Clone + 'static,
{
    /// `get` returns the value (if any) and a boolean representing whether the
    /// value was found or not. The value can be nil and the boolean can be true at
    /// the same time.
    pub fn get(&self, key: &K) -> Option<ValueRef<V, SS>> {
        if self.is_closed.load(Ordering::SeqCst) {
            return None;
        }

        let (index, conflict) = self.key_to_hash.build_key(key);

        match self.store.get(&index, conflict) {
            None => {
                self.metrics.add(MetricType::Hit, index, 1);
                None
            }
            Some(v) => {
                self.metrics.add(MetricType::Miss, index, 1);
                Some(v)
            }
        }
    }

    /// `get_mut` returns the mutable value (if any) and a boolean representing whether the
    /// value was found or not. The value can be nil and the boolean can be true at
    /// the same time.
    pub fn get_mut(&self, key: &K) -> Option<ValueRefMut<V, SS>> {
        if self.is_closed.load(Ordering::SeqCst) {
            return None;
        }

        let (index, conflict) = self.key_to_hash.build_key(key);
        match self.store.get_mut(&index, conflict) {
            None => {
                self.metrics.add(MetricType::Hit, index, 1);
                None
            }
            Some(v) => {
                self.metrics.add(MetricType::Miss, index, 1);
                Some(v)
            }
        }
    }

    // GetTTL returns the TTL for the specified key if the
    // item was found and is not expired.
    pub fn get_ttl(&self, key: &K) -> Option<Duration> {
        let (index, conflict) = self.key_to_hash.build_key(key);
        self.store
            .get(&index, conflict)
            .and_then(|_| self.store.expiration(&index).map(|time| time.get_ttl()))
    }

    pub fn clear(&self) -> Result<(), CacheError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        // stop the process item thread.
        self.clear_tx.send(()).map_err(|e| {
            CacheError::SendError(format!(
                "fail to send clear signal to working thread {}",
                e.to_string()
            ))
        })?;

        self.policy.clear();
        self.store.clear();
        self.metrics.clear();

        Ok(())
    }

    /// `max_cost` returns the max cost of the cache.
    pub fn max_cost(&self) -> i64 {
        self.policy.max_cost()
    }

    /// `update_max_cost` updates the maxCost of an existing cache.
    pub fn update_max_cost(&self, max_cost: i64) {
        self.policy.update_max_cost(max_cost)
    }

    fn update(
        &self,
        key: K,
        val: V,
        cost: i64,
        ttl: Duration,
        only_update: bool,
    ) -> Option<(u64, Item<V>)> {
        let expiration = if ttl.is_zero() {
            Time::now()
        } else {
            Time::now_with_expiration(ttl)
        };

        let (index, conflict) = self.key_to_hash.build_key(&key);

        // cost is eventually updated. The expiration must also be immediately updated
        // to prevent items from being prematurely removed from the map.
        let external_cost = if cost == 0 { self.coster.cost(&val) } else { 0 };
        match self.store.update(index, val, conflict, expiration) {
            UpdateResult::NotExist(v) | UpdateResult::Reject(v) | UpdateResult::Conflict(v) => {
                if only_update {
                    None
                } else {
                    Some((
                        index,
                        Item::new(index, conflict, cost + external_cost, v, expiration),
                    ))
                }
            }
            UpdateResult::Update(v) => {
                self.callback.on_exit(Some(v));
                Some((index, Item::update(index, cost, external_cost)))
            }
        }
    }
}

impl<V, C, U, CB, PS, ES, SS> CacheProcessor<V, C, U, CB, PS, ES, SS>
where
    V: Send + Sync + 'static,
    C: Coster<V>,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
    PS: BuildHasher + Clone + 'static,
    ES: BuildHasher + Clone + 'static,
    SS: BuildHasher + Clone + 'static,
{
    #[inline]
    fn handle_item(&mut self, item: Item<V>) {
        match item {
            Item::New {
                key,
                conflict,
                cost,
                value,
                expiration,
            } => {
                let cost = self.calculate_internal_cost(cost);
                let (victims, added) = self.policy.add(key, cost);
                if added {
                    self.store.insert(key, value, conflict, expiration);
                    self.track_admission(key);
                } else {
                    self.callback.on_reject(CrateItem {
                        val: Some(value),
                        index: key,
                        conflict,
                        cost,
                        exp: expiration,
                    });
                }

                victims.iter().for_each(|victims| {
                    victims.iter().for_each(|victim| {
                        let sitem = self.store.remove(&victim.key, 0);
                        if let Some(sitem) = sitem {
                            let item = CrateItem {
                                index: victim.key,
                                val: Some(sitem.value.into_inner()),
                                cost: victim.cost,
                                conflict: sitem.conflict,
                                exp: sitem.expiration,
                            };
                            self.on_evict(item);
                        }
                    })
                });
            }
            Item::Update {
                key,
                cost,
                external_cost,
            } => {
                let cost = self.calculate_internal_cost(cost) + external_cost;
                self.policy.update(&key, cost)
            }
            Item::Delete { key, conflict } => {
                self.policy.remove(&key); // deals with metrics updates.
                if let Some(sitem) = self.store.remove(&key, conflict) {
                    self.callback.on_exit(Some(sitem.value.into_inner()));
                }
            }
            Item::Wait(wg) => {
                drop(wg);
            }
        }
    }

    #[inline]
    fn on_evict(&mut self, item: CrateItem<V>) {
        self.prepare_evict(&item);
        self.callback.on_evict(item);
    }

    #[inline]
    fn calculate_internal_cost(&self, cost: i64) -> i64 {
        if !self.ignore_internal_cost {
            // Add the cost of internally storing the object.
            cost + (self.item_size as i64)
        } else {
            cost
        }
    }

    #[inline]
    fn track_admission(&mut self, key: u64) {
        let added = self.metrics.add(MetricType::KeyAdd, key, 1);

        if added {
            if self.start_ts.len() > self.num_to_keep {
                self.start_ts = self.start_ts.drain().take(self.num_to_keep - 1).collect();
                self.start_ts.insert(key, Time::now());
            }
        }
    }

    #[inline]
    fn prepare_evict(&mut self, item: &CrateItem<V>) {
        if let Some(ts) = self.start_ts.get(&item.index) {
            self.metrics.track_eviction(ts.elapsed().as_secs() as i64);
            self.start_ts.remove(&item.index);
        }
    }
}

struct CacheCleaner<'a, V, C, U, CB, PS, ES, SS>
where
    V: Send + Sync + 'static,
    C: Coster<V>,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
    PS: BuildHasher + Clone + 'static,
    ES: BuildHasher + Clone + 'static,
    SS: BuildHasher + Clone + 'static,
{
    processor: &'a mut CacheProcessor<V, C, U, CB, PS, ES, SS>,
}

impl<'a, V, C, U, CB, PS, ES, SS> CacheCleaner<'a, V, C, U, CB, PS, ES, SS>
where
    V: Send + Sync + 'static,
    C: Coster<V>,
    U: UpdateValidator<V>,
    CB: CacheCallback<V>,
    PS: BuildHasher + Clone + 'static,
    ES: BuildHasher + Clone + 'static,
    SS: BuildHasher + Clone + 'static,
{
    fn new(processor: &'a mut CacheProcessor<V, C, U, CB, PS, ES, SS>) -> Self {
        Self { processor }
    }

    fn handle_item(&mut self, item: Item<V>) {
        match item {
            Item::New {
                key,
                conflict,
                cost,
                value,
                expiration,
            } => self.processor.callback.on_evict(CrateItem {
                val: Some(value),
                index: key,
                conflict,
                cost,
                exp: expiration,
            }),
            Item::Delete { .. } | Item::Update { .. } => {}
            Item::Wait(wg) => drop(wg),
        }
    }
}
