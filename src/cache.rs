macro_rules! impl_builder {
    ($ty: ident) => {
        impl<K, V, KH, C, U, CB, S> $ty<K, V, KH, C, U, CB, S>
        where
            K: Hash + Eq,
            V: Send + Sync + 'static,
            KH: KeyBuilder<K>,
            C: Coster<V>,
            U: UpdateValidator<V>,
            CB: CacheCallback<V>,
            S: BuildHasher + Clone + 'static,
        {
            /// Set the number of counters for the Cache.
            ///
            /// `num_counters` is the number of 4-bit access counters to keep for admission and eviction.
            /// Dgraph's developers have seen good performance in setting this to 10x the number of items
            /// you expect to keep in the cache when full.
            ///
            /// For example, if you expect each item to have a cost of 1 and `max_cost` is 100, set `num_counters` to 1,000.
            /// Or, if you use variable cost values but expect the cache to hold around 10,000 items when full,
            /// set num_counters to 100,000. The important thing is the *number of unique items* in the full cache,
            /// not necessarily the `max_cost` value.
            #[inline]
            pub fn set_num_counters(self, num_counters: usize) -> Self {
                Self {
                    inner: self.inner.set_num_counters(num_counters),
                }
            }

            /// Set the max_cost for the Cache.
            ///
            /// `max_cost` is how eviction decisions are made. For example, if max_cost is 100 and a new item
            /// with a cost of 1 increases total cache cost to 101, 1 item will be evicted.
            ///
            /// `max_cost` can also be used to denote the max size in bytes. For example,
            /// if max_cost is 1,000,000 (1MB) and the cache is full with 1,000 1KB items,
            /// a new item (that's accepted) would cause 5 1KB items to be evicted.
            ///
            /// `max_cost` could be anything as long as it matches how you're using the cost values when calling `insert`.
            #[inline]
            pub fn set_max_cost(self, max_cost: i64) -> Self {
                Self {
                    inner: self.inner.set_max_cost(max_cost),
                }
            }

            /// Set the insert buffer size for the Cache.
            ///
            /// `buffer_size` is the size of the insert buffers. The Dgraph's developers find that 32 * 1024 gives a good performance.
            ///
            /// If for some reason you see insert performance decreasing with lots of contention (you shouldn't),
            /// try increasing this value in increments of 32 * 1024.
            /// This is a fine-tuning mechanism and you probably won't have to touch this.
            #[inline]
            pub fn set_buffer_size(self, sz: usize) -> Self {
                Self {
                    inner: self.inner.set_buffer_size(sz),
                }
            }

            /// Set whether record the metrics or not.
            ///
            /// Metrics is true when you want real-time logging of a variety of stats.
            /// The reason this is a Builder flag is because there's a 10% throughput performance overhead.
            #[inline]
            pub fn set_metrics(self, val: bool) -> Self {
                Self {
                    inner: self.inner.set_metrics(val),
                }
            }

            /// Set whether ignore the internal cost or not.
            ///
            /// By default, when `insert` a value in the Cache, there will always 56 for internal cost,
            /// because the size of stored item in Cache is 56(excluding the size of value).
            /// Set it to true to ignore the internal cost.
            #[inline]
            pub fn set_ignore_internal_cost(self, val: bool) -> Self {
                Self {
                    inner: self.inner.set_ignore_internal_cost(val),
                }
            }

            /// Set the cleanup ticker for Cache, each tick the Cache will clean the expired entries.
            #[inline]
            pub fn set_cleanup_duration(self, d: Duration) -> Self {
                Self {
                    inner: self.inner.set_cleanup_duration(d),
                }
            }

            /// Set the [`KeyBuilder`] for the Cache
            ///
            /// [`KeyBuilder`] is the hashing algorithm used for every key. In Stretto, the Cache will never store the real key.
            /// The key will be processed by [`KeyBuilder`]. Stretto has two default built-in key builder,
            /// one is [`TransparentKeyBuilder`], the other is [`DefaultKeyBuilder`]. If your key implements [`TransparentKey`] trait,
            /// you can use [`TransparentKeyBuilder`] which is faster than [`DefaultKeyBuilder`]. Otherwise, you should use [`DefaultKeyBuilder`]
            /// You can also write your own key builder for the Cache, by implementing [`KeyBuilder`] trait.
            ///
            /// Note that if you want 128bit hashes you should use the full `(u64, u64)`,
            /// otherwise just fill the `u64` at the `0` position, and it will behave like
            /// any 64bit hash.
            ///
            /// [`KeyBuilder`]: trait.KeyBuilder.html
            /// [`TransparentKey`]: trait.TransparentKey.html
            /// [`TransparentKeyBuilder`]: struct.TransparentKeyBuilder.html
            /// [`DefaultKeyBuilder`]: struct.DefaultKeyBuilder.html
            #[inline]
            pub fn set_key_builder<NKH: KeyBuilder<K>>(
                self,
                kh: NKH,
            ) -> $ty<K, V, NKH, C, U, CB, S> {
                $ty {
                    inner: self.inner.set_key_builder(kh),
                }
            }

            /// Set the coster for the Cache.
            ///
            /// [`Coster`] is a trait you can pass to the `Builder` in order to evaluate
            /// item cost at runtime, and only for the `insert` calls that aren't dropped (this is
            /// useful if calculating item cost is particularly expensive, and you don't want to
            /// waste time on items that will be dropped anyways).
            ///
            /// To signal to Stretto that you'd like to use this [`Coster`] trait:
            ///
            /// 1. Set the [`Coster`] field to your own [`Coster`] implementation.
            /// 2. When calling `insert` for new items or item updates, use a cost of 0.
            ///
            /// [`Coster`]: trait.Coster.html
            #[inline]
            pub fn set_coster<NC: Coster<V>>(self, coster: NC) -> $ty<K, V, KH, NC, U, CB, S> {
                $ty {
                    inner: self.inner.set_coster(coster),
                }
            }

            /// Set the update validator for the Cache.
            ///
            /// By default, the Cache will always update the value if the value already exists in the cache.
            /// [`UpdateValidator`] is a trait to support customized update policy (check if the value should be updated
            /// if the value already exists in the cache).
            ///
            /// [`UpdateValidator`]: trait.UpdateValidator.html
            #[inline]
            pub fn set_update_validator<NU: UpdateValidator<V>>(
                self,
                uv: NU,
            ) -> $ty<K, V, KH, C, NU, CB, S> {
                $ty {
                    inner: self.inner.set_update_validator(uv),
                }
            }

            /// Set the callbacks for the Cache.
            ///
            /// [`CacheCallback`] is for customize some extra operations on values when related event happens.
            ///
            /// [`CacheCallback`]: trait.CacheCallback.html
            #[inline]
            pub fn set_callback<NCB: CacheCallback<V>>(
                self,
                cb: NCB,
            ) -> $ty<K, V, KH, C, U, NCB, S> {
                $ty {
                    inner: self.inner.set_callback(cb),
                }
            }

            /// Set the hasher for the Cache.
            /// Default is SipHasher.
            #[inline]
            pub fn set_hasher<NS: BuildHasher + Clone + 'static>(
                self,
                hasher: NS,
            ) -> $ty<K, V, KH, C, U, CB, NS> {
                $ty {
                    inner: self.inner.set_hasher(hasher),
                }
            }
        }
    };
}

macro_rules! impl_cache {
    ($cache: ident, $builder: ident, $item: ident) => {
        use crate::store::UpdateResult;
        use crate::{ValueRef, ValueRefMut};

        impl<K, V, KH, C, U, CB, S> $cache<K, V, KH, C, U, CB, S>
        where
            K: Hash + Eq,
            V: Send + Sync + 'static,
            KH: KeyBuilder<K>,
            C: Coster<V>,
            U: UpdateValidator<V>,
            CB: CacheCallback<V>,
            S: BuildHasher + Clone + 'static,
        {
            /// `get` returns a `Option<ValueRef<V, SS>>` (if any) representing whether the
            /// value was found or not.
            pub fn get(&self, key: &K) -> Option<ValueRef<V, S>> {
                if self.is_closed.load(Ordering::SeqCst) {
                    return None;
                }

                let (index, conflict) = self.key_to_hash.build_key(key);

                match self.store.get(&index, conflict) {
                    None => {
                        self.metrics.add(MetricType::Miss, index, 1);
                        None
                    }
                    Some(v) => {
                        self.metrics.add(MetricType::Hit, index, 1);
                        Some(v)
                    }
                }
            }

            /// `get_mut` returns a `Option<ValueRefMut<V, SS>>` (if any) representing whether the
            /// value was found or not.
            pub fn get_mut(&self, key: &K) -> Option<ValueRefMut<V, S>> {
                if self.is_closed.load(Ordering::SeqCst) {
                    return None;
                }

                let (index, conflict) = self.key_to_hash.build_key(key);
                match self.store.get_mut(&index, conflict) {
                    None => {
                        self.metrics.add(MetricType::Miss, index, 1);
                        None
                    }
                    Some(v) => {
                        self.metrics.add(MetricType::Hit, index, 1);
                        Some(v)
                    }
                }
            }

            /// Returns the TTL for the specified key if the
            /// item was found and is not expired.
            pub fn get_ttl(&self, key: &K) -> Option<Duration> {
                let (index, conflict) = self.key_to_hash.build_key(key);
                self.store
                    .get(&index, conflict)
                    .and_then(|_| self.store.expiration(&index).map(|time| time.get_ttl()))
            }

            /// `max_cost` returns the max cost of the cache.
            #[inline]
            pub fn max_cost(&self) -> i64 {
                self.policy.max_cost()
            }

            /// `update_max_cost` updates the maxCost of an existing cache.
            #[inline]
            pub fn update_max_cost(&self, max_cost: i64) {
                self.policy.update_max_cost(max_cost)
            }

            /// Returns the number of items in the Cache
            #[inline]
            pub fn len(&self) -> usize {
                self.store.len()
            }

            /// Returns true if the cache is empty
            #[inline]
            pub fn is_empty(&self) -> bool {
                self.store.len() == 0
            }

            #[inline]
            fn try_update(
                &self,
                key: K,
                val: V,
                cost: i64,
                ttl: Duration,
                only_update: bool,
            ) -> Result<Option<(u64, $item<V>)>, CacheError> {
                let expiration = if ttl.is_zero() {
                    Time::now()
                } else {
                    Time::now_with_expiration(ttl)
                };

                let (index, conflict) = self.key_to_hash.build_key(&key);

                // cost is eventually updated. The expiration must also be immediately updated
                // to prevent items from being prematurely removed from the map.
                let external_cost = if cost == 0 { self.coster.cost(&val) } else { 0 };
                match self.store.try_update(index, val, conflict, expiration)? {
                    UpdateResult::NotExist(v)
                    | UpdateResult::Reject(v)
                    | UpdateResult::Conflict(v) => {
                        if only_update {
                            Ok(None)
                        } else {
                            Ok(Some((
                                index,
                                $item::new(index, conflict, cost + external_cost, v, expiration),
                            )))
                        }
                    }
                    UpdateResult::Update(v) => {
                        self.callback.on_exit(Some(v));
                        Ok(Some((index, $item::update(index, cost, external_cost))))
                    }
                }
            }
        }

        impl<K, V, KH, C, U, CB, S> AsRef<$cache<K, V, KH, C, U, CB, S>>
            for $cache<K, V, KH, C, U, CB, S>
        where
            K: Hash + Eq,
            V: Send + Sync + 'static,
            KH: KeyBuilder<K>,
            C: Coster<V>,
            U: UpdateValidator<V>,
            CB: CacheCallback<V>,
            S: BuildHasher + Clone + 'static,
        {
            fn as_ref(&self) -> &$cache<K, V, KH, C, U, CB, S> {
                self
            }
        }

        impl<K, V, KH, C, U, CB, S> Clone for $cache<K, V, KH, C, U, CB, S>
        where
            K: Hash + Eq,
            V: Send + Sync + 'static,
            KH: KeyBuilder<K>,
            C: Coster<V>,
            U: UpdateValidator<V>,
            CB: CacheCallback<V>,
            S: BuildHasher + Clone + 'static,
        {
            fn clone(&self) -> Self {
                Self {
                    store: self.store.clone(),
                    policy: self.policy.clone(),
                    insert_buf_tx: self.insert_buf_tx.clone(),
                    stop_tx: self.stop_tx.clone(),
                    clear_tx: self.clear_tx.clone(),
                    callback: self.callback.clone(),
                    key_to_hash: self.key_to_hash.clone(),
                    is_closed: self.is_closed.clone(),
                    coster: self.coster.clone(),
                    metrics: self.metrics.clone(),
                    _marker: self._marker,
                }
            }
        }
    };
}

macro_rules! impl_cache_processor {
    ($processor: ident, $item: ident) => {
        use crate::cache::CrateItem;

        impl<V, U, CB, S> $processor<V, U, CB, S>
        where
            V: Send + Sync + 'static,
            U: UpdateValidator<V>,
            CB: CacheCallback<V>,
            S: BuildHasher + Clone + 'static + Send,
        {
            #[inline]
            fn handle_item(&mut self, item: $item<V>) -> Result<(), CacheError> {
                match item {
                    $item::New {
                        key,
                        conflict,
                        cost,
                        value,
                        expiration,
                    } => {
                        let cost = self.calculate_internal_cost(cost);
                        let (victim_sets, added) = self.policy.add(key, cost);
                        if added {
                            self.store.try_insert(key, value, conflict, expiration)?;
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

                        for victims in victim_sets {
                            for victim in victims {
                                let sitem = self.store.try_remove(&victim.key, 0)?;
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
                            }
                        }

                        Ok(())
                    }
                    $item::Update {
                        key,
                        cost,
                        external_cost,
                    } => {
                        let cost = self.calculate_internal_cost(cost) + external_cost;
                        self.policy.update(&key, cost);

                        Ok(())
                    }
                    $item::Delete { key, conflict } => {
                        self.policy.remove(&key); // deals with metrics updates.
                        if let Some(sitem) = self.store.try_remove(&key, conflict)? {
                            self.callback.on_exit(Some(sitem.value.into_inner()));
                        }

                        Ok(())
                    }
                    $item::Wait(wg) => {
                        wg.done();
                        Ok(())
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
                        let mut ctr = 0;
                        self.start_ts.retain(|_, _| {
                            ctr += 1;
                            ctr < self.num_to_keep - 1
                        });
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
    };
}

macro_rules! impl_cache_cleaner {
    ($cleaner: ident, $processor: ident, $item: ident) => {
        impl<'a, V, U, CB, S> $cleaner<'a, V, U, CB, S>
        where
            V: Send + Sync + 'static,
            U: UpdateValidator<V>,
            CB: CacheCallback<V>,
            S: BuildHasher + Clone + 'static + Send,
        {
            #[inline]
            fn new(processor: &'a mut $processor<V, U, CB, S>) -> Self {
                Self { processor }
            }

            #[inline]
            fn handle_item(&mut self, item: $item<V>) {
                match item {
                    $item::New {
                        key,
                        conflict,
                        cost,
                        value,
                        expiration,
                    } => self.processor.callback.on_evict(CrateItem::new(
                        key,
                        conflict,
                        cost,
                        Some(value),
                        expiration,
                    )),
                    $item::Delete { .. } | $item::Update { .. } => {}
                    $item::Wait(wg) => wg.done(),
                }
            }
        }
    };
}

mod builder;
#[cfg(test)]
mod test;

use crate::Item as CrateItem;
use std::time::Duration;

#[cfg(feature = "sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
mod sync;
#[cfg(feature = "sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
pub use sync::{Cache, CacheBuilder};

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
mod axync;
#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use axync::{AsyncCache, AsyncCacheBuilder};

// TODO: find the optimal value for this
const DEFAULT_INSERT_BUF_SIZE: usize = 32 * 1024;
// const DEFAULT_BUFFER_ITEMS: usize = 64;
const DEFAULT_CLEANUP_DURATION: Duration = Duration::from_millis(500);
