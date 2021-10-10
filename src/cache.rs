#[cfg(all(test, not(feature = "tokio")))]
mod sync_test;

#[cfg(all(test, feature = "tokio"))]
mod async_test;

cfg_not_async!(
    mod sync_impl;
    pub use sync_impl::Cache;
);

cfg_async!(
    mod async_impl;
    pub use async_impl::Cache;
);

use crate::{
    ttl::Time, CacheCallback, Coster, DefaultCacheCallback, DefaultCoster, DefaultUpdateValidator,
    KeyBuilder, UpdateValidator,
};
use crossbeam_utils::sync::WaitGroup;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::time::Duration;

// TODO: find the optimal value for this or make it configurable
const DEFAULT_INSERT_BUF_SIZE: usize = 32 * 1024;
const DEFAULT_BUFFER_ITEMS: usize = 64;
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
    /// Default is 2500ms.
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
