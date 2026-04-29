macro_rules! impl_builder {
  ($ty: ident) => {
    impl<K, V, KH, C, U, CB, S> $ty<K, V, KH, C, U, CB, S>
    where
      K: Hash + Eq,
      V: Send + Sync + 'static,
      KH: KeyBuilder<Key = K>,
      C: Coster<Value = V>,
      U: UpdateValidator<Value = V>,
      CB: CacheCallback<Value = V>,
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
      #[cfg_attr(not(tarpaulin), inline(always))]
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
      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn set_max_cost(self, max_cost: i64) -> Self {
        Self {
          inner: self.inner.set_max_cost(max_cost),
        }
      }

      /// Set the per-stripe high-water mark for the striped insert buffer.
      ///
      /// When a stripe accumulates this many items, the full batch is sent
      /// to the policy processor.
      ///
      /// Default `64`. Min `1`. Larger values amortize channel sends but
      /// delay admission decisions; recommended `≤ 256` for caches under
      /// ~10 K capacity.
      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn set_insert_stripe_high_water(self, items: usize) -> Self {
        Self {
          inner: self.inner.set_insert_stripe_high_water(items),
        }
      }

      /// Set the processor's drain-tick interval. The processor wakes every
      /// `interval` to drain every stripe inline (bypassing the bounded
      /// channel) and to run TTL cleanup.
      ///
      /// Default `500ms`. Zero is silently promoted to the default.
      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn set_drain_interval(self, interval: Duration) -> Self {
        Self {
          inner: self.inner.set_drain_interval(interval),
        }
      }

      /// Set the insert buffer items for the Cache.
      ///
      /// `buffer_items` determines the size of Get buffers.
      ///
      /// Unless you have a rare use case, using `64` as the BufferItems value
      /// results in good performance.
      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn set_buffer_items(self, sz: usize) -> Self {
        Self {
          inner: self.inner.set_buffer_items(sz),
        }
      }

      /// Set whether record the metrics or not.
      ///
      /// Metrics is true when you want real-time logging of a variety of stats.
      /// The reason this is a Builder flag is because there's a 10% throughput performance overhead.
      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn set_metrics(self, val: bool) -> Self {
        Self {
          inner: self.inner.set_metrics(val),
        }
      }

      /// Set whether ignore the internal cost or not.
      ///
      /// Default is `true`: each `insert` is charged only the caller-supplied cost,
      /// so `max_cost` behaves as an entry budget when you pass `1` per insert.
      ///
      /// Set to `false` when `max_cost` represents a byte budget and you need each
      /// stored item to also account for ~56 bytes of per-entry bookkeeping.
      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn set_ignore_internal_cost(self, val: bool) -> Self {
        Self {
          inner: self.inner.set_ignore_internal_cost(val),
        }
      }

      /// Set the cleanup ticker for Cache, each tick the Cache will clean the expired entries.
      #[cfg_attr(not(tarpaulin), inline(always))]
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
      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn set_key_builder<NKH: KeyBuilder<Key = K>>(
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
      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn set_coster<NC: Coster<Value = V>>(self, coster: NC) -> $ty<K, V, KH, NC, U, CB, S> {
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
      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn set_update_validator<NU: UpdateValidator<Value = V>>(
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
      #[cfg_attr(not(tarpaulin), inline(always))]
      pub fn set_callback<NCB: CacheCallback<Value = V>>(
        self,
        cb: NCB,
      ) -> $ty<K, V, KH, C, U, NCB, S> {
        $ty {
          inner: self.inner.set_callback(cb),
        }
      }

      /// Set the hasher for the Cache.
      /// Default is SipHasher.
      #[cfg_attr(not(tarpaulin), inline(always))]
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

macro_rules! impl_cache_processor {
  ($processor: ident, $item: ident) => {
    impl<V, U, CB, S> $processor<V, U, CB, S>
    where
      V: Send + Sync + 'static,
      U: UpdateValidator<Value = V>,
      CB: CacheCallback<Value = V>,
      S: BuildHasher + Clone + 'static + Send,
    {
      #[cfg_attr(not(tarpaulin), inline(always))]
      fn handle_item(&mut self, item: $item<V>) -> Result<(), CacheError> {
        match item {
          $item::New {
            key,
            conflict,
            cost,
            expiration,
            version,
            generation,
            ..
          } => {
            // A `clear()` may have run between the caller's eager store
            // write and this admission. If so, the eager write either was
            // wiped by `store.clear()` already or is stale state that must
            // not be admitted to policy. In either case: remove any matching
            // entry from the store and skip admission. Acquire pairs with
            // the Release-ordered fetch_add in the Clear handler.
            let current_gen = self
              .clear_generation
              .load(std::sync::atomic::Ordering::Acquire);
            if generation != current_gen {
              if let Some(sitem) = self.store.try_remove_if_version(&key, conflict, version)? {
                self.callback.on_exit(Some(sitem.value));
              }
              return Ok(());
            }

            // Gate policy admission on "store still holds our eager row at
            // this exact version". The insert buffer is MPSC and the eager
            // `store.try_insert` / `store.try_remove` happen outside the
            // channel's ordering guarantee — a concurrent `remove()` can
            // therefore land its Item::Delete (or even overlap with a later
            // insert's Item::New) in the queue ahead of our Item::New, in
            // which case our eager row has already been deleted by the time
            // this handler runs. Admitting to policy anyway would create a
            // ghost: policy tracks the key, but no store row exists to serve
            // reads or be evicted — so the entry permanently escapes the
            // cost/eviction accounting and can silently break max_cost.
            if !self.store.contains_version(&key, conflict, version) {
              return Ok(());
            }

            let cost = self.calculate_internal_cost(cost);
            // The four `AddOutcome` variants correspond to genuinely different
            // next actions; the old `(Option<Vec>, bool)` return collapsed
            // `UpdatedExisting` and the rejection paths, which forced a
            // separate `policy.contains` probe to avoid tearing down a fresh
            // store row on a same-key reinsert race. See `policy::AddOutcome`.
            let (admitted, rejected, victims) = match self.policy.add(key, cost) {
              AddOutcome::Admitted { victims } => (true, false, victims),
              AddOutcome::UpdatedExisting => {
                // Policy already held an entry for this key; its cost has
                // been updated in place. The caller's eager store row is
                // the current value, so leave it alone. A stale pending
                // Item::Delete for the previous version (if any) is
                // handled by the Delete branch's `contains_key` gate.
                (false, false, Vec::new())
              }
              AddOutcome::RejectedByCost => (false, true, Vec::new()),
              AddOutcome::RejectedBySampling { victims } => (false, true, victims),
            };

            if admitted {
              self.track_admission(key);
            } else if rejected {
              // Undo the caller's eager write. Version-gated so a concurrent
              // reinsert that landed a newer value survives.
              if let Some(sitem) = self.store.try_remove_if_version(&key, conflict, version)? {
                self.callback.on_reject(CrateItem {
                  val: Some(sitem.value),
                  index: key,
                  conflict,
                  cost,
                  exp: expiration,
                });
                // Ghost-entry cleanup. `policy.add`'s `cost > max_cost`
                // branch returns before the `costs.update` check, so
                // `RejectedByCost` for a key that was already in policy
                // leaves that entry untouched. If this rejection was the
                // rollback of a same-key remove+reinsert race ([Delete,
                // New] order where the Delete's `contains_key` gate saw
                // our eager row and skipped `policy.remove`), policy now
                // tracks a key whose store row we just removed — a ghost
                // that corrupts cost accounting until a future insert of
                // the same key lands. If the store has no row at this
                // index after the rollback, the only way policy still
                // contains it is that ghost, so wipe it. The processor
                // is single-threaded, so nothing else has mutated policy
                // since we last touched it.
                //
                // Pass conflict=0 ("any row at this index") rather than
                // our own conflict. Policy is keyed by index alone, so a
                // live row at the same index but a different conflict
                // (index collision between distinct keys) already owns
                // the shared policy entry; wiping policy would strand it.
                if !self.store.contains_key(&key, 0) {
                  self.policy.remove(&key);
                }
              }
            }

            for victim in victims {
              if let Some(sitem) = self.store.try_remove(&victim.key, 0)? {
                self.on_evict(CrateItem {
                  index: victim.key,
                  val: Some(sitem.value),
                  cost: victim.cost,
                  conflict: sitem.conflict,
                  exp: sitem.expiration,
                });
              }
            }

            Ok(())
          }
          $item::Update {
            key,
            conflict,
            cost,
            external_cost,
            expiration,
            version,
            generation,
          } => {
            // A `clear()` after the eager store update (but before this
            // admission) has already wiped policy state; applying the
            // captured cost now would either resurrect a nonexistent entry
            // or corrupt a post-clear admission's cost accounting.
            //
            // The store's generation-stamp gate refuses any stale caller
            // whose captured generation is less than an existing row's —
            // so a pre-clear writer cannot overwrite a post-clear row.
            // The remaining path into this stale-gen arm is a pre-clear
            // writer whose eager `store.try_update` committed BEFORE the
            // Clear handler wiped the store. In that case the wipe already
            // removed the row; the `try_remove_if_version` below is
            // defensive and normally a no-op.
            let current_gen = self
              .clear_generation
              .load(std::sync::atomic::Ordering::Acquire);
            if generation != current_gen {
              if let Some(sitem) = self.store.try_remove_if_version(&key, conflict, version)? {
                self.callback.on_exit(Some(sitem.value));
                if !self.store.contains_key(&key, 0) {
                  self.policy.remove(&key);
                }
              }
              return Ok(());
            }
            // Gate on "our eager write is still the live version at this
            // (key, conflict)". If a later writer has since bumped the
            // version, they own the row and will handle its policy
            // accounting (via their own Item::Update / Item::New);
            // touching policy or store here would corrupt their bookkeep-
            // ing. Symmetric to the `contains_version` gate in Item::New.
            if !self.store.contains_version(&key, conflict, version) {
              return Ok(());
            }
            let cost = self.calculate_internal_cost(cost) + external_cost;
            if self.policy.update(&key, cost) {
              return Ok(());
            }
            // Policy doesn't track this key yet. Two writers raced on the
            // same key: our eager `store.try_update` was actually the
            // FIRST write (caller-side) but `try_update` took the
            // UpdateResult::Update branch because a concurrent
            // `try_insert` happened to land between our read and our
            // store access — OR, more commonly, our Item::New for this
            // row's predecessor was skipped by its `contains_version`
            // gate because we bumped the version in the gap. Either way,
            // the store holds our row and the key is absent from policy:
            // an orphan unless we admit it ourselves. Treat this like a
            // fresh admission — mirroring Item::New's branches so that
            // rejections roll back the row (same `try_remove_if_version`
            // + ghost-entry cleanup) and admissions track victims.
            let (admitted, rejected, victims) = match self.policy.add(key, cost) {
              AddOutcome::Admitted { victims } => (true, false, victims),
              // A racing handler just admitted our key between our
              // `update` probe and our `add`. Policy now tracks it; the
              // cost is what that handler set, not necessarily ours, but
              // that's the same outcome concurrent updates have always
              // produced (one wins) and is self-consistent with the live
              // store row.
              AddOutcome::UpdatedExisting => (true, false, Vec::new()),
              AddOutcome::RejectedByCost => (false, true, Vec::new()),
              AddOutcome::RejectedBySampling { victims } => (false, true, victims),
            };
            if admitted {
              self.track_admission(key);
            } else if rejected {
              if let Some(sitem) = self.store.try_remove_if_version(&key, conflict, version)? {
                self.callback.on_reject(CrateItem {
                  val: Some(sitem.value),
                  index: key,
                  conflict,
                  cost,
                  exp: expiration,
                });
                // Ghost-entry cleanup: same rationale as Item::New's
                // rejection branch. `policy.add`'s cost>max_cost path
                // returns before touching an existing entry, so a prior
                // Delete that skipped cleanup could have left a stale
                // policy entry that only the store-is-empty check can
                // catch. conflict=0 ("any row at this index") to respect
                // index collisions between distinct keys.
                if !self.store.contains_key(&key, 0) {
                  self.policy.remove(&key);
                }
              }
            }
            for victim in victims {
              if let Some(sitem) = self.store.try_remove(&victim.key, 0)? {
                self.on_evict(CrateItem {
                  index: victim.key,
                  val: Some(sitem.value),
                  cost: victim.cost,
                  conflict: sitem.conflict,
                  exp: sitem.expiration,
                });
              }
            }

            Ok(())
          }
          $item::Delete {
            key,
            conflict,
            generation,
            version,
          } => {
            // Same reason as Update: if a `clear()` landed between the
            // eager `store.try_remove` and this handler, policy was wiped
            // and a post-clear insert may already be live. Skip both the
            // policy removal and the follow-up store sweep so the new
            // entry is preserved.
            let current_gen = self
              .clear_generation
              .load(std::sync::atomic::Ordering::Acquire);
            if generation != current_gen {
              // Policy-ghost cleanup, symmetric to the Update stale arm.
              // The caller's eager `store.try_remove` already wiped a
              // row (version != 0 means the remove actually removed
              // something). A post-clear insert at the same key may
              // already have been admitted to policy between clear and
              // this handler. If the store is now empty at this index,
              // any surviving policy entry is that orphan; wipe it.
              // version == 0 means the eager remove was a no-op, so no
              // row was torn down and policy must not be touched (a
              // live post-clear admission would own it). conflict=0
              // for the same reason as the Update arm: policy is
              // keyed by index alone, so a live row under a different
              // conflict still owns the shared entry.
              if version != 0 && !self.store.contains_key(&key, 0) {
                self.policy.remove(&key);
              }
              return Ok(());
            }
            // `try_remove` only enqueues Item::Delete when the eager remove
            // actually removed a row, so `version` is always a real store
            // version here (store versions start at 1; 0 is the reserved
            // "no row" sentinel). The zero check remains as defense in
            // depth against future callers that might synthesize a Delete
            // without an eager remove — touching policy then would risk
            // orphaning a concurrent admission outside policy accounting.
            if version == 0 {
              return Ok(());
            }
            // Gate `policy.remove` on the store being empty at this index. In
            // the normal case (no racing reinsert) the caller's eager
            // `store.try_remove` left the index absent, so this is true and we
            // clean up policy. But if a concurrent `insert()` landed a newer
            // row between the caller's remove and this handler, and that new
            // Item::New was processed before us, the New handler already
            // refreshed the policy entry in-place (via
            // `AddOutcome::UpdatedExisting`) to reflect the fresh admission.
            // Wiping policy now would orphan that fresh row outside
            // cost/eviction accounting — a ghost store row.
            //
            // The gate passes conflict=0 to ask "does the store hold ANY row
            // at this index", not "at this (index, conflict)". Policy is
            // keyed by index alone, so an index collision between distinct
            // keys (different conflicts) shares a single policy entry.
            // Checking with our own conflict would miss a post-remove insert
            // at a different conflict: `contains_key(&key, C_A)` would return
            // false even though the store now holds a row at this index
            // (conflict C_B) that the New handler already merged into the
            // shared policy entry via UpdatedExisting. We'd then wipe policy
            // and strand C_B as a ghost.
            if !self.store.contains_key(&key, 0) {
              self.policy.remove(&key); // deals with metrics updates.
            }
            // Version-guarded removal: the eager remove already took
            // whatever was there at the caller's `try_remove`. If a
            // concurrent insert has since landed a new value at the same
            // (key, conflict) under a different version, leaving it alone
            // is the correct outcome — removing by (key, conflict) alone
            // would destroy that fresh data.
            if let Some(sitem) = self.store.try_remove_if_version(&key, conflict, version)? {
              self.callback.on_exit(Some(sitem.value));
            }

            Ok(())
          }
          $item::Wait(wg) => {
            wg.done();
            Ok(())
          }
          $item::Clear(wg) => {
            // Ordered-clear barrier. Items enqueued before this marker have
            // already been processed above; items enqueued after will be
            // processed against the freshly cleared state, so callers that
            // run `insert()` after `clear()` returns never see their writes
            // drained as part of the clear.
            //
            // Wipe store/policy/metrics/start_ts FIRST, then bump the
            // generation. If the bump happened first there would be a
            // window between the bump and `store.clear()` where a
            // concurrent `try_update` could load the NEW generation,
            // write a row the wipe is about to erase, and enqueue an
            // `Item::New` whose generation matches current — the
            // processor would then admit a key that no longer exists in
            // the store. By bumping AFTER the wipe, any eager write
            // racing with this handler captured the OLD generation and
            // its `Item::New` becomes stale: the New handler's
            // `try_remove_if_version` cleans up whatever ghost row it
            // may have left (nothing, usually — the wipe ate it). The
            // bump still happens before `wg.done()`, so by the time
            // `clear()` returns to its caller, the new generation is
            // visible to any subsequent insert. Release ordering pairs
            // with `try_update`'s Acquire load.
            //
            // `store.clear()` returns the drained values so we can run
            // `on_exit` AFTER the generation bump: an `on_exit` that
            // re-enters the cache via `insert` otherwise captures the
            // pre-bump generation and enqueues an `Item::New` that the
            // processor will then reject as stale — silently dropping
            // the caller's insert. Running callbacks last also means
            // shard write locks are fully released before any user
            // code runs, so a callback that calls `get`/`len` on the
            // processor thread cannot self-deadlock on a shard lock
            // (parking_lot RwLocks are not reentrant).
            let drained = self.store.clear();
            self.policy.clear();
            self.metrics.clear();
            self.start_ts.clear();
            self
              .clear_generation
              .fetch_add(1, std::sync::atomic::Ordering::Release);
            for v in drained {
              self.callback.on_exit(Some(v));
            }
            wg.done();
            Ok(())
          }
        }
      }

      /// Drive a per-batch loop of `handle_item`. Each item still takes
      /// the policy `Mutex` independently — fusing them under one
      /// acquisition is a follow-up optimization gated by the spec's
      /// "out of scope: per-batch policy lock fusion" decision. The
      /// per-call lock cost is small relative to the channel send the
      /// caller no longer pays, so this is acceptable for v0.9.0.
      #[cfg_attr(not(tarpaulin), inline(always))]
      fn handle_insert_batch(&mut self, items: Vec<$item<V>>) -> Result<(), CacheError> {
        for item in items {
          if let Err(e) = self.handle_item(item) {
            tracing::error!("fail to handle insert event item: {}", e);
          }
        }
        Ok(())
      }

      #[cfg_attr(not(tarpaulin), inline(always))]
      fn on_evict(&mut self, item: CrateItem<V>) {
        self.prepare_evict(&item);
        self.callback.on_evict(item);
      }

      #[cfg_attr(not(tarpaulin), inline(always))]
      fn calculate_internal_cost(&self, cost: i64) -> i64 {
        if !self.ignore_internal_cost {
          // Add the cost of internally storing the object.
          cost + (self.item_size as i64)
        } else {
          cost
        }
      }

      #[cfg_attr(not(tarpaulin), inline(always))]
      fn track_admission(&mut self, key: u64) {
        let added = self.metrics.add(MetricType::KeyAdd, key, 1);

        if added {
          if self.start_ts.len() > self.num_to_keep {
            let mut ctr = 0;
            self.start_ts.retain(|_, _| {
              ctr += 1;
              ctr < self.num_to_keep - 1
            });
          }
          self.start_ts.insert(key, Time::now());
        }
      }

      #[cfg_attr(not(tarpaulin), inline(always))]
      fn prepare_evict(&mut self, item: &CrateItem<V>) {
        if let Some(ts) = self.start_ts.get(&item.index) {
          self.metrics.track_eviction(ts.elapsed().as_secs() as i64);
          self.start_ts.remove(&item.index);
        }
      }
    }
  };
}

mod builder;
mod insert_stripe;
#[cfg(all(
  test,
  any(
    feature = "sync",
    all(feature = "async", feature = "tokio"),
    all(feature = "async", feature = "smol"),
  ),
))]
mod test;

use std::time::Duration;

#[cfg(feature = "sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
mod sync;
#[cfg(feature = "sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
pub use sync::{Cache, CacheBuilder};

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
mod r#async;
#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use r#async::{AsyncCache, AsyncCacheBuilder};

pub(crate) const DEFAULT_BUFFER_ITEMS: usize = 64;
const DEFAULT_CLEANUP_DURATION: Duration = Duration::from_secs(2);
/// Default interval for the processor's drain-tick + TTL cleanup arm.
/// Matches the spec's `drain_interval` default (500ms) so worst-case
/// admission latency for stripe-buffered items is bounded to one tick.
pub(crate) const DEFAULT_DRAIN_INTERVAL: Duration = Duration::from_millis(500);
