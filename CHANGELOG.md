# Version 0.7 (2022/08/30)
- change function definition to 
  ```rust
  pub fn get<Q>(&self, key: &Q) -> Option<ValueRef<V, S>>
      where
          K: core::borrow::Borrow<Q>,
          Q: core::hash::Hash + Eq,

  pub fn get_mut<Q>(&self, key: &Q) -> Option<ValueRefMut<V, S>>
      where
          K: core::borrow::Borrow<Q>,
          Q: core::hash::Hash + Eq,

  pub fn get_ttl<Q>(&self, key: &Q) -> Option<Duration>
      where
          K: core::borrow::Borrow<Q>,
          Q: core::hash::Hash + Eq,
  ```

# Version 0.6 (2022/08)
- Use associated type for traits

# Version 0.5 (2022/07/07)
- Support runtime agnostic `AsyncCache`
