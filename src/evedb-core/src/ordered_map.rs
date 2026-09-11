// SPDX-License-Identifier: AGPL-3.0-only

//! Immutable AVL nodes shared by committed views. Updates copy only their search path.

use std::{
    cmp::Ordering,
    ops::{Bound, RangeBounds},
    sync::Arc,
};

type Link<K, V> = Option<Arc<Node<K, V>>>;
struct Node<K, V> {
    key: K,
    value: V,
    left: Link<K, V>,
    right: Link<K, V>,
    height: usize,
}
fn height<K, V>(node: &Link<K, V>) -> usize {
    node.as_ref().map_or(0, |node| node.height)
}
fn node<K, V>(key: K, value: V, left: Link<K, V>, right: Link<K, V>) -> Arc<Node<K, V>> {
    let height = 1 + height(&left).max(height(&right));
    Arc::new(Node {
        key,
        value,
        left,
        right,
        height,
    })
}
pub(crate) struct OrderedMap<K, V> {
    root: Link<K, V>,
}
impl<K, V> Clone for OrderedMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
        }
    }
}
impl<K, V> OrderedMap<K, V> {
    pub fn new() -> Self {
        Self { root: None }
    }
    pub fn clear(&mut self) {
        self.root = None;
    }
}
impl<K: Ord + Clone, V: Clone> OrderedMap<K, V> {
    pub fn floor(&self, key: &K) -> Option<(&K, &V)> {
        let mut current = self.root.as_deref();
        let mut result = None;
        while let Some(node) = current {
            if node.key <= *key {
                result = Some((&node.key, &node.value));
                current = node.right.as_deref();
            } else {
                current = node.left.as_deref();
            }
        }
        result
    }
    pub fn retain_from(&mut self, key: &K) {
        self.root = retain_from(&self.root, key);
    }
    pub fn get(&self, key: &K) -> Option<&V> {
        let mut current = self.root.as_deref();
        while let Some(node) = current {
            match key.cmp(&node.key) {
                Ordering::Less => current = node.left.as_deref(),
                Ordering::Greater => current = node.right.as_deref(),
                Ordering::Equal => return Some(&node.value),
            }
        }
        None
    }
    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }
    pub fn insert(&mut self, key: K, value: V) {
        self.root = Some(insert(&self.root, key, value));
    }
    pub fn range(&self, bounds: impl RangeBounds<K>) -> Range<'_, K, V> {
        let mut stack = Vec::new();
        let mut current = self.root.as_deref();
        while let Some(node) = current {
            let below = match bounds.start_bound() {
                Bound::Included(key) => node.key < *key,
                Bound::Excluded(key) => node.key <= *key,
                Bound::Unbounded => false,
            };
            if below {
                current = node.right.as_deref();
            } else {
                stack.push(node);
                current = node.left.as_deref();
            }
        }
        let end = match bounds.end_bound() {
            Bound::Included(key) => Bound::Included(key.clone()),
            Bound::Excluded(key) => Bound::Excluded(key.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Range { stack, end }
    }
}
fn join<K: Clone, V: Clone>(
    left: Link<K, V>,
    key: K,
    value: V,
    right: Link<K, V>,
) -> Arc<Node<K, V>> {
    if height(&left) > height(&right) + 1 {
        let top = left.as_ref().unwrap();
        balance(node(
            top.key.clone(),
            top.value.clone(),
            top.left.clone(),
            Some(join(top.right.clone(), key, value, right)),
        ))
    } else if height(&right) > height(&left) + 1 {
        let top = right.as_ref().unwrap();
        balance(node(
            top.key.clone(),
            top.value.clone(),
            Some(join(left, key, value, top.left.clone())),
            top.right.clone(),
        ))
    } else {
        node(key, value, left, right)
    }
}
fn retain_from<K: Ord + Clone, V: Clone>(root: &Link<K, V>, key: &K) -> Link<K, V> {
    let top = root.as_ref()?;
    if top.key < *key {
        retain_from(&top.right, key)
    } else {
        Some(join(
            retain_from(&top.left, key),
            top.key.clone(),
            top.value.clone(),
            top.right.clone(),
        ))
    }
}
impl<K: Ord + Clone, V: Clone> Extend<(K, V)> for OrderedMap<K, V> {
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        for (key, value) in iter {
            self.insert(key, value);
        }
    }
}
pub(crate) struct Range<'a, K, V> {
    stack: Vec<&'a Node<K, V>>,
    end: Bound<K>,
}
impl<'a, K: Ord, V> Iterator for Range<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;
        let beyond = match &self.end {
            Bound::Included(key) => node.key > *key,
            Bound::Excluded(key) => node.key >= *key,
            Bound::Unbounded => false,
        };
        if beyond {
            self.stack.clear();
            return None;
        }
        let mut current = node.right.as_deref();
        while let Some(next) = current {
            self.stack.push(next);
            current = next.left.as_deref();
        }
        Some((&node.key, &node.value))
    }
}
fn insert<K: Ord + Clone, V: Clone>(root: &Link<K, V>, key: K, value: V) -> Arc<Node<K, V>> {
    let Some(old) = root else {
        return node(key, value, None, None);
    };
    let updated = match key.cmp(&old.key) {
        Ordering::Less => node(
            old.key.clone(),
            old.value.clone(),
            Some(insert(&old.left, key, value)),
            old.right.clone(),
        ),
        Ordering::Greater => node(
            old.key.clone(),
            old.value.clone(),
            old.left.clone(),
            Some(insert(&old.right, key, value)),
        ),
        Ordering::Equal => node(key, value, old.left.clone(), old.right.clone()),
    };
    balance(updated)
}
fn balance<K: Clone, V: Clone>(root: Arc<Node<K, V>>) -> Arc<Node<K, V>> {
    if height(&root.left) > height(&root.right) + 1 {
        let left = root.left.as_ref().expect("left-heavy tree");
        let root = if height(&left.right) > height(&left.left) {
            node(
                root.key.clone(),
                root.value.clone(),
                Some(rotate_left(left)),
                root.right.clone(),
            )
        } else {
            root
        };
        rotate_right(&root)
    } else if height(&root.right) > height(&root.left) + 1 {
        let right = root.right.as_ref().expect("right-heavy tree");
        let root = if height(&right.left) > height(&right.right) {
            node(
                root.key.clone(),
                root.value.clone(),
                root.left.clone(),
                Some(rotate_right(right)),
            )
        } else {
            root
        };
        rotate_left(&root)
    } else {
        root
    }
}
fn rotate_left<K: Clone, V: Clone>(root: &Arc<Node<K, V>>) -> Arc<Node<K, V>> {
    let pivot = root.right.as_ref().expect("left rotation");
    let left = node(
        root.key.clone(),
        root.value.clone(),
        root.left.clone(),
        pivot.left.clone(),
    );
    node(
        pivot.key.clone(),
        pivot.value.clone(),
        Some(left),
        pivot.right.clone(),
    )
}
fn rotate_right<K: Clone, V: Clone>(root: &Arc<Node<K, V>>) -> Arc<Node<K, V>> {
    let pivot = root.left.as_ref().expect("right rotation");
    let right = node(
        root.key.clone(),
        root.value.clone(),
        pivot.right.clone(),
        root.right.clone(),
    );
    node(
        pivot.key.clone(),
        pivot.value.clone(),
        pivot.left.clone(),
        Some(right),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicUsize, Ordering as AtomicOrdering},
    };
    #[test]
    fn prefix_removal_and_floor_preserve_balance_and_old_roots() {
        let mut original = OrderedMap::new();
        for key in 0..2048 {
            original.insert(key, key);
        }
        for threshold in 0..=2048 {
            let mut map = original.clone();
            map.retain_from(&threshold);
            validate(&map.root, None, None);
            assert_eq!(
                map.range(..).map(|(&key, _)| key).collect::<Vec<_>>(),
                (threshold..2048).collect::<Vec<_>>()
            );
            assert_eq!(
                map.floor(&threshold.saturating_sub(1)).map(|(&key, _)| key),
                if threshold == 0 { Some(0) } else { None }
            );
            assert_eq!(
                map.floor(&4096).map(|(&key, _)| key),
                (threshold < 2048).then_some(2047)
            );
        }
        assert_eq!(original.range(..).count(), 2048);
    }
    fn validate(root: &Link<u64, u64>, lower: Option<u64>, upper: Option<u64>) -> usize {
        let Some(node) = root else {
            return 0;
        };
        assert!(lower.is_none_or(|key| key < node.key));
        assert!(upper.is_none_or(|key| key > node.key));
        let left = validate(&node.left, lower, Some(node.key));
        let right = validate(&node.right, Some(node.key), upper);
        assert!(left.abs_diff(right) <= 1);
        assert_eq!(node.height, left.max(right) + 1);
        node.height
    }
    #[test]
    fn generated_updates_ranges_and_old_roots_match_a_reference_map() {
        for descending in [false, true] {
            let mut map = OrderedMap::new();
            let mut reference = BTreeMap::new();
            let mut snapshots = Vec::new();
            let mut random = 37u64;
            for step in 0..4000u64 {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = if step < 1000 {
                    if descending { 1000 - step } else { step }
                } else {
                    random % 2000
                };
                map.insert(key, step);
                reference.insert(key, step);
                validate(&map.root, None, None);
                if step % 193 == 0 {
                    snapshots.push((map.clone(), reference.clone()));
                }
            }
            for (map, reference) in snapshots {
                assert_eq!(
                    map.range(..).map(|(k, v)| (*k, *v)).collect::<Vec<_>>(),
                    reference.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>()
                );
                for (start, end) in [(0, 0), (0, 500), (99, 1000), (1999, u64::MAX)] {
                    assert_eq!(
                        map.range(start..=end)
                            .map(|(k, v)| (*k, *v))
                            .collect::<Vec<_>>(),
                        reference
                            .range(start..=end)
                            .map(|(k, v)| (*k, *v))
                            .collect::<Vec<_>>()
                    );
                    assert_eq!(
                        map.range((Bound::Excluded(start), Bound::Excluded(end)))
                            .map(|(k, v)| (*k, *v))
                            .collect::<Vec<_>>(),
                        reference
                            .iter()
                            .filter(|(k, _)| **k > start && **k < end)
                            .map(|(k, v)| (*k, *v))
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
    }
    struct Counted(Arc<AtomicUsize>);
    impl Clone for Counted {
        fn clone(&self) -> Self {
            self.0.fetch_add(1, AtomicOrdering::Relaxed);
            Self(self.0.clone())
        }
    }
    #[test]
    fn pinning_copies_no_entries_and_one_update_copies_only_a_bounded_path() {
        let count = Arc::new(AtomicUsize::new(0));
        let mut map = OrderedMap::new();
        for key in 0..10_000 {
            map.insert(key, Counted(count.clone()));
        }
        count.store(0, AtomicOrdering::Relaxed);
        let old = map.clone();
        assert_eq!(count.load(AtomicOrdering::Relaxed), 0);
        map.insert(10_000, Counted(count.clone()));
        assert!(count.load(AtomicOrdering::Relaxed) < 64);
        assert!(old.get(&10_000).is_none());
        assert!(map.get(&10_000).is_some());
        map.clear();
        assert!(old.get(&9999).is_some());
    }
}
