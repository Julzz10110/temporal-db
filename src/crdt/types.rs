//! CRDT type implementations

use crate::core::temporal::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Trait for CRDT types
pub trait CRDT: Clone + Send + Sync {
    /// Merge another CRDT instance into this one
    fn merge(&mut self, other: &Self);

    /// Check if this CRDT is equal to another (for testing)
    fn equals(&self, other: &Self) -> bool;
}

/// Last-Writer-Wins Register
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LWWRegister<T> {
    value: T,
    timestamp: Timestamp,
}

impl<T: Clone + PartialEq> LWWRegister<T> {
    pub fn new(value: T, timestamp: Timestamp) -> Self {
        Self { value, timestamp }
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    pub fn set(&mut self, value: T, timestamp: Timestamp) {
        if timestamp >= self.timestamp {
            self.value = value;
            self.timestamp = timestamp;
        }
    }
}

impl<T: Clone + PartialEq + Send + Sync> CRDT for LWWRegister<T> {
    fn merge(&mut self, other: &Self) {
        if other.timestamp >= self.timestamp {
            self.value = other.value.clone();
            self.timestamp = other.timestamp;
        }
    }

    fn equals(&self, other: &Self) -> bool {
        self.value == other.value && self.timestamp == other.timestamp
    }
}

/// Grow-Only Set
#[derive(Debug, Clone)]
pub struct GSet<T> {
    elements: HashSet<T>,
}

impl<T: std::hash::Hash + Eq + Clone> GSet<T> {
    pub fn new() -> Self {
        Self {
            elements: HashSet::new(),
        }
    }

    pub fn add(&mut self, element: T) {
        self.elements.insert(element);
    }

    pub fn contains(&self, element: &T) -> bool {
        self.elements.contains(element)
    }

    pub fn elements(&self) -> &HashSet<T> {
        &self.elements
    }
}

impl<T: std::hash::Hash + Eq + Clone + Send + Sync> CRDT for GSet<T> {
    fn merge(&mut self, other: &Self) {
        self.elements.extend(other.elements.iter().cloned());
    }

    fn equals(&self, other: &Self) -> bool {
        self.elements == other.elements
    }
}

impl<T: std::hash::Hash + Eq + Clone> Default for GSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

// Manual serialization for GSet
impl<T: std::hash::Hash + Eq + serde::Serialize> serde::Serialize for GSet<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.elements.len()))?;
        for element in &self.elements {
            seq.serialize_element(element)?;
        }
        seq.end()
    }
}

impl<'de, T: std::hash::Hash + Eq + serde::Deserialize<'de>> serde::Deserialize<'de> for GSet<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let vec = Vec::<T>::deserialize(deserializer)?;
        Ok(Self {
            elements: vec.into_iter().collect(),
        })
    }
}

/// Grow-Only Counter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GCounter {
    counts: Vec<u64>,
    node_id: usize,
}

impl GCounter {
    pub fn new(node_id: usize, num_nodes: usize) -> Self {
        Self {
            counts: vec![0; num_nodes],
            node_id,
        }
    }

    pub fn increment(&mut self, by: u64) {
        self.counts[self.node_id] += by;
    }

    pub fn value(&self) -> u64 {
        self.counts.iter().sum()
    }
}

impl CRDT for GCounter {
    fn merge(&mut self, other: &Self) {
        for (i, count) in other.counts.iter().enumerate() {
            if i < self.counts.len() {
                self.counts[i] = self.counts[i].max(*count);
            }
        }
    }

    fn equals(&self, other: &Self) -> bool {
        self.counts == other.counts
    }
}
