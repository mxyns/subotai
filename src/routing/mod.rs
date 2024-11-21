use crate::{hash, node};
use hash::SubotaiHash;
use hash::HASH_SIZE;
use serde::{Deserialize, Serialize};
use std::cmp::PartialEq;
use std::collections::VecDeque;
use std::time::Instant;
use std::{iter, mem, net, sync};

#[cfg(test)]
mod tests;

/// Routing table with `HASH_SIZE` buckets of `K_FACTOR` node
/// identifiers each, constructed around a parent node ID.
///
/// The structure employs least-recently seen eviction. Conflicts generated
/// by evicting a node by inserting a newer one remain tracked, so they can
/// be resolved later.
#[derive(Debug)]
pub struct Table {
    buckets: Vec<sync::RwLock<Bucket>>,
    parent_id: SubotaiHash,
    configuration: node::Configuration,
}

/// ID - Address pair that identifies a unique Subotai node in the network.
#[derive(Serialize, Deserialize, Debug, Clone, Eq)]
pub struct NodeInfo {
    pub id: SubotaiHash,
    pub address: net::SocketAddr,
}

/// Result of a table lookup.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum LookupResult {
    /// The requested ID was found on the table.
    Found(NodeInfo),
    /// The requested ID was not found, but here are the next
    /// closest nodes to consult.
    ClosestNodes(Vec<NodeInfo>),
    /// The table is empty or the blacklist provided doesn't allow
    /// returning any close nodes.
    Nothing,
}

/// Result of updating the table with a recently contacted node.
pub enum UpdateResult {
    /// There wasn't an entry for the node, so it has been added.
    AddedNode,
    /// There was an entry for the node, so it has been moved to
    /// the tail of its bucket.
    UpdatedNode,
    /// There wasn't an entry for the node and the bucket was full,
    /// so it has been added, evicting an older node.
    CausedConflict(EvictionConflict),
}

impl Table {
    /// Constructs a routing table based on a parent node id. Other nodes
    /// will be stored in this table based on their distance to the node id provided.
    pub fn new(id: hash::SubotaiHash, configuration: node::Configuration) -> Table {
        Table {
            buckets: (0..HASH_SIZE)
                .map(|_| sync::RwLock::new(Bucket::with_capacity(configuration.k_factor)))
                .collect(),
            parent_id: id,
            configuration,
        }
    }

    /// Returns the number of nodes currently on the table.
    pub fn len(&self) -> usize {
        self.buckets
            .iter()
            .map(|bucket| bucket.read().unwrap().entries.len())
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Inserts a node in the routing table. Employs least-recently-seen eviction
    /// by kicking out the oldest node in case the bucket is full, and registering
    /// an eviction conflict that can be revised later.
    ///
    /// This differs to Kademlia in that newer nodes take preference until
    /// older nodes respond to the conflict resolution ping. However, there
    /// is a mechanism against DDoS attacks in the form of a defensive
    /// mode, that is adopted when too many conflicts happen in a short period
    /// of time. Defensive mode causes the node to reject any updates that would
    /// cause conflicts until a given time period has elapsed.
    pub fn update_node(&self, info: NodeInfo) -> UpdateResult {
        let mut result = UpdateResult::AddedNode;
        let index = self.bucket_for_node(&info.id);
        let mut bucket = self.buckets[index].write().unwrap();

        if bucket.entries.contains(&info) {
            result = UpdateResult::UpdatedNode;
        }

        bucket
            .entries
            .retain(|stored_info| info.id != stored_info.id);
        if bucket.entries.len() == self.configuration.k_factor {
            let conflict = EvictionConflict {
                evicted: bucket.entries.pop_front().unwrap(),
                evictor: info.clone(),
                times_pinged: 0,
            };

            result = UpdateResult::CausedConflict(conflict);
        }
        bucket.entries.push_back(info);

        result
    }

    /// Removes a node from the routing table, if present.
    pub fn remove_node(&self, id: &hash::SubotaiHash) {
        let index = self.bucket_for_node(id);
        let mut bucket = self.buckets[index].write().unwrap();
        bucket.entries.retain(|stored_info| id != &stored_info.id);
    }

    /// Performs a node lookup on the routing table. The lookup result may
    /// contain the specific node, a list of up to the N closest nodes, or
    /// report that the parent node itself was requested.
    ///
    /// This employs an algorithm I have named "bounce lookup", which obtains
    /// the closest nodes to a given origin walking through the minimum
    /// amount of buckets. It may exist already, but I haven't
    /// found it any other implementation. It consists of:
    ///
    /// * Calculating the XOR distance between the parent node ID and the
    ///   lookup node ID.
    ///
    /// * Checking the buckets indexed by the position of every "1" in said
    ///   distance hash, in descending order.
    ///
    /// * "Bounce" back up, checking the buckets indexed by the position of
    ///   every "0" in that distance hash, in ascending order.
    ///
    /// This algorithm should be as efficient as the one proposed in the
    /// Kademlia paper with bucket splitting, but it avoids the necessity of
    /// splitting the buckets, reducing the amount of dynamic allocations
    /// needed.
    pub fn lookup(
        &self,
        id: &SubotaiHash,
        n: usize,
        blacklist: Option<&Vec<SubotaiHash>>,
    ) -> LookupResult {
        match self.specific_node(id) {
            Some(info) => LookupResult::Found(info),
            None => {
                let closest: Vec<NodeInfo> = self
                    .closest_nodes_to(id)
                    .filter(|info| Self::is_allowed(&info.id, blacklist))
                    .take(n)
                    .collect();

                if closest.is_empty() {
                    LookupResult::Nothing
                } else {
                    LookupResult::ClosestNodes(closest)
                }
            }
        }
    }

    fn is_allowed(id: &SubotaiHash, blacklist: Option<&Vec<SubotaiHash>>) -> bool {
        if let Some(blacklist) = blacklist {
            !blacklist.contains(id)
        } else {
            true
        }
    }

    /// Returns an iterator over all stored nodes, ordered by ascending
    /// distance to the parent node. This iterator is designed for concurrent
    /// access to the data structure, and as such it isn't guaranteed that it
    /// will return a "snapshot" of all nodes for a specific moment in time.
    /// Buckets already visited may be modified elsewhere through iteraton,
    /// and unvisited buckets may accrue new nodes.
    pub fn all_nodes(&self) -> AllNodes {
        AllNodes {
            table: self,
            current_bucket: Vec::with_capacity(self.configuration.k_factor),
            bucket_index: 0,
        }
    }

    /// Produces copies of all nodes from a particular bucket.
    pub fn nodes_from_bucket(&self, index: usize) -> Vec<NodeInfo> {
        let bucket = self.buckets[index].read().unwrap();
        bucket.entries.iter().cloned().collect()
    }

    /// Returns an iterator over all stored nodes, ordered by ascending
    /// distance to a given reference ID. This iterator is designed for concurrent
    /// access to the data structure, and as such it isn't guaranteed that it
    /// will return a "snapshot" of all nodes for a specific moment in time.
    /// Buckets already visited may be modified elsewhere through iteraton,
    /// and unvisited buckets may accrue new nodes.
    pub fn closest_nodes_to<'a, 'b>(&'a self, id: &'b SubotaiHash) -> ClosestNodesTo<'a, 'b> {
        let distance = &self.parent_id ^ id;
        let descent = distance.clone().into_ones().rev();
        let ascent = distance.into_zeroes();
        let lookup_order = descent.chain(ascent);

        ClosestNodesTo {
            table: self,
            reference: id,
            lookup_order,
            current_bucket: Vec::with_capacity(self.configuration.k_factor),
        }
    }

    /// Returns a table entry for the specific node with a given hash.
    pub fn specific_node(&self, id: &SubotaiHash) -> Option<NodeInfo> {
        let index = self.bucket_for_node(id);
        let entries = &self.buckets[index].read().unwrap().entries;
        entries.iter().find(|info| *id == info.id).cloned()
    }

    /// Returns the appropriate position for a node, by computing
    /// the index where their prefix starts differing.
    pub fn bucket_for_node(&self, id: &SubotaiHash) -> usize {
        (&self.parent_id ^ id).height().unwrap_or(0)
    }

    pub fn revert_conflict(&self, conflict: EvictionConflict) {
        let index = self.bucket_for_node(&conflict.evictor.id);
        let bucket = &self.buckets[index];
        let entries = &mut bucket.write().unwrap().entries;

        if let Some(ref mut evictor) = entries
            .iter_mut()
            .find(|info| conflict.evictor.id == info.id)
        {
            let _ = mem::replace::<NodeInfo>(evictor, conflict.evicted);
        }
    }

    pub fn mark_bucket_as_probed(&self, id: &SubotaiHash) {
        let index = self.bucket_for_node(id);
        let mut bucket = self.buckets[index].write().unwrap();
        bucket.last_probe = Some(Instant::now());
    }

    /// Returns the bucket index and the time for the bucket that we haven't
    /// probed for the longest. None on the second tuple value would mean the bucket
    /// has never been probed.
    pub fn oldest_bucket(&self) -> (usize, Option<Instant>) {
        let times: Vec<Option<Instant>> = self
            .buckets
            .iter()
            .map(|bucket| bucket.read().unwrap().last_probe)
            .collect();

        if let Some(index) = times.iter().position(|option| option.is_none()) {
            return (index, None);
        }

        let now = Instant::now();
        times
            .into_iter()
            .enumerate()
            .max_by_key(|&(_, time)| now - time.unwrap())
            .unwrap()
    }
}

/// Produces copies of all known nodes, ordered in ascending
/// distance from self. The table may be modified through iteration.
pub struct AllNodes<'a> {
    table: &'a Table,
    current_bucket: Vec<NodeInfo>,
    bucket_index: usize,
}

/// Produces copies of all known nodes, ordered in ascending
/// distance from a reference ID.
pub struct ClosestNodesTo<'a, 'b> {
    table: &'a Table,
    reference: &'b hash::SubotaiHash,
    lookup_order: iter::Chain<iter::Rev<hash::IntoOnes>, hash::IntoZeroes>,
    current_bucket: Vec<NodeInfo>,
}

/// Represents a conflict derived from attempting to insert a node in a full
/// bucket.
#[derive(Debug, Clone)]
pub struct EvictionConflict {
    pub evicted: NodeInfo,
    evictor: NodeInfo,
    pub times_pinged: u8,
}

/// Bucket size is estimated to be small enough not to warrant
/// the downsides of using a linked list.
///
/// Each vector of bucket entries is protected under its own mutex, to guarantee
/// concurrent access to the table.
#[derive(Debug)]
struct Bucket {
    entries: VecDeque<NodeInfo>,
    last_probe: Option<Instant>,
}

impl PartialEq for NodeInfo {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

#[allow(clippy::while_let_on_iterator)]
impl<'a, 'b> Iterator for ClosestNodesTo<'a, 'b> {
    type Item = NodeInfo;

    fn next(&mut self) -> Option<NodeInfo> {
        if !self.current_bucket.is_empty() {
            return self.current_bucket.pop();
        }

        while let Some(index) = self.lookup_order.next() {
            let mut new_bucket = {
                // Lock scope
                let bucket = &self.table.buckets[index].read().unwrap();
                if bucket.entries.is_empty() {
                    continue;
                }
                bucket.entries.clone()
            }
            .into_iter()
            .collect::<Vec<NodeInfo>>();

            new_bucket.sort_by(|info_a, info_b| {
                (&info_b.id ^ self.reference).cmp(&(&info_a.id ^ self.reference))
            });
            self.current_bucket.append(&mut new_bucket);
            return self.current_bucket.pop();
        }
        None
    }
}

impl<'a> Iterator for AllNodes<'a> {
    type Item = NodeInfo;

    fn next(&mut self) -> Option<NodeInfo> {
        while self.bucket_index < HASH_SIZE && self.current_bucket.is_empty() {
            let mut new_bucket = {
                // Lock scope
                self.table.buckets[self.bucket_index]
                    .read()
                    .unwrap()
                    .entries
                    .clone()
            }
            .into_iter()
            .collect::<Vec<NodeInfo>>();

            new_bucket.sort_by_key(|info| &info.id ^ &self.table.parent_id);
            self.current_bucket.append(&mut new_bucket);
            self.bucket_index += 1;
        }
        self.current_bucket.pop()
    }
}

impl Bucket {
    fn with_capacity(capacity: usize) -> Bucket {
        Bucket {
            entries: VecDeque::with_capacity(capacity),
            last_probe: None,
        }
    }
}
