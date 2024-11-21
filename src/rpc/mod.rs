//! #Remote Procedure Call.
//!
//! Subotai RPCs are the packets sent over TCP between nodes. They
//! contain information about the sender, as well as an optional payload.

use crate::hash::SubotaiHash;
use crate::{node, routing, storage};
use bincode::Options;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Serializable struct implementation of an RPC.
#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub struct Rpc {
    /// Category of RPC.
    pub kind: Kind,
    /// Sender node info (IP address updated on reception).
    pub sender: routing::NodeInfo,
}

impl Rpc {
    /// Constructs a ping RPC. Pings simply carry information about the
    /// sender, and expect a response indicating that the receiving node
    /// is alive.
    pub fn ping(sender: routing::NodeInfo) -> Rpc {
        Rpc {
            kind: Kind::Ping,
            sender,
        }
    }

    /// Constructs a ping response.
    pub fn ping_response(sender: routing::NodeInfo) -> Rpc {
        Rpc {
            kind: Kind::PingResponse,
            sender,
        }
    }

    /// Constructs an RPC asking for a the results of a table node lookup. The objective
    /// of this RPC is to locate a particular node while minimizing network traffic. In other
    /// words, the process short-circuits when the target node is found.
    pub fn locate(sender: routing::NodeInfo, id_to_find: SubotaiHash) -> Rpc {
        let payload = Arc::new(LocatePayload { id_to_find });
        Rpc {
            kind: Kind::Locate(payload),
            sender,
        }
    }

    /// Constructs an RPC with the response to a locate RPC.
    pub fn locate_response(
        sender: routing::NodeInfo,
        id_to_find: SubotaiHash,
        result: routing::LookupResult,
    ) -> Rpc {
        let payload = Arc::new(LocateResponsePayload { id_to_find, result });
        Rpc {
            kind: Kind::LocateResponse(payload),
            sender,
        }
    }

    /// Constructs an RPC asking for a the results of a storage lookup.
    pub fn retrieve(sender: routing::NodeInfo, key_to_find: SubotaiHash) -> Rpc {
        let payload = Arc::new(RetrievePayload { key_to_find });
        Rpc {
            kind: Kind::Retrieve(payload),
            sender,
        }
    }

    /// Constructs an RPC asking for a the results of a storage lookup.
    pub fn retrieve_response(
        sender: routing::NodeInfo,
        key_to_find: SubotaiHash,
        result: RetrieveResult,
    ) -> Rpc {
        let payload = Arc::new(RetrieveResponsePayload {
            key_to_find,
            result,
        });
        Rpc {
            kind: Kind::RetrieveResponse(payload),
            sender,
        }
    }

    /// Constructs a probe RPC. It asks the receiving node to provide a list of
    /// K nodes close to a given node. It's a simpler version of the locate
    /// RPC, that doesn't end early if the node is found.
    pub fn probe(sender: routing::NodeInfo, id_to_probe: SubotaiHash) -> Rpc {
        let payload = Arc::new(ProbePayload { id_to_probe });
        Rpc {
            kind: Kind::Probe(payload),
            sender,
        }
    }

    /// Constructs the response to a probe RPC.
    pub fn probe_response(
        sender: routing::NodeInfo,
        nodes: Vec<routing::NodeInfo>,
        id_to_probe: SubotaiHash,
    ) -> Rpc {
        let payload = Arc::new(ProbeResponsePayload { id_to_probe, nodes });
        Rpc {
            kind: Kind::ProbeResponse(payload),
            sender,
        }
    }

    /// Constructs a store RPC. It asks the receiving node to store a key->value pair.
    pub fn store(
        sender: routing::NodeInfo,
        key: SubotaiHash,
        entry: storage::StorageEntry,
        expiration: DateTime<Utc>,
    ) -> Rpc {
        let payload = Arc::new(StorePayload {
            key,
            entry,
            expiration,
        });
        Rpc {
            kind: Kind::Store(payload),
            sender,
        }
    }
    /// Constructs a mass store RPC. It asks the receiving node to store several key->value pairs
    pub fn mass_store(
        sender: routing::NodeInfo,
        key: SubotaiHash,
        entries_and_expirations: Vec<(storage::StorageEntry, DateTime<Utc>)>,
    ) -> Rpc {
        let payload = Arc::new(MassStorePayload {
            key,
            entries_and_expirations,
        });
        Rpc {
            kind: Kind::MassStore(payload),
            sender,
        }
    }

    /// Constructs a response to the store RPC, including the key and the operation result.
    pub fn store_response(
        sender: routing::NodeInfo,
        key: SubotaiHash,
        result: storage::StoreResult,
    ) -> Rpc {
        let payload = Arc::new(StoreResponsePayload { key, result });
        Rpc {
            kind: Kind::StoreResponse(payload),
            sender,
        }
    }

    /// Serializes an RPC to be send over TCP.
    pub fn serialize(&self) -> Vec<u8> {
        bincode::DefaultOptions::new()
            .with_limit(node::SOCKET_BUFFER_SIZE_BYTES as u64)
            .serialize(&self)
            .unwrap()
    }

    /// Deserializes into an RPC structure.
    pub fn deserialize(serialized: &[u8]) -> bincode::Result<Rpc> {
        bincode::DefaultOptions::new()
            .with_limit(node::SOCKET_BUFFER_SIZE_BYTES as u64)
            .deserialize(serialized)
    }

    /// Reports whether the RPC is a LocateResponse that found
    /// a particular node. If it was, returns the node.
    pub fn successfully_located(&self, id: &SubotaiHash) -> Option<routing::NodeInfo> {
        if let Kind::LocateResponse(ref payload) = self.kind {
            match payload.result {
                routing::LookupResult::Found(ref node) if &payload.id_to_find == id => {
                    return Some(node.clone())
                }
                _ => return None,
            }
        }
        None
    }

    /// Reports whether the RPC is a LocateResponse that failed to locate.
    /// If so, provides the closest nodes.
    pub fn is_helping_locate(&self, id: &SubotaiHash) -> Option<Vec<routing::NodeInfo>> {
        if let Kind::LocateResponse(ref payload) = self.kind {
            match payload.result {
                routing::LookupResult::ClosestNodes(ref nodes) if &payload.id_to_find == id => {
                    return Some(nodes.clone())
                }
                _ => return None,
            }
        }
        None
    }

    /// Reports whether the RPC is a RetrieveResponse that found
    /// a particular key.
    pub fn successfully_retrieved(&self, key: &SubotaiHash) -> Option<Vec<storage::StorageEntry>> {
        if let Kind::RetrieveResponse(ref payload) = self.kind {
            match payload.result {
                RetrieveResult::Found(ref entries) if &payload.key_to_find == key => {
                    return Some(entries.clone())
                }
                _ => return None,
            }
        }
        None
    }

    pub fn successfully_stored(&self, key: &SubotaiHash) -> bool {
        if let Kind::StoreResponse(ref payload) = self.kind {
            match payload.result {
                storage::StoreResult::Success if &payload.key == key => return true,
                _ => return false,
            }
        }
        false
    }

    /// Reports whether the RPC is a RetrieveResponse looking
    /// for a particular key
    pub fn is_helping_retrieve(&self, key: &SubotaiHash) -> Option<Vec<routing::NodeInfo>> {
        if let Kind::RetrieveResponse(ref payload) = self.kind {
            match payload.result {
                RetrieveResult::Closest(ref nodes) if &payload.key_to_find == key => {
                    return Some(nodes.clone())
                }
                _ => return None,
            }
        }
        None
    }

    pub fn is_probe_response(&self, target: &SubotaiHash) -> Option<Vec<routing::NodeInfo>> {
        if let Kind::ProbeResponse(ref payload) = self.kind {
            if &payload.id_to_probe == target {
                return Some(payload.nodes.clone());
            }
        }
        None
    }
}

/// Types of Subotai RPCs. Some of them contain reference counted payloads.
#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub enum Kind {
    Ping,
    PingResponse,
    Store(Arc<StorePayload>),
    MassStore(Arc<MassStorePayload>),
    StoreResponse(Arc<StoreResponsePayload>),
    Locate(Arc<LocatePayload>),
    LocateResponse(Arc<LocateResponsePayload>),
    Retrieve(Arc<RetrievePayload>),
    RetrieveResponse(Arc<RetrieveResponsePayload>),
    Probe(Arc<ProbePayload>),
    ProbeResponse(Arc<ProbeResponsePayload>),
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub struct StorePayload {
    pub key: SubotaiHash,
    pub entry: storage::StorageEntry,
    pub expiration: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub struct StoreResponsePayload {
    pub key: SubotaiHash,
    pub result: storage::StoreResult,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub struct MassStorePayload {
    pub key: SubotaiHash,
    pub entries_and_expirations: Vec<(storage::StorageEntry, DateTime<Utc>)>,
}

/// Includes the ID to find and the amount of nodes required.
#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub struct LocatePayload {
    pub id_to_find: SubotaiHash,
}

/// Includes the ID to find and the results of the table lookup.
#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub struct LocateResponsePayload {
    pub id_to_find: SubotaiHash,
    pub result: routing::LookupResult,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub enum RetrieveResult {
    Found(Vec<storage::StorageEntry>),
    Closest(Vec<routing::NodeInfo>),
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub struct RetrievePayload {
    pub key_to_find: SubotaiHash,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub struct RetrieveResponsePayload {
    pub key_to_find: SubotaiHash,
    pub result: RetrieveResult,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub struct ProbePayload {
    pub id_to_probe: SubotaiHash,
}

/// Includes a vector of up to 'K' nodes close to the id to probe.
/// If the ID provided corresponded to the key in a key-value pair,
/// the corresponding value is also included in the response.
#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub struct ProbeResponsePayload {
    pub id_to_probe: SubotaiHash,
    pub nodes: Vec<routing::NodeInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::SubotaiHash;
    use std::net;
    use std::str::FromStr;
    use {routing, storage};

    #[test]
    fn serdes_for_ping() {
        let ping = Rpc::ping(node_info_no_net(SubotaiHash::random()));
        let serialized_ping = ping.serialize();
        let deserialized_ping = Rpc::deserialize(&serialized_ping).unwrap();
        assert_eq!(ping, deserialized_ping);
    }

    #[test]
    fn serdes_for_store() {
        let now = Utc::now();
        let store = Rpc::store(
            node_info_no_net(SubotaiHash::random()),
            SubotaiHash::random(),
            storage::StorageEntry::Blob(Vec::<u8>::new()),
            now,
        );
        let deserialized_store = Rpc::deserialize(&store.serialize()).unwrap();
        if let Kind::Store(ref payload) = deserialized_store.kind {
            assert_eq!(now, payload.expiration);
        } else {
            panic!();
        }
    }

    fn node_info_no_net(id: SubotaiHash) -> routing::NodeInfo {
        routing::NodeInfo {
            id,
            address: net::SocketAddr::from_str("0.0.0.0:0").unwrap(),
        }
    }
}
