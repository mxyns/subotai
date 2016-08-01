//! # Subotai
//!
//! Subotai is a Kademlia based distributed hash table. It's designed to be easy to use, safe
//! and quick. Here are some of the ideas that differentiate it from other DHTs:
//!
//! * **Externally synchronous, internally concurrent**: I believe blocking calls make it easier
//!   to reason about networking code than callbacks. All public methods are blocking and return
//!   a sane result or an explicit timeout. Internally however, subotai is fully concurrent,
//!   and parallel operations will often help each other complete!
//!
//! * **Introduce nodes first, resolve conflicts later**: ...
//!
//! * ...
//!
//! # Examples
//!
//! Node ping:
//!
//! ```rust
//! # extern crate time;
//! # extern crate subotai;
//! use subotai::node::Node;
//! # fn main() {
//!
//! let alpha = Node::new().unwrap();
//! let beta = Node::new().unwrap();
//!
//! alpha.bootstrap_until(beta.local_info(), 1);
//!
//! let receptions = beta.receptions().during(time::Duration::seconds(1));
//!
//! alpha.ping(beta.local_info().id);
//!  
//! assert_eq!(receptions.count(), 1);
//! # }
//!
//! ```
#![allow(dead_code, unknown_lints, wrong_self_convention)]
#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]

extern crate itertools;
extern crate rand;
extern crate bincode;
extern crate bus;
extern crate time;

pub mod node;
mod hash;
mod routing;
mod storage;
mod rpc;

mod error;
pub use error::SubotaiError as SubotaiError;
pub use error::SubotaiResult as SubotaiResult;
