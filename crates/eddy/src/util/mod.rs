//! Small allocation-free building blocks shared by later runtime phases.

#![allow(dead_code)]

mod linked_list;
mod rand;

#[allow(unused_imports)]
pub(crate) use linked_list::{Linked, LinkedList, Pointers};
#[allow(unused_imports)]
pub(crate) use rand::FastRand;
