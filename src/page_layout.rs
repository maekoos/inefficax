use std::mem::size_of;

// pub const PAGE_SIZE: usize = 4096;
pub const PAGE_SIZE: usize = 8192;
pub const PTR_SIZE: usize = size_of::<usize>();

pub const KEY_MAX_SIZE: usize = 0xff; // Length must fit in one byte
pub const VALUE_SIZE: usize = size_of::<u64>();

// Node header
pub const IS_ROOT_SIZE: usize = 1;
pub const IS_ROOT_OFFSET: usize = 0;
pub const NODE_KIND_SIZE: usize = 1;
pub const NODE_KIND_OFFSET: usize = 1;
pub const PARENT_POINTER_SIZE: usize = PTR_SIZE;
pub const PARENT_POINTER_OFFSET: usize = 2;
pub const NODE_HEADER_SIZE: usize = IS_ROOT_SIZE + NODE_KIND_SIZE + PARENT_POINTER_SIZE;

// Leaf node layout
pub const LEAF_NEXT_SIZE: usize = PTR_SIZE;
pub const LEAF_NEXT_OFFSET: usize = NODE_HEADER_SIZE;
pub const LEAF_PREVIOUS_SIZE: usize = PTR_SIZE;
pub const LEAF_PREVIOUS_OFFSET: usize = LEAF_NEXT_OFFSET + LEAF_NEXT_SIZE;
pub const LEAF_KEY_COUNT_SIZE: usize = PTR_SIZE;
pub const LEAF_KEY_COUNT_OFFSET: usize = LEAF_PREVIOUS_OFFSET + LEAF_PREVIOUS_SIZE;
pub const LEAF_HEADER_SIZE: usize = LEAF_KEY_COUNT_OFFSET + LEAF_KEY_COUNT_SIZE;
// results in 8192-10-24=8158 bytes of key-value data

// Internal node layout
pub const INTERNAL_CHILD_COUNT_SIZE: usize = PTR_SIZE;
pub const INTERNAL_CHILD_COUNT_OFFSET: usize = NODE_HEADER_SIZE;
pub const INTERNAL_HEADER_SIZE: usize = NODE_HEADER_SIZE + PTR_SIZE;
// results in 8192-10-8=8174 bytes of key-child data

/// Wrappers for converting byte to bool and back.
/// The convention used throughout the index file is: one is true; otherwise - false.
pub trait FromByte {
    fn from_byte(&self) -> bool;
}

pub trait ToByte {
    fn to_byte(&self) -> u8;
}

impl FromByte for u8 {
    fn from_byte(&self) -> bool {
        matches!(self, 0x01)
    }
}

impl ToByte for bool {
    fn to_byte(&self) -> u8 {
        match self {
            true => 0x01,
            false => 0x00,
        }
    }
}
