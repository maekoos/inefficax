use std::{cmp::Ordering, vec};

use crate::{
    error::Error,
    page::Page,
    page_layout::{
        FromByte, ToByte, INTERNAL_CHILD_COUNT_OFFSET, INTERNAL_CHILD_COUNT_SIZE,
        INTERNAL_HEADER_SIZE, IS_ROOT_OFFSET, KEY_MAX_SIZE, LEAF_HEADER_SIZE,
        LEAF_KEY_COUNT_OFFSET, LEAF_KEY_COUNT_SIZE, LEAF_NEXT_OFFSET, LEAF_NEXT_SIZE,
        LEAF_PREVIOUS_OFFSET, LEAF_PREVIOUS_SIZE, NODE_KIND_OFFSET, PAGE_SIZE,
        PARENT_POINTER_OFFSET, PARENT_POINTER_SIZE, PTR_SIZE, VALUE_SIZE,
    },
    pager::Offset,
};

#[derive(PartialEq, Debug, Clone)]
pub struct Node {
    pub(crate) node_kind: NodeKind,
    pub(crate) parent_offset: Option<Offset>,
}

impl Node {
    pub fn new(node_kind: NodeKind, parent_offset: Option<Offset>) -> Self {
        Self {
            node_kind,
            parent_offset,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum NodeKind {
    Internal {
        keys: Vec<String>,
        children: Vec<Offset>,
        occupied_space: usize,
    },
    Leaf {
        next: Option<Offset>,
        previous: Option<Offset>,
        key_value_pairs: Vec<KeyValuePair>,
        occupied_space: usize,
    },
}

impl From<&NodeKind> for u8 {
    fn from(nk: &NodeKind) -> Self {
        match nk {
            NodeKind::Internal {
                keys: _,
                children: _,
                occupied_space: _,
            } => 0x00,
            NodeKind::Leaf {
                next: _,
                previous: _,
                key_value_pairs: _,
                occupied_space: _,
            } => 0x01,
        }
    }
}

impl TryFrom<u8> for NodeKind {
    type Error = Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Self::Internal {
                keys: vec![],
                children: vec![],
                occupied_space: 0,
            }),
            0x01 => Ok(Self::Leaf {
                next: None,
                previous: None,
                key_value_pairs: vec![],
                occupied_space: 0,
            }),
            _ => Err(Error::InvalidNodeKind),
        }
    }
}

#[derive(Eq, Debug, Clone)]
pub struct KeyValuePair {
    pub key: String,
    pub value: u64,
}

impl Ord for KeyValuePair {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for KeyValuePair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for KeyValuePair {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value
    }
}

impl KeyValuePair {
    pub fn new(key: String, value: u64) -> KeyValuePair {
        KeyValuePair { key, value }
    }
}

impl TryFrom<Page> for Node {
    type Error = Error;
    fn try_from(page: Page) -> Result<Self, Self::Error> {
        let raw = page.get_data();
        let node_kind = NodeKind::try_from(raw[NODE_KIND_OFFSET])?;
        let is_root = raw[IS_ROOT_OFFSET].from_byte();
        let parent_offset = if is_root {
            None
        } else {
            Some(Offset(page.get_usize_from_offset(PARENT_POINTER_OFFSET)?))
        };

        match node_kind {
            NodeKind::Internal {
                mut keys,
                mut children,
                occupied_space: _,
            } => {
                let child_count = page.get_usize_from_offset(INTERNAL_CHILD_COUNT_OFFSET)?;

                let mut offset = INTERNAL_HEADER_SIZE;

                // Number of keys is one less than the number of children
                for _ in 1..child_count {
                    let key_length = raw[offset] as usize; // in bytes
                    offset += 1;
                    if key_length == 0 {
                        return Err(Error::KeyParseError);
                    }

                    let bytes = raw[offset..offset + key_length].to_owned();
                    let key = String::from_utf8(bytes).map_err(|_| Error::KeyParseError)?;
                    offset += key_length;

                    keys.push(key);
                }

                for _ in 0..child_count {
                    // TODO: Error here should be virtually impossible, since it is checked in the node serialization (?)
                    let child = page.get_usize_from_offset(offset).map_err(|_| {
                        Error::UnexpectedError(
                            "Failed to get usize when reading child offset in internal node"
                                .to_string(),
                        )
                    })?;
                    offset += PTR_SIZE;

                    children.push(Offset(child));
                }

                let occupied_space = offset + 1;

                Ok(Node {
                    parent_offset,
                    node_kind: NodeKind::Internal {
                        keys,
                        children,
                        occupied_space,
                    },
                })
            }
            NodeKind::Leaf {
                next: _,
                previous: _,
                key_value_pairs: _,
                occupied_space: _,
            } => {
                let next_addr = page.get_usize_from_offset(LEAF_NEXT_OFFSET)?;
                let prev_addr = page.get_usize_from_offset(LEAF_PREVIOUS_OFFSET)?;
                let number_of_keys = page.get_usize_from_offset(LEAF_KEY_COUNT_OFFSET)?;

                let mut key_value_pairs = vec![];
                let mut idx = LEAF_HEADER_SIZE;
                for _ in 0..number_of_keys {
                    let key_length = raw[idx] as usize; // in bytes
                    if key_length == 0 {
                        return Err(Error::KeyParseError);
                    }

                    let bytes = raw[idx + 1..idx + 1 + key_length].to_owned();
                    let key = String::from_utf8(bytes).map_err(|_| Error::KeyParseError)?;

                    let offset = idx + 1 + key_length;
                    let value = page.get_usize_from_offset(offset).map_err(|_| {
                        Error::UnexpectedError("Failed to get value (overflow)".to_owned())
                    })? as u64;
                    idx = offset + PTR_SIZE;

                    key_value_pairs.push(KeyValuePair { key, value });
                }

                let occupied_space = key_value_pairs
                    .iter()
                    .map(|x| 1 + x.key.as_bytes().len() + VALUE_SIZE)
                    .sum::<usize>()
                    + LEAF_HEADER_SIZE
                    + 1; // THIS PLUS 1 IS REALLY IMPORTANT AND MUST NOT BE REMOVED. (todo: find out why (: )

                Ok(Node {
                    parent_offset,
                    node_kind: NodeKind::Leaf {
                        next: if next_addr == 0 {
                            None
                        } else {
                            Some(Offset(next_addr))
                        },
                        previous: if prev_addr == 0 {
                            None
                        } else {
                            Some(Offset(prev_addr))
                        },
                        key_value_pairs,
                        occupied_space,
                    },
                })
            }
        }
    }
}

impl TryFrom<&Node> for Page {
    type Error = Error;

    fn try_from(node: &Node) -> Result<Self, Self::Error> {
        let mut data = [0x00; PAGE_SIZE];

        data[IS_ROOT_OFFSET] = (node.parent_offset == None).to_byte();
        data[NODE_KIND_OFFSET] = u8::from(&node.node_kind);

        if let Some(po) = &node.parent_offset {
            data[PARENT_POINTER_OFFSET..PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE]
                .clone_from_slice(&po.0.to_be_bytes())
        }

        match &node.node_kind {
            NodeKind::Internal {
                keys,
                children,
                occupied_space: _,
            } => {
                //  Child count
                data[INTERNAL_CHILD_COUNT_OFFSET
                    ..INTERNAL_CHILD_COUNT_OFFSET + INTERNAL_CHILD_COUNT_SIZE]
                    .clone_from_slice(&children.len().to_be_bytes());

                // Keys
                let mut offset = INTERNAL_HEADER_SIZE;
                for key in keys {
                    let key_bytes = key.as_bytes();
                    let key_length = key_bytes.len();
                    if key_length > KEY_MAX_SIZE {
                        return Err(Error::KeyOverflowError);
                    }

                    // Key length
                    data[offset] = key_length as u8;
                    offset += 1;

                    // Key
                    data[offset..offset + key_length].clone_from_slice(key_bytes);
                    offset += key_length;
                }

                // Child offsets
                for child in children {
                    if offset + PTR_SIZE >= PAGE_SIZE {
                        return Err(Error::UnexpectedError(format!(
                            "Node has too many children - overflowing: {} children ({})",
                            children.len(),
                            offset
                        )));
                    }

                    let offset_bytes = child.0.to_be_bytes();
                    data[offset..offset + PTR_SIZE].clone_from_slice(&offset_bytes);
                    offset += PTR_SIZE;
                }
            }
            NodeKind::Leaf {
                next,
                previous,
                key_value_pairs,
                occupied_space: _,
            } => {
                // Next pointer
                if let Some(next) = next {
                    data[LEAF_NEXT_OFFSET..LEAF_NEXT_OFFSET + LEAF_NEXT_SIZE]
                        .clone_from_slice(&next.0.to_be_bytes());
                }

                // Previous pointer
                if let Some(previous) = previous {
                    data[LEAF_PREVIOUS_OFFSET..LEAF_PREVIOUS_OFFSET + LEAF_PREVIOUS_SIZE]
                        .clone_from_slice(&previous.0.to_be_bytes());
                }

                // Key count
                data[LEAF_KEY_COUNT_OFFSET..LEAF_KEY_COUNT_OFFSET + LEAF_KEY_COUNT_SIZE]
                    .clone_from_slice(&key_value_pairs.len().to_be_bytes());

                // Key value pairs
                let mut offset = LEAF_HEADER_SIZE;
                for pair in key_value_pairs {
                    let key_bytes = pair.key.as_bytes();
                    let key_length = key_bytes.len();
                    if key_length > KEY_MAX_SIZE {
                        return Err(Error::KeyOverflowError);
                    }

                    if offset + key_length + 1 + VALUE_SIZE >= PAGE_SIZE {
                        return Err(Error::UnexpectedError(format!(
                            "Leaf node has too many children - overflowing: {} children ({})",
                            key_value_pairs.len(),
                            offset
                        )));
                    }

                    data[offset] = key_length as u8;
                    offset += 1;
                    data[offset..offset + key_length].clone_from_slice(key_bytes);
                    offset += key_length;

                    let value_bytes = pair.value.to_be_bytes();
                    data[offset..offset + VALUE_SIZE].clone_from_slice(&value_bytes);
                    offset += VALUE_SIZE;
                }
            }
        }

        Ok(Page::new(data))
    }
}

// mod tests {
//     use super::{Node, NodeKind};
//     use crate::{
//         error::Error,
//         node::KeyValuePair,
//         page::Page,
//         page_layout::{INTERNAL_HEADER_SIZE, LEAF_HEADER_SIZE, PTR_SIZE, VALUE_SIZE},
//         pager::Offset,
//     };

//     #[test]
//     fn serialize_and_deserialize_leaf_node() -> Result<(), Error> {
//         let some_leaf = Node::new(
//             NodeKind::Leaf {
//                 next: None,
//                 previous: None,
//                 key_value_pairs: vec![],
//                 occupied_space: 0,
//             },
//             None,
//         );

//         let page = Page::try_from(&some_leaf)?;

//         let res = Node::try_from(page)?;
//         assert_eq!(res, some_leaf);

//         Ok(())
//     }

//     #[test]
//     fn serialize_and_deserialize_internal_node() -> Result<(), Error> {
//         let some_node = Node::new(
//             NodeKind::Internal {
//                 keys: vec!["200".to_owned(), "300".to_owned()],
//                 children: vec![Offset(100), Offset(200), Offset(300)],
//                 occupied_space: 0,
//             },
//             None,
//         );

//         let page = Page::try_from(&some_node)?;

//         let res = Node::try_from(page)?;
//         assert_eq!(res, some_node);

//         Ok(())
//     }

//     #[test]
//     fn test_occupied_space_internal() -> Result<(), Error> {
//         let keys = vec![
//             "Key number 1".to_owned(),
//             "A second".to_owned(),
//             "And a third key".to_owned(),
//         ];

//         let children = vec![Offset(0), Offset(1), Offset(2), Offset(3)];

//         let actual_occupied_space = INTERNAL_HEADER_SIZE
//             + children.len() * PTR_SIZE
//             + keys.iter().map(|x| x.as_bytes().len() + 1).sum::<usize>();

//         let some_node = Node::new(
//             NodeKind::Internal {
//                 keys: keys.to_owned(),
//                 children: children.to_owned(),
//                 occupied_space: actual_occupied_space,
//             },
//             None,
//         );

//         let page = Page::try_from(&some_node)?;
//         let res = Node::try_from(page)?;

//         assert_eq!(res, some_node);

//         Ok(())
//     }

//     #[test]
//     fn test_occupied_space_leaf() -> Result<(), Error> {
//         let kvp = vec![
//             KeyValuePair {
//                 key: "A simple key".to_string(),
//                 value: 100,
//             },
//             KeyValuePair {
//                 key: "Behold this simple key".to_string(),
//                 value: 100,
//             },
//             KeyValuePair {
//                 key: "Circus circles etc".to_string(),
//                 value: 100,
//             },
//         ];

//         let actual_occupied_space = LEAF_HEADER_SIZE
//             + kvp
//                 .iter()
//                 .map(|x| x.key.as_bytes().len() + 1 + VALUE_SIZE)
//                 .sum::<usize>();

//         let some_node = Node::new(
//             NodeKind::Leaf {
//                 next: None,
//                 previous: None,
//                 key_value_pairs: kvp,
//                 occupied_space: actual_occupied_space,
//             },
//             None,
//         );

//         let page = Page::try_from(&some_node)?;
//         let res = Node::try_from(page)?;

//         assert_eq!(res, some_node);

//         Ok(())
//     }
// }
