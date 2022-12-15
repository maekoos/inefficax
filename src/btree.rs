use std::{path::Path, vec};

use crate::{
    error::Error,
    node::{KeyValuePair, Node, NodeKind},
    page::Page,
    page_layout::{INTERNAL_HEADER_SIZE, LEAF_HEADER_SIZE, PTR_SIZE, VALUE_SIZE},
    pager::{FreeQueue, Offset, Pager},
    PAGE_SIZE,
};

pub struct BTree {
    pager: Pager,
}

// Underflow at less than half of page size
const UNDERFLOW_SPACE: usize = PAGE_SIZE / 3;

impl BTree {
    pub fn open(db_fp: &Path) -> Result<Self, Error> {
        let mut pager = Pager::open(db_fp)?;

        if pager.config.root_page == None {
            let root = Node::new(
                NodeKind::Leaf {
                    next: None,
                    previous: None,
                    key_value_pairs: vec![],
                    occupied_space: 0,
                },
                None,
            );
            let root_offset = pager.write_page(&Page::try_from(&root)?)?;

            pager.set_root_page(root_offset)?;
        }

        Ok(Self { pager })
    }

    pub fn get_file_size(&self) -> Result<u64, Error> {
        self.pager.get_file_size()
    }

    fn root_offset(&self) -> Result<Offset, Error> {
        self.pager
            .config
            .root_page
            .to_owned()
            .ok_or(Error::InvalidRootOffset)
    }

    pub fn insert(&mut self, key: String, value: u64) -> Result<(), Error> {
        let mut fq = FreeQueue::new();

        let root_offset = self.root_offset()?;
        let root_page = self.pager.get_page(&root_offset)?;
        let root = Node::try_from(root_page)?;

        let status = self.insert_cow(&mut fq, root, &root_offset, KeyValuePair { key, value })?;
        match status {
            InsertCOWStatus::NewOffset(o) => {
                fq.add(root_offset);
                self.pager.set_root_page(o)?;
            }
            InsertCOWStatus::DidSplit {
                promoted_key,
                first,
                second,
            } => {
                // The root node was split in two
                let new_root_node = Node::new(
                    NodeKind::Internal {
                        keys: vec![promoted_key],
                        children: vec![first, second],
                        occupied_space: 0,
                    },
                    None,
                );

                // Save the new root node + free
                let new_root_offset = self.pager.write_page(&Page::try_from(&new_root_node)?)?;
                fq.add(root_offset);

                self.pager.set_root_page(new_root_offset)?;
            }
        }

        self.pager.free_pages(fq)?;
        Ok(())
    }

    fn insert_cow(
        &mut self,
        fq: &mut FreeQueue,
        node: Node,
        node_offset: &Offset,
        kv: KeyValuePair,
    ) -> Result<InsertCOWStatus, Error> {
        // TODO: Need to update the new child's parent_node offset
        // TODO: Unless we find a way to never need the parent's offset...

        match node.node_kind {
            NodeKind::Internal {
                mut keys,
                mut children,
                occupied_space,
            } => {
                // Find where to put this key
                let idx = keys.binary_search(&kv.key).unwrap_or_else(|x| x);
                let child_offset = children.get(idx).ok_or(Error::InternalNodeNoChild)?;
                let child_page = self.pager.get_page(child_offset)?;
                let child = Node::try_from(child_page)?;

                let status = self.insert_cow(fq, child, &child_offset, kv)?;

                match status {
                    InsertCOWStatus::NewOffset(new_child_offset) => {
                        // Update the child's offset
                        children[idx] = new_child_offset;
                        // Write this node to disk
                        let o = self.pager.write_page(&Page::try_from(&Node::new(
                            NodeKind::Internal {
                                keys,
                                children,
                                occupied_space,
                            },
                            None,
                        ))?)?;
                        // Free the old version of this node
                        fq.add(node_offset.to_owned());

                        Ok(InsertCOWStatus::NewOffset(o))
                    }
                    InsertCOWStatus::DidSplit {
                        promoted_key,
                        first,
                        second,
                    } => {
                        let available_space = PAGE_SIZE - occupied_space;
                        // A new key and a new child (reusing one child) + 1 for some reason?
                        let required_space = PTR_SIZE + promoted_key.bytes().len() + 1 + 1;

                        if available_space < required_space {
                            // Add the new node and update child position
                            // THIS WILL OVERFLOW THIS NODE - make sure it is split later
                            children[idx] = first;
                            children.insert(idx + 1, second);
                            keys.insert(idx, promoted_key);

                            // TODO: Replace the following with find_median_key_idx:
                            let median_idx = find_median_key_idx(&keys);
                            if median_idx == 0 {
                                return Err(Error::ImpossibleSplit);
                            }

                            // Get the sibling keys
                            let mut sibling_keys = keys.split_off(median_idx);
                            // Pop the median key from the sibling keys, since we are in an internal node
                            let new_promoted_key = sibling_keys.remove(0);
                            // Get the sibling's children
                            let sibling_children = children.split_off(median_idx + 1);

                            let first_offset =
                                self.pager.write_page(&Page::try_from(&Node::new(
                                    NodeKind::Internal {
                                        keys,
                                        children,
                                        occupied_space: 0,
                                    },
                                    None,
                                ))?)?;
                            let second_offset =
                                self.pager.write_page(&Page::try_from(&Node::new(
                                    NodeKind::Internal {
                                        keys: sibling_keys,
                                        children: sibling_children,
                                        occupied_space: 0,
                                    },
                                    None,
                                ))?)?;

                            Ok(InsertCOWStatus::DidSplit {
                                promoted_key: new_promoted_key,
                                first: first_offset,
                                second: second_offset,
                            })
                        } else {
                            // Add the new node and update child position
                            children[idx] = first;
                            children.insert(idx + 1, second);
                            keys.insert(idx, promoted_key);

                            // Write this node to disk
                            let o = self.pager.write_page(&Page::try_from(&Node::new(
                                NodeKind::Internal {
                                    keys,
                                    children,
                                    occupied_space,
                                },
                                None,
                            ))?)?;
                            // Free the old version of this node
                            fq.add(node_offset.to_owned());

                            Ok(InsertCOWStatus::NewOffset(o))
                        }
                    }
                }
            }
            NodeKind::Leaf {
                next,
                previous,
                mut key_value_pairs,
                occupied_space,
            } => {
                let available_space = PAGE_SIZE - occupied_space;
                let required_space = 1 + kv.key.bytes().len() + VALUE_SIZE;

                // Check if we have enough space to fit this key
                if available_space < required_space {
                    // Split the leaf node in two
                    let (promoted_key, mut sibling_key_value_pairs) =
                        split_key_value_pairs(&mut key_value_pairs)?;

                    // We assume we have enough space now that the node is split
                    // Insert into appropriate node
                    if kv.key <= promoted_key {
                        let idx = key_value_pairs.binary_search(&kv).unwrap_or_else(|x| x);
                        key_value_pairs.insert(idx, kv);
                    } else {
                        let idx = sibling_key_value_pairs
                            .binary_search(&kv)
                            .unwrap_or_else(|x| x);
                        sibling_key_value_pairs.insert(idx, kv);
                    }

                    // TODO: Next and previous
                    let sibling = Node::new(
                        NodeKind::Leaf {
                            next: None,
                            previous: None,
                            key_value_pairs: sibling_key_value_pairs,
                            occupied_space: 0, // Won't be used
                        },
                        node.parent_offset.to_owned(),
                    );

                    // Write this node and it's sibling to disk
                    // let new_node_offset = self.pager.write_page(&Page::try_from(&node)?)?;
                    let new_node_offset = self.pager.write_page(&Page::try_from(&Node::new(
                        NodeKind::Leaf {
                            next,
                            previous,
                            key_value_pairs,
                            occupied_space: 0, // Won't be used
                        },
                        None,
                    ))?)?;
                    fq.add(node_offset.to_owned());
                    // TODO: Update sibling's previous address before writing
                    let sibling_offset = self.pager.write_page(&Page::try_from(&sibling)?)?;
                    // TODO: Update new_node's next address

                    Ok(InsertCOWStatus::DidSplit {
                        promoted_key,
                        first: new_node_offset,
                        second: sibling_offset,
                    })
                } else {
                    // Since we have enough space, we can simply insert the new kv
                    let idx = key_value_pairs.binary_search(&kv).unwrap_or_else(|x| x);
                    key_value_pairs.insert(idx, kv);

                    // Copy on write requires us to write the updated data to a new node
                    let new_addr = self.pager.write_page(&Page::try_from(&Node::new(
                        NodeKind::Leaf {
                            next,
                            previous,
                            key_value_pairs,
                            occupied_space,
                        },
                        None,
                    ))?)?;
                    fq.add(node_offset.to_owned());

                    // Return the new address to the parent node
                    Ok(InsertCOWStatus::NewOffset(new_addr))
                }
            }
        }
    }

    pub fn delete(&mut self, key: &str) -> Result<Option<u64>, Error> {
        let mut fq = FreeQueue::new();

        let root_offset = self.root_offset()?;
        let root_page = self.pager.get_page(&root_offset)?;
        let root = Node::try_from(root_page)?;

        let (removed_value, status) = self.delete_cow(&mut fq, root, &root_offset, key)?;

        match status {
            DeleteCOWStatus::NewOffset(o) => {
                fq.add(root_offset);
                self.pager.set_root_page(o)?;
            }
            DeleteCOWStatus::DidUnderflow(node) => {
                // It's totally fine for the root node to underflow, as long as it has enough keys.
                // Save the new node as root and free old one

                if let NodeKind::Internal {
                    keys,
                    children,
                    occupied_space: _,
                } = &node.node_kind
                {
                    // If the node only has one child, aka keys.len() = 0, we promote
                    // that lonely child to be the new root. Otherwise we just write
                    // the underflowing but not lonely node to disk.
                    if keys.len() == 0 {
                        // Promote this child
                        self.pager.set_root_page(children[0].to_owned())?;

                        fq.add(root_offset);
                        self.pager.free_pages(fq)?;

                        return Ok(removed_value);
                    }
                }

                fq.add(root_offset);
                let new_root_offset = self.pager.write_page(&Page::try_from(&node)?)?;
                self.pager.set_root_page(new_root_offset)?;
            }
        }

        self.pager.free_pages(fq)?;
        Ok(removed_value)
    }

    fn delete_cow(
        &mut self,
        fq: &mut FreeQueue,
        node: Node,
        node_offset: &Offset,
        key: &str,
    ) -> Result<(Option<u64>, DeleteCOWStatus), Error> {
        match node.node_kind {
            NodeKind::Internal {
                keys,
                mut children,
                occupied_space,
            } => {
                let child_idx = keys.binary_search(&key.to_owned()).unwrap_or_else(|x| x);

                // Get the child page
                let child_offset = children.get(child_idx).ok_or(Error::InternalNodeNoChild)?;
                let child_node = Node::try_from(self.pager.get_page(child_offset)?)?;

                let (removed_value, status) = self.delete_cow(fq, child_node, child_offset, key)?;
                match status {
                    DeleteCOWStatus::NewOffset(o) => {
                        // Update the child position to the copy and free old
                        children[child_idx] = o;
                        fq.add(node_offset.to_owned());

                        // Write this node and return success
                        let offset = self.pager.write_page(&Page::try_from(&Node::new(
                            NodeKind::Internal {
                                keys,
                                children,
                                occupied_space,
                            },
                            None,
                        ))?)?;

                        Ok((removed_value, DeleteCOWStatus::NewOffset(offset)))
                    }
                    DeleteCOWStatus::DidUnderflow(node) => {
                        // Get the index of the node to borrow or merge with
                        let sibling_idx = if child_idx == 0 { 1 } else { child_idx - 1 };
                        if sibling_idx >= children.len() {
                            // If we always make sure no nodes underflow this should never happen.
                            // Not even in the root node - since it becomes a leaf node.
                            dbg!(keys);
                            unreachable!("DidUnderflow - only one child");
                        }
                        let sibling_offset = &children[sibling_idx];
                        let sibling_node = Node::try_from(self.pager.get_page(&sibling_offset)?)?;

                        match node.node_kind {
                            NodeKind::Internal {
                                keys: mut child_keys,
                                children: mut child_children,
                                occupied_space: child_occupied_space,
                            } => {
                                if let NodeKind::Internal {
                                    keys: mut sibling_keys,
                                    children: mut sibling_children,
                                    occupied_space: sibling_occupied_space,
                                } = sibling_node.node_kind
                                {
                                    // If it is possible to merge, we merge
                                    // Otherwise we borrow
                                    if sibling_occupied_space + child_occupied_space
                                        - INTERNAL_HEADER_SIZE
                                        < PAGE_SIZE
                                    {
                                        // println!("Merge - internal underflow + internal children");

                                        // Merge is possible!
                                        if sibling_idx < child_idx {
                                            // TODO: Could the added key possibly result in overflow?
                                            // TODO: This would be a very rare issue, since we are
                                            // TODO: underflowing...
                                            // Add the key in parent between the two children
                                            sibling_keys
                                                .push(keys[child_idx.min(sibling_idx)].to_owned());

                                            sibling_keys.extend(child_keys);
                                            sibling_children.extend(child_children);
                                            child_keys = sibling_keys;
                                            child_children = sibling_children;
                                        } else {
                                            // TODO: Could the added key possibly result in overflow?
                                            // Add the key in parent between the two children
                                            child_keys
                                                .push(keys[child_idx.min(sibling_idx)].to_owned());

                                            child_keys.extend(sibling_keys);
                                            child_children.extend(sibling_children);
                                        }

                                        assert_eq!(child_children.len(), child_keys.len() + 1);

                                        // Write the merged child to disk
                                        let new_child_offset =
                                            self.pager.write_page(&Page::try_from(&Node::new(
                                                NodeKind::Internal {
                                                    keys: child_keys,
                                                    children: child_children,
                                                    occupied_space: 0,
                                                },
                                                None,
                                            ))?)?;

                                        // TODO: Is it possible to avoid cloning?
                                        let mut new_children = children.clone();
                                        let mut new_keys = keys.clone();

                                        // Update child and remove the merged sibling + key
                                        new_children[child_idx] = new_child_offset;
                                        new_children.remove(sibling_idx);
                                        let removed_key =
                                            new_keys.remove(child_idx.min(sibling_idx));

                                        fq.add(child_offset.to_owned());
                                        fq.add(sibling_offset.to_owned());

                                        assert_ne!(new_children.len(), 0);

                                        // Check whether or not the merge results in
                                        // this node underflowing
                                        // (occupied - removed key - removed sibling)
                                        let new_occupied = occupied_space
                                            - (removed_key.as_bytes().len() + 1 + PTR_SIZE);

                                        let new_node = Node::new(
                                            NodeKind::Internal {
                                                keys: new_keys,
                                                children: new_children,
                                                occupied_space: new_occupied,
                                            },
                                            None,
                                        );

                                        // Either return underflow or write the new
                                        // node to disk and return the new offset
                                        if new_occupied < UNDERFLOW_SPACE {
                                            Ok((
                                                removed_value,
                                                DeleteCOWStatus::DidUnderflow(new_node),
                                            ))
                                        } else {
                                            let offset = self
                                                .pager
                                                .write_page(&Page::try_from(&new_node)?)?;

                                            fq.add(node_offset.to_owned());
                                            Ok((removed_value, DeleteCOWStatus::NewOffset(offset)))
                                        }
                                    } else {
                                        // We have to split!
                                        // println!("Split - internal underflow + internal children");

                                        // Merge all keys and split in middle
                                        let median_key;
                                        if child_idx < sibling_idx {
                                            child_keys
                                                .push(keys[child_idx.min(sibling_idx)].to_owned());
                                            child_keys.extend(sibling_keys);
                                            child_children.extend(sibling_children);

                                            let median_idx = find_median_key_idx(&child_keys);

                                            if median_idx == 0 {
                                                unreachable!("Impossible split");
                                            }

                                            // Get the sibling keys
                                            sibling_keys = child_keys.split_off(median_idx);
                                            // Pop the median key from the sibling keys
                                            median_key = sibling_keys.remove(0);
                                            // Get the sibling's children
                                            sibling_children =
                                                child_children.split_off(median_idx + 1);
                                        } else {
                                            sibling_keys
                                                .push(keys[child_idx.min(sibling_idx)].to_owned());
                                            sibling_keys.extend(child_keys);
                                            sibling_children.extend(child_children);

                                            let median_idx = find_median_key_idx(&sibling_keys);
                                            if median_idx == 0 {
                                                unreachable!("Impossible split");
                                            }

                                            // Get the child keys
                                            child_keys = sibling_keys.split_off(median_idx);
                                            // Pop the median key from the child keys
                                            median_key = child_keys.remove(0);
                                            // Get the child's children
                                            child_children =
                                                sibling_children.split_off(median_idx + 1)
                                        }

                                        assert_eq!(child_children.len(), child_keys.len() + 1);
                                        assert_eq!(sibling_children.len(), sibling_keys.len() + 1);

                                        // Write the child and its sibling
                                        let new_child_offset =
                                            self.pager.write_page(&Page::try_from(&Node::new(
                                                NodeKind::Internal {
                                                    keys: child_keys,
                                                    children: child_children,
                                                    occupied_space: 0,
                                                },
                                                None,
                                            ))?)?;
                                        let new_sibling_offset =
                                            self.pager.write_page(&Page::try_from(&Node::new(
                                                NodeKind::Internal {
                                                    keys: sibling_keys,
                                                    children: sibling_children,
                                                    occupied_space: 0,
                                                },
                                                None,
                                            ))?)?;

                                        // TODO: Is it possible to avoid cloning?
                                        let mut new_children = children.clone();
                                        let mut new_keys = keys.clone();

                                        // Update child and remove the merged sibling + key
                                        new_children[sibling_idx] = new_sibling_offset;
                                        new_children[child_idx] = new_child_offset;
                                        // new_keys[if child_idx == 0 { 0 } else { child_idx - 1 }] =
                                        new_keys[child_idx.min(sibling_idx)] = median_key;

                                        fq.add(child_offset.to_owned());
                                        fq.add(sibling_offset.to_owned());

                                        // Neither of the two nodes (child and sibling) should
                                        // theoretically be underflowing, considering the
                                        // combined size is larger than PAGE_SIZE.

                                        let offset =
                                            self.pager.write_page(&Page::try_from(&Node::new(
                                                NodeKind::Internal {
                                                    keys: new_keys,
                                                    children: new_children,
                                                    occupied_space: 0,
                                                },
                                                None,
                                            ))?)?;

                                        fq.add(node_offset.to_owned());
                                        Ok((removed_value, DeleteCOWStatus::NewOffset(offset)))
                                    }
                                } else {
                                    unreachable!("An internal node can only have either leaf *or* internal children.");
                                }
                            }
                            NodeKind::Leaf {
                                next: _,
                                previous: _,
                                key_value_pairs: mut child_kv_pairs,
                                occupied_space: child_occupied_space,
                            } => {
                                if let NodeKind::Leaf {
                                    next: _,
                                    previous: _,
                                    key_value_pairs: mut sibling_kv_pairs,
                                    occupied_space: sibling_occupied_space,
                                } = sibling_node.node_kind
                                {
                                    // If it is possible to merge, we merge
                                    // Otherwise we borrow
                                    if sibling_occupied_space + child_occupied_space
                                        - LEAF_HEADER_SIZE
                                        < PAGE_SIZE
                                    {
                                        // Merge all keys
                                        if sibling_idx < child_idx {
                                            sibling_kv_pairs.extend(child_kv_pairs);
                                            child_kv_pairs = sibling_kv_pairs;
                                        } else {
                                            child_kv_pairs.extend(sibling_kv_pairs);
                                        }

                                        assert_ne!(child_kv_pairs.len(), 0);

                                        // Write the child
                                        let new_child_offset =
                                            self.pager.write_page(&Page::try_from(&Node::new(
                                                NodeKind::Leaf {
                                                    next: None,
                                                    previous: None,
                                                    key_value_pairs: child_kv_pairs,
                                                    occupied_space: 0,
                                                },
                                                None,
                                            ))?)?;

                                        // TODO: Is it possible to avoid cloning?
                                        let mut new_children = children.clone();
                                        let mut new_keys = keys.clone();

                                        // Update child and remove the merged sibling + key
                                        new_children[child_idx] = new_child_offset;
                                        new_children.remove(sibling_idx);
                                        let removed_key =
                                            new_keys.remove(child_idx.min(sibling_idx));

                                        fq.add(child_offset.to_owned());
                                        fq.add(sibling_offset.to_owned());

                                        assert_ne!(new_children.len(), 0);

                                        // Check whether or not the merge results in
                                        // this node underflowing
                                        // (occupied - removed key - removed sibling)
                                        let new_occupied = occupied_space
                                            - (removed_key.as_bytes().len() + 1 + PTR_SIZE);

                                        let new_node = Node::new(
                                            NodeKind::Internal {
                                                keys: new_keys,
                                                children: new_children,
                                                occupied_space: new_occupied,
                                            },
                                            None,
                                        );

                                        // Either return underflow or write the new
                                        // node to disk and return the new offset
                                        if new_occupied < UNDERFLOW_SPACE {
                                            Ok((
                                                removed_value,
                                                DeleteCOWStatus::DidUnderflow(new_node),
                                            ))
                                        } else {
                                            let offset = self
                                                .pager
                                                .write_page(&Page::try_from(&new_node)?)?;

                                            fq.add(node_offset.to_owned());
                                            Ok((removed_value, DeleteCOWStatus::NewOffset(offset)))
                                        }
                                    } else {
                                        // println!("Split");
                                        // Merge all keys and split in middle
                                        let median_key;
                                        // The split_key_value_pairs function assumes the keys are ordered
                                        if child_idx < sibling_idx {
                                            child_kv_pairs.extend(sibling_kv_pairs);
                                            (median_key, sibling_kv_pairs) =
                                                split_key_value_pairs(&mut child_kv_pairs)?;
                                        } else {
                                            sibling_kv_pairs.extend(child_kv_pairs);
                                            (median_key, child_kv_pairs) =
                                                split_key_value_pairs(&mut sibling_kv_pairs)?;
                                        }

                                        // println!(
                                        //     "Splitting - sibling first: {} - KVP ch: {} KVP sib: {}",
                                        //     sibling_idx < child_idx,
                                        //     child_kv_pairs[0].key,
                                        //     sibling_kv_pairs[0].key
                                        // );

                                        // Write the child and its sibling
                                        let new_child_offset =
                                            self.pager.write_page(&Page::try_from(&Node::new(
                                                NodeKind::Leaf {
                                                    next: None,
                                                    previous: None,
                                                    key_value_pairs: child_kv_pairs,
                                                    occupied_space: 0,
                                                },
                                                None,
                                            ))?)?;
                                        let new_sibling_offset =
                                            self.pager.write_page(&Page::try_from(&Node::new(
                                                NodeKind::Leaf {
                                                    next: None,
                                                    previous: None,
                                                    key_value_pairs: sibling_kv_pairs,
                                                    occupied_space: 0,
                                                },
                                                None,
                                            ))?)?;

                                        // TODO: Is it possible to avoid cloning?
                                        let mut new_children = children.clone();
                                        let mut new_keys = keys.clone();

                                        // Update child and remove the merged sibling + key
                                        new_children[sibling_idx] = new_sibling_offset;
                                        new_children[child_idx] = new_child_offset;
                                        // new_keys[if child_idx == 0 { 0 } else { child_idx - 1 }] =
                                        new_keys[child_idx.min(sibling_idx)] = median_key;

                                        fq.add(child_offset.to_owned());
                                        fq.add(sibling_offset.to_owned());

                                        // Neither of the two nodes (child and sibling) should
                                        // theoretically be underflowing, considering the
                                        // combined size is larger than PAGE_SIZE.

                                        let offset =
                                            self.pager.write_page(&Page::try_from(&Node::new(
                                                NodeKind::Internal {
                                                    keys: new_keys,
                                                    children: new_children,
                                                    occupied_space: 0,
                                                },
                                                None,
                                            ))?)?;

                                        fq.add(node_offset.to_owned());
                                        Ok((removed_value, DeleteCOWStatus::NewOffset(offset)))
                                    }
                                } else {
                                    unreachable!("An internal node can only have either leaf *or* internal children.");
                                }
                            }
                        }
                    }
                }
            }
            NodeKind::Leaf {
                next,
                previous,
                mut key_value_pairs,
                occupied_space,
            } => {
                // Find the index of the value to remove
                let idx = key_value_pairs
                    .binary_search_by_key(&key, |kv| &*kv.key)
                    .map_err(|_| Error::KeyNotFound(key.to_owned()))?;

                // Remove and calculate the space difference
                let removed = key_value_pairs.remove(idx);
                let removed_space = removed.key.as_bytes().len() + 1 + VALUE_SIZE;

                // This is fine on root:
                // assert_ne!(key_value_pairs.len(), 0);

                if occupied_space - removed_space < UNDERFLOW_SPACE {
                    Ok((
                        Some(removed.value),
                        DeleteCOWStatus::DidUnderflow(Node::new(
                            NodeKind::Leaf {
                                next,
                                previous,
                                key_value_pairs,
                                occupied_space: occupied_space - removed_space,
                            },
                            None,
                        )),
                    ))
                } else {
                    let offset = self.pager.write_page(&Page::try_from(&Node::new(
                        NodeKind::Leaf {
                            next,
                            previous,
                            key_value_pairs,
                            occupied_space,
                        },
                        None,
                    ))?)?;
                    fq.add(node_offset.to_owned());

                    Ok((Some(removed.value), DeleteCOWStatus::NewOffset(offset)))
                }
            }
        }
    }

    pub fn search(&mut self, key: &str) -> Result<Option<u64>, Error> {
        let root_page = self.pager.get_page(&self.root_offset()?)?;
        let root_node = Node::try_from(root_page)?;

        self.search_node(&root_node, &key)
    }

    fn search_node(&mut self, node: &Node, key: &str) -> Result<Option<u64>, Error> {
        match &node.node_kind {
            NodeKind::Internal {
                keys,
                children,
                occupied_space: _,
            } => {
                let idx = keys.binary_search(&key.to_string()).unwrap_or_else(|x| x);
                let child_offset = children.get(idx).ok_or(Error::InternalNodeNoChild)?;
                let child_page = self.pager.get_page(child_offset)?;
                let child_node = Node::try_from(child_page)?;

                self.search_node(&child_node, key)
            }
            NodeKind::Leaf {
                next: _,
                previous: _,
                key_value_pairs,
                occupied_space: _,
            } => Ok(key_value_pairs
                .binary_search_by_key(&key, |a| &a.key)
                .ok()
                .map(|x| key_value_pairs[x].value)),
        }
    }

    pub fn print(&mut self) -> Result<(), Error> {
        println!();

        let offset = self.root_offset()?;
        self.print_sub_tree("".to_string(), &offset)?;

        Ok(())
    }

    fn print_sub_tree(&mut self, prefix: String, offset: &Offset) -> Result<(), Error> {
        println!("{}Node at offset: {}", prefix, offset.0);
        let cur_prefix = format!("{}|->", prefix);
        let page = self.pager.get_page(&offset)?;
        let node = Node::try_from(page)?;

        match node.node_kind {
            NodeKind::Internal {
                keys,
                children,
                occupied_space,
            } => {
                println!("{}Internal child count: {:?}", cur_prefix, children.len());
                println!("{}Occupied space: {:?}", cur_prefix, occupied_space);
                println!("{}Keys: {:?}", cur_prefix, keys);

                let child_prefix = format!("{}   |  ", prefix);
                for child_offset in children {
                    println!("{}{}:", cur_prefix, child_offset.0);
                    self.print_sub_tree(child_prefix.clone(), &child_offset)?;
                }
                Ok(())
            }
            NodeKind::Leaf {
                next: _,
                previous: _,
                key_value_pairs,
                occupied_space,
            } => {
                println!(
                    "{}Leaf kv-pair count: {:?}",
                    cur_prefix,
                    key_value_pairs.len()
                );
                println!("{}Occupied space: {:?}", cur_prefix, occupied_space);
                println!("{}Leaf kv-pairs: {:?}", cur_prefix, key_value_pairs);
                Ok(())
            }
        }
    }

    pub fn count_nodes(&mut self) -> Result<usize, Error> {
        fn sub(s: &mut BTree, offset: &Offset) -> Result<usize, Error> {
            let page = s.pager.get_page(&offset)?;
            let node = Node::try_from(page)?;

            match node.node_kind {
                NodeKind::Internal {
                    keys: _,
                    children,
                    occupied_space: _,
                } => {
                    let mut sum = 0;
                    for child_offset in children {
                        let c = sub(s, &child_offset)?;
                        sum += c;
                    }
                    Ok(sum + 1)
                }
                NodeKind::Leaf {
                    next: _,
                    previous: _,
                    key_value_pairs: _,
                    occupied_space: _,
                } => Ok(1),
            }
        }

        let offset = self.root_offset()?;
        sub(self, &offset)
    }

    /// Get the depth of the current b-tree, including root.
    pub fn get_depth(&mut self) -> Result<usize, Error> {
        fn sub(s: &mut BTree, offset: &Offset) -> Result<usize, Error> {
            let page = s.pager.get_page(&offset)?;
            let node = Node::try_from(page)?;

            match node.node_kind {
                NodeKind::Internal {
                    keys: _,
                    children,
                    occupied_space: _,
                } => {
                    let mut sum = 0;
                    for child_offset in children {
                        let c = sub(s, &child_offset)?;
                        sum = sum.max(c);
                    }
                    Ok(sum + 1)
                }
                NodeKind::Leaf {
                    next: _,
                    previous: _,
                    key_value_pairs: _,
                    occupied_space: _,
                } => Ok(1),
            }
        }

        let offset = self.root_offset()?;
        sub(self, &offset)
    }

    pub fn insert_object(&mut self, key: String, object: Vec<u8>) -> Result<(), Error> {
        let o = self.pager.write_object(&object)?;

        assert_eq!(object, self.pager.get_object(&o)?);

        self.insert(key, o.0 as u64)?;

        Ok(())
    }

    pub fn search_object(&mut self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let o = self.search(key)?;
        if let Some(v) = o {
            let v = Offset(v as usize);
            let obj = self.pager.get_object(&v)?;
            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }

    pub fn delete_object(&mut self, key: &str) -> Result<(), Error> {
        let obj_offset = self.delete(key)?;

        if let Some(obj_offset) = obj_offset {
            let offset = Offset(obj_offset as usize);
            self.pager.free_object(offset)?;
        }

        Ok(())
    }
}

enum InsertCOWStatus {
    NewOffset(Offset),
    /// Returned when a node is split in two
    DidSplit {
        promoted_key: String,
        first: Offset,
        second: Offset,
    },
}

enum DeleteCOWStatus {
    NewOffset(Offset),
    /// Returned when a node underflows
    DidUnderflow(Node),
}

fn split_key_value_pairs(
    key_value_pairs: &mut Vec<KeyValuePair>,
) -> Result<(String, Vec<KeyValuePair>), Error> {
    // Get the total length of all keys, in order to find the middle key
    let total_key_size: usize = key_value_pairs.iter().map(|x| x.key.as_bytes().len()).sum();

    // Find the median key (total_key_size/2)
    let mut key_sum = 0;
    let mut median_idx = 0;
    for (idx, kvp) in key_value_pairs.iter().enumerate() {
        key_sum += kvp.key.as_bytes().len();

        if key_sum > total_key_size / 2 + 1 {
            median_idx = idx;
            break;
        }
    }

    if median_idx == 0 {
        return Err(Error::ImpossibleSplit);
    }

    // Get siblings pairs
    let sibling_pairs = key_value_pairs.split_off(median_idx);

    // Get the median key
    let median_key = key_value_pairs
        .get(median_idx - 1)
        .ok_or(Error::ImpossibleSplit)?
        .key
        .to_owned();

    Ok((median_key, sibling_pairs))
}

fn find_median_key_idx(keys: &Vec<String>) -> usize {
    // Get the total length of all keys, in order to find the middle key
    // TODO: Use occupied_space instead?
    let total_key_size: usize = keys.iter().map(|x| x.as_bytes().len()).sum();

    // Find the median key (total_key_size/2)
    let mut key_sum = 0;
    let mut median_idx = 0;
    for (idx, key) in keys.iter().enumerate() {
        key_sum += key.as_bytes().len();

        if key_sum > total_key_size / 2 + 1 {
            median_idx = idx;
            break;
        }
    }

    median_idx
}
