// TODO : SkipList implementation

use crate::{
    constants::SKIPLIST_MAX_LEVELS,
    storage::{engine::IoResult, storage_types::ValueLog},
};
use fastrand as random;
use std::{
    io,
    sync::{Arc, RwLock},
};

use super::storage_types;

#[derive(Debug, Clone, PartialEq)]
struct NodeData {
    key: String,
    value: Arc<ValueLog>,
}

impl NodeData {
    fn new(key: String, value: Arc<ValueLog>) -> Self {
        Self { key, value }
    }
}

#[derive(Debug, Clone)]
pub struct HeadNode {
    level: i32,
    next: Option<Arc<RwLock<Node>>>,
    next_head: Option<Arc<RwLock<HeadNode>>>,
}

impl HeadNode {
    pub fn new(level: i32) -> Self {
        Self {
            level,
            next: None,
            next_head: None,
        }
    }

    pub fn set_next(&mut self, node: Option<Arc<RwLock<Node>>>) {
        self.next = node;
    }

    pub fn set_next_head(&mut self, node: Option<Arc<RwLock<HeadNode>>>) {
        self.next_head = node;
    }
}

#[derive(Debug, Clone)]
struct Node {
    data: NodeData,
    level: i32,
    next: Option<Arc<RwLock<Node>>>,
    down: Option<Arc<RwLock<Node>>>,
}

impl Node {
    pub fn new(lvl: i32, val: Arc<ValueLog>) -> Self {
        let data = NodeData::new(val.key.clone(), val);

        Self {
            data,
            level: lvl,
            next: None,
            down: None,
        }
    }

    pub fn set_next(&mut self, node: Option<Arc<RwLock<Node>>>) {
        self.next = node;
    }

    pub fn set_down(&mut self, node: Option<Arc<RwLock<Node>>>) {
        self.down = node;
    }
}

fn random_level() -> i32 {
    let mut lvl = 0;
    while lvl < SKIPLIST_MAX_LEVELS && random::bool() {
        lvl += 1;
    }
    lvl
}

#[derive(Debug)]
pub struct SkipList {
    // pub size: i32,
    // pub max_level: i32,
    pub head: Arc<RwLock<HeadNode>>,
}

impl SkipList {
    pub fn new() -> Self {
        Self {
            head: Arc::new(RwLock::new(HeadNode::new(0))),
        }
    }

    fn search_util(&self, key: &String) -> IoResult<Vec<Arc<RwLock<Node>>>> {
        let mut curr_head = self.head.clone();
        let mut maybe_curr_node = None;

        while let Ok(reader) = curr_head.clone().read() {
            if let Some(node) = reader.next_head.clone() {
                curr_head = node.clone();
            } else {
                let nxt = reader.next.clone().ok_or(io::Error::new(
                    io::ErrorKind::Other,
                    "val not found".to_string(),
                ))?;

                maybe_curr_node = Some(nxt);
                break;
            }
        }

        let mut curr_node = maybe_curr_node.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "Node not found".to_string(),
        ))?;

        let mut stack = Vec::new();

        while let Ok(reader) = curr_node.clone().read() {
            let val_equal = &reader.data.key == key;
            let go_next = reader
                .next
                .as_ref()
                .map(|nxt| -> IoResult<bool> {
                    let next_reader = nxt
                        .read()
                        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
                    let res = &next_reader.data.key <= key;

                    Ok(res)
                })
                .transpose()?
                .unwrap_or(false);

            if val_equal {
                stack.push(curr_node.clone());
                return Ok(stack);
            }

            curr_node = if go_next {
                reader.next.clone().ok_or(io::Error::new(
                    io::ErrorKind::Other,
                    "val not found".to_string(),
                ))?
            } else if let Some(node) = reader.down.clone() {
                stack.push(curr_node.clone());
                node
            } else {
                stack.push(curr_node.clone());
                break;
            };
        }

        Ok(stack)
    }

    pub fn search(&self, key: String) -> IoResult<storage_types::ValueType> {
        let node = self
            .search_util(&key)?
            .last()
            .ok_or(io::Error::new(
                io::ErrorKind::Other,
                "val not found".to_string(),
            ))?
            .clone();

        let reader = node
            .read()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        if reader.data.key == key {
            return Ok(reader.data.value.value.clone());
        }

        Err(io::Error::new(
            io::ErrorKind::Other,
            "val not found".to_string(),
        ))
    }

    fn check_and_set(
        &self,
        key: &String,
        val: Arc<ValueLog>,
        curr_node: Arc<RwLock<Node>>,
        down_ref: Option<Arc<RwLock<Node>>>,
        curr_lvl: i32,
    ) -> IoResult<Option<Arc<RwLock<Node>>>> {
        let mut node = curr_node.clone();
        let mut res = None;

        while let Ok(mut writer) = node.clone().write() {
            let go_next = writer
                .next
                .as_ref()
                .map(|nxt| -> IoResult<bool> {
                    let next_reader = nxt
                        .read()
                        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
                    let res = &next_reader.data.key <= key;

                    Ok(res)
                })
                .transpose()?
                .unwrap_or(false);

            node = if go_next {
                writer.next.clone().ok_or(io::Error::new(
                    io::ErrorKind::Other,
                    "val not found".to_string(),
                ))?
            } else {
                let mut new_node = Node::new(curr_lvl, val.clone());
                new_node.set_next(writer.next.clone());
                new_node.set_down(down_ref.clone());

                writer.set_next(Some(Arc::new(RwLock::new(new_node))));
                res = writer.next.clone();
                break;
            }
        }

        Ok(res)
    }

    fn check_and_set_level(
        &self,
        key: &String,
        val: Arc<ValueLog>,
        down_ref: Option<Arc<RwLock<Node>>>,
        curr_lvl: i32,
    ) -> IoResult<Option<Arc<RwLock<Node>>>> {
        let mut node = self.head.clone();
        let mut res = None;
        let mut down_ref = down_ref.clone();

        while let Ok(mut writer) = node.clone().write() {
            let go_next = writer
                .next_head
                .as_ref()
                .map(|nxt| -> IoResult<bool> {
                    let next_reader = nxt
                        .read()
                        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
                    let res = next_reader.level >= curr_lvl;

                    Ok(res)
                })
                .transpose()?
                .unwrap_or(false);

            node = if go_next {
                let next_head = writer.next_head.clone().ok_or(io::Error::new(
                    io::ErrorKind::Other,
                    "val not found".to_string(),
                ))?;

                let next_node = writer.next.clone().ok_or(io::Error::new(
                    io::ErrorKind::Other,
                    "val not found".to_string(),
                ))?;
                let next_reader = next_node
                    .read()
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
                let should_update = &next_reader.data.key > key;

                down_ref = if should_update {
                    let mut new_node = Node::new(curr_lvl, val.clone());
                    new_node.set_next(Some(next_node.clone()));
                    new_node.set_down(down_ref.clone());

                    writer.set_next(Some(Arc::new(RwLock::new(new_node))));
                    writer.next.clone()
                } else {
                    self.check_and_set(&key, val.clone(), next_node.clone(), down_ref, curr_lvl)?
                };

                next_head
            } else {
                let mut new_head = HeadNode::new(curr_lvl);
                let mut new_node = Node::new(curr_lvl, val.clone());
                new_node.set_down(down_ref.clone());

                new_head.set_next(Some(Arc::new(RwLock::new(new_node))));
                res = new_head.next.clone();

                writer.set_next_head(Some(Arc::new(RwLock::new(new_head))));

                break;
            }
        }

        Ok(res)
    }

    pub fn insert(&self, key: String, val: Arc<ValueLog>) -> IoResult<()> {
        let mut search_stack = self.search_util(&key)?;
        let expected_level = random_level();
        let mut curr_lvl = 0;
        let mut new_node_down_ref = None;

        let mut curr_node = search_stack
            .pop()
            .ok_or(io::Error::new(
                io::ErrorKind::Other,
                "Insert failed".to_string(),
            ))?
            .clone();

        let exists = {
            let reader = curr_node
                .read()
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

            reader.data.key == key
        };

        while let Ok(mut writer) = curr_node.clone().write() {
            if exists {
                writer.data.value = val.clone();

                if let Some(node) = writer.down.clone() {
                    curr_node = node;
                } else {
                    break;
                }
            } else {
                new_node_down_ref = self.check_and_set(
                    &key,
                    val.clone(),
                    curr_node,
                    new_node_down_ref.clone(),
                    curr_lvl,
                )?;
                curr_lvl += 1;

                curr_node = if curr_lvl <= expected_level {
                    if let Some(node) = search_stack.pop() {
                        node.clone()
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        while curr_lvl <= expected_level {
            // adjust levels
            new_node_down_ref =
                self.check_and_set_level(&key, val.clone(), new_node_down_ref.clone(), curr_lvl)?;
            curr_lvl += 1;
        }

        Ok(())
    }

    pub fn remove(&self, key: String) -> IoResult<()> {
        let mut curr_head = self.head.clone();
        let mut head_stack = Vec::new();
        let mut curr_node = None;

        while let Ok(reader) = curr_head.clone().read() {
            head_stack.push(curr_head.clone());
            if let Some(node) = reader.next_head.clone() {
                curr_head = node.clone();
            } else {
                break;
            }
        }

        while curr_node.is_none() {
            if head_stack.is_empty() {
                return Ok(());
            }

            let head = head_stack
                .pop()
                .ok_or(io::Error::new(
                    io::ErrorKind::Other,
                    "Remove failed".to_string(),
                ))?
                .clone();

            let mut key_node = None;

            let mut writer = head
                .write()
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

            curr_node = writer
                .next
                .as_ref()
                .map(|nxt| -> IoResult<Option<Arc<RwLock<Node>>>> {
                    let next_reader = nxt
                        .read()
                        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
                    let res = if next_reader.data.key < key {
                        Some(nxt.clone())
                    } else if next_reader.data.key == key {
                        key_node = next_reader.next.clone();
                        None
                    } else {
                        None
                    };

                    Ok(res)
                })
                .transpose()?
                .flatten();

            if let Some(node) = key_node {
                let reader = node
                    .read()
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

                writer.next = reader.next.clone();
            }
        }

        let mut node = curr_node
            .ok_or(io::Error::new(
                io::ErrorKind::Other,
                "Remove failed".to_string(),
            ))?
            .clone();

        while let Ok(mut writer) = node.clone().write() {
            let should_update = writer
                .next
                .as_ref()
                .map(|nxt| -> IoResult<bool> {
                    let next_reader = nxt
                        .read()
                        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
                    let res = next_reader.data.key == key;

                    Ok(res)
                })
                .transpose()?
                .unwrap_or(false);

            let go_next = writer
                .next
                .as_ref()
                .map(|nxt| -> IoResult<bool> {
                    let next_reader = nxt
                        .read()
                        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
                    let res = next_reader.data.key < key;

                    Ok(res)
                })
                .transpose()?
                .unwrap_or(false);

            if should_update {
                let next_node = writer.next.clone().ok_or(io::Error::new(
                    io::ErrorKind::Other,
                    "Remove failed".to_string(),
                ))?;
                let next_reader = next_node
                    .read()
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
                writer.next = next_reader.next.clone();

                node = if let Some(down) = writer.down.clone() {
                    down
                } else {
                    break;
                }
            } else if go_next {
                let next_node = writer.next.clone().ok_or(io::Error::new(
                    io::ErrorKind::Other,
                    "Remove failed".to_string(),
                ))?;

                node = next_node;
            } else if let Some(down) = writer.down.clone() {
                node = down.clone()
            } else {
                break;
            }
        }

        Ok(())
    }
}
