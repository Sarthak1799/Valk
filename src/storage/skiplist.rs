// TODO : SkipList implementation

use crate::{
    constants::SKIPLIST_MAX_LEVELS,
    storage::{engine::IoResult, vlog_manager::ValueLog},
};
use fastrand as random;
use std::{
    io,
    sync::{Arc, RwLock},
};

#[derive(Debug, Clone, PartialEq)]
struct NodeData {
    key: String,
    value: Arc<ValueLog>,
}

impl NodeData {
    fn new(key: String, value: ValueLog) -> Self {
        Self {
            key,
            value: Arc::new(value),
        }
    }
}

#[derive(Debug, Clone)]
pub enum NodeType {
    Node(Node),
    Null,
}

impl NodeType {
    fn get(&self) -> &Self {
        self
    }

    fn get_mut(&mut self) -> &mut Self {
        self
    }
}

#[derive(Debug, Clone)]
struct NodeTypeLevelIter<'a> {
    node: &'a NodeType,
    idx: usize,
}

impl<'a> Iterator for NodeTypeLevelIter<'a> {
    type Item = &'a Arc<RwLock<NodeType>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.node {
            NodeType::Null => None,
            NodeType::Node(n) => {
                if self.idx < n.lvl_arr.len() {
                    let out = &n.lvl_arr[self.idx];
                    Some(out)
                } else {
                    None
                }
            }
        }
    }
}

// #[derive(Debug, Clone)]
// struct HeadNode {
//     max_level: i32,
//     lvl_arr: Vec<Arc<RwLock<NodeType>>>,
// }

// impl HeadNode {
//     fn new(lvl: i32) -> Self {
//         let mut lvl_arr = Vec::with_capacity(lvl as usize);
//         lvl_arr.resize_with(lvl as usize, || Arc::new(RwLock::new(NodeType::Null)));

//         Self {
//             max_level: lvl,
//             lvl_arr,
//         }
//     }
// }

#[derive(Debug, Clone)]
struct Node {
    data: Option<NodeData>,
    max_level: i32,
    lvl_arr: Vec<Arc<RwLock<NodeType>>>,
}

impl Node {
    pub fn new(lvl: i32, data: Option<NodeData>) -> Self {
        let mut lvl_arr = Vec::with_capacity(lvl as usize);
        lvl_arr.resize_with(lvl as usize, || Arc::new(RwLock::new(NodeType::Null)));

        Self {
            data,
            max_level: lvl,
            lvl_arr,
        }
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
    pub size: i32,
    pub max_level: i32,
    pub head: Arc<RwLock<NodeType>>,
}

impl SkipList {
    pub fn new() -> Self {
        Self {
            size: 0,
            max_level: -1,
            head: Arc::new(RwLock::new(NodeType::Null)),
        }
    }

    pub fn adjust_level(&mut self, new_lvl: i32) -> IoResult<()> {
        let mut head_writer = self
            .head
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let temp = match head_writer.get() {
            NodeType::Node(node) => node,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Invalid head node config".to_string(),
                ))
            }
        };

        let prev_lvl = self.max_level;
        let mut new_head = Node::new(new_lvl, None);

        for idx in 0..prev_lvl {
            new_head.lvl_arr[idx as usize] = temp.lvl_arr[idx as usize].clone();
        }

        drop(head_writer);

        self.max_level = new_lvl;
        self.head = Arc::new(RwLock::new(NodeType::Node(new_head)));

        Ok(())
    }

    pub fn insert(&mut self, key: String, val: ValueLog) -> IoResult<()> {
        let expected_lvl = random_level();

        if expected_lvl > self.max_level {
            self.adjust_level(expected_lvl)?;
        }

        let mut temp_node = self.head.clone();

        let mut update_arr = Vec::with_capacity(expected_lvl as usize);

        for lvl in (0..=self.max_level).rev() {
            // let temp_reader = temp_node
            //     .read()
            //     .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

            // if let NodeType::Head(node) = temp_reader.get() {
            //     let next_node = node.lvl_arr[lvl as usize].clone();

            //     let node_reader = next_node
            //         .read()
            //         .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

            //     if let NodeType::Node(next) = node_reader.get() {
            //         if next.data.key < key {
            //             drop(temp_reader);
            //             temp_node = next_node.clone();
            //         }
            //     }
            // }

            // drop(temp_reader);
            loop {
                let temp_reader = temp_node
                    .read()
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

                if let NodeType::Node(node) = temp_reader.get() {
                    let next_node = node.lvl_arr[lvl as usize].clone();

                    let node_reader = next_node
                        .read()
                        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

                    if let NodeType::Node(next) = node_reader.get() {
                        if let Some(da) = next.data {
                            if da.key < key {
                                drop(temp_reader);
                                temp_node = next_node.clone();
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }

            update_arr.push(temp_node.clone());
        }

        let data = NodeData::new(key, val);
        let new_node = Node::new(expected_lvl, Some(data));
        let mut arr = new_node.lvl_arr;
        for lvl in 0..expected_lvl {
            let update_node = update_arr[lvl as usize];
            let node_writer = update_node
                .write()
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

            let internal_node = node_writer.get();

            arr[lvl as usize] = match internal_node {
                NodeType::Node(node) => node.lvl_arr[lvl as usize].clone(),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Invalid head node config".to_string(),
                    ))
                }
            };

            if let NodeType::Node(node) = internal_node {
                node.lvl_arr[lvl as usize] = Arc::new(RwLock::new(NodeType::Node(new_node)));
            }
        }

        self.size += 1;
        Ok(())
    }
}
