use core::fmt;
use std::fmt::Debug;
use std::mem;
use std::sync::RwLock;

use rand::rngs::ThreadRng;

const WAL_THRESHOLD: u16 = 100;
const KV_THRESHOLD: u16 = 100;
const SKIPLIST_LEVELS: u8 = 3;

pub trait Key: Ord + Default + Clone + fmt::Debug {}
impl<T: Ord + Default + Clone + fmt::Debug> Key for T {}

pub trait Value: Default + Clone + fmt::Debug {}
impl<T: Default + Clone + fmt::Debug> Value for T {}

struct WAL {
    writes: u16,
}

impl WAL {
    pub fn new() -> Self {
        WAL { writes: 0 }
    }
    pub fn write<K: Key, V: Value>(&self, k: &K, v: &V) -> u16 {
        let res = self.writes;
        res
    }
}

struct PageCache {}

#[derive(Clone, Debug)]
struct Node<K: Key, V: Value> {
    key: K,
    value: V,
    next: Vec<Option<Box<Node<K, V>>>>,
}

impl<K: Key, V: Value> Node<K, V> {
    fn new(k: &K, v: &V) -> Self {
        let key = k.clone();
        let value = v.clone();
        let mut next = Vec::new();
        next.resize(SKIPLIST_LEVELS as usize, None);
        Node { key, value, next }
    }

    fn empty() -> Self {
        let key = K::default();
        let value = V::default();
        let mut next = Vec::new();
        next.resize(SKIPLIST_LEVELS as usize, None);
        Node { key, value, next }
    }

    fn push(&mut self, key: &K, value: &V, level: usize, new_node: &mut Box<Node<K, V>>) {
        let mut pushed = false;
        match self.next[level] {
            Some(ref mut node) => {
                if node.key > *key {
                    new_node.next[level] = Some(mem::replace(node, new_node.clone()));
                    // let mut new_node = Box::new(Node::new(key, value));
                    // new_node.next = Some(mem::replace(node, new_node));
                    mem::swap(&mut new_node.next[level], &mut self.next[level]);
                    self.next[level] = Some(Box::clone(new_node));

                    pushed = true;
                } else {
                    node.push(key, value, level, new_node);
                }
            }
            None => {
                // let new_node = Node::new(key, value);
                self.next[level] = Some(Box::clone(&new_node));
                pushed = true;
            }
        }
        if pushed && level > 0 {
            self.push(key, value, level - 1, new_node)
        }
    }

    fn get(&self, key: &K, level: usize) -> Option<V> {
        println!(
            "looking for {:?} at level {}. currently comparing to {:?}: {}",
            key,
            level,
            self.key,
            *key == self.key
        );
        if self.key == *key {
            println!("returning {:?}", self.value.clone());
            return Some(self.value.clone());
        }
        match &self.next[level] {
            Some(n) => {
                match (n.key > *key, level) {
                    (true, 0) => {
                        return None;
                    }
                    (false, _) => {
                        return self.get(key, level - 1);
                    }
                    (true, _) => {
                        return n.get(key, level);
                    }
                }
                // we go down if possible
            }
            None => match level {
                0 => {
                    return None;
                }
                _ => {
                    return self.get(key, level - 1);
                }
            },
        }
    }
}

#[derive(Debug)]
struct SkipList<K: Key, V: Value> {
    levels: u8,
    heads: RwLock<Box<Node<K, V>>>,
}

impl<K: Key, V: Value> SkipList<K, V> {
    fn init(levels: u8) -> Self {
        let heads = RwLock::new(Box::new(Node::empty()));
        Self { levels, heads }
    }

    fn randlvl(&self) -> u8 {
        fn coin_flip() -> bool {
            rand::random::<bool>()
        }
        let mut lvl = SKIPLIST_LEVELS - 1;
        while lvl > 0 && coin_flip() {
            lvl -= 1;
        }
        lvl
    }

    fn insert(&self, key: &K, value: &V) {
        let level = self.randlvl();
        {
            let mut heads = self.heads.write().unwrap();
            let mut new_node = Box::new(Node::new(key, value));
            heads.push(&key, &value, level as usize, &mut new_node);
        }
        println!("state: {:#?}", self.heads);
    }

    fn get(&self, key: &K) -> Option<V> {
        let heads = self.heads.read().unwrap();
        let mut current_level = (SKIPLIST_LEVELS - 1) as usize;
        let mut searched = false;
        let mut res: Option<V> = None;
        while !searched {
            res = match (&heads.next[current_level], current_level) {
                (Some(node), x) => {
                    searched = true;
                    node.get(key, current_level)
                }
                (None, 0) => {
                    searched = true;
                    None
                }
                (None, _) => {
                    current_level -= 1;
                    None
                }
            }
        }
        println!("res is {:?}", res);
        res
    }
}

struct MemTable<K: Key, V: Value> {
    cardinality: u16,
    map: SkipList<K, V>,
}

impl<K: Key, V: Value> MemTable<K, V> {
    fn init(levels: u8) -> Self {
        MemTable {
            cardinality: 0,
            map: SkipList::init(levels),
        }
    }

    fn insert(&self, k: &K, v: &V) -> u16 {
        self.map.insert(k, v);

        // TODO: track cardinality on inserts
        0
    }

    fn get(&self, k: &K) -> Option<V> {
        self.map.get(k)
    }
}

struct DiskManager {}

pub struct DB<K: Key, V: Value> {
    wal: WAL,
    kv: MemTable<K, V>,
    buffer: PageCache,
    disk_manager: DiskManager,
}

impl<K: Key, V: Value> DB<K, V> {
    pub fn new() -> Self {
        let disk = DiskManager {};
        let wal = WAL::new();
        let buffer = PageCache {};
        let kv: MemTable<K, V> = MemTable::init(SKIPLIST_LEVELS);
        DB {
            wal,
            buffer,
            kv,
            disk_manager: disk,
        }
    }

    pub fn insert(&self, k: K, v: V) {
        // TODO wal and compaction
        // let wal_writes = self.wal.write(&k, &v);
        // let kv_usage = self.kv.insert(&k, &v);
        // if wal_writes > WAL_THRESHOLD || kv_usage > KV_THRESHOLD {
        //     self.freeze();
        // }
        self.kv.insert(&k, &v);
    }

    // TODO get
    pub fn get(&self, k: &K) -> Option<V> {
        self.kv.get(k)
    }

    // TODO freeze
    fn freeze(&self) {
        // need to signal that we cant answer queries right now
        // self.lock.take();
        // self.wal.flush();
        // self.kv.reset();
        // self.unlock();
    }
}
