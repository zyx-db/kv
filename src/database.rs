use core::fmt;
use std::fmt::Debug;
use std::mem;
use std::rc::Rc;
use std::sync::atomic::AtomicUsize;
use std::sync::RwLock;

const WAL_THRESHOLD: u16 = 100;
const KV_THRESHOLD: u16 = 100;
const SKIPLIST_LEVELS: usize = 3;
static OBJECT_COUNTER: AtomicUsize = AtomicUsize::new(0);

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
    next: Vec<Option<Rc<RwLock<Node<K, V>>>>>,
    debug_id: usize,
}

impl<K: Key, V: Value> Node<K, V> {
    fn new(k: &K, v: &V) -> Self {
        let key = k.clone();
        let value = v.clone();
        let mut next = Vec::new();
        next.resize(SKIPLIST_LEVELS as usize, None);
        Node {
            key,
            value,
            next,
            debug_id: OBJECT_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        }
    }

    fn empty() -> Self {
        let key = K::default();
        let value = V::default();
        let mut next = Vec::new();
        next.resize(SKIPLIST_LEVELS as usize, None);
        Node {
            key,
            value,
            next,
            debug_id: OBJECT_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        }
    }

    fn debug_print(&self, level: usize) {
        print!("( {}; {:?}) ->", self.debug_id, self.key);
        let next = &self.next[level];
        match next {
            Some(lock) => {
                let node = lock.read().unwrap();
                node.debug_print(level);
            }
            None => {}
        }
    }

    fn push(&mut self, key: &K, value: &V, level: usize, new_node_rc: &Rc<RwLock<Node<K, V>>>) {
        let mut pushed = false;
        match self.next[level] {
            Some(ref mut node) => {
                if node.write().unwrap().key > *key {
                    let mut new_node = new_node_rc.write().unwrap();
                    new_node.next[level] = Some(Rc::clone(new_node_rc));
                    mem::swap(&mut new_node.next[level], &mut self.next[level]);
                    self.next[level] = Some(Rc::clone(new_node_rc));
                    pushed = true;
                } else {
                    node.write().unwrap().push(key, value, level, new_node_rc);
                }
            }
            None => {
                self.next[level] = Some(Rc::clone(&new_node_rc));
                pushed = true;
            }
        }
        if pushed && level > 0 {
            self.push(key, value, level - 1, new_node_rc)
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
                let l = n.read().unwrap();
                match (l.key > *key, level) {
                    (true, 0) => {
                        println!("lowest level");
                        return None;
                    }
                    (false, _) => {
                        println!("valid, iterating");
                        return l.get(key, level);
                    }
                    (true, _) => {
                        println!("overshot, dropping level");
                        return self.get(key, level - 1);
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
    levels: usize,
    heads: RwLock<Box<Node<K, V>>>,
}

impl<K: Key, V: Value> SkipList<K, V> {
    fn init(levels: usize) -> Self {
        let heads = RwLock::new(Box::new(Node::empty()));
        Self { levels, heads }
    }

    fn randlvl(&self) -> usize {
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
        self.insert_with_level(key, value, level);
    }

    fn insert_with_level(&self, key: &K, value: &V, level: usize) {
        {
            let mut heads = self.heads.write().unwrap();
            let new_node = Rc::new(RwLock::new(Node::new(key, value)));
            heads.push(&key, &value, level as usize, &new_node);
        }
        self.debug_print();
    }

    fn get(&self, key: &K) -> Option<V> {
        let heads = self.heads.read().unwrap();
        let top_level = (SKIPLIST_LEVELS - 1) as usize;
        // let mut searched = false;
        // let mut res: Option<V> = heads[(SKIPLIST_LEVELS - 1) as usize];
        heads.get(key, top_level)
        // while !searched {
        //     res = match (&heads.next[current_level], current_level) {
        //         (Some(node), x) => {
        //             searched = true;
        //             node.read().unwrap().get(key, current_level)
        //         }
        //         (None, 0) => {
        //             searched = true;
        //             None
        //         }
        //         (None, _) => {
        //             current_level -= 1;
        //             None
        //         }
        //     }
        // }
        // println!("res is {:?}", res);
        // res
    }

    fn debug_print(&self) {
        let root_node = self.heads.read().unwrap();
        for i in 0..SKIPLIST_LEVELS {
            print!("level {}: ", i);
            match &root_node.next[i as usize] {
                Some(node) => {
                    node.read().unwrap().debug_print(i as usize);
                }
                None => {}
            }
            println!();
        }
    }
}

struct MemTable<K: Key, V: Value> {
    cardinality: u16,
    map: SkipList<K, V>,
}

impl<K: Key, V: Value> MemTable<K, V> {
    fn init(levels: usize) -> Self {
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

#[cfg(test)]
mod tests {
    use rand::prelude::*;
    use rand_chacha::ChaCha8Rng;

    use super::*;
    #[test]
    fn test_add() {
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        let levels = SKIPLIST_LEVELS;
        let skiplist: SkipList<String, String> = SkipList::init(levels);
        let key_value = vec![
            ("abc", "first"),
            ("db", "me"),
            ("bill", "random name"),
            ("cat", "meow"),
            ("wordd", "freaky"),
        ];
        for kv in &key_value {
            let level = rng.gen_range(0..levels - 1);
            println!("inserting at level {}", level);
            skiplist.insert_with_level(&kv.0.to_string(), &kv.1.to_string(), level)
        }
        for kv in &key_value {
            assert_eq!(skiplist.get(&kv.0.to_string()), Some(kv.1.to_string()));
        }
    }
}
