const WAL_THRESHOLD: u16 = 100;
const KV_THRESHOLD: u16 = 100;

struct WAL  {
    writes: u16,
}

impl WAL {
    pub fn new() -> Self{
        WAL {writes: 0}
    }
    pub fn write<T: Ord + Clone>(&self, k: &T, v: &T) -> u16 {
        let res = self.writes;
        res
    }
}

struct PageCache {
}

struct SkipList {}

struct MemTable {
    cardinality: u16,
    map: SkipList
}

impl MemTable {
    fn insert<T: Ord + Clone>(&self, k: &T, v: &T) -> u16 {
        0
    }
}

struct DiskManager {}

pub struct DB {
    wal: WAL,
    kv: MemTable,
    buffer: PageCache,
    disk_manager: DiskManager,
}

impl DB {
    pub fn new() -> Self {
        let disk = DiskManager {};
        let wal = WAL::new();
        let buffer = PageCache {};
        let kv = MemTable {};
        DB {wal, buffer, kv, disk_manager: disk}
    }

    pub fn insert<T: Ord + Clone>(&self, k: T, v: T) {
        let wal_writes = self.wal.write(&k, &v);
        let kv_usage = self.kv.insert(&k, &v);
        if wal_writes > WAL_THRESHOLD || kv_usage > KV_THRESHOLD{
            self.freeze();
        }
    }

    pub fn get<T: Ord + Clone>(&self, k: T) -> Option<String>{

        None
    }

    fn freeze(&self){
        // need to signal that we cant answer queries right now
        // self.lock.take();
        // self.wal.flush();    
        // self.kv.reset();
        // self.unlock();
    }
}
