use crate::id_generator::Generator;
use bytes::Bytes;
use dashmap::DashMap;
use nohash_hasher::NoHashHasher;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::hash::BuildHasherDefault;
use std::sync::Arc;

// add bool for memory only
// Maybe add to btree and add byte counter have write thread check ad if bytes is over 1mb clean out hashmap and write to disk

#[derive(Debug, Clone)]
pub struct Item {
    pub key: String,
    pub flags: u32,
    pub cas: u64,
    pub expiration: Option<u32>,
    pub data: Bytes,
}

#[derive(Debug, Clone)]
pub struct MemoryItem {
    flags: u32,
    expiration: Option<u32>,
    cas: u64,
    data: Bytes,
}

impl MemoryItem {
    fn from_item(item: Item) -> MemoryItem {
        MemoryItem {
            flags: item.flags,
            expiration: item.expiration,
            cas: item.cas,
            data: item.data,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Cache {
    id: Arc<Generator>,
    index: Arc<RwLock<BTreeMap<String, u64>>>,
    cache: Arc<DashMap<u64, MemoryItem, BuildHasherDefault<NoHashHasher<u64>>>>,
}

impl Cache {
    pub fn new() -> Cache {
        Cache {
            id: Arc::new(Generator::new()),
            index: Arc::new(RwLock::new(BTreeMap::new())),
            cache: Arc::new(DashMap::with_capacity_and_hasher(
                1000,
                BuildHasherDefault::default(),
            )),
        }
    }

    pub async fn get(&self, key: &String) -> Option<Item> {
        let index = self.index.read();
        match index.get(key) {
            Some(id) => {
                let item = self.cache.get(id).unwrap().clone();
                Some(Item {
                    key: key.clone(),
                    flags: item.flags,
                    cas: item.cas,
                    expiration: item.expiration,
                    data: item.data,
                })
            }
            None => None,
        }
    }

    pub async fn set(&self, key: String, flags: u32, expiration: Option<u32>, data: Bytes) -> bool {
        let mut index = self.index.upgradable_read();
        match index.get(&key) {
            // Updates an existing `Item`
            Some(id) => {
                //downgrade index lock
                // Get and increament CAS on update
                let cas = self.cache.get_mut(id).unwrap().cas;
                let mut mi = MemoryItem { flags, expiration, cas, data };
                mi.cas = cas + 1;

                self.cache.insert(*id, mi);
                false
            }
            // Inserts a new `Item`
            None => {
                let new_id = self.id.gen();
                index.with_upgraded(|index| index.insert(key, new_id));
                self.cache.insert(new_id, MemoryItem { flags, expiration, cas: 0, data });
                true
            }
        }
    }
}
