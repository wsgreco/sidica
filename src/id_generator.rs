// Maybe use duration since first timestamp, but how to persit on disk

use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug)]
pub struct Generator {
    ts: AtomicU32, // Unix Timestamp
    count: AtomicU32,
}

impl Generator {
    pub fn new() -> Generator {
        Generator {
            ts: AtomicU32::new(Self::current_ts()),
            count: AtomicU32::new(0),
        }
    }

    fn current_ts() -> u32 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("getting time since unix epoch")
            .as_secs() as u32
    }

    fn combine(timestamp: u32, count: u32) -> u64 {
        let mut id = [0u8; 8];
        id[..4].copy_from_slice(&timestamp.to_be_bytes());
        id[4..].copy_from_slice(&count.to_be_bytes());
        // println!("{:?}", id); // change to debug
        u64::from_be_bytes(id)
    }

    pub fn gen(&self) -> u64 {
        let now = Self::current_ts();
        let last_ts = self.ts.swap(now, Ordering::SeqCst);

        let count: u32;
        if now == last_ts {
            count = self.count.fetch_add(1, Ordering::SeqCst);
        } else {
            count = 0;
            // Resets count to 0
            self.count.store(count, Ordering::SeqCst);
        }

        Self::combine(now, count)
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::*;

    #[test]
    fn test_combine() {
        assert_eq!(Generator::combine(1, 5), 4294967301);
    }

    #[test]
    fn test_same_second() {
        let gen = Generator::new();
        let id_1 = gen.gen();
        let id_2 = gen.gen();
        let id_3 = gen.gen();
        let id_4 = gen.gen();
        let id_5 = gen.gen();
        assert_eq!(id_1 + 1, id_2);
        assert_eq!(id_2 + 1, id_3);
        assert_eq!(id_3 + 1, id_4);
        assert_eq!(id_4 + 1, id_5);
    }

    #[test]
    fn test_different_seconds() {
        let gen = Generator::new();
        let id = gen.gen();
        thread::sleep(Duration::from_secs(1));
        let id_minus_one_sec = gen.gen() - 4294967296;
        assert_eq!(id, id_minus_one_sec);
    }
}
