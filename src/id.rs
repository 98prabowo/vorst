use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::Utc;

pub struct IdGenerator {
    node_id: u64,
    state: AtomicU64,
}

impl IdGenerator {
    const VORST_EPOCH: u64 = 1735689600000;

    pub fn new(node_id: u8) -> Self {
        Self {
            node_id: node_id as u64,
            state: AtomicU64::new(0),
        }
    }

    fn current_time() -> u64 {
        Utc::now().timestamp_millis() as u64
    }

    pub fn next_id(&self) -> u64 {
        loop {
            let now = Self::current_time().saturating_sub(Self::VORST_EPOCH);
            let current_state = self.state.load(Ordering::Acquire);

            let last_time = current_state >> 12;
            let last_seq = current_state & 0xFFF;

            let (next_time, next_seq) = if now > last_time {
                (now, 0)
            } else if last_seq < 4095 {
                (last_time, last_seq + 1)
            } else {
                continue;
            };

            let next_state = (next_time << 12) | next_seq;

            if self
                .state
                .compare_exchange_weak(
                    current_state,
                    next_state,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return (next_time << 20) | (self.node_id << 12) | next_seq;
            }
        }
    }
}
