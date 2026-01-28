use crate::heap_page::HeapPage;
use crate::page::Page;
use common::prelude::*;
use common::testutil::*;
use rand::rngs::SmallRng;
use rand::Rng;
use std::hint::black_box;

pub fn bench_page_insert(vals: &[Vec<u8>]) {
    let mut p = Page::new(0);
    for i in vals {
        p.add_value(i).unwrap();
    }
}

pub enum BenchOp {
    Insert(Vec<u8>),
    DeleteSlot(SlotId),
    UpdateSlot(SlotId, Vec<u8>),
    ReadSlot(SlotId),
    DeleteValId(usize),          // Find ValId from offset
    ReadValId(usize),            // Find ValId from offset
    UpdateValId(usize, Vec<u8>), // Find ValId from offset
    Scan,
}

pub fn gen_page_bench_workload(
    rng: &mut SmallRng,
    num_ops: usize,
    min_size: usize,
    max_size: usize,
) -> Vec<BenchOp> {
    let mut res = Vec::new();
    let mut random_bytes = get_random_vec_of_byte_vec(rng, num_ops, min_size, max_size);
    let mut expected_max_slot = 0;
    let seed_insert = 5;
    // Seed the first SEED_INSERT ops to be inserts
    for _ in 0..seed_insert {
        expected_max_slot += 1;
        res.push(BenchOp::Insert(random_bytes.pop().unwrap()));
    }
    for _ in seed_insert..num_ops {
        let op = match rng.random_range(0..100) {
            0..20 => {
                expected_max_slot += 1;
                BenchOp::Insert(random_bytes.pop().unwrap())
            }
            20..30 => BenchOp::DeleteSlot(rng.random_range(0..expected_max_slot)),
            30..50 => BenchOp::UpdateSlot(
                rng.random_range(0..expected_max_slot),
                random_bytes.pop().unwrap(),
            ),
            50..60 => BenchOp::Scan,
            _ => BenchOp::ReadSlot(rng.random_range(0..expected_max_slot)),
        };
        res.push(op);
    }
    res
}

pub fn bench_page_mixed(workload: &Vec<BenchOp>) {
    let mut p = Page::new(23500);
    p.init_heap_page();
    for op in workload {
        match op {
            BenchOp::Insert(v) => {
                let res = p.add_value(v);
                black_box(res);
            }
            BenchOp::DeleteSlot(sid) => {
                let res = p.delete_value(*sid);
                black_box(res);
            }
            BenchOp::UpdateSlot(sid, v) => {
                let res = p.update_value(*sid, v);
                black_box(res);
            }
            BenchOp::ReadSlot(sid) => {
                let res = p.get_value(*sid);
                black_box(res);
            }
            BenchOp::Scan => {
                for (i, slot) in p.iter() {
                    black_box(i);
                    black_box(slot);
                }
            }
            _ => {
                panic!("Unsupported operation");
            }
        }
    }
}
