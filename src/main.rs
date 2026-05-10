mod blob;
mod coordinator;
mod error;
mod id;
mod metadata;
mod sys;
mod vector;
mod wal;

pub const PAGE_SIZE: u64 = 4 * 1024;

fn main() {
    println!("Hello VORST");
}
