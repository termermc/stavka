use crate::hash::{create_file_block_hash, FileBlockInfo};

mod cachestate;
mod hash;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let num_threads = std::thread::available_parallelism()?.get();
    //
    // // Launch thread
    // let mut threads = Vec::new();
    //
    // let mut thread_num = 0;
    // for _ in 0..num_threads {
    //     println!("Spawning thread {}", thread_num);
    //     thread_num += 1;
    //
    //     threads.push(std::thread::spawn(|| {
    //         tokio_uring::start(async {
    //             let mut num = 0;
    //             loop {
    //                 if num == 0 {
    //                     num = 1;
    //                 } else {
    //                     num = 0;
    //                 }
    //             }
    //         });
    //     }));
    // }
    //
    // for thread in threads {
    //     thread.join().unwrap();
    // }

    tokio_uring::start(async {
        let hash = create_file_block_hash("foo.txt", FileBlockInfo { block_size: 1024, block_num: 0 });
        
    });

    Ok(())
}
