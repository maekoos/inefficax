use rand::seq::SliceRandom;
use std::{path::Path, time::Instant};

use inefficax::{BTree, PAGE_SIZE};

fn main() {
    let test_size: u32 = 10_000;
    let mut keys: Vec<u64> = (1..test_size as u64 + 1).collect();
    let mut rng = rand::thread_rng();

    let mut db = BTree::open(Path::new("./db")).unwrap();

    keys.shuffle(&mut rng);
    let start_time = Instant::now();
    for n in &keys {
        // db.insert(format!("n{:1}", n * 1000), *n)
        //     .map_err(|e| {
        //         println!("Failing at key: {} n{:1}", n, n);
        //         e
        //     })
        //     .unwrap()
        db.insert_object(
            format!("n{:1}", n * 1000),
            format!("Key value: {:10}", n).into(),
        )
        .unwrap()
    }
    let elapsed = start_time.elapsed();
    println!(
        "Write time: {:?} ({:?} / insert)",
        elapsed,
        elapsed.checked_div(test_size).unwrap()
    );

    keys.shuffle(&mut rng);
    let start_time = Instant::now();
    for n in &keys {
        // assert_eq!(db.search(&format!("n{:1}", n * 1000)).unwrap(), Some(*n));
        assert_eq!(
            db.search_object(&format!("n{:1}", n * 1000)).unwrap(),
            Some(format!("Key value: {:10}", n).into())
        );
    }
    let elapsed = start_time.elapsed();
    println!(
        "Read time: {:?} ({:?} / read)",
        elapsed,
        elapsed.checked_div(test_size).unwrap()
    );

    keys.shuffle(&mut rng);
    let start_time = Instant::now();
    for idx in 0..keys.len() {
        let n = keys[idx];
        // db.delete(&format!("n{:1}", n * 1000)).unwrap();
        db.delete_object(&format!("n{:1}", n * 1000)).unwrap();
    }
    let elapsed = start_time.elapsed();
    println!(
        "Delete time: {:?} ({:?} / delete)",
        elapsed,
        elapsed.checked_div(test_size).unwrap()
    );

    println!("\tTree depth: {}", db.get_depth().unwrap());
    let c = db.count_nodes().unwrap();
    println!("\tNode count: {}", c);
    println!(
        "\tOptimal size: {}kb ({} pages)",
        ((c + 1) * PAGE_SIZE) / 1000, // +1 for config page
        c + 1
    );
    let file_size = db.get_file_size().unwrap() as usize;
    println!(
        "\tActual size: {}kb ({} pages)",
        file_size / 1000,
        file_size / PAGE_SIZE
    );
}
