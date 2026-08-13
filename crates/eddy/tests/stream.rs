use eddy::stream::{iter, StreamExt};

#[test]
fn stream_iter_next_and_exhaustion() {
    let runtime = eddy::Builder::new_current_thread().build();
    runtime.block_on(async {
        let mut stream = iter(vec![1, 2, 3]);
        assert_eq!(stream.next().await, Some(1));
        assert_eq!(stream.next().await, Some(2));
        assert_eq!(stream.next().await, Some(3));
        assert_eq!(stream.next().await, None);
        // Once exhausted, a fused stream stays exhausted.
        assert_eq!(stream.next().await, None);
    });
}

#[test]
fn stream_map_filter_count_collect() {
    let runtime = eddy::Builder::new_current_thread().build();
    runtime.block_on(async {
        let doubled: Vec<i32> = iter(vec![1, 2, 3]).map(|x| x * 2).collect().await;
        assert_eq!(doubled, vec![2, 4, 6]);

        let evens: Vec<i32> = iter(1..=6).filter(|x| x % 2 == 0).collect().await;
        assert_eq!(evens, vec![2, 4, 6]);

        let count = iter(1..=10).count().await;
        assert_eq!(count, 10);

        let empty: Vec<i32> = iter(Vec::<i32>::new()).collect().await;
        assert!(empty.is_empty());
    });
}

#[test]
fn stream_fold_and_for_each() {
    let runtime = eddy::Builder::new_current_thread().build();
    runtime.block_on(async {
        let sum = iter(1..=4).fold(0, |acc, x| acc + x).await;
        assert_eq!(sum, 10);

        let mut seen = Vec::new();
        iter(vec![5, 6, 7]).for_each(|x| seen.push(x)).await;
        assert_eq!(seen, vec![5, 6, 7]);
    });
}

#[test]
fn stream_fuse_stops_polling_after_none() {
    let runtime = eddy::Builder::new_current_thread().build();
    runtime.block_on(async {
        // A non-fused stream yields None and then garbage afterwards; fusing
        // must latch the exhausted state.
        let mut stream = iter(vec![1, 2]).fuse();
        assert_eq!(stream.next().await, Some(1));
        assert_eq!(stream.next().await, Some(2));
        assert_eq!(stream.next().await, None);
        assert_eq!(stream.next().await, None);
    });
}

#[test]
fn select_drains_two_streams_in_one_loop() {
    let runtime = eddy::Builder::new_current_thread().build();
    runtime.block_on(async {
        let mut left = iter(vec![1, 2, 3]);
        let mut right = iter(vec![4, 5, 6]);
        let mut values = Vec::new();
        loop {
            eddy::select! {
                Some(v) = left.next() => values.push(v),
                Some(v) = right.next() => values.push(v),
                else => break,
            }
        }
        values.sort();
        assert_eq!(values, vec![1, 2, 3, 4, 5, 6]);
    });
}

#[test]
fn select_stream_with_timeout() {
    let runtime = eddy::Builder::new_current_thread().build();
    runtime.block_on(async {
        let mut stream = iter(vec![1, 2, 3]);
        let mut values = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
        loop {
            eddy::select! {
                Some(v) = stream.next() => values.push(v),
                _ = eddy::time::sleep_until(deadline) => break,
            }
        }
        assert_eq!(values, vec![1, 2, 3]);
    });
}
