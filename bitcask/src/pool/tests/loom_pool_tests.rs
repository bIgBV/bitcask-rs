use crate::pool::Pool;
use std::time::Duration;

#[test]
fn loom_cloned_execution() {
    loom::model(|| {
        let n_jobs = 10;
        let pool = Pool::new(1);
        let (send, recv) = loom::sync::mpsc::channel();

        let send_copy = send.clone();
        let pool_copy = pool.clone();

        loom::thread::spawn(move || {
            pool_copy.execute(move || {
                let _ = send_copy.send(2);
            });
        });

        for i in 0..n_jobs {
            let send = send.clone();
            pool.execute(move || {
                let _ = send.send(1);
            });
        }

        drop(send);

        let mut num = 0;
        let mut sum = 0;
        loop {
            if let Ok(item) = recv.recv() {
                sum += item;
            }
            num += 1;

            if num == n_jobs + 1 {
                break;
            }
        }

        assert_eq!(sum, 12);
    });
}
