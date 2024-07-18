use std::sync::{mpsc, Arc};

#[derive(Debug, Clone)]
pub(super) struct Sender {
    send: Arc<mpsc::Sender<()>>,
}

impl Sender {
    fn new(send: mpsc::Sender<()>) -> Self {
        Self {
            send: Arc::new(send),
        }
    }
}

pub(super) struct Receiver {
    pub recv: mpsc::Receiver<()>,
}

impl Receiver {
    fn new(recv: mpsc::Receiver<()>) -> Self {
        Self { recv }
    }
}

pub(super) fn channel() -> (Sender, Receiver) {
    let (send, recv) = mpsc::channel();
    (Sender::new(send), Receiver::new(recv))
}
