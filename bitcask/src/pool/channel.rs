use crossbeam_channel::unbounded;

#[derive(Debug, Clone)]
pub(super) struct Sender {
    send: crossbeam_channel::Sender<()>,
}

#[derive(Debug, Clone)]
pub(super) struct Receiver {
    pub recv: crossbeam_channel::Receiver<()>,
}

pub(super) fn channel() -> (Sender, Receiver) {
    let (send, recv) = unbounded();
    (Sender { send }, Receiver { recv })
}
