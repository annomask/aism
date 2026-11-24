use crossbeam_channel::{Receiver, Sender};
use snafu::{ResultExt, Snafu};
use tokio::sync::{
    oneshot,
    oneshot::{Receiver as OneshotReceiver, Sender as OneshotSender},
};

/// Error type for sync.
#[derive(Debug, Snafu)]
pub enum Error {
    /// Cancel send error.
    #[snafu(display("tsdb: {}", source))]
    CancelSendErr {
        source: crossbeam_channel::SendError<()>,
    },

    /// Cancel recv error.
    #[snafu(display("tsdb: {}", source))]
    CancelRecvErr {
        source: crossbeam_channel::RecvError,
    },

    /// Oneshot recv error.
    #[snafu(display("tsdb: {}", source))]
    OneshotCancelRecvErr { source: oneshot::error::RecvError },
}

/// Result type for Partition.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct CancellationToken(Sender<()>, Receiver<()>);

impl CancellationToken {
    pub fn new() -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded::<()>();
        CancellationToken(sender, receiver)
    }

    pub fn send(&self) -> Result<()> {
        self.0.send(()).context(CancelSendErrSnafu)
    }

    pub fn recv(&self) -> Result<()> {
        self.1.recv().context(CancelRecvErrSnafu)
    }

    pub fn receiver(&self) -> &Receiver<()> {
        &self.1
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

// #[derive(Clone)]
// pub struct CancellationTokenBounded(Sender<()>, Receiver<()>);

// impl CancellationTokenBounded {
//     pub fn new(_sz: usize) -> Self {
//         let (sender, receiver) = crossbeam_channel::bounded::<()>(sz);
//         CancellationTokenBounded(sender, receiver)
//     }

//     pub fn send(&self) -> Result<()> {
//         self.0.send(()).context(CancelSendErrSnafu)
//     }

//     pub fn recv(&self) -> Result<()> {
//         self.1.recv().context(CancelRecvErrSnafu)
//     }

//     pub fn receiver(&self) -> &Receiver<()> {
//         &self.1
//     }
// }

pub struct CancellationTokenBounded(Option<OneshotSender<()>>, Option<OneshotReceiver<()>>);

impl CancellationTokenBounded {
    pub fn new(_sz: usize) -> Self {
        let (sender, receiver) = oneshot::channel();
        CancellationTokenBounded(Some(sender), Some(receiver))
    }

    pub fn send(&mut self) -> Result<(), ()> {
        self.0.take().unwrap().send(())
    }

    pub fn recv(&mut self) -> Result<()> {
        self.1
            .take()
            .unwrap()
            .blocking_recv()
            .context(OneshotCancelRecvErrSnafu)
    }
}
