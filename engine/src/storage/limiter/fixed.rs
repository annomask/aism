//! Fixed is a simple channel-based concurrency limiter.  It uses a fixed
//! size channel to limit callers from proceeding until there is a value available
//! in the channel.  If all are in-use, the caller blocks until one is freed.
use std::ops::{Deref, DerefMut};

use crossbeam_channel::{Receiver, Sender, bounded, select};

#[derive(Clone)]
pub struct Fixed((Sender<()>, Receiver<()>));

impl Deref for Fixed {
    type Target = (Sender<()>, Receiver<()>);

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Fixed {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Fixed {
    pub fn new(limit: i32) -> Self {
        Fixed(bounded(limit as usize))
    }

    /// Returns true if the limiter has all its capacity is available.
    pub fn idle(&self) -> bool {
        self.0.0.len() == self.0.0.capacity().unwrap()
    }

    /// Returns the number of available tokens that may be taken.
    pub fn available(&self) -> i32 {
        (self.0.0.capacity().unwrap() - self.0.0.len()) as i32
    }

    /// Returns the number of tokens can be taken.
    pub fn capacity(&self) -> i64 {
        self.0.0.capacity().unwrap() as i64
    }

    /// Attempts to take a token and return true if successful, otherwise returns false.
    pub fn try_take(&self) -> bool {
        select! {
            send(self.0.0, ()) -> _ => true,
            default => false,
        }
    }

    /// Attempts to take a token and blocks until one is available.
    pub fn take(&self) {
        let _ = self.0.0.send(());
    }

    /// Releases a token back to the limiter.
    pub fn release(&self) {
        let _ = self.0.1.recv();
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::limiter::fixed::*;

    #[test]
    fn test_fixed_available() {
        let f = Fixed::new(10);
        let got = f.available();
        if got != 10 {
            panic!("available mismatch: exp 10, got {}", got);
        }

        f.take();

        let got = f.available();
        if got != 9 {
            panic!("available mismatch: exp 9, got {}", got);
        }

        f.release();
        let got = f.available();
        if got != 10 {
            panic!("available mismatch: exp 10, got {}", got);
        }
    }
}
