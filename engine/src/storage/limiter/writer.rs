//! LimitWriter is a writer that implement std::io::Write trait with rate limiting.
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use crate::common::time::Time;
use snafu::{ResultExt, Snafu, ensure};

use crate::storage::limiter::rate::{self, Limiter};

/// Error type for limiter writer.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("tsm writer error: {source}"))]
    IO { source: std::io::Error },

    /// IO error.
    #[snafu(display("rate limiter error: {source}"))]
    RateLimiter { source: rate::Error },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },
}

/// Result type for limiter writer.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IO { source: err }
    }
}

pub trait SyncWrite: Write {
    fn path(&self) -> &Path; // inner file path
    fn close(&self) -> std::io::Result<()>;
    fn sync_all(&self) -> std::io::Result<()>;
}

#[derive(Debug)]
pub struct LimitWriter {
    path: PathBuf,
    w: File,
    limiter: Option<rate::Limiter>,
}

pub fn new_rate_limiter(bytes_per_sec: i64, burst_limit: i64) -> Limiter {
    let limiter = Limiter::new(rate::Limit(bytes_per_sec as f64), burst_limit);
    limiter.allow_n(Time::now(), burst_limit); // spend initial burst
    limiter
}

impl LimitWriter {
    /// Returns a writer that implements io.Writer with rate limiting.
    /// The limiter use a token bucket approach and limits the rate to bytesPerSec
    /// with a maximum burst of burstLimit.
    pub fn new(path: PathBuf, w: File, bytes_per_sec: i64, burst_limit: i64) -> Self {
        let limiter = Some(new_rate_limiter(bytes_per_sec, burst_limit));
        Self { path, w, limiter }
    }

    /// Returns a Writer with the specified rate limiter.
    pub fn new_with_rate_limiter(path: PathBuf, w: File, limiter: Option<Limiter>) -> Self {
        Self { path, w, limiter }
    }

    // fn path(&self) -> &Path {
    //     &self.path
    // }

    // fn close(&self) -> std::io::Result<()> {
    //     Ok(())
    // }

    // fn sync_all(&self) -> std::io::Result<()> {
    //     self.w.sync_all()
    // }
}

impl Write for LimitWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.limiter.is_none() {
            return self.w.write(buf);
        }

        let limiter = self.limiter.as_ref().unwrap();
        let mut n = 0;
        while n < buf.len() {
            let mut want_to_write_n = buf[n..].len();
            if want_to_write_n > limiter.burst() as usize {
                want_to_write_n = limiter.burst() as usize;
            }

            let wrote_n = self.w.write(&buf[n..n + want_to_write_n])?;
            n += wrote_n;

            if let Err(e) = limiter.wait_n(wrote_n as i64, Duration::ZERO) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ));
            }
        }

        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}

impl SyncWrite for LimitWriter {
    fn path(&self) -> &Path {
        &self.path
    }

    fn close(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.w.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, path::PathBuf};

    use crate::common::time::Time;

    use super::LimitWriter;
    use crate::storage::tsm1::cache::tests::{must_temp_tsm_dir, must_temp_tsm_file};

    #[test]
    fn test_writer_limited() {
        let r = vec![0u8; 1024 * 1024];

        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let limit = 512 * 1024;
        let mut w = LimitWriter::new(PathBuf::new(), f, limit, 10 * 1024 * 1024);

        let start = std::time::Instant::now();
        let n = w.write(&r).unwrap();
        let elapsed = start.elapsed();

        let rate = (n as f64) / elapsed.as_secs_f64();
        assert!(
            rate <= limit as f64,
            "rate limit mismath: exp {}, got {}",
            limit as f64,
            rate
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_writer_limiter_exceed_burst() {
        let limit = 10;
        let burst_limit = 20;

        let twenty_one_bytes = vec![0u8; 21];

        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();
        let mut w = LimitWriter::new(PathBuf::new(), f, limit, burst_limit);

        let n = w.write(&twenty_one_bytes).unwrap();

        assert_eq!(
            n,
            twenty_one_bytes.len(),
            "exected {} bytes written, but got {}",
            twenty_one_bytes.len(),
            n
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }
}
