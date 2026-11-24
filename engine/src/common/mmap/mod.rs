//! memory mapped buffers.

// #[cfg(windows)]
// extern crate winapi;
// #[cfg(windows)]
// mod windows;
// #[cfg(windows)]
// use windows::Inner;

// #[cfg(unix)]
// mod unix;
use std::{
    fmt,
    fs::File,
    io::{Error, ErrorKind, Result},
    ops::Deref,
    slice, usize,
};

#[cfg(unix)]
use unix::Inner;

mod unix;

/// A memory map builder
#[derive(Clone, Debug, Default)]
pub struct MmapOptions {
    offset: u64,
    len: Option<usize>,
}

impl MmapOptions {
    /// Creates a new set of options for configuring and creating a memory map.
    pub fn new() -> MmapOptions {
        MmapOptions::default()
    }

    /// Set the memory map to start at byte `offset` from the beginning of the file.
    pub fn offset(&mut self, offset: u64) -> &mut Self {
        self.offset = offset;
        self
    }

    /// Set the created memory mapped buffer to be `len` bytes long.
    /// For file-backed memory maps, the length will default to the file length.
    pub fn len(&mut self, len: usize) -> &mut Self {
        self.len = Some(len);
        self
    }

    /// Returns the configured length, or the length of the provided file.
    fn get_len(&self, file: &File) -> Result<usize> {
        self.len.map(Ok).unwrap_or_else(|| {
            let len = file.metadata()?.len() - self.offset;
            if len > (usize::MAX as u64) {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "memory map length overflows usize",
                ));
            }
            Ok(len as usize)
        })
    }

    /// Creates a read-only memory map backed by a file.
    ///
    /// # Safety
    ///
    /// The file must be opened.
    pub unsafe fn map(&self, file: &File) -> Result<Mmap> {
        Inner::map(self.get_len(file)?, file, self.offset).map(|inner| Mmap { inner })
    }
}

/// An immutable memory mapped buffer backed by a file.
#[derive(Clone)]
pub struct Mmap {
    inner: Inner,
}

impl Mmap {
    /// Creates a read-only memory map backed by a file.
    ///
    /// # Safety
    ///
    /// The file must be opened.
    pub unsafe fn map(file: &File) -> Result<Mmap> {
        unsafe { MmapOptions::new().map(file) }
    }

    /// Returns the bytes length of the memory map.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Return `true` if the memory map is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Deref for Mmap {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.inner.ptr(), self.inner.len()) }
    }
}

impl AsRef<[u8]> for Mmap {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl fmt::Debug for Mmap {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Mmap")
            .field("ptr", &self.as_ptr())
            .field("len", &self.len())
            .finish()
    }
}

impl PartialEq for Mmap {
    fn eq(&self, other: &Self) -> bool {
        self.inner.ptr() == other.inner.ptr()
    }
}
