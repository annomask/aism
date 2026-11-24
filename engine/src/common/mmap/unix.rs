extern crate libc;

use std::{
    fs::File,
    io,
    os::unix::io::{AsRawFd, RawFd},
    ptr,
};

#[cfg(any(
    all(target_os = "linux", not(target_arch = "mips")),
    target_os = "freebsd",
    target_os = "android"
))]
#[cfg(not(any(
    all(target_os = "linux", not(target_arch = "mips")),
    target_os = "freebsd",
    target_os = "android"
)))]
const MAP_STACK: libc::c_int = 0;

pub struct Inner {
    ptr: *mut libc::c_void,
    len: usize,
    is_mapped: bool, // true if ptr is mapped, false if ptr is malloced
}

impl Inner {
    /// Creates a new `Inner`.
    ///
    /// This is a thin wrapper around the `mmap` sytem call.
    fn new(
        len: usize,
        prot: libc::c_int,
        flags: libc::c_int,
        file: RawFd,
        offset: u64,
    ) -> io::Result<Inner> {
        let alignment = offset % page_size() as u64;
        let aligned_offset = offset - alignment;
        let aligned_len = len + alignment as usize;
        if aligned_len == 0 {
            // Normally the OS would catch this, but it segfaults under QEMU.
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "memory map must have a non-zero length",
            ));
        }

        unsafe {
            let ptr = libc::mmap(
                ptr::null_mut(),
                aligned_len as libc::size_t,
                prot,
                flags,
                file,
                aligned_offset as libc::off_t,
            );

            if ptr == libc::MAP_FAILED {
                Err(io::Error::last_os_error())
            } else {
                Ok(Inner {
                    ptr: ptr.offset(alignment as isize),
                    len,
                    is_mapped: true,
                })
            }
        }
    }

    pub fn map(len: usize, file: &File, offset: u64) -> io::Result<Inner> {
        Inner::new(
            len,
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            offset,
        )
    }

    pub fn flush(&self, offset: usize, len: usize) -> io::Result<()> {
        let alignment = (self.ptr as usize + offset) % page_size();
        let offset = offset as isize - alignment as isize;
        let len = len + alignment;
        let result =
            unsafe { libc::msync(self.ptr.offset(offset), len as libc::size_t, libc::MS_SYNC) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn flush_async(&self, offset: usize, len: usize) -> io::Result<()> {
        let alignment = offset % page_size();
        let aligned_offset = offset - alignment;
        let aligned_len = len + alignment;
        let result = unsafe {
            libc::msync(
                self.ptr.add(aligned_offset),
                aligned_len as libc::size_t,
                libc::MS_ASYNC,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[inline]
    pub fn ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        let alignment = self.ptr as usize % page_size();
        unsafe {
            if self.is_mapped {
                assert!(
                    libc::munmap(
                        self.ptr.offset(-(alignment as isize)),
                        (self.len + alignment) as libc::size_t
                    ) == 0,
                    "unable to unmap mmap: {}",
                    io::Error::last_os_error()
                );
            } else {
                libc::free(self.ptr);
            }
        }
    }
}

impl Clone for Inner {
    fn clone(&self) -> Self {
        unsafe {
            let ptr = libc::malloc(self.len);
            assert!(!ptr.is_null(), "unable to malloc");
            let ptr = libc::memcpy(ptr, self.ptr, self.len());

            Self {
                ptr,
                len: self.len(),
                is_mapped: false,
            }
        }
    }
}

unsafe impl Sync for Inner {}
unsafe impl Send for Inner {}

fn page_size() -> usize {
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
}
