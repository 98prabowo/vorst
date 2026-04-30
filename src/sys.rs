use std::os::unix::io::AsRawFd;
use std::{fs::File, io};

#[cfg(target_os = "linux")]
pub fn preallocate(file: &File, size: u64) -> io::Result<()> {
    use nix::fcntl::{FallocateFlags, fallocate};

    fallocate(file.as_raw_fd(), FallocateFlags::empty(), 0, size as i64)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

#[cfg(target_os = "macos")]
pub fn preallocate(file: &File, size: u64) -> io::Result<()> {
    use nix::libc::{F_ALLOCATEALL, F_ALLOCATECONTIG, F_PREALLOCATE, fcntl, fstore_t};

    let fd = file.as_raw_fd();

    let mut store = fstore_t {
        fst_flags: F_ALLOCATECONTIG,
        fst_posmode: 1,
        fst_offset: 0,
        fst_length: size as i64,
        fst_bytesalloc: 0,
    };

    let mut ret = unsafe { fcntl(fd, F_PREALLOCATE, &store) };
    if ret == -1 {
        store.fst_flags = F_ALLOCATEALL;
        ret = unsafe { fcntl(fd, F_PREALLOCATE, &store) };
    }

    if ret == -1 {
        return Err(io::Error::last_os_error());
    }

    file.set_len(size)
}
