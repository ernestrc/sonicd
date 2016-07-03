use nix::unistd;
use nix::errno::Errno::*;
use error::Result;

#[macro_export]
macro_rules! eagain {
    ($syscall:expr, $name:expr, $arg1: expr, $arg2: expr) => {{
        let res;
        loop {
            match $syscall($arg1, $arg2) {
                Ok(m) => {
                    res = m;
                    break;
                },
                Err(::nix::Error::Sys(a@EAGAIN)) | Err(::nix::Error::Sys(a@EINTR)) => {
                    debug!("{} {}, re-submitting syscall", $name, a);
                },
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        res
    }}
}

pub fn read(fd: i32, buf: &mut [u8]) -> Result<usize> {

    let b = eagain!(unistd::read, "unistd::read", fd, buf);

    Ok(b)
}

pub fn read_next(len: usize, fd: i32, buf: &mut [u8]) -> Result<usize> {
    let b = eagain!(unistd::read, "unistd::read", fd, buf);

    if b == len {
        // result as intended
        Ok(b)
    } else if b > 0 {
        // signal interruped the read or error occurred
        // or less than 'len' bytes were available for read.
        // reissuing the read will either indicate
        // the cause of the error or read the remaining bytes
        let rem = len - b;
        let mut rembuf = vec!(0; rem);

        debug!("unistd::read {} bytes: intended {}", b, len);

        let r = read_next(rem, fd, rembuf.as_mut_slice());
        buf.split_at_mut(b).1.copy_from_slice(rembuf.as_slice());
        r
    } else {
        // EOF reached, no data to read at this point
        debug!("unistd::read 0 bytes: EOF");
        Ok(b)
    }

}
