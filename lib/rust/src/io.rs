use nix::unistd;
use nix::errno::Errno::*;
use error::{ErrorKind, Result};

#[macro_export]
macro_rules! eagain {
	($syscall:expr, $name:expr, $arg1: expr, $arg2: expr) => {{
		let mut res = None;
		loop {
			match $syscall($arg1, $arg2) {
				Err(nix::Error::Sys(a@nix::Errno::EAGAIN)) => {
					debug!("{}: {}", $name, a);
					continue;
				},
				Ok(m) => {
					res = Some(m);
					break;
				}
				Err(e) => {
					perror!(format!("{}", $name))(e);
					break;
				}
			}
		}
		res
	}}
}

pub fn read(len: usize, fd: i32, buf: &mut [u8]) -> Result<usize> {
    match unistd::read(fd, buf) {
        Ok(b) => {
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

                let r = read(rem, fd, rembuf.as_mut_slice());
                buf.split_at_mut(b).1.copy_from_slice(rembuf.as_slice());
                r
            } else {
                // EOF reached, no data to read at this point
                debug!("unistd::read 0 bytes: EOF");
                Ok(b)
            }
        }
        Err(::nix::Error::Sys(EAGAIN)) |
        Err(::nix::Error::Sys(EINTR)) => {
            debug!("unistd::read: EAGAIN | EINTR, resubmitting read");
            read(len, fd, buf)
        }
        Err(e) => Err(ErrorKind::NixIo(e).into()),
    }
}
