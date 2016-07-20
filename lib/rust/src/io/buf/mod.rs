use std::cmp;

use error::{Result, ErrorKind};
use io::DEFAULT_BUF_SIZE;

/// Naive buffer.
///
/// FIXME: if pos and limit never catch up, buffer will
/// overflow, when there might be some extra capacity
#[derive(Debug)]
pub struct ByteBuffer {
    limit: usize,
    pos: usize,
    buf: Vec<u8>,
}

impl ByteBuffer {
    pub fn new() -> ByteBuffer {
        ByteBuffer {
            pos: 0,
            limit: 0,
            buf: vec!(0; DEFAULT_BUF_SIZE),
        }
    }

    pub fn with_capacity(cap: usize) -> ByteBuffer {
        ByteBuffer {
            pos: 0,
            limit: 0,
            buf: vec!(0; cap),
        }
    }

    pub fn len(&self) -> usize {
        self.limit - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.len() > 0
    }

    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    pub fn write(&mut self, b: &[u8]) -> Result<()> {
        let len = b.len();
        if self.limit + len > self.buf.capacity() {
            Err(ErrorKind::BufferOverflowError(self.buf.capacity()).into())
        } else {
            self.buf[self.limit..self.limit + len].copy_from_slice(b);
            self.extend(len);
            Ok(())
        }
    }

    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let amt = cmp::min(self.limit - self.pos, buf.len());
        let (a, _) = self.buf[self.pos..self.limit].split_at(amt);
        buf[..amt].copy_from_slice(a);

        Ok(amt)
    }

    #[inline]
    pub fn extend(&mut self, cnt: usize) {
        self.limit += cnt;
    }

    #[inline]
    pub fn consume(&mut self, cnt: usize) {
        self.pos += cnt;
        if self.pos == self.limit {
            self.clear();
        }
    }

    pub fn clear(&mut self) {
        self.pos = 0;
        self.limit = 0;
    }
}

impl<'a> From<&'a ByteBuffer> for &'a [u8] {
    fn from(b: &'a ByteBuffer) -> &'a [u8] {
        &b.buf[b.pos..b.limit]
    }
}

impl<'a> From<&'a mut ByteBuffer> for &'a mut [u8] {
    fn from(b: &'a mut ByteBuffer) -> &'a mut [u8] {
        &mut b.buf[b.limit..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Cursor};
    use error::ErrorKind;

    #[test]
    fn does_buffer() {
        let mut buffer = ByteBuffer::new();

        let a = [1, 2, 3];
        buffer.write(&a).unwrap();

        let mut b = [0; 3];
        let bcnt = buffer.read(&mut b).unwrap();
        assert!(b == a);
        assert!(bcnt == 3);

        let a2 = [4, 5, 6];
        buffer.write(&a2).unwrap();

        let mut c = [0; 3];
        let ccnt = buffer.read(&mut c).unwrap();
        assert!(c == a);
        assert!(ccnt == 3);

        buffer.consume(3);

        let mut e = [0; 3];
        let ecnt = buffer.read(&mut e).unwrap();
        assert!(e == a2);
        assert!(ecnt == 3);

        buffer.consume(3);

        let mut d = [0; 3];
        let dcnt = buffer.read(&mut d).unwrap();
        assert!(dcnt == 0);
        assert!(d != a);
        assert!(d == [0, 0, 0]);
    }

    #[test]
    fn share_read_ref() {
        let mut buffer = ByteBuffer::new();

        let a = [4, 5, 6];
        buffer.write(&a).unwrap();

        let mut b: Vec<u8> = vec![1; 3];

        b.extend_from_slice(From::from(&buffer));

        assert!(b == vec![1, 1, 1, 4, 5, 6]);
    }

    #[test]
    fn share_write_ref() {
        let mut buffer = ByteBuffer::with_capacity(10);

        let a = [4, 5];
        buffer.write(&a).unwrap();

        let mut b = Cursor::new([1, 2, 3]);

        let r = {
            let dst: &mut [u8] = From::from(&mut buffer);
            b.read(dst).unwrap()
        };

        buffer.extend(r);

        assert!(r == 3, format!("read {}", r));

        let mut c = [0; 5];
        let ccnt = buffer.read(&mut c).unwrap();
        assert!(c == [4, 5, 1, 2, 3], "res: {:?}; buffer: {:?}", c, buffer);
        assert!(ccnt == 5, format!("res: {}", ccnt));
    }

    #[test]
    fn overflow_error() {
        let mut buffer = ByteBuffer {
            pos: 0,
            limit: 0,
            buf: Vec::with_capacity(5),
        };

        let a = [1, 2, 3, 4, 5, 6];
        let res = buffer.write(&a);

        assert!(res.is_err());
        match res.err().unwrap().into_kind() {
            ErrorKind::BufferOverflowError(max) => assert!(max == 5),
            e => panic!("different error: {:?}", e),
        }
    }
}
