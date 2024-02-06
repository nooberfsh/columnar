pub mod borrow;
pub mod region;

use crate::borrow::Borrow;
use crate::region::Region;

pub trait Columnar: Borrow + Sized {
    type Buf: ColumnarBuf<Self>;
}

pub trait ColumnarBuf<C: Columnar> {
    type ReadItem<'a>
    where
        Self: 'a;
    fn copy(&mut self, b: &C::Borrowed);
    fn idx(&self, i: usize) -> Self::ReadItem<'_>;
    fn len(&self) -> usize;
    fn with_capacity(s: usize) -> Self;
}

impl Columnar for u64 {
    type Buf = Region<u64>;
}

impl ColumnarBuf<u64> for Region<u64> {
    type ReadItem<'a> = u64;

    fn copy(&mut self, c: &u64) {
        self.copy(c)
    }

    fn idx(&self, i: usize) -> Self::ReadItem<'_> {
        *Region::idx(self, i)
    }

    fn len(&self) -> usize {
        Region::len(self)
    }

    fn with_capacity(s: usize) -> Self {
        Region::with_limit_and_capacity(1_000_000, s)
    }
}

mod string {
    use crate::region::Region;
    use crate::{Columnar, ColumnarBuf};

    pub struct StringBuf {
        idx: Vec<usize>,
        data: Region<u8>,
    }

    impl Columnar for String {
        type Buf = StringBuf;
    }

    impl ColumnarBuf<String> for StringBuf {
        type ReadItem<'a> = &'a str;

        fn copy(&mut self, c: &str) {
            self.data.copy_slice(c.as_bytes());
            self.idx.push(self.data.len());
        }

        fn idx(&self, i: usize) -> Self::ReadItem<'_> {
            let start = if i == 0 { 0 } else { self.idx[i - 1] };
            let end = self.idx[i];
            unsafe { std::str::from_utf8_unchecked(&self.data.slice(start, end)) }
        }

        fn len(&self) -> usize {
            self.idx.len()
        }

        fn with_capacity(s: usize) -> Self {
            let idx = Vec::with_capacity(s);
            let data = Region::with_limit_and_capacity(1_000_000 * 16, s);
            StringBuf { idx, data }
        }
    }
}

mod vector {
    use crate::{Columnar, ColumnarBuf};

    pub struct VecBuf<T: Columnar> {
        idx: Vec<usize>,
        buf: T::Buf,
    }

    impl<T: Columnar> Columnar for Vec<T> {
        type Buf = VecBuf<T>;
    }

    impl<T: Columnar> ColumnarBuf<Vec<T>> for VecBuf<T> {
        type ReadItem<'a> = IdxIter<'a, T> where T: 'a;

        fn copy(&mut self, c: &[T]) {
            for e in c {
                self.buf.copy(e.borrow());
            }
            let len = self.buf.len();
            self.idx.push(len);
        }

        fn idx(&self, i: usize) -> IdxIter<'_, T> {
            let start = if i == 0 { 0 } else { self.idx[i - 1] };
            let end = self.idx[i];
            IdxIter {
                end,
                current: start,
                buf: &self.buf,
            }
        }

        fn len(&self) -> usize {
            self.idx.len()
        }

        fn with_capacity(s: usize) -> Self {
            let idx = Vec::with_capacity(s);
            let buf = T::Buf::with_capacity(s * 8);
            VecBuf { idx, buf }
        }
    }

    pub struct IdxIter<'a, T>
    where
        T: Columnar,
        T: 'a,
    {
        end: usize,
        current: usize,
        buf: &'a T::Buf,
    }

    impl<'a, T> Iterator for IdxIter<'a, T>
    where
        T: Columnar,
        T: 'a,
    {
        type Item = <<T as Columnar>::Buf as ColumnarBuf<T>>::ReadItem<'a>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.current == self.end {
                None
            } else {
                let ret = self.buf.idx(self.current);
                self.current += 1;
                Some(ret)
            }
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            let len = self.end - self.current;
            (len, Some(len))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::string::StringBuf;
    use crate::ColumnarBuf;

    #[test]
    fn test_string_buf() {
        let mut string_buf = StringBuf::with_capacity(1);
        string_buf.copy("abc");
        string_buf.copy("xx");
        string_buf.copy("xx2");

        assert_eq!(string_buf.len(), 3);
        assert_eq!(string_buf.idx(0), "abc");
        assert_eq!(string_buf.idx(1), "xx");
        assert_eq!(string_buf.idx(2), "xx2");
    }
}
