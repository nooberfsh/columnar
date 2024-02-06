pub trait Borrow {
    type Borrowed: ?Sized;
    fn borrow(&self) -> &Self::Borrowed;
}

impl Borrow for u64 {
    type Borrowed = u64;
    fn borrow(&self) -> &Self::Borrowed {
        self
    }
}

impl Borrow for String {
    type Borrowed = str;

    fn borrow(&self) -> &Self::Borrowed {
        self.as_str()
    }
}

impl<T> Borrow for Vec<T> {
    type Borrowed = [T];

    fn borrow(&self) -> &Self::Borrowed {
        self
    }
}
