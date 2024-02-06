// adapted from https://github.com/frankmcsherry/columnation/blob/master/src/lib.rs

/// A region allocator which holds items at stable memory locations.
///
/// Items once inserted will not be moved, and their locations in memory
/// can be relied on by others, until the region is cleared.
///
/// This type accepts owned data, rather than references, and does not
/// itself intend to implement `Region`. Rather, it is a useful building
/// block for other less-safe code that wants allocated data to remain at
/// fixed memory locations.
pub struct Region<T: Copy> {
    /// The active allocation into which we are writing.
    local: Vec<T>,
    /// All previously active allocations.
    stash: Vec<Vec<T>>,
    /// The maximum allocation size
    limit: usize,
}

impl<T: Copy> Region<T> {
    /// Construct a [Region] with a allocation size limit.
    pub fn with_limit(limit: usize) -> Self {
        Self {
            local: Default::default(),
            stash: Default::default(),
            limit,
        }
    }

    /// Allocates a new `Self` that can accept `count` items without reallocation.
    pub fn with_limit_and_capacity(limit: usize, count: usize) -> Self {
        let mut region = Self::with_limit(limit);
        region.reserve(count);
        region
    }

    pub fn idx(&self, i: usize) -> &T {
        let mut l = 0;
        for s in &self.stash {
            if s.len() + l > i {
                return &s[i - l];
            }
            l += s.len();
        }
        &self.local[i - l]
    }

    pub fn slice(&self, start: usize, end: usize) -> &[T] {
        let mut l = 0;
        for s in &self.stash {
            if s.len() + l > start {
                let start = start - l;
                let end = end - l;
                return &s[start..end];
            }
            l += s.len();
        }
        let start = start - l;
        let end = end - l;
        &self.local[start..end]
    }

    /// Clears the contents without dropping any elements.
    #[inline]
    pub fn clear(&mut self) {
        self.local.clear();
        self.stash.clear();
    }

    pub fn copy(&mut self, t: &T) {
        self.reserve(1);
        self.local.push(*t);
    }

    /// Copies a slice of cloneable items into the region.
    #[inline]
    pub fn copy_slice(&mut self, items: &[T]) {
        self.reserve(items.len());
        self.local.extend_from_slice(items);
    }

    /// Ensures that there is space in `self.local` to copy at least `count` items.
    #[inline(always)]
    pub fn reserve(&mut self, count: usize) {
        // Check if `item` fits into `self.local` without reallocation.
        // If not, stash `self.local` and increase the allocation.
        if count > self.local.capacity() - self.local.len() {
            // Increase allocated capacity in powers of two.
            // We could choose a different rule here if we wanted to be
            // more conservative with memory (e.g. page size allocations).
            let mut next_len = (self.local.capacity() + 1).next_power_of_two();
            next_len = std::cmp::min(next_len, self.limit);
            next_len = std::cmp::max(count, next_len);
            let new_local = Vec::with_capacity(next_len);
            if self.local.is_empty() {
                self.local = new_local;
            } else {
                self.stash
                    .push(std::mem::replace(&mut self.local, new_local));
            }
        }
    }

    /// The number of items current held in the region.
    pub fn len(&self) -> usize {
        self.local.len() + self.stash.iter().map(|r| r.len()).sum::<usize>()
    }

    #[inline]
    pub fn heap_size(&self, mut callback: impl FnMut(usize, usize)) {
        // Calculate heap size for local, stash, and stash entries
        let size_of_t = std::mem::size_of::<T>();
        callback(
            self.local.len() * size_of_t,
            self.local.capacity() * size_of_t,
        );
        callback(
            self.stash.len() * std::mem::size_of::<Vec<T>>(),
            self.stash.capacity() * std::mem::size_of::<Vec<T>>(),
        );
        for stash in &self.stash {
            callback(stash.len() * size_of_t, stash.capacity() * size_of_t);
        }
    }
}
