/// A fixed-capacity ring buffer that overwrites the oldest entries when full.
/// Efficient for time-series metric storage with bounded memory.
#[derive(Debug, Clone)]
pub struct RingBuffer<T> {
    data: Vec<T>,
    head: usize, // next write position
    len: usize,
    capacity: usize,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            head: 0,
            len: 0,
            capacity,
        }
    }

    pub fn push(&mut self, item: T) {
        if self.len < self.capacity {
            self.data.push(item);
            self.len += 1;
            self.head = self.len % self.capacity;
        } else {
            self.data[self.head] = item;
            self.head = (self.head + 1) % self.capacity;
        }
    }

    /// Returns items in chronological order (oldest first) as a Vec.
    pub fn to_vec(&self) -> Vec<&T> {
        let start = if self.len < self.capacity {
            0
        } else {
            self.head
        };
        let mut result = Vec::with_capacity(self.len);
        for i in 0..self.len {
            result.push(&self.data[(start + i) % self.capacity]);
        }
        result
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn last(&self) -> Option<&T> {
        if self.is_empty() {
            return None;
        }
        if self.len < self.capacity {
            self.data.last()
        } else {
            let idx = if self.head == 0 {
                self.capacity - 1
            } else {
                self.head - 1
            };
            Some(&self.data[idx])
        }
    }

    /// Returns the most recent `n` items in chronological order.
    pub fn recent(&self, n: usize) -> Vec<&T> {
        let count = n.min(self.len);
        let all = self.to_vec();
        all.into_iter().skip(self.len - count).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer_basic() {
        let mut buf: RingBuffer<i32> = RingBuffer::new(3);
        buf.push(1);
        buf.push(2);
        buf.push(3);
        assert_eq!(buf.to_vec().into_iter().copied().collect::<Vec<_>>(), vec![1, 2, 3]);

        buf.push(4);
        assert_eq!(buf.to_vec().into_iter().copied().collect::<Vec<_>>(), vec![2, 3, 4]);
        assert_eq!(buf.last(), Some(&4));
    }

    #[test]
    fn test_ring_buffer_recent() {
        let mut buf: RingBuffer<i32> = RingBuffer::new(5);
        for i in 1..=8 {
            buf.push(i);
        }
        let recent: Vec<i32> = buf.recent(3).into_iter().copied().collect();
        assert_eq!(recent, vec![6, 7, 8]);
    }
}
