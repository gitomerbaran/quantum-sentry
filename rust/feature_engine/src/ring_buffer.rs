//! Fixed-capacity ring buffer for rolling mid prices and incremental aggregates.
//! O(1) push; safe handling of NaN/empty.

use std::collections::VecDeque;

/// Ring buffer storing (mid_price, ts_exchange) with incremental sum and sum_sq for O(1) mean/var.
#[derive(Debug, Clone)]
pub struct RingBuffer {
    buf: VecDeque<(f64, i64)>,
    capacity: usize,
    min_samples: usize,
    sum: f64,
    sum_sq: f64,
}

impl RingBuffer {
    /// New ring buffer with given capacity. `min_samples` defaults to capacity (full window).
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: VecDeque::with_capacity(capacity),
            capacity,
            min_samples: capacity.max(2),
            sum: 0.0,
            sum_sq: 0.0,
        }
    }

    /// Push (mid_price, ts). Drops oldest if at capacity. Ignores non-finite mid.
    pub fn push(&mut self, mid: f64, ts: i64) {
        if !mid.is_finite() || mid <= 0.0 {
            return;
        }
        if self.buf.len() == self.capacity {
            if let Some((old_mid, _)) = self.buf.pop_front() {
                self.sum -= old_mid;
                self.sum_sq -= old_mid * old_mid;
            }
        }
        self.sum += mid;
        self.sum_sq += mid * mid;
        self.buf.push_back((mid, ts));
    }

    /// True when buffer has at least min_samples (full window for log_return_window).
    pub fn is_ready(&self) -> bool {
        self.buf.len() >= self.min_samples
    }

    /// Number of samples currently in buffer.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// True if buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Oldest (first) sample: (mid, ts). None if empty.
    pub fn first(&self) -> Option<(f64, i64)> {
        self.buf.front().copied()
    }

    /// Newest (last) sample: (mid, ts). None if empty.
    pub fn last(&self) -> Option<(f64, i64)> {
        self.buf.back().copied()
    }

    /// Previous mid (one before last). Used for log_return_1 when len >= 2.
    pub fn prev_mid(&self) -> Option<f64> {
        if self.buf.len() >= 2 {
            self.buf.get(self.buf.len() - 2).map(|(m, _)| *m)
        } else {
            None
        }
    }

    /// Rolling mean. Returns None if empty or no finite values.
    pub fn mean(&self) -> Option<f64> {
        let n = self.buf.len();
        if n == 0 || !self.sum.is_finite() {
            return None;
        }
        let m = self.sum / (n as f64);
        if m.is_finite() && m > 0.0 {
            Some(m)
        } else {
            None
        }
    }

    /// Rolling variance (population). Returns None if len < 2 or invalid.
    pub fn var(&self) -> Option<f64> {
        let n = self.buf.len();
        if n < 2 || !self.sum.is_finite() || !self.sum_sq.is_finite() {
            return None;
        }
        let m = self.sum / (n as f64);
        let v = (self.sum_sq / (n as f64)) - (m * m);
        if v.is_finite() && v >= 0.0 {
            Some(v)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_buffer_not_ready_before_n_pushes() {
        let mut rb = RingBuffer::new(5);
        assert!(!rb.is_ready());
        rb.push(100.0, 1);
        assert!(!rb.is_ready());
        rb.push(101.0, 2);
        assert!(!rb.is_ready());
        rb.push(102.0, 3);
        rb.push(103.0, 4);
        assert!(!rb.is_ready()); // 4 < 5
        rb.push(104.0, 5);
        assert!(rb.is_ready());
    }

    #[test]
    fn ring_buffer_ready_after_n_pushes() {
        let mut rb = RingBuffer::new(3);
        rb.push(10.0, 1);
        rb.push(20.0, 2);
        rb.push(30.0, 3);
        assert!(rb.is_ready());
        assert_eq!(rb.len(), 3);
        assert_eq!(rb.first(), Some((10.0, 1)));
        assert_eq!(rb.last(), Some((30.0, 3)));
        assert_eq!(rb.prev_mid(), Some(20.0));
    }

    #[test]
    fn ring_buffer_drops_oldest_when_full() {
        let mut rb = RingBuffer::new(3);
        rb.push(1.0, 1);
        rb.push(2.0, 2);
        rb.push(3.0, 3);
        rb.push(4.0, 4);
        assert_eq!(rb.len(), 3);
        assert_eq!(rb.first(), Some((2.0, 2)));
        assert_eq!(rb.last(), Some((4.0, 4)));
    }
}
