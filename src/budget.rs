//! Non-blocking process byte budget for live transport, decode, and IPC buffers.
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

pub const DEFAULT_PROCESS_BUDGET_BYTES: usize = 1 << 30;

#[derive(Debug)]
pub struct ByteBudget {
    limit: usize,
    used: AtomicUsize,
}

impl ByteBudget {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
        })
    }

    pub fn global() -> &'static Arc<Self> {
        static GLOBAL: OnceLock<Arc<ByteBudget>> = OnceLock::new();
        GLOBAL.get_or_init(|| Self::new(DEFAULT_PROCESS_BUDGET_BYTES))
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    pub fn empty_lease(self: &Arc<Self>) -> ByteLease {
        ByteLease {
            budget: self.clone(),
            bytes: 0,
        }
    }

    pub fn try_acquire(self: &Arc<Self>, bytes: usize) -> Option<ByteLease> {
        let mut lease = self.empty_lease();
        if lease.try_grow(bytes) {
            Some(lease)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct ByteLease {
    budget: Arc<ByteBudget>,
    bytes: usize,
}

impl ByteLease {
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn try_grow(&mut self, additional: usize) -> bool {
        if additional == 0 {
            return true;
        }
        let mut current = self.budget.used.load(Ordering::Relaxed);
        loop {
            let Some(next) = current.checked_add(additional) else {
                return false;
            };
            if next > self.budget.limit {
                return false;
            }
            match self.budget.used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.bytes += additional;
                    return true;
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn replacement_lease(&self, capacity: usize, needed: usize, limit: usize) -> Option<Self> {
        if needed > limit {
            return None;
        }
        let target = needed.max(capacity.saturating_mul(2)).min(limit);
        self.budget.try_acquire(target).or_else(|| {
            (target != needed)
                .then(|| self.budget.try_acquire(needed))
                .flatten()
        })
    }

    /// Reserve a replacement before copying, accounting for both allocations
    /// until the old buffer is freed. `needed` is the total required length.
    pub fn reserve_vec(&mut self, buffer: &mut Vec<u8>, needed: usize, limit: usize) -> bool {
        if needed <= buffer.capacity() {
            return needed <= limit;
        }
        let Some(mut replacement_lease) = self.replacement_lease(buffer.capacity(), needed, limit)
        else {
            return false;
        };
        let mut replacement = Vec::new();
        if replacement
            .try_reserve_exact(replacement_lease.bytes())
            .is_err()
            || !replacement_lease.try_grow(
                replacement
                    .capacity()
                    .saturating_sub(replacement_lease.bytes()),
            )
        {
            return false;
        }
        replacement.extend_from_slice(buffer);
        let old_capacity = buffer.capacity();
        *buffer = replacement;
        self.shrink_by(old_capacity);
        self.merge(replacement_lease);
        true
    }

    pub fn reserve_string(&mut self, buffer: &mut String, needed: usize, limit: usize) -> bool {
        if needed <= buffer.capacity() {
            return needed <= limit;
        }
        let Some(mut replacement_lease) = self.replacement_lease(buffer.capacity(), needed, limit)
        else {
            return false;
        };
        let mut replacement = String::new();
        if replacement
            .try_reserve_exact(replacement_lease.bytes())
            .is_err()
            || !replacement_lease.try_grow(
                replacement
                    .capacity()
                    .saturating_sub(replacement_lease.bytes()),
            )
        {
            return false;
        }
        replacement.push_str(buffer);
        let old_capacity = buffer.capacity();
        *buffer = replacement;
        self.shrink_by(old_capacity);
        self.merge(replacement_lease);
        true
    }

    pub fn shrink_to(&mut self, retained: usize) {
        if retained < self.bytes {
            let delta = self.bytes - retained;
            self.budget.used.fetch_sub(delta, Ordering::AcqRel);
            self.bytes = retained;
        }
    }

    pub fn shrink_by(&mut self, released: usize) {
        self.shrink_to(self.bytes.saturating_sub(released));
    }

    pub fn release(&mut self) {
        self.shrink_to(0);
    }

    pub fn split(&mut self, bytes: usize) -> Option<Self> {
        if bytes > self.bytes {
            return None;
        }
        self.bytes -= bytes;
        Some(Self {
            budget: self.budget.clone(),
            bytes,
        })
    }

    pub fn take(&mut self) -> Self {
        let bytes = self.bytes;
        self.bytes = 0;
        Self {
            budget: self.budget.clone(),
            bytes,
        }
    }

    pub fn merge(&mut self, mut other: Self) {
        debug_assert!(Arc::ptr_eq(&self.budget, &other.budget));
        self.bytes += other.bytes;
        other.bytes = 0;
    }
}

impl Drop for ByteLease {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

/// A value whose allocation remains charged while its caller retains it.
#[derive(Debug)]
pub struct Leased<T> {
    value: T,
    lease: ByteLease,
}

impl<T> Leased<T> {
    pub fn new(value: T, lease: ByteLease) -> Self {
        Self { value, lease }
    }

    pub(crate) fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }

    pub fn into_parts(self) -> (T, ByteLease) {
        (self.value, self.lease)
    }
}

impl<T> std::ops::Deref for Leased<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: AsRef<[u8]>> AsRef<[u8]> for Leased<T> {
    fn as_ref(&self) -> &[u8] {
        self.value.as_ref()
    }
}

pub fn format_bytes(bytes: usize) -> String {
    const MIB: usize = 1 << 20;
    const GIB: usize = 1 << 30;
    if bytes >= GIB && bytes.is_multiple_of(GIB) {
        format!("{} GiB", bytes / GIB)
    } else if bytes >= MIB && bytes.is_multiple_of(MIB) {
        format!("{} MiB", bytes / MIB)
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_growth_accounts_capacity_and_overlapping_replacements() {
        let budget = ByteBudget::new(40);
        let mut lease = budget.empty_lease();
        let mut buffer = Vec::new();
        assert!(lease.reserve_vec(&mut buffer, 16, 64));
        buffer.extend_from_slice(&[1; 16]);
        assert_eq!(lease.bytes(), buffer.capacity());
        // Doubling would need 16 + 32 live bytes; an exact replacement fits.
        assert!(lease.reserve_vec(&mut buffer, 17, 64));
        assert_eq!(buffer.capacity(), 17);
        assert_eq!(lease.bytes(), 17);
        assert_eq!(buffer, [1; 16]);
        assert!(!lease.reserve_vec(&mut buffer, 24, 64));
        assert_eq!(buffer.capacity(), 17);
        assert_eq!(budget.used(), 17);
        drop(buffer);
        drop(lease);
        assert_eq!(budget.used(), 0);
        let mut lease = budget.empty_lease();
        let mut text = String::new();
        assert!(lease.reserve_string(&mut text, 16, 64));
        text.push_str("abcdefghijklmnop");
        assert!(lease.reserve_string(&mut text, 17, 64));
        assert_eq!(text, "abcdefghijklmnop");
        assert_eq!(lease.bytes(), text.capacity());
        assert!(!lease.reserve_string(&mut text, 24, 64));
        drop(text);
        drop(lease);
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn cancelled_future_drops_payload_before_its_lease() {
        struct Payload(Arc<ByteBudget>);
        impl Drop for Payload {
            fn drop(&mut self) {
                assert_eq!(self.0.used(), 64);
            }
        }
        for polled in [false, true] {
            let budget = ByteBudget::new(64);
            let payload = Leased::new(Payload(budget.clone()), budget.try_acquire(64).unwrap());
            let future = async move {
                let _payload = payload;
                std::future::pending::<()>().await;
            };
            if polled {
                assert!(futures_util::FutureExt::now_or_never(future).is_none());
            } else {
                drop(future);
            }
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn non_blocking_admission_handoff_and_release_leave_zero_debt() {
        let budget = ByteBudget::new(100);
        let mut a = budget.try_acquire(60).unwrap();
        assert_eq!(budget.used(), 60);
        assert!(budget.try_acquire(41).is_none());
        assert_eq!(budget.used(), 60);

        let b = budget.try_acquire(40).unwrap();
        assert_eq!(budget.used(), 100);
        assert!(!a.try_grow(1));

        // Same-buffer handoff transfers ownership without double charging.
        let handed = a.take();
        assert_eq!(a.bytes(), 0);
        assert_eq!(handed.bytes(), 60);
        assert_eq!(budget.used(), 100);

        drop(b);
        assert_eq!(budget.used(), 60);
        drop(handed);
        assert_eq!(budget.used(), 0);
    }
}
