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
