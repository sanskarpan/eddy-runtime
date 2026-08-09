use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use super::semaphore::{Semaphore, SemaphorePermit};

pub struct Mutex<T: ?Sized> {
    semaphore: Semaphore,
    data: UnsafeCell<T>,
}

// SAFETY: semaphore ownership serializes mutable access to data.
unsafe impl<T: Send + ?Sized> Send for Mutex<T> {}
// SAFETY: shared access is immutable while the semaphore protects mutation.
unsafe impl<T: Send + Sync + ?Sized> Sync for Mutex<T> {}

pub struct MutexGuard<'a, T: ?Sized> {
    data: &'a mut T,
    _permit: SemaphorePermit,
}

impl<T> Mutex<T> {
    pub fn new(value: T) -> Mutex<T> {
        Mutex {
            semaphore: Semaphore::new(1),
            data: UnsafeCell::new(value),
        }
    }
}

impl<T: ?Sized> Mutex<T> {
    pub async fn lock(&self) -> MutexGuard<'_, T> {
        let permit = self
            .semaphore
            .acquire()
            .await
            .expect("eddy: mutex semaphore unexpectedly closed");
        // SAFETY: the semaphore has granted the only permit, so no other
        // guard can access this cell until `_permit` is dropped.
        let data = unsafe { &mut *self.data.get() };
        MutexGuard {
            data,
            _permit: permit,
        }
    }

    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        let permit = self.semaphore.try_acquire().ok()?;
        // SAFETY: try_acquire proves exclusive ownership of the permit.
        let data = unsafe { &mut *self.data.get() };
        Some(MutexGuard {
            data,
            _permit: permit,
        })
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.data.get_mut()
    }

    pub fn into_inner(self) -> T
    where
        T: Sized,
    {
        self.data.into_inner()
    }

    pub async fn lock_owned(self: Arc<Self>) -> OwnedMutexGuard<T>
    where
        T: Sized,
    {
        let permit = self
            .semaphore
            .acquire()
            .await
            .expect("eddy: mutex semaphore unexpectedly closed");
        OwnedMutexGuard {
            mutex: self,
            _permit: permit,
        }
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.data
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.data
    }
}

pub struct OwnedMutexGuard<T: ?Sized> {
    mutex: Arc<Mutex<T>>,
    _permit: SemaphorePermit,
}

impl<T: ?Sized> Deref for OwnedMutexGuard<T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: this guard owns the semaphore permit for the cell.
        unsafe { &*self.mutex.data.get() }
    }
}

impl<T: ?Sized> DerefMut for OwnedMutexGuard<T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: this guard owns the semaphore permit for the cell.
        unsafe { &mut *self.mutex.data.get() }
    }
}
