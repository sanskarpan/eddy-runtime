//! Intrusive doubly-linked list for pinned nodes.
//!
//! The list never allocates and never moves a node. Callers must keep each
//! node pinned and must not mutate its `Pointers` while it is linked.
//!
//! The links live behind an `UnsafeCell` so nodes managed through shared
//! references (e.g. `Arc`-owned timer or I/O waiter nodes) can be linked
//! without ever creating `&mut` references into them: every mutation goes
//! through the cell's interior pointer, which Stacked Borrows exempts from
//! shared retags. Node pointers must therefore be derived without retagging
//! the node (e.g. via `Arc::as_ptr`, never `Arc::as_ref`). The module is
//! verified under Miri (`cargo miri test -p eddy --lib -- task:: sync::
//! time::wheel::`).

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ptr::NonNull;

pub(crate) struct Pointers<T> {
    inner: UnsafeCell<PointersInner<T>>,
}

struct PointersInner<T> {
    prev: Option<NonNull<T>>,
    next: Option<NonNull<T>>,
    linked: bool,
    _marker: PhantomData<T>,
}

impl<T> Pointers<T> {
    pub(crate) const fn new() -> Pointers<T> {
        Pointers {
            inner: UnsafeCell::new(PointersInner {
                prev: None,
                next: None,
                linked: false,
                _marker: PhantomData,
            }),
        }
    }

    pub(crate) fn is_linked(&self) -> bool {
        // SAFETY: the interior is only written by list operations holding
        // exclusive access, and only read here or under the same lock.
        unsafe { (*self.inner.get()).linked }
    }
}

pub(crate) trait Linked: Sized {
    fn pointers(&self) -> &Pointers<Self>;
}

/// # Safety
/// `node` must point to a live, pinned `T`; the returned pointer targets the
/// node's link storage and is only valid while the caller holds exclusive
/// access to the links.
unsafe fn links_of<T: Linked>(node: NonNull<T>) -> *mut PointersInner<T> {
    // SAFETY: the caller proves the node is live, so the shared reference is
    // well-formed; the cell interior is exempt from shared retags.
    unsafe { node.as_ref().pointers().inner.get() }
}

pub(crate) struct LinkedList<T: Linked> {
    head: Option<NonNull<T>>,
    tail: Option<NonNull<T>>,
    len: usize,
    _marker: PhantomData<T>,
}

impl<T: Linked> LinkedList<T> {
    pub(crate) const fn new() -> LinkedList<T> {
        LinkedList {
            head: None,
            tail: None,
            len: 0,
            _marker: PhantomData,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    /// # Safety
    /// `node` must point to a live, pinned `T` whose links are not currently
    /// owned by another list.
    pub(crate) unsafe fn push_back(&mut self, node: NonNull<T>) {
        // SAFETY: the caller guarantees the node is live and pinned.
        debug_assert!(!unsafe { node.as_ref() }.pointers().is_linked());
        let previous = self.tail;
        {
            // SAFETY: `node` is live and its links are exclusively owned here;
            // the interior pointer is exempt from shared retags.
            unsafe {
                let links = links_of(node);
                (*links).prev = previous;
                (*links).next = None;
                (*links).linked = true;
            }
        }
        if let Some(tail) = previous {
            // SAFETY: `tail` is a live pinned node owned by this list.
            unsafe {
                let links = links_of(tail);
                (*links).next = Some(node);
            }
        } else {
            self.head = Some(node);
        }
        self.tail = Some(node);
        self.len += 1;
    }

    /// # Safety
    /// `node` must point to a live, pinned `T` whose links are not currently
    /// owned by another list.
    pub(crate) unsafe fn push_front(&mut self, node: NonNull<T>) {
        // SAFETY: the caller guarantees the node is live and pinned.
        debug_assert!(!unsafe { node.as_ref() }.pointers().is_linked());
        let next = self.head;
        {
            // SAFETY: `node` is live and its links are exclusively owned here;
            // the interior pointer is exempt from shared retags.
            unsafe {
                let links = links_of(node);
                (*links).prev = None;
                (*links).next = next;
                (*links).linked = true;
            }
        }
        if let Some(head) = next {
            // SAFETY: `head` is a live pinned node owned by this list.
            unsafe {
                let links = links_of(head);
                (*links).prev = Some(node);
            }
        } else {
            self.tail = Some(node);
        }
        self.head = Some(node);
        self.len += 1;
    }

    /// # Safety
    /// The list must contain only live, pinned nodes and its links must not be
    /// concurrently modified.
    pub(crate) unsafe fn pop_front(&mut self) -> Option<NonNull<T>> {
        let head = self.head?;
        // SAFETY: `head` is owned by this list and remains pinned; the
        // interior read is exempt from shared retags.
        let next = unsafe { (*links_of(head)).next };
        self.unlink(head);
        self.head = next;
        if let Some(next_node) = next {
            // SAFETY: `next_node` is the new live pinned head.
            unsafe {
                let links = links_of(next_node);
                (*links).prev = None;
            }
        } else {
            self.tail = None;
        }
        Some(head)
    }

    /// # Safety
    /// `node` must be a live node currently linked in this list.
    pub(crate) unsafe fn remove(&mut self, node: NonNull<T>) {
        // SAFETY: `node` is live and linked in this list.
        let is_linked = unsafe { node.as_ref() }.pointers().is_linked();
        debug_assert!(is_linked);
        let (previous, next) = {
            // SAFETY: `node` is linked in this list and remains pinned; the
            // interior reads are exempt from shared retags.
            unsafe {
                let links = links_of(node);
                ((*links).prev, (*links).next)
            }
        };
        if let Some(prev_node) = previous {
            // SAFETY: `prev_node` is a live pinned neighbor in this list.
            unsafe {
                let links = links_of(prev_node);
                (*links).next = next;
            }
        } else {
            self.head = next;
        }
        if let Some(next_node) = next {
            // SAFETY: `next_node` is a live pinned neighbor in this list.
            unsafe {
                let links = links_of(next_node);
                (*links).prev = previous;
            }
        } else {
            self.tail = previous;
        }
        self.unlink(node);
    }

    unsafe fn unlink(&mut self, node: NonNull<T>) {
        // SAFETY: all callers prove `node` is a live node owned by this list.
        unsafe {
            let links = links_of(node);
            (*links).prev = None;
            (*links).next = None;
            (*links).linked = false;
        }
        self.len = self
            .len
            .checked_sub(1)
            .expect("eddy: intrusive list underflow");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Node {
        links: Pointers<Node>,
        value: u32,
    }

    impl Linked for Node {
        fn pointers(&self) -> &Pointers<Self> {
            &self.links
        }
    }

    #[test]
    fn push_pop_and_remove_preserve_links() {
        let mut nodes = [
            Node {
                links: Pointers::new(),
                value: 1,
            },
            Node {
                links: Pointers::new(),
                value: 2,
            },
            Node {
                links: Pointers::new(),
                value: 3,
            },
        ];
        let mut list = LinkedList::new();
        // SAFETY: every node is stored in the fixed array for the list's life.
        unsafe {
            list.push_back(NonNull::from(&mut nodes[0]));
            list.push_back(NonNull::from(&mut nodes[1]));
            list.push_back(NonNull::from(&mut nodes[2]));
            list.remove(NonNull::from(&mut nodes[1]));
        }
        assert_eq!(list.len(), 2);
        // SAFETY: the list owns these live nodes.
        assert_eq!(unsafe { list.pop_front().unwrap().as_ref().value }, 1);
        // SAFETY: the list owns this live node.
        assert_eq!(unsafe { list.pop_front().unwrap().as_ref().value }, 3);
        assert!(list.is_empty());
    }
}
