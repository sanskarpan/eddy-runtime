//! Intrusive doubly-linked list for pinned nodes.
//!
//! The list never allocates and never moves a node. Callers must keep each
//! node pinned and must not mutate its `Pointers` while it is linked.

use std::marker::PhantomData;
use std::ptr::NonNull;

pub(crate) struct Pointers<T> {
    prev: Option<NonNull<T>>,
    next: Option<NonNull<T>>,
    linked: bool,
    _marker: PhantomData<T>,
}

impl<T> Pointers<T> {
    pub(crate) const fn new() -> Pointers<T> {
        Pointers {
            prev: None,
            next: None,
            linked: false,
            _marker: PhantomData,
        }
    }

    pub(crate) fn is_linked(&self) -> bool {
        self.linked
    }
}

pub(crate) trait Linked: Sized {
    fn pointers(&self) -> &Pointers<Self>;
    fn pointers_mut(&mut self) -> &mut Pointers<Self>;
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
        debug_assert!(!node.as_ref().pointers().is_linked());
        let previous = self.tail;
        {
            // SAFETY: the caller guarantees that `node` is live and pinned.
            let pointers = unsafe { &mut *node.as_ptr().cast::<T>() }.pointers_mut();
            pointers.prev = previous;
            pointers.next = None;
            pointers.linked = true;
        }
        if let Some(mut tail) = previous {
            // SAFETY: `tail` is the list's live pinned tail.
            unsafe { tail.as_mut().pointers_mut().next = Some(node) };
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
        debug_assert!(!node.as_ref().pointers().is_linked());
        let next = self.head;
        {
            // SAFETY: the caller guarantees that `node` is live and pinned.
            let pointers = unsafe { &mut *node.as_ptr().cast::<T>() }.pointers_mut();
            pointers.prev = None;
            pointers.next = next;
            pointers.linked = true;
        }
        if let Some(mut next) = next {
            // SAFETY: `next` is the list's live pinned head.
            unsafe { next.as_mut().pointers_mut().prev = Some(node) };
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
        // SAFETY: `head` is owned by this list and remains pinned.
        let next = unsafe { head.as_ref().pointers().next };
        self.unlink(head);
        self.head = next;
        if let Some(mut next) = next {
            // SAFETY: `next` is the new live pinned head.
            unsafe { next.as_mut().pointers_mut().prev = None };
        } else {
            self.tail = None;
        }
        Some(head)
    }

    /// # Safety
    /// `node` must be a live node currently linked in this list.
    pub(crate) unsafe fn remove(&mut self, node: NonNull<T>) {
        debug_assert!(node.as_ref().pointers().is_linked());
        let (previous, next) = {
            // SAFETY: `node` is linked in this list and remains pinned.
            let pointers = unsafe { node.as_ref().pointers() };
            (pointers.prev, pointers.next)
        };
        if let Some(mut previous) = previous {
            // SAFETY: `previous` is a live pinned neighbor in this list.
            unsafe { previous.as_mut().pointers_mut().next = next };
        } else {
            self.head = next;
        }
        if let Some(mut next) = next {
            // SAFETY: `next` is a live pinned neighbor in this list.
            unsafe { next.as_mut().pointers_mut().prev = previous };
        } else {
            self.tail = previous;
        }
        self.unlink(node);
    }

    unsafe fn unlink(&mut self, mut node: NonNull<T>) {
        // SAFETY: all callers prove `node` is a live node owned by this list.
        let pointers = unsafe { node.as_mut().pointers_mut() };
        pointers.prev = None;
        pointers.next = None;
        pointers.linked = false;
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

        fn pointers_mut(&mut self) -> &mut Pointers<Self> {
            &mut self.links
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
