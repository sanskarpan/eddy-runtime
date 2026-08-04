// examples/spike.rs — build a Waker by hand, poll a future to completion.
// No dependencies beyond std. If this is wrong, everything above it is
// unfixable: this is the minimal proof that clone/wake/wake_by_ref/drop
// can maintain an Arc refcount correctly through a RawWakerVTable.
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

struct Shared {
    woken: AtomicBool,
}

// SAFETY: `p` is always a pointer previously produced by `Arc::into_raw`
// on an `Arc<Shared>`, per the `Waker`/`RawWaker` contract that only our
// own `clone_waker`/`Waker::from_raw` calls ever construct one.
unsafe fn clone(p: *const ()) -> RawWaker {
    // +1: a new Waker now exists that shares ownership with the original.
    Arc::increment_strong_count(p as *const Shared);
    RawWaker::new(p, &VTABLE)
}

// SAFETY: see `clone`. `wake` takes ownership (consumes) the reference
// this RawWaker represented.
unsafe fn wake(p: *const ()) {
    let arc = Arc::from_raw(p as *const Shared); // reclaims the +1 from clone/creation
    arc.woken.store(true, Ordering::Release);
    // arc dropped here -> -1, correct because wake() CONSUMES the waker.
}

// SAFETY: see `clone`. `wake_by_ref` borrows; it must leave the refcount
// unchanged, hence `ManuallyDrop` around the reconstructed `Arc`.
unsafe fn wake_by_ref(p: *const ()) {
    let arc = std::mem::ManuallyDrop::new(Arc::from_raw(p as *const Shared));
    arc.woken.store(true, Ordering::Release);
}

// SAFETY: see `clone`. `drop` always corresponds to one prior +1.
unsafe fn drop_it(p: *const ()) {
    Arc::decrement_strong_count(p as *const Shared);
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_it);

fn main() {
    let shared = Arc::new(Shared {
        woken: AtomicBool::new(false),
    });
    // SAFETY: VTABLE's four functions uphold the RawWaker contract
    // documented above (exact +1/consume/neutral/-1 refcount discipline).
    let waker = unsafe {
        Waker::from_raw(RawWaker::new(
            Arc::into_raw(shared.clone()) as *const (),
            &VTABLE,
        ))
    };
    let mut cx = Context::from_waker(&waker);
    let mut fut = Box::pin(async { 42 });
    assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(42));
    println!("waker + poll works");
}
