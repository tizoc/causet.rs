//! Erased message payloads. A queued Action, Effect, Output or Event travels
//! as a `Payload`: stored inline when it fits `INLINE` bytes at an alignment
//! of at most 16, else boxed. Most messages fit, so queueing one allocates
//! nothing; a larger one costs the allocation a `Box<dyn Any + Send>` always
//! did.

use std::any::Any;
use std::fmt;
use std::marker::PhantomData;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::ptr;

/// Inline capacity, in bytes. A `Payload` is this plus 16.
pub(crate) const INLINE: usize = 48;

#[repr(C, align(16))]
struct Buf([MaybeUninit<u8>; INLINE]);

/// How to reach and drop the value in a `Buf`: the value itself (inline) or
/// a `Box` holding it.
struct VTable {
    as_any: unsafe fn(*const Buf) -> *const (dyn Any + Send),
    drop: unsafe fn(*mut Buf),
    boxed: bool,
}

struct Inline<T>(PhantomData<T>);

impl<T: Any + Send> Inline<T> {
    const VTABLE: VTable = VTable {
        as_any: Self::as_any,
        drop: Self::drop,
        boxed: false,
    };

    unsafe fn as_any(buf: *const Buf) -> *const (dyn Any + Send) {
        buf.cast::<T>()
    }
    unsafe fn drop(buf: *mut Buf) {
        // SAFETY: the caller owns the initialized `T` in `buf`.
        unsafe { ptr::drop_in_place(buf.cast::<T>()) }
    }
}

struct Boxed<T>(PhantomData<T>);

impl<T: Any + Send> Boxed<T> {
    const VTABLE: VTable = VTable {
        as_any: Self::as_any,
        drop: Self::drop,
        boxed: true,
    };

    unsafe fn as_any(buf: *const Buf) -> *const (dyn Any + Send) {
        // SAFETY: `buf` holds an initialized `Box<T>`.
        unsafe { &**buf.cast::<Box<T>>() as *const T }
    }
    unsafe fn drop(buf: *mut Buf) {
        // SAFETY: the caller owns the initialized `Box<T>` in `buf`.
        unsafe { ptr::drop_in_place(buf.cast::<Box<T>>()) }
    }
}

const fn fits<T>() -> bool {
    size_of::<T>() <= INLINE && align_of::<T>() <= align_of::<Buf>()
}

/// An owned value of some `Send + 'static` type, like `Box<dyn Any + Send>`.
pub(crate) struct Payload {
    buf: Buf,
    vtable: &'static VTable,
    /// `Send` only, like `Box<dyn Any + Send>`: the value need not be `Sync`.
    _send_only: PhantomData<*mut ()>,
}

// SAFETY: the value is `Send` (every constructor requires it), and `Payload`
// gives out references only through `&self`/`&mut self`.
unsafe impl Send for Payload {}

impl Payload {
    pub(crate) fn new<T: Any + Send>(value: T) -> Self {
        let mut buf = Buf([MaybeUninit::uninit(); INLINE]);
        let vtable = if fits::<T>() {
            // SAFETY: `T` fits `buf`'s size and alignment.
            unsafe { ptr::write(buf.0.as_mut_ptr().cast::<T>(), value) };
            &Inline::<T>::VTABLE
        } else {
            // SAFETY: a `Box` is one pointer, which always fits.
            unsafe { ptr::write(buf.0.as_mut_ptr().cast::<Box<T>>(), Box::new(value)) };
            &Boxed::<T>::VTABLE
        };
        Self {
            buf,
            vtable,
            _send_only: PhantomData,
        }
    }

    pub(crate) fn as_any(&self) -> &dyn Any {
        // SAFETY: `buf` holds the value `vtable` was made for.
        unsafe { &*(self.vtable.as_any)(&self.buf) }
    }

    pub(crate) fn is<T: Any>(&self) -> bool {
        self.as_any().is::<T>()
    }

    pub(crate) fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.as_any().downcast_ref()
    }

    /// The value, if it is a `T`; the payload back otherwise.
    pub(crate) fn take<T: Any>(self) -> Result<T, Self> {
        if !self.is::<T>() {
            return Err(self);
        }
        let this = ManuallyDrop::new(self);
        let at = this.buf.0.as_ptr();
        // SAFETY: the value is a `T`, stored as `vtable.boxed` says; `this`
        // is never dropped, so it is read out exactly once.
        Ok(unsafe {
            if this.vtable.boxed {
                *ptr::read(at.cast::<Box<T>>())
            } else {
                ptr::read(at.cast::<T>())
            }
        })
    }
}

impl Drop for Payload {
    fn drop(&mut self) {
        // SAFETY: `buf` holds the value `vtable` was made for, dropped once.
        unsafe { (self.vtable.drop)(&mut self.buf) }
    }
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Payload")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct Counted(Arc<AtomicUsize>, [u64; 2]);
    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct Big(Arc<AtomicUsize>, [u8; 256]);
    impl Drop for Big {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[repr(align(64))]
    struct Aligned(u8);

    #[test]
    fn small_values_are_inline_and_large_or_overaligned_ones_boxed() {
        assert!(!Payload::new(7u64).vtable.boxed);
        assert!(!Payload::new([0u8; INLINE]).vtable.boxed);
        assert!(Payload::new([0u8; INLINE + 1]).vtable.boxed);
        assert!(Payload::new(Aligned(1)).vtable.boxed);
        assert!(!Payload::new(()).vtable.boxed);
        assert_eq!(size_of::<Payload>(), INLINE + 16);
    }

    #[test]
    fn take_moves_the_value_out_once_and_refuses_another_type() {
        let drops = Arc::new(AtomicUsize::new(0));
        let p = Payload::new(Counted(Arc::clone(&drops), [1, 2]));
        let p = p.take::<u32>().unwrap_err();
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        let value = p.take::<Counted>().ok().unwrap();
        assert_eq!(value.1, [1, 2]);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        drop(value);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        let drops = Arc::new(AtomicUsize::new(0));
        let big = Payload::new(Big(Arc::clone(&drops), [9; 256]))
            .take::<Big>()
            .ok()
            .unwrap();
        assert_eq!(big.1[255], 9);
        drop(big);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        let aligned = Payload::new(Aligned(5)).take::<Aligned>().ok().unwrap();
        assert_eq!(aligned.0, 5);
    }

    #[test]
    fn dropping_a_payload_drops_its_value_once_either_way() {
        let drops = Arc::new(AtomicUsize::new(0));
        drop(Payload::new(Counted(Arc::clone(&drops), [0; 2])));
        drop(Payload::new(Big(Arc::clone(&drops), [0; 256])));
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn references_reach_the_value_inline_and_boxed() {
        let small = Payload::new(String::from("ab"));
        assert_eq!(
            small.downcast_ref::<String>().map(String::as_str),
            Some("ab")
        );
        let mut array = [1u64; 32];
        array[31] = 2;
        let large = Payload::new(array);
        assert_eq!(large.downcast_ref::<[u64; 32]>().unwrap()[31], 2);
        assert!(large.downcast_ref::<u64>().is_none());
    }

    #[test]
    fn a_payload_crosses_threads_and_moves_keep_it_valid() {
        let payloads: Vec<Payload> = (0..64).map(|i| Payload::new(vec![i; 3])).collect();
        let back = std::thread::spawn(move || {
            payloads
                .into_iter()
                .map(|p| p.take::<Vec<i32>>().ok().unwrap())
                .collect::<Vec<_>>()
        })
        .join()
        .unwrap();
        assert_eq!(back[63], [63, 63, 63]);
        // Not `Sync`, so a `!Sync` value cannot be shared through one.
        fn send<T: Send>() {}
        send::<Payload>();
    }
}
