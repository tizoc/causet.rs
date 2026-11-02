//! Pool-backed record payloads and JSON emission.
//!
//! This crate keeps structured record payloads allocation-light by storing
//! rendered strings and raw JSON fragments in a pooled byte buffer. `with_field`
//! writes into the pool at call time, and fields store `(start, len)` offsets so
//! the payload owns its data without extra `String` allocations.
//!
//! The `display()` and `json()` adapters format values directly into the pool:
//! - `display(&T)` uses `fmt::Display` and stores a string field.
//! - `json(&T)` uses `serde::Serialize` and stores a raw JSON fragment.
//!
//! Serialization uses a custom JSON writer that can emit raw JSON fragments
//! without quoting. Serde `Serialize` is still implemented for compatibility,
//! but raw fragments are represented as strings in that path.
//!
//! Note: `Deserialize` for `Payload` goes through `serde_json::Value`
//! and is allocation-heavy; it is intended for compatibility/testing, not hot paths.

use std::borrow::Cow;
use std::cell::UnsafeCell;
use std::fmt;
use std::fmt::Write as _;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smallvec::SmallVec;

const DEFAULT_POOL_CAPACITY: usize = 4 * 1024;

/// Fired when a pool is exhausted and falls back to an owned payload.
#[derive(Clone, Copy, Debug, Default)]
pub struct PoolExhausted {
    /// Slot limit of the pool that ran out.
    pub max: usize,
    /// Slots in use by the pool when the fallback happened.
    pub total: usize,
    /// Initial byte capacity of the owned payload handed out instead.
    pub buf_capacity: usize,
    /// Fallbacks to an owned payload so far, including this one.
    pub fallbacks: usize,
}

/// Callback run on every fallback to an owned payload.
pub type PoolExhaustedHook = Arc<dyn Fn(PoolExhausted) + Send + Sync + 'static>;

/// Configuration for pooled record buffers.
#[derive(Clone)]
pub struct PoolConfig {
    /// Slots put on the free list when the pool is created.
    pub initial: usize,
    /// Most slots the pool hands out; raised to `initial` if lower.
    pub max: usize,
    /// Initial byte capacity of each slot's pool buffer.
    pub buf_capacity: usize,
    /// Called each time a payload falls back to an owned buffer.
    pub on_exhausted: Option<PoolExhaustedHook>,
}

impl fmt::Debug for PoolConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolConfig")
            .field("initial", &self.initial)
            .field("max", &self.max)
            .field("buf_capacity", &self.buf_capacity)
            .field("on_exhausted", &self.on_exhausted.is_some())
            .finish()
    }
}

/// Snapshot of a pool's counters, for tests.
#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub struct PoolStats {
    /// Slots made available so far: `initial` plus one per grow.
    pub total: usize,
    /// Slots on the free list.
    pub free: usize,
    /// Times a slot beyond `initial` was taken into use.
    pub grows: usize,
    /// Payloads that fell back to an owned buffer.
    pub fallbacks: usize,
}

#[derive(Debug)]
struct PoolState {
    free: Vec<usize>,
    total: usize,
    grows: usize,
    fallbacks: usize,
}

struct PoolInner {
    state: Mutex<PoolState>,
    slots: Vec<UnsafeCell<PayloadInner>>,
    refs: Vec<AtomicUsize>,
    max: usize,
    buf_capacity: usize,
    on_exhausted: Option<PoolExhaustedHook>,
}

// Safety: slots are handed out by index, and the slot vector is never
// reallocated after initialization. Slots may be shared across clones; callers
// must treat pooled payloads as immutable after cloning.
unsafe impl Send for PoolInner {}
unsafe impl Sync for PoolInner {}

/// Pool of reusable buffers for record payloads.
#[derive(Clone)]
pub struct PoolSet {
    inner: Arc<PoolInner>,
}

impl PoolSet {
    /// Creates a pool and allocates all `max` slots up front.
    pub fn new(config: PoolConfig) -> Self {
        let max = config.max.max(config.initial);
        let mut free = Vec::with_capacity(max);
        for idx in 0..config.initial {
            free.push(idx);
        }
        let mut slots = Vec::with_capacity(max);
        for _ in 0..max {
            slots.push(UnsafeCell::new(PayloadInner::with_capacity(
                config.buf_capacity,
            )));
        }
        let refs = (0..max).map(|_| AtomicUsize::new(0)).collect();
        let state = PoolState {
            free,
            total: config.initial,
            grows: 0,
            fallbacks: 0,
        };
        Self {
            inner: Arc::new(PoolInner {
                state: Mutex::new(state),
                slots,
                refs,
                max,
                buf_capacity: config.buf_capacity,
                on_exhausted: config.on_exhausted,
            }),
        }
    }

    /// Starts a payload in a free slot, or in an owned buffer when every slot is in use.
    pub fn payload(&self) -> PayloadBuilder {
        PayloadBuilder {
            payload: self.acquire(),
        }
    }

    /// Returns the pool's current counters.
    #[cfg(test)]
    pub fn stats(&self) -> PoolStats {
        let state = self.inner.state.lock().expect("pool lock");
        PoolStats {
            total: state.total,
            free: state.free.len(),
            grows: state.grows,
            fallbacks: state.fallbacks,
        }
    }

    fn acquire(&self) -> Payload {
        let mut state = self.inner.state.lock().expect("pool lock");
        if let Some(slot) = state.free.pop() {
            let counter = self
                .inner
                .refs
                .get(slot)
                .expect("pool ref slot out of bounds");
            counter.store(1, Ordering::Release);
            return Payload::pooled(Arc::clone(&self.inner), slot);
        }
        if state.total < self.inner.max {
            let slot = state.total;
            state.total = state.total.saturating_add(1);
            state.grows = state.grows.saturating_add(1);
            let counter = self
                .inner
                .refs
                .get(slot)
                .expect("pool ref slot out of bounds");
            counter.store(1, Ordering::Release);
            return Payload::pooled(Arc::clone(&self.inner), slot);
        }
        state.fallbacks = state.fallbacks.saturating_add(1);
        if let Some(hook) = &self.inner.on_exhausted {
            hook(PoolExhausted {
                max: self.inner.max,
                total: state.total,
                buf_capacity: self.inner.buf_capacity,
                fallbacks: state.fallbacks,
            });
        }
        Payload::owned_with_capacity(self.inner.buf_capacity)
    }
}

impl PoolInner {
    fn slot(&self, idx: usize) -> &PayloadInner {
        let slot = self.slots.get(idx).expect("pool slot out of bounds");
        // Safety: slot indices are only handed out once at a time.
        unsafe { &*slot.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn slot_mut(&self, idx: usize) -> &mut PayloadInner {
        let slot = self.slots.get(idx).expect("pool slot out of bounds");
        // Safety: slot indices are only handed out once at a time.
        unsafe { &mut *slot.get() }
    }

    fn reset_slot(&self, idx: usize) {
        let slot = self.slot_mut(idx);
        slot.reset();
    }
}

#[derive(Clone, Debug)]
struct PayloadInner {
    object: Object,
    pool: Vec<u8>,
}

impl PayloadInner {
    fn with_capacity(buf_capacity: usize) -> Self {
        Self {
            object: Object::default(),
            pool: Vec::with_capacity(buf_capacity),
        }
    }

    fn reset(&mut self) {
        self.object.clear();
        self.pool.clear();
    }
}

/// Note: cloning a pooled payload shares the underlying pool slot.
/// Treat pooled payloads as immutable after cloning.
pub struct Payload {
    inner: PayloadRepr,
}

/// Builder for `Payload`. Mutations are only allowed on the builder;
/// finished payloads should be treated as immutable.
pub struct PayloadBuilder {
    payload: Payload,
}

enum PayloadRepr {
    Pooled { pool: Arc<PoolInner>, slot: usize },
    Owned(Box<PayloadInner>),
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            PayloadRepr::Pooled { slot, .. } => f
                .debug_struct("Payload")
                .field("kind", &"Pooled")
                .field("slot", slot)
                .finish(),
            PayloadRepr::Owned(inner) => f
                .debug_struct("Payload")
                .field("kind", &"Owned")
                .field("pool_len", &inner.pool.len())
                .field("fields", &inner.object.fields.len())
                .field("sections", &inner.object.sections.len())
                .field("arrays", &inner.object.arrays.len())
                .finish(),
        }
    }
}

/// A JSON object, with its members grouped by kind.
///
/// Members are written in group order (fields, then sections, then
/// arrays) and in insertion order within each group.
#[derive(Clone, Debug, Default)]
pub struct Object {
    /// Scalar and string members.
    pub fields: SmallVec<[Field; 8]>,
    /// Members whose value is a nested object.
    pub sections: SmallVec<[Section; 4]>,
    /// Members whose value is an array.
    pub arrays: SmallVec<[Array; 2]>,
}

impl Object {
    fn clear(&mut self) {
        self.fields.clear();
        self.sections.clear();
        self.arrays.clear();
    }
}

/// An object member whose value is a nested object.
#[derive(Clone, Debug)]
pub struct Section {
    /// Member key.
    pub name: Cow<'static, str>,
    /// Nested object.
    pub object: Box<Object>,
}

/// A JSON array, named by its member key.
#[derive(Clone, Debug)]
pub struct Array {
    /// Member key; empty for an array nested directly in another array.
    pub name: Cow<'static, str>,
    /// Elements in order.
    pub items: SmallVec<[ArrayItem; 8]>,
}

/// One element of an `Array`.
#[derive(Clone, Debug)]
pub enum ArrayItem {
    /// A nested object.
    Object(Box<Object>),
    /// A nested array.
    Array(Box<Array>),
    /// A scalar, string or raw JSON value.
    Value(Value),
}

/// An object member holding a `Value`.
#[derive(Clone, Debug)]
pub struct Field {
    /// Member key.
    pub key: Cow<'static, str>,
    /// Member value.
    pub value: Value,
}

/// A leaf value. String data lives in the owning payload's pool.
#[derive(Clone, Debug)]
pub enum Value {
    /// JSON `null`.
    Null,
    /// JSON boolean.
    Bool(bool),
    /// Unsigned integer.
    U64(u64),
    /// Signed integer.
    I64(i64),
    /// Floating-point number; written as `null` when not finite.
    F64(f64),
    /// UTF-8 text in the pool, written as a quoted JSON string.
    PoolStr {
        /// Byte offset in the pool.
        start: u32,
        /// Length in bytes.
        len: u32,
    },
    /// Serialized JSON in the pool, written as is.
    RawJson {
        /// Byte offset in the pool.
        start: u32,
        /// Length in bytes.
        len: u32,
    },
}

/// A byte range in a payload's pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Slice {
    /// Byte offset of the first byte.
    pub start: u32,
    /// Length in bytes.
    pub len: u32,
}

impl Object {
    /// Returns the value of the first field with this key.
    pub fn field_value(&self, key: &str) -> Option<&Value> {
        self.fields
            .iter()
            .find(|field| field.key.as_ref() == key)
            .map(|field| &field.value)
    }

    /// Returns the object of the first section with this name.
    pub fn section(&self, name: &str) -> Option<&Object> {
        self.sections
            .iter()
            .find(|section| section.name.as_ref() == name)
            .map(|section| section.object.as_ref())
    }

    /// Returns the first array with this name.
    pub fn array(&self, name: &str) -> Option<&Array> {
        self.arrays.iter().find(|array| array.name.as_ref() == name)
    }
}

impl Value {
    /// Returns the value as a number: booleans as 0 or 1, integers converted, strings and raw JSON as `None`.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Null => None,
            Value::Bool(value) => Some(f64::from(*value)),
            Value::U64(value) => Some(*value as f64),
            Value::I64(value) => Some(*value as f64),
            Value::F64(value) => Some(*value),
            Value::PoolStr { .. } => None,
            Value::RawJson { .. } => None,
        }
    }
}

/// A value that can be stored in a payload.
pub trait FieldValue {
    /// Converts `self` into a `Value`, writing any text or JSON into `pool`.
    fn to_value(self, pool: &mut Vec<u8>) -> Value;
}

/// Adapter returned by `display`.
pub struct DisplayValue<'a, T: fmt::Display + ?Sized> {
    value: &'a T,
}

/// Adapter returned by `json`.
pub struct JsonValue<'a, T: Serialize + ?Sized> {
    value: &'a T,
}

/// Stores `value` as a string formatted with `fmt::Display`.
///
/// Formatting writes straight into the pool. If it fails, the field is `Null`.
pub fn display<T: fmt::Display + ?Sized>(value: &T) -> DisplayValue<'_, T> {
    DisplayValue { value }
}

/// Stores `value` as raw JSON serialized with `serde`.
///
/// Serialization writes straight into the pool. If it fails, the field is
/// the string `serialize_failed`.
pub fn json<T: Serialize + ?Sized>(value: &T) -> JsonValue<'_, T> {
    JsonValue { value }
}

impl Default for Payload {
    fn default() -> Self {
        Self::owned()
    }
}

impl Payload {
    /// Creates an empty payload outside any pool, with a pool buffer of `buf_capacity` bytes.
    pub fn owned_with_capacity(buf_capacity: usize) -> Self {
        Self {
            inner: PayloadRepr::Owned(Box::new(PayloadInner::with_capacity(buf_capacity))),
        }
    }

    /// Creates an empty payload outside any pool, with the default buffer capacity.
    pub fn owned() -> Self {
        Self::owned_with_capacity(DEFAULT_POOL_CAPACITY)
    }

    fn pooled(pool: Arc<PoolInner>, slot: usize) -> Self {
        Self {
            inner: PayloadRepr::Pooled { pool, slot },
        }
    }

    fn inner(&self) -> &PayloadInner {
        match &self.inner {
            PayloadRepr::Pooled { pool, slot } => pool.slot(*slot),
            PayloadRepr::Owned(inner) => inner,
        }
    }

    fn inner_mut(&mut self) -> &mut PayloadInner {
        match &mut self.inner {
            PayloadRepr::Pooled { pool, slot } => pool.slot_mut(*slot),
            PayloadRepr::Owned(inner) => inner,
        }
    }

    /// Returns the top-level object.
    pub fn object(&self) -> &Object {
        &self.inner().object
    }

    fn object_mut(&mut self) -> &mut Object {
        &mut self.inner_mut().object
    }

    fn pool(&self) -> &[u8] {
        &self.inner().pool
    }

    fn pool_mut(&mut self) -> &mut Vec<u8> {
        &mut self.inner_mut().pool
    }

    fn with_field(mut self, key: impl Into<Cow<'static, str>>, value: impl FieldValue) -> Self {
        let value = value.to_value(self.pool_mut());
        self.object_mut().fields.push(Field {
            key: key.into(),
            value,
        });
        self
    }

    fn push_field(&mut self, key: impl Into<Cow<'static, str>>, value: impl FieldValue) {
        let value = value.to_value(self.pool_mut());
        self.object_mut().fields.push(Field {
            key: key.into(),
            value,
        });
    }

    fn store_str(&mut self, value: &str) -> Option<Slice> {
        let pool = self.pool_mut();
        let (start, len) = pool_write_bytes(pool, value.as_bytes())?;
        Some(Slice { start, len })
    }

    fn store_fmt(&mut self, args: fmt::Arguments<'_>) -> Option<Slice> {
        let pool = self.pool_mut();
        let start = pool.len();
        {
            let mut writer = PoolWriter { buf: pool };
            writer.write_fmt(args).ok()?;
        }
        let len = pool.len().saturating_sub(start);
        let start = u32::try_from(start).ok()?;
        let len = u32::try_from(len).ok()?;
        Some(Slice { start, len })
    }

    fn with_section(
        mut self,
        name: impl Into<Cow<'static, str>>,
        builder: impl FnOnce(ObjectBuilder<'_>) -> ObjectBuilder<'_>,
    ) -> Self {
        let object = builder(ObjectBuilder::new(self.pool_mut())).finish();
        self.object_mut().sections.push(Section {
            name: name.into(),
            object: Box::new(object),
        });
        self
    }

    fn with_array(
        mut self,
        name: impl Into<Cow<'static, str>>,
        builder: impl FnOnce(&mut ArrayBuilder<'_>),
    ) -> Self {
        let array = {
            let mut array = ArrayBuilder::new(name.into(), self.pool_mut());
            builder(&mut array);
            array.finish()
        };
        self.object_mut().arrays.push(array);
        self
    }

    /// Returns the value of the first top-level field with this key.
    pub fn field_value(&self, key: &str) -> Option<&Value> {
        self.object().field_value(key)
    }

    /// Returns the top-level fields.
    pub fn fields(&self) -> &[Field] {
        &self.object().fields
    }

    /// Returns the top-level sections.
    pub fn sections(&self) -> &[Section] {
        &self.object().sections
    }

    /// Returns the top-level arrays.
    pub fn arrays(&self) -> &[Array] {
        &self.object().arrays
    }

    /// Returns the object of the first top-level section with this name.
    pub fn section(&self, name: &str) -> Option<&Object> {
        self.object().section(name)
    }

    /// Returns the first top-level array with this name.
    pub fn array(&self, name: &str) -> Option<&Array> {
        self.object().array(name)
    }

    /// Returns a value of this payload as text; `None` for `Null` and raw JSON.
    pub fn value_as_string(&self, value: &Value) -> Option<Cow<'_, str>> {
        match value {
            Value::Null => None,
            Value::Bool(value) => Some(Cow::Owned(value.to_string())),
            Value::U64(value) => Some(Cow::Owned(value.to_string())),
            Value::I64(value) => Some(Cow::Owned(value.to_string())),
            Value::F64(value) => Some(Cow::Owned(value.to_string())),
            Value::PoolStr { start, len } => self.pool_str(*start, *len).map(Cow::Borrowed),
            Value::RawJson { .. } => None,
        }
    }

    /// Returns pool bytes `start..start + len` as UTF-8, or `None` if out of range or invalid.
    pub fn pool_str(&self, start: u32, len: u32) -> Option<&str> {
        let start = start as usize;
        let len = len as usize;
        let end = start.checked_add(len)?;
        let slice = self.pool().get(start..end)?;
        std::str::from_utf8(slice).ok()
    }

    /// Returns pool bytes `start..start + len`, or `None` if out of range.
    pub fn pool_slice(&self, start: u32, len: u32) -> Option<&[u8]> {
        let start = start as usize;
        let len = len as usize;
        let end = start.checked_add(len)?;
        self.pool().get(start..end)
    }
}

impl Drop for Payload {
    fn drop(&mut self) {
        let PayloadRepr::Pooled { pool, slot } = &self.inner else {
            return;
        };
        let counter = pool.refs.get(*slot).expect("pool ref slot out of bounds");
        let remaining = counter.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(remaining > 0, "pool slot refcount underflow");
        if remaining == 1 {
            pool.reset_slot(*slot);
            let mut state = pool.state.lock().expect("pool lock");
            state.free.push(*slot);
        }
    }
}

impl Clone for Payload {
    fn clone(&self) -> Self {
        // TODO: This refcounting exists because actions are clonable today.
        // Once actions become non-clonable, we can remove slot refcounts and
        // avoid pooled sharing on clone.
        match &self.inner {
            PayloadRepr::Pooled { pool, slot } => {
                let counter = pool.refs.get(*slot).expect("pool ref slot out of bounds");
                counter.fetch_add(1, Ordering::AcqRel);
                Payload::pooled(Arc::clone(pool), *slot)
            }
            PayloadRepr::Owned(inner) => Payload {
                inner: PayloadRepr::Owned(Box::new((**inner).clone())),
            },
        }
    }
}

/// Builds a nested object, writing its strings into the enclosing payload's pool.
pub struct ObjectBuilder<'a> {
    object: Object,
    pool: &'a mut Vec<u8>,
}

impl<'a> ObjectBuilder<'a> {
    fn new(pool: &'a mut Vec<u8>) -> Self {
        Self {
            object: Object::default(),
            pool,
        }
    }

    /// Adds a field.
    pub fn with_field(mut self, key: impl Into<Cow<'static, str>>, value: impl FieldValue) -> Self {
        let value = value.to_value(self.pool);
        self.object.fields.push(Field {
            key: key.into(),
            value,
        });
        self
    }

    /// Adds a field when `value` is `Some`.
    pub fn with_field_opt<V>(self, key: impl Into<Cow<'static, str>>, value: Option<V>) -> Self
    where
        V: FieldValue,
    {
        match value {
            Some(value) => self.with_field(key, value),
            None => self,
        }
    }

    /// Adds a section built by `builder`.
    pub fn with_section(
        mut self,
        name: impl Into<Cow<'static, str>>,
        builder: impl FnOnce(ObjectBuilder<'_>) -> ObjectBuilder<'_>,
    ) -> Self {
        let object = builder(ObjectBuilder::new(self.pool)).finish();
        self.object.sections.push(Section {
            name: name.into(),
            object: Box::new(object),
        });
        self
    }

    /// Adds a section with one field per `(key, value)` entry.
    pub fn with_section_fields<K, V, I>(self, name: impl Into<Cow<'static, str>>, fields: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<Cow<'static, str>>,
        V: FieldValue,
    {
        self.with_section(name, |mut section| {
            for (key, value) in fields {
                section = section.with_field(key, value);
            }
            section
        })
    }

    /// Adds a section with one nested section per `(key, item)` entry, each built by `builder`.
    pub fn with_section_objects<K, T, I, F>(
        self,
        name: impl Into<Cow<'static, str>>,
        entries: I,
        mut builder: F,
    ) -> Self
    where
        I: IntoIterator<Item = (K, T)>,
        K: Into<Cow<'static, str>>,
        F: for<'b> FnMut(ObjectBuilder<'b>, T) -> ObjectBuilder<'b>,
    {
        self.with_section(name, |mut section| {
            for (key, value) in entries {
                section = section.with_section(key, |object| builder(object, value));
            }
            section
        })
    }

    /// Adds an array filled by `builder`.
    pub fn with_array(
        mut self,
        name: impl Into<Cow<'static, str>>,
        builder: impl FnOnce(&mut ArrayBuilder<'_>),
    ) -> Self {
        let mut array = ArrayBuilder::new(name.into(), self.pool);
        builder(&mut array);
        self.object.arrays.push(array.finish());
        self
    }

    /// Adds an array of `values`.
    pub fn with_array_values<V, I>(self, name: impl Into<Cow<'static, str>>, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        self.with_array(name, |array| {
            for value in values {
                array.push_value(value);
            }
        })
    }

    /// Adds an array with one object per item, each built by `builder`.
    pub fn with_array_objects<T, I, F>(
        self,
        name: impl Into<Cow<'static, str>>,
        items: I,
        mut builder: F,
    ) -> Self
    where
        I: IntoIterator<Item = T>,
        F: for<'b> FnMut(ObjectBuilder<'b>, T) -> ObjectBuilder<'b>,
    {
        self.with_array(name, |array| {
            for item in items {
                array.push_object(|object| builder(object, item));
            }
        })
    }

    fn finish(self) -> Object {
        self.object
    }
}

/// Builds an array, writing its strings into the enclosing payload's pool.
pub struct ArrayBuilder<'a> {
    array: Array,
    pool: &'a mut Vec<u8>,
}

impl<'a> ArrayBuilder<'a> {
    fn new(name: Cow<'static, str>, pool: &'a mut Vec<u8>) -> Self {
        Self {
            array: Array {
                name,
                items: SmallVec::new(),
            },
            pool,
        }
    }

    /// Appends an object built by `builder`.
    pub fn push_object(&mut self, builder: impl FnOnce(ObjectBuilder<'_>) -> ObjectBuilder<'_>) {
        let object = builder(ObjectBuilder::new(self.pool)).finish();
        self.array.items.push(ArrayItem::Object(Box::new(object)));
    }

    /// Appends a nested array filled by `builder`.
    pub fn push_array(&mut self, builder: impl FnOnce(&mut ArrayBuilder<'_>)) {
        let mut array = ArrayBuilder::new(Cow::Borrowed(""), self.pool);
        builder(&mut array);
        self.array
            .items
            .push(ArrayItem::Array(Box::new(array.finish())));
    }

    /// Appends a value.
    pub fn push_value(&mut self, value: impl FieldValue) {
        let value = value.to_value(self.pool);
        self.array.items.push(ArrayItem::Value(value));
    }

    fn finish(self) -> Array {
        self.array
    }
}

struct PoolWriter<'a> {
    buf: &'a mut Vec<u8>,
}

impl<'a> fmt::Write for PoolWriter<'a> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.buf.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

impl<'a> io::Write for PoolWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn pool_write_bytes(pool: &mut Vec<u8>, bytes: &[u8]) -> Option<(u32, u32)> {
    let start = pool.len();
    pool.extend_from_slice(bytes);
    let len = pool.len().saturating_sub(start);
    let start = u32::try_from(start).ok()?;
    let len = u32::try_from(len).ok()?;
    Some((start, len))
}

fn pool_write_str(pool: &mut Vec<u8>, value: &str) -> Option<(u32, u32)> {
    pool_write_bytes(pool, value.as_bytes())
}

fn pool_value(start: usize, len: usize, raw_json: bool) -> Value {
    let start = u32::try_from(start).ok();
    let len = u32::try_from(len).ok();
    match (start, len) {
        (Some(start), Some(len)) => {
            if raw_json {
                Value::RawJson { start, len }
            } else {
                Value::PoolStr { start, len }
            }
        }
        _ => Value::Null,
    }
}

impl FieldValue for Value {
    fn to_value(self, _pool: &mut Vec<u8>) -> Value {
        self
    }
}

impl FieldValue for bool {
    fn to_value(self, _pool: &mut Vec<u8>) -> Value {
        Value::Bool(self)
    }
}

macro_rules! impl_field_value_num {
    ($ty:ty, $variant:ident) => {
        impl FieldValue for $ty {
            fn to_value(self, _pool: &mut Vec<u8>) -> Value {
                Value::$variant(self as _)
            }
        }
    };
}

impl_field_value_num!(u8, U64);
impl_field_value_num!(u16, U64);
impl_field_value_num!(u32, U64);
impl_field_value_num!(u64, U64);
impl_field_value_num!(usize, U64);
impl_field_value_num!(i8, I64);
impl_field_value_num!(i16, I64);
impl_field_value_num!(i32, I64);
impl_field_value_num!(i64, I64);

impl FieldValue for f32 {
    fn to_value(self, _pool: &mut Vec<u8>) -> Value {
        Value::F64(self as f64)
    }
}

impl FieldValue for f64 {
    fn to_value(self, _pool: &mut Vec<u8>) -> Value {
        Value::F64(self)
    }
}

impl FieldValue for &str {
    fn to_value(self, pool: &mut Vec<u8>) -> Value {
        match pool_write_str(pool, self) {
            Some((start, len)) => Value::PoolStr { start, len },
            None => Value::Null,
        }
    }
}

impl FieldValue for String {
    fn to_value(self, pool: &mut Vec<u8>) -> Value {
        self.as_str().to_value(pool)
    }
}

impl FieldValue for Cow<'static, str> {
    fn to_value(self, pool: &mut Vec<u8>) -> Value {
        self.as_ref().to_value(pool)
    }
}

impl<'a, T: fmt::Display + ?Sized> FieldValue for DisplayValue<'a, T> {
    fn to_value(self, pool: &mut Vec<u8>) -> Value {
        let start = pool.len();
        let mut writer = PoolWriter { buf: pool };
        if fmt::write(&mut writer, format_args!("{}", self.value)).is_err() {
            writer.buf.truncate(start);
            return Value::Null;
        }
        let len = writer.buf.len().saturating_sub(start);
        pool_value(start, len, false)
    }
}

impl<'a, T: Serialize + ?Sized> FieldValue for JsonValue<'a, T> {
    fn to_value(self, pool: &mut Vec<u8>) -> Value {
        let start = pool.len();
        let mut writer = PoolWriter { buf: pool };
        let mut raw_json = true;
        if serde_json::to_writer(&mut writer, self.value).is_err() {
            writer.buf.truncate(start);
            writer.buf.extend_from_slice(b"serialize_failed");
            raw_json = false;
        }
        let len = writer.buf.len().saturating_sub(start);
        pool_value(start, len, raw_json)
    }
}

impl<'de> Deserialize<'de> for Payload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let obj = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("payload must be a JSON object"))?;

        let mut payload = Payload::owned();

        for (key, value) in obj {
            match value {
                serde_json::Value::Null => {
                    payload.object_mut().fields.push(Field {
                        key: Cow::Owned(key.clone()),
                        value: Value::Null,
                    });
                }
                serde_json::Value::Bool(value) => {
                    payload.object_mut().fields.push(Field {
                        key: Cow::Owned(key.clone()),
                        value: Value::Bool(*value),
                    });
                }
                serde_json::Value::Number(number) => {
                    let value = if let Some(value) = number.as_i64() {
                        Value::I64(value)
                    } else if let Some(value) = number.as_u64() {
                        Value::U64(value)
                    } else if let Some(value) = number.as_f64() {
                        Value::F64(value)
                    } else {
                        Value::Null
                    };
                    payload.object_mut().fields.push(Field {
                        key: Cow::Owned(key.clone()),
                        value,
                    });
                }
                serde_json::Value::String(value) => {
                    let value = value.as_str().to_value(payload.pool_mut());
                    payload.object_mut().fields.push(Field {
                        key: Cow::Owned(key.clone()),
                        value,
                    });
                }
                serde_json::Value::Object(map) => {
                    let object = {
                        let pool = payload.pool_mut();
                        parse_object(map, pool)
                    };
                    payload.object_mut().sections.push(Section {
                        name: Cow::Owned(key.clone()),
                        object: Box::new(object),
                    });
                }
                serde_json::Value::Array(items) => {
                    let array = {
                        let pool = payload.pool_mut();
                        parse_array(key, items, pool)
                    };
                    payload.object_mut().arrays.push(array);
                }
            }
        }

        Ok(payload)
    }
}

fn parse_object(map: &serde_json::Map<String, serde_json::Value>, pool: &mut Vec<u8>) -> Object {
    let mut object = Object::default();
    for (key, value) in map {
        match value {
            serde_json::Value::Null => {
                object.fields.push(Field {
                    key: Cow::Owned(key.clone()),
                    value: Value::Null,
                });
            }
            serde_json::Value::Bool(value) => {
                object.fields.push(Field {
                    key: Cow::Owned(key.clone()),
                    value: Value::Bool(*value),
                });
            }
            serde_json::Value::Number(number) => {
                let value = if let Some(value) = number.as_i64() {
                    Value::I64(value)
                } else if let Some(value) = number.as_u64() {
                    Value::U64(value)
                } else if let Some(value) = number.as_f64() {
                    Value::F64(value)
                } else {
                    Value::Null
                };
                object.fields.push(Field {
                    key: Cow::Owned(key.clone()),
                    value,
                });
            }
            serde_json::Value::String(value) => {
                let value = value.as_str().to_value(pool);
                object.fields.push(Field {
                    key: Cow::Owned(key.clone()),
                    value,
                });
            }
            serde_json::Value::Object(map) => {
                object.sections.push(Section {
                    name: Cow::Owned(key.clone()),
                    object: Box::new(parse_object(map, pool)),
                });
            }
            serde_json::Value::Array(items) => {
                object.arrays.push(parse_array(key, items, pool));
            }
        }
    }
    object
}

fn parse_array(name: &str, items: &[serde_json::Value], pool: &mut Vec<u8>) -> Array {
    let mut array = Array {
        name: Cow::Owned(name.to_owned()),
        items: SmallVec::new(),
    };
    for item in items {
        match item {
            serde_json::Value::Object(map) => {
                array
                    .items
                    .push(ArrayItem::Object(Box::new(parse_object(map, pool))));
            }
            serde_json::Value::Null => {
                array.items.push(ArrayItem::Value(Value::Null));
            }
            serde_json::Value::Bool(value) => {
                array.items.push(ArrayItem::Value(Value::Bool(*value)));
            }
            serde_json::Value::Number(number) => {
                let value = if let Some(value) = number.as_i64() {
                    Value::I64(value)
                } else if let Some(value) = number.as_u64() {
                    Value::U64(value)
                } else if let Some(value) = number.as_f64() {
                    Value::F64(value)
                } else {
                    Value::Null
                };
                array.items.push(ArrayItem::Value(value));
            }
            serde_json::Value::String(value) => {
                array
                    .items
                    .push(ArrayItem::Value(value.as_str().to_value(pool)));
            }
            serde_json::Value::Array(items) => {
                array
                    .items
                    .push(ArrayItem::Array(Box::new(parse_array("", items, pool))));
            }
        }
    }
    array
}

struct ValueRef<'a> {
    payload: &'a Payload,
    value: &'a Value,
}

struct ObjectRef<'a> {
    payload: &'a Payload,
    object: &'a Object,
}

struct ArrayRef<'a> {
    payload: &'a Payload,
    array: &'a Array,
}

impl<'a> Serialize for ValueRef<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.value {
            Value::Null => serializer.serialize_none(),
            Value::Bool(value) => serializer.serialize_bool(*value),
            Value::U64(value) => serializer.serialize_u64(*value),
            Value::I64(value) => serializer.serialize_i64(*value),
            Value::F64(value) => {
                if value.is_finite() {
                    serializer.serialize_f64(*value)
                } else {
                    serializer.serialize_none()
                }
            }
            Value::PoolStr { start, len } => {
                let value = self.payload.pool_str(*start, *len);
                match value {
                    Some(value) => serializer.serialize_str(value),
                    None => serializer.serialize_none(),
                }
            }
            Value::RawJson { start, len } => {
                let slice = self.payload.pool_slice(*start, *len);
                let Some(slice) = slice else {
                    return serializer.serialize_none();
                };
                let Ok(value) = serde_json::from_slice::<serde_json::Value>(slice) else {
                    return serializer.serialize_none();
                };
                value.serialize(serializer)
            }
        }
    }
}

impl<'a> Serialize for ObjectRef<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let len = object_map_len(self.object);
        let mut map = serializer.serialize_map(Some(len))?;
        serialize_object_into_map(self.payload, self.object, &mut map)?;
        map.end()
    }
}

impl<'a> Serialize for ArrayRef<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.array.items.len()))?;
        for item in &self.array.items {
            match item {
                ArrayItem::Object(object) => {
                    seq.serialize_element(&ObjectRef {
                        payload: self.payload,
                        object: object.as_ref(),
                    })?;
                }
                ArrayItem::Array(array) => {
                    seq.serialize_element(&ArrayRef {
                        payload: self.payload,
                        array: array.as_ref(),
                    })?;
                }
                ArrayItem::Value(value) => {
                    seq.serialize_element(&ValueRef {
                        payload: self.payload,
                        value,
                    })?;
                }
            }
        }
        seq.end()
    }
}

fn serialize_object_into_map<S: SerializeMap>(
    payload: &Payload,
    object: &Object,
    map: &mut S,
) -> Result<(), S::Error> {
    for field in &object.fields {
        map.serialize_entry(
            field.key.as_ref(),
            &ValueRef {
                payload,
                value: &field.value,
            },
        )?;
    }
    for section in &object.sections {
        map.serialize_entry(
            section.name.as_ref(),
            &ObjectRef {
                payload,
                object: section.object.as_ref(),
            },
        )?;
    }
    for array in &object.arrays {
        map.serialize_entry(array.name.as_ref(), &ArrayRef { payload, array })?;
    }
    Ok(())
}

fn object_map_len(object: &Object) -> usize {
    object
        .fields
        .len()
        .saturating_add(object.sections.len())
        .saturating_add(object.arrays.len())
}

impl Serialize for Payload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let object = self.object();
        let len = object_map_len(object);
        let mut map = serializer.serialize_map(Some(len))?;
        serialize_object_into_map(self, object, &mut map)?;
        map.end()
    }
}

impl Payload {
    /// Writes the payload to `out` as one compact JSON object, with raw JSON values copied as is.
    pub fn write_json<W: io::Write>(&self, out: &mut W) -> io::Result<()> {
        write_payload(out, self)
    }
}

impl PayloadBuilder {
    /// Adds a top-level field.
    pub fn with_field(mut self, key: impl Into<Cow<'static, str>>, value: impl FieldValue) -> Self {
        self.payload = self.payload.with_field(key, value);
        self
    }

    /// Adds a top-level field when `value` is `Some`.
    pub fn with_field_opt<V>(self, key: impl Into<Cow<'static, str>>, value: Option<V>) -> Self
    where
        V: FieldValue,
    {
        match value {
            Some(value) => self.with_field(key, value),
            None => self,
        }
    }

    /// Adds a top-level field in place.
    pub fn push_field(&mut self, key: impl Into<Cow<'static, str>>, value: impl FieldValue) {
        self.payload.push_field(key, value);
    }

    /// Copies `value` into the pool without adding a field, and returns where it is.
    ///
    /// Returns `None` if the range does not fit in `u32`.
    pub fn store_str(&mut self, value: &str) -> Option<Slice> {
        self.payload.store_str(value)
    }

    /// Formats `args` into the pool without adding a field, and returns where it is.
    ///
    /// Returns `None` if formatting fails or the range does not fit in `u32`.
    pub fn store_fmt(&mut self, args: fmt::Arguments<'_>) -> Option<Slice> {
        self.payload.store_fmt(args)
    }

    /// Adds a top-level section built by `builder`.
    pub fn with_section(
        mut self,
        name: impl Into<Cow<'static, str>>,
        builder: impl FnOnce(ObjectBuilder<'_>) -> ObjectBuilder<'_>,
    ) -> Self {
        self.payload = self.payload.with_section(name, builder);
        self
    }

    /// Adds a top-level array filled by `builder`.
    pub fn with_array(
        mut self,
        name: impl Into<Cow<'static, str>>,
        builder: impl FnOnce(&mut ArrayBuilder<'_>),
    ) -> Self {
        self.payload = self.payload.with_array(name, builder);
        self
    }

    /// Returns the finished payload.
    pub fn finish(self) -> Payload {
        self.payload
    }
}

fn write_payload<W: io::Write>(out: &mut W, payload: &Payload) -> io::Result<()> {
    out.write_all(b"{")?;
    let mut first = true;
    write_object_fields(out, payload, payload.object(), &mut first)?;
    out.write_all(b"}")
}

fn write_object_fields<W: io::Write>(
    out: &mut W,
    payload: &Payload,
    object: &Object,
    first: &mut bool,
) -> io::Result<()> {
    for field in &object.fields {
        write_key(out, field.key.as_ref(), first)?;
        write_value(out, payload, &field.value)?;
    }
    for section in &object.sections {
        write_key(out, section.name.as_ref(), first)?;
        write_object(out, payload, section.object.as_ref())?;
    }
    for array in &object.arrays {
        write_key(out, array.name.as_ref(), first)?;
        write_array(out, payload, array)?;
    }
    Ok(())
}

fn write_object<W: io::Write>(out: &mut W, payload: &Payload, object: &Object) -> io::Result<()> {
    out.write_all(b"{")?;
    let mut first = true;
    write_object_fields(out, payload, object, &mut first)?;
    out.write_all(b"}")
}

fn write_array<W: io::Write>(out: &mut W, payload: &Payload, array: &Array) -> io::Result<()> {
    out.write_all(b"[")?;
    let mut first = true;
    for item in &array.items {
        if !first {
            out.write_all(b",")?;
        }
        first = false;
        match item {
            ArrayItem::Object(object) => {
                write_object(out, payload, object.as_ref())?;
            }
            ArrayItem::Array(array) => {
                write_array(out, payload, array.as_ref())?;
            }
            ArrayItem::Value(value) => {
                write_value(out, payload, value)?;
            }
        }
    }
    out.write_all(b"]")
}

fn write_value<W: io::Write>(out: &mut W, payload: &Payload, value: &Value) -> io::Result<()> {
    match value {
        Value::Null => out.write_all(b"null"),
        Value::Bool(value) => {
            if *value {
                out.write_all(b"true")
            } else {
                out.write_all(b"false")
            }
        }
        Value::U64(value) => write_fmt(out, format_args!("{}", value)),
        Value::I64(value) => write_fmt(out, format_args!("{}", value)),
        Value::F64(value) => {
            if value.is_finite() {
                write_fmt(out, format_args!("{}", value))
            } else {
                out.write_all(b"null")
            }
        }
        Value::PoolStr { start, len } => {
            if let Some(value) = payload.pool_str(*start, *len) {
                write_str(out, value)
            } else {
                out.write_all(b"null")
            }
        }
        Value::RawJson { start, len } => {
            if let Some(slice) = payload.pool_slice(*start, *len) {
                out.write_all(slice)
            } else {
                out.write_all(b"null")
            }
        }
    }
}

fn write_key<W: io::Write>(out: &mut W, key: &str, first: &mut bool) -> io::Result<()> {
    if !*first {
        out.write_all(b",")?;
    }
    *first = false;
    write_str(out, key)?;
    out.write_all(b":")
}

fn write_str<W: io::Write>(out: &mut W, value: &str) -> io::Result<()> {
    out.write_all(b"\"")?;
    for &byte in value.as_bytes() {
        match byte {
            b'"' => out.write_all(b"\\\"")?,
            b'\\' => out.write_all(b"\\\\")?,
            b'\n' => out.write_all(b"\\n")?,
            b'\r' => out.write_all(b"\\r")?,
            b'\t' => out.write_all(b"\\t")?,
            0x00..=0x1F => {
                let hi = byte >> 4;
                let lo = byte & 0x0F;
                let hex = [
                    b'\\',
                    b'u',
                    b'0',
                    b'0',
                    nibble_to_hex(hi),
                    nibble_to_hex(lo),
                ];
                out.write_all(&hex)?;
            }
            _ => out.write_all(&[byte])?,
        }
    }
    out.write_all(b"\"")
}

fn nibble_to_hex(nibble: u8) -> u8 {
    match nibble {
        0..=9 => char::from_digit(u32::from(nibble), 16)
            .unwrap_or('0')
            .to_ascii_lowercase() as u8,
        10..=15 => char::from_digit(u32::from(nibble), 16)
            .unwrap_or('a')
            .to_ascii_lowercase() as u8,
        _ => b'0',
    }
}

fn write_fmt<W: io::Write>(out: &mut W, args: fmt::Arguments<'_>) -> io::Result<()> {
    struct FmtWriter<'a, W: io::Write> {
        out: &'a mut W,
    }

    impl<'a, W: io::Write> fmt::Write for FmtWriter<'a, W> {
        fn write_str(&mut self, s: &str) -> fmt::Result {
            self.out.write_all(s.as_bytes()).map_err(|_| fmt::Error)
        }
    }

    let mut writer = FmtWriter { out };
    fmt::write(&mut writer, args).map_err(|_| io::Error::other("formatting failed"))
}

#[cfg(test)]
mod tests;
#[cfg(test)]
// Test-only global allocator so we can assert allocation-free payload building.
#[global_allocator]
static ALLOC: tests::TrackingAlloc = tests::TrackingAlloc;

#[doc(hidden)]
#[macro_export]
macro_rules! __pool_record_key {
    ([$key:expr]) => {
        $key
    };
    ($key:ident) => {
        stringify!($key)
    };
    ($key:literal) => {
        $key
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __pool_record_object {
    ($builder:expr,) => {
        $builder
    };
    ($builder:expr) => {
        $builder
    };
    ($builder:expr, $key:tt : { $($inner:tt)* } , $($rest:tt)*) => {
        $crate::__pool_record_object!(
            $builder.with_section($crate::__pool_record_key!($key), |section| {
                let section = $crate::__pool_record_object!(section, $($inner)*);
                section
            }),
            $($rest)*
        )
    };
    ($builder:expr, $key:tt : { $($inner:tt)* }) => {
        $builder.with_section($crate::__pool_record_key!($key), |section| {
            let section = $crate::__pool_record_object!(section, $($inner)*);
            section
        })
    };
    ($builder:expr, $key:tt : [ $($inner:tt)* ] , $($rest:tt)*) => {
        $crate::__pool_record_object!(
            $builder.with_array($crate::__pool_record_key!($key), |array| {
                $crate::__pool_record_array_items!(array, $($inner)*);
            }),
            $($rest)*
        )
    };
    ($builder:expr, $key:tt : [ $($inner:tt)* ]) => {
        $builder.with_array($crate::__pool_record_key!($key), |array| {
            $crate::__pool_record_array_items!(array, $($inner)*);
        })
    };
    ($builder:expr, $key:ident ? , $($rest:tt)*) => {
        $crate::__pool_record_object!(
            $builder.with_field_opt($crate::__pool_record_key!($key), $key),
            $($rest)*
        )
    };
    ($builder:expr, $key:ident ?) => {
        $builder.with_field_opt($crate::__pool_record_key!($key), $key)
    };
    ($builder:expr, $key:ident , $($rest:tt)*) => {
        $crate::__pool_record_object!(
            $builder.with_field($crate::__pool_record_key!($key), $key),
            $($rest)*
        )
    };
    ($builder:expr, $key:ident) => {
        $builder.with_field($crate::__pool_record_key!($key), $key)
    };
    ($builder:expr, $key:tt ? : $value:expr , $($rest:tt)*) => {
        $crate::__pool_record_object!(
            $builder.with_field_opt($crate::__pool_record_key!($key), $value),
            $($rest)*
        )
    };
    ($builder:expr, $key:tt ? : $value:expr) => {
        $builder.with_field_opt($crate::__pool_record_key!($key), $value)
    };
    ($builder:expr, $key:tt : $value:expr , $($rest:tt)*) => {
        $crate::__pool_record_object!(
            $builder.with_field($crate::__pool_record_key!($key), $value),
            $($rest)*
        )
    };
    ($builder:expr, $key:tt : $value:expr) => {
        $builder.with_field($crate::__pool_record_key!($key), $value)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __pool_record_array_items {
    ($array:ident,) => {};
    ($array:ident) => {};
    ($array:ident, { $($inner:tt)* } , $($rest:tt)*) => {
        $array.push_object(|object| {
            $crate::__pool_record_object!(object, $($inner)*)
        });
        $crate::__pool_record_array_items!($array, $($rest)*);
    };
    ($array:ident, { $($inner:tt)* }) => {
        $array.push_object(|object| {
            $crate::__pool_record_object!(object, $($inner)*)
        });
    };
    ($array:ident, [ $($inner:tt)* ] , $($rest:tt)*) => {
        $array.push_array(|array| {
            $crate::__pool_record_array_items!(array, $($inner)*);
        });
        $crate::__pool_record_array_items!($array, $($rest)*);
    };
    ($array:ident, [ $($inner:tt)* ]) => {
        $array.push_array(|array| {
            $crate::__pool_record_array_items!(array, $($inner)*);
        });
    };
    ($array:ident, $value:expr , $($rest:tt)*) => {
        $array.push_value($value);
        $crate::__pool_record_array_items!($array, $($rest)*);
    };
    ($array:ident, $value:expr) => {
        $array.push_value($value);
    };
}

/// Apply a JSON-like object body to a payload/object builder.
///
/// Supported key forms:
/// - `ident: expr`
/// - `"literal": expr`
/// - `[expr]: expr` (dynamic key)
/// - `ident` shorthand for `ident: ident`
///
/// Optional field forms (skip field when value is `None`):
/// - `ident?: expr`
/// - `[expr]?: expr`
/// - `ident?` shorthand for `ident?: ident`
///
/// Nested values:
/// - section object: `key: { ... }`
/// - array: `key: [ ... ]`
#[macro_export]
macro_rules! pool_record_object {
    ($builder:expr, { $($tt:tt)* }) => {{
        $crate::__pool_record_object!($builder, $($tt)*)
    }};
}

/// Build and finish a pooled record payload with JSON-like syntax.
#[macro_export]
macro_rules! pool_record {
    ($pool:expr, { $($tt:tt)* }) => {{
        let __payload = ($pool).payload();
        let __payload = $crate::pool_record_object!(__payload, { $($tt)* });
        __payload.finish()
    }};
}
