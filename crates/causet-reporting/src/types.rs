//! Reports, the records the pipeline makes of them, their payloads and the
//! pipeline's counters.

use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

pub(crate) use causet_pooled_payload::{ArrayItem, Object, Slice, Value};
use causet_pooled_payload::{
    Payload as PoolPayload, PayloadBuilder as PoolPayloadBuilder, PoolConfig, PoolSet,
};
use serde::{Deserialize, Serialize};
#[cfg(target_pointer_width = "64")]
use static_assertions::const_assert;

/// What a report is; sampling rates and dedupe are set per kind.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum ReportKind {
    /// Something went wrong.
    Error,
    /// A measurement, whose numeric fields a metric policy can aggregate.
    Metric,
    /// Something that happened, which a metric policy can also aggregate.
    Event,
    /// The result of one of the application's health checks.
    Healthcheck,
}

/// Where a report comes from: a name the application chooses, such as
/// `"p2p_rpc"`. Equal names are the same source. It is a `&'static str`, so it
/// copies freely and rate limits key on it without allocating; a source read
/// back from JSON is interned, so each distinct name is leaked at most once.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Source(&'static str);

impl Source {
    /// The pipeline's own reports: flushed metric aggregates.
    pub const PIPELINE: Source = Source("pipeline");

    /// The source called `name`.
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// The source's name, as written into JSON.
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl Serialize for Source {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0)
    }
}

impl<'de> Deserialize<'de> for Source {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Ok(Source(intern(&name)))
    }
}

fn intern(name: &str) -> &'static str {
    static NAMES: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    let mut names = NAMES
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(&interned) = names.get(name) {
        return interned;
    }
    let interned: &'static str = Box::leak(name.to_owned().into_boxed_str());
    names.insert(interned);
    interned
}

/// What the application hands the pipeline.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Report {
    /// Selects the sampling rate and whether dedupe applies.
    pub kind: ReportKind,
    /// Rate limits and metric policies key on it.
    pub source: Source,
    /// The report's content; its `event` field selects a metric policy.
    pub payload: Payload,
    /// When set, dedupe compares this key, with the kind, source and event,
    /// instead of the whole payload. Not serialized.
    #[serde(skip)]
    pub dedupe_key: Option<DedupeKey>,
}

/// A report the pipeline accepted, numbered and timestamped.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    /// Position in the order the pipeline accepted reports, from 1.
    pub id: u64,
    /// The `now_ms` the report was accepted at.
    pub timestamp_ms: u64,
    /// The accepted report, unchanged.
    pub report: Report,
    /// The pipeline's run ID, on its first record only. `write_json` puts it
    /// first in the payload; serde skips it.
    #[serde(skip)]
    pub run_id: Option<Arc<str>>,
}

/// Counts kept by `Pipeline`, one per outcome; they saturate.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct AcceptStats {
    /// Reports made into records, aggregates included.
    pub accepted: u64,
    /// Reports dropped because the pipeline was disabled.
    pub dropped_disabled: u64,
    /// Reports over their source's rate limit.
    pub dropped_rate_limited: u64,
    /// Reports seen again within the dedupe window.
    pub dropped_deduped: u64,
    /// Reports sampling left out.
    pub dropped_sampled: u64,
    /// Reports that could not be encoded to JSON for hashing.
    pub dropped_invalid: u64,
}

/// What `Pipeline::accept_report` makes of one report.
#[derive(Debug, Default)]
pub struct AcceptOutcome {
    /// The report as a record, if it was kept.
    pub record: Option<Record>,
    /// The metric aggregate of a window the report closed, if it was kept.
    pub aggregate: Option<Record>,
}

/// A report's content: a JSON object whose strings live in a buffer from a
/// thread-local pool. Built with `PayloadBuilder` or `payload!`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Payload {
    inner: PoolPayload,
}

/// Builder for `Payload`. Mutations are only allowed on the builder;
/// finished payloads should be treated as immutable.
pub struct PayloadBuilder {
    inner: PoolPayloadBuilder,
}

// A source is a name, 16 bytes, so an application names its own sources
// without a shared enum; a report is 64 bytes and a record 96.
#[cfg(target_pointer_width = "64")]
const PAYLOAD_BYTES: usize = 16;
#[cfg(target_pointer_width = "64")]
const REPORT_BYTES: usize = 64;
#[cfg(target_pointer_width = "64")]
const RECORD_BYTES: usize = 96;
#[cfg(target_pointer_width = "64")]
const KIND_BYTES: usize = 1;
#[cfg(target_pointer_width = "64")]
const SOURCE_BYTES: usize = 16;
#[cfg(target_pointer_width = "64")]
const DEDUPE_KEY_BYTES: usize = 24;

#[cfg(target_pointer_width = "64")]
const_assert!(std::mem::size_of::<Payload>() == PAYLOAD_BYTES);
#[cfg(target_pointer_width = "64")]
const_assert!(std::mem::size_of::<Report>() == REPORT_BYTES);
#[cfg(target_pointer_width = "64")]
const_assert!(std::mem::size_of::<Record>() == RECORD_BYTES);
#[cfg(target_pointer_width = "64")]
const_assert!(std::mem::size_of::<ReportKind>() == KIND_BYTES);
#[cfg(target_pointer_width = "64")]
const_assert!(std::mem::size_of::<Source>() == SOURCE_BYTES);
#[cfg(target_pointer_width = "64")]
const_assert!(std::mem::size_of::<DedupeKey>() == DEDUPE_KEY_BYTES);

/// What dedupe compares instead of the whole payload; see
/// `Report::dedupe_key`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DedupeKey {
    /// A fixed string.
    Static(Cow<'static, str>),
    /// A 64-bit hash of a string that could not be stored in the pool.
    Hashed(u64),
    /// A string stored in the payload's pool; it resolves only against that
    /// payload.
    Pooled(Slice),
}

impl DedupeKey {
    /// Stores `value` in the builder's pool, falling back to its hash when it
    /// cannot be stored.
    pub fn pooled(payload: &mut PayloadBuilder, value: &str) -> Self {
        payload
            .pool_slice_for_key(value)
            .map(DedupeKey::Pooled)
            .unwrap_or_else(|| DedupeKey::Hashed(hash_str(value)))
    }

    /// Like `pooled`, for text formatted straight into the pool.
    pub fn pooled_fmt(payload: &mut PayloadBuilder, args: fmt::Arguments<'_>) -> Self {
        payload
            .pool_slice_for_key_fmt(args)
            .map(DedupeKey::Pooled)
            .unwrap_or_else(|| DedupeKey::Hashed(hash_fmt(args)))
    }

    /// The key's text, read from `payload` for a pooled key. `None` for a
    /// hashed key or a pooled one `payload` cannot resolve.
    pub fn as_str<'a>(&'a self, payload: &'a Payload) -> Option<&'a str> {
        match self {
            DedupeKey::Static(value) => Some(value.as_ref()),
            DedupeKey::Pooled(slice) => payload.pool_str(slice.start, slice.len),
            DedupeKey::Hashed(_) => None,
        }
    }

    /// Like `as_str`, as bytes.
    pub fn as_bytes<'a>(&'a self, payload: &'a Payload) -> Option<&'a [u8]> {
        match self {
            DedupeKey::Static(value) => Some(value.as_bytes()),
            DedupeKey::Pooled(slice) => payload.pool_slice(slice.start, slice.len),
            DedupeKey::Hashed(_) => None,
        }
    }
}

fn hash_str(value: &str) -> u64 {
    let hash = blake3::hash(value.as_bytes());
    u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap_or([0u8; 8]))
}

fn hash_fmt(args: fmt::Arguments<'_>) -> u64 {
    let mut hasher = blake3::Hasher::new();
    let mut writer = HashWriter {
        hasher: &mut hasher,
    };
    let _ = fmt::write(&mut writer, args);
    let hash = hasher.finalize();
    u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap_or([0u8; 8]))
}

struct HashWriter<'a> {
    hasher: &'a mut blake3::Hasher,
}

impl fmt::Write for HashWriter<'_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.hasher.update(s.as_bytes());
        Ok(())
    }
}

thread_local! {
    static POOL: PoolSet = PoolSet::new(PoolConfig {
        initial: 16,
        max: 256,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
}

impl Payload {
    /// The top-level `event` field, when it is a string.
    pub fn event_str(&self) -> Option<&str> {
        let value = self.inner.field_value("event")?;
        match value {
            Value::PoolStr { start, len } => self.inner.pool_str(*start, *len),
            _ => None,
        }
    }

    pub(crate) fn field_value(&self, key: &str) -> Option<&Value> {
        self.inner.field_value(key)
    }

    #[cfg(test)]
    pub(crate) fn section(&self, name: &str) -> Option<&Object> {
        self.inner.section(name)
    }

    #[cfg(test)]
    pub(crate) fn value_as_string(&self, value: &Value) -> Option<Cow<'_, str>> {
        self.inner.value_as_string(value)
    }

    pub(crate) fn pool_str(&self, start: u32, len: u32) -> Option<&str> {
        self.inner.pool_str(start, len)
    }

    pub(crate) fn pool_slice(&self, start: u32, len: u32) -> Option<&[u8]> {
        self.inner.pool_slice(start, len)
    }

    pub(crate) fn object(&self) -> &Object {
        self.inner.object()
    }

    /// Writes the payload as one JSON object.
    pub fn write_json<W: std::io::Write>(&self, out: &mut W) -> std::io::Result<()> {
        self.inner.write_json(out)
    }
}

impl PayloadBuilder {
    /// A payload from the thread-local pool whose first field, `event`,
    /// holds `event`.
    pub fn new(event: impl Into<Cow<'static, str>>) -> Self {
        let payload = POOL.with(|pool| pool.payload());
        let payload = payload.with_field("event", event.into());
        Self { inner: payload }
    }

    /// A payload from the thread-local pool with no fields.
    pub fn empty() -> Self {
        let payload = POOL.with(|pool| pool.payload());
        Self { inner: payload }
    }

    /// Appends a top-level field. Keys are not checked for repeats.
    pub fn with_field(
        mut self,
        key: impl Into<Cow<'static, str>>,
        value: impl causet_pooled_payload::FieldValue,
    ) -> Self {
        self.inner = self.inner.with_field(key, value);
        self
    }

    /// Appends the field when `value` is `Some`.
    pub fn with_field_opt<V>(self, key: impl Into<Cow<'static, str>>, value: Option<V>) -> Self
    where
        V: causet_pooled_payload::FieldValue,
    {
        match value {
            Some(value) => self.with_field(key, value),
            None => self,
        }
    }

    /// Adds the object `name` holding one field per pair in `fields`.
    pub fn with_section_fields<K, V, I>(self, name: impl Into<Cow<'static, str>>, fields: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<Cow<'static, str>>,
        V: causet_pooled_payload::FieldValue,
    {
        self.with_section(name, |mut section| {
            for (key, value) in fields {
                section = section.with_field(key, value);
            }
            section
        })
    }

    /// Adds the object `name` holding, for each `(key, item)` in `entries`, an
    /// object `key` that `builder` fills from `item`.
    pub fn with_section_objects<K, T, I, F>(
        self,
        name: impl Into<Cow<'static, str>>,
        entries: I,
        mut builder: F,
    ) -> Self
    where
        I: IntoIterator<Item = (K, T)>,
        K: Into<Cow<'static, str>>,
        F: for<'a> FnMut(
            causet_pooled_payload::ObjectBuilder<'a>,
            T,
        ) -> causet_pooled_payload::ObjectBuilder<'a>,
    {
        self.with_section(name, |mut section| {
            for (key, value) in entries {
                section = section.with_section(key, |object| builder(object, value));
            }
            section
        })
    }

    /// Adds the array `name` holding `values`.
    pub fn with_array_values<V, I>(self, name: impl Into<Cow<'static, str>>, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: causet_pooled_payload::FieldValue,
    {
        self.with_array(name, |array| {
            for value in values {
                array.push_value(value);
            }
        })
    }

    /// Adds the array `name` holding one object per item, filled by `builder`.
    pub fn with_array_objects<T, I, F>(
        self,
        name: impl Into<Cow<'static, str>>,
        items: I,
        mut builder: F,
    ) -> Self
    where
        I: IntoIterator<Item = T>,
        F: for<'a> FnMut(
            causet_pooled_payload::ObjectBuilder<'a>,
            T,
        ) -> causet_pooled_payload::ObjectBuilder<'a>,
    {
        self.with_array(name, |array| {
            for item in items {
                array.push_object(|object| builder(object, item));
            }
        })
    }

    /// Appends a top-level field in place, like `with_field`.
    pub fn insert_field(
        &mut self,
        key: impl Into<Cow<'static, str>>,
        value: impl causet_pooled_payload::FieldValue,
    ) {
        self.inner.push_field(key, value);
    }

    /// Appends the field in place when `value` is `Some`.
    pub fn insert_field_opt<V>(&mut self, key: impl Into<Cow<'static, str>>, value: Option<V>)
    where
        V: causet_pooled_payload::FieldValue,
    {
        if let Some(value) = value {
            self.insert_field(key, value);
        }
    }

    fn pool_slice_for_key(&mut self, value: &str) -> Option<Slice> {
        self.inner.store_str(value)
    }

    fn pool_slice_for_key_fmt(&mut self, args: fmt::Arguments<'_>) -> Option<Slice> {
        self.inner.store_fmt(args)
    }

    /// Adds the object `name`, filled by `builder`.
    pub fn with_section(
        mut self,
        name: impl Into<Cow<'static, str>>,
        builder: impl FnOnce(
            causet_pooled_payload::ObjectBuilder<'_>,
        ) -> causet_pooled_payload::ObjectBuilder<'_>,
    ) -> Self {
        self.inner = self.inner.with_section(name, builder);
        self
    }

    /// Adds the array `name`, filled by `builder`.
    pub fn with_array(
        mut self,
        name: impl Into<Cow<'static, str>>,
        builder: impl FnOnce(&mut causet_pooled_payload::ArrayBuilder<'_>),
    ) -> Self {
        self.inner = self.inner.with_array(name, builder);
        self
    }

    /// Ends building; the payload cannot be changed after this.
    pub fn finish(self) -> Payload {
        Payload {
            inner: self.inner.finish(),
        }
    }

    /// Finish the payload and return it together with a pooled dedupe key.
    ///
    /// If the key cannot be pooled, a hashed fallback key is returned.
    pub fn finish_with_dedupe_key(mut self, value: &str) -> (Payload, DedupeKey) {
        let dedupe_key = DedupeKey::pooled(&mut self, value);
        let payload = self.finish();
        (payload, dedupe_key)
    }

    /// Finish the payload and return it together with a pooled dedupe key built from `format_args!`.
    ///
    /// If the formatted key cannot be pooled, a hashed fallback key is returned.
    pub fn finish_with_dedupe_key_fmt(mut self, args: fmt::Arguments<'_>) -> (Payload, DedupeKey) {
        let dedupe_key = DedupeKey::pooled_fmt(&mut self, args);
        let payload = self.finish();
        (payload, dedupe_key)
    }
}

impl From<PoolPayload> for Payload {
    fn from(inner: PoolPayload) -> Self {
        Self { inner }
    }
}

impl Report {
    /// A report without a dedupe key.
    pub fn new(kind: ReportKind, source: Source, payload: Payload) -> Self {
        Self {
            kind,
            source,
            payload,
            dedupe_key: None,
        }
    }

    /// Sets or clears the dedupe key.
    pub fn with_dedupe_key(mut self, dedupe_key: Option<DedupeKey>) -> Self {
        self.dedupe_key = dedupe_key;
        self
    }

    /// Writes the report as one JSON object of `kind`, `source` and `payload`.
    pub fn write_json<W: std::io::Write>(&self, out: &mut W) -> std::io::Result<()> {
        write_report_json(self, out)
    }
}

impl Record {
    /// Writes the record as one JSON object of `id`, `timestamp_ms`, `kind`,
    /// `source` and `payload`, with `run_id` first in the payload when set.
    /// This is the dump's line format; serde's nests `report` instead.
    pub fn write_json<W: std::io::Write>(&self, out: &mut W) -> std::io::Result<()> {
        write_record_json(self, out)
    }
}

fn write_record_json<W: std::io::Write>(record: &Record, out: &mut W) -> std::io::Result<()> {
    out.write_all(b"{")?;
    let mut first = true;
    write_key(out, "id", &mut first)?;
    write_fmt(out, format_args!("{}", record.id))?;
    write_key(out, "timestamp_ms", &mut first)?;
    write_fmt(out, format_args!("{}", record.timestamp_ms))?;
    write_key(out, "kind", &mut first)?;
    write_kind(out, &record.report.kind)?;
    write_key(out, "source", &mut first)?;
    write_str(out, record.report.source.as_str())?;
    write_key(out, "payload", &mut first)?;
    if let Some(run_id) = record.run_id.as_deref() {
        write_payload_with_run_id(out, &record.report.payload, run_id)?;
    } else {
        record.report.payload.write_json(out)?;
    }
    out.write_all(b"}")
}

fn write_report_json<W: std::io::Write>(report: &Report, out: &mut W) -> std::io::Result<()> {
    out.write_all(b"{")?;
    let mut first = true;
    write_key(out, "kind", &mut first)?;
    write_kind(out, &report.kind)?;
    write_key(out, "source", &mut first)?;
    write_str(out, report.source.as_str())?;
    write_key(out, "payload", &mut first)?;
    report.payload.write_json(out)?;
    out.write_all(b"}")
}

fn write_kind<W: std::io::Write>(out: &mut W, kind: &ReportKind) -> std::io::Result<()> {
    match kind {
        ReportKind::Error => write_str(out, "error"),
        ReportKind::Metric => write_str(out, "metric"),
        ReportKind::Event => write_str(out, "event"),
        ReportKind::Healthcheck => write_str(out, "healthcheck"),
    }
}

fn write_payload_with_run_id<W: std::io::Write>(
    out: &mut W,
    payload: &Payload,
    run_id: &str,
) -> std::io::Result<()> {
    out.write_all(b"{")?;
    let mut first = true;
    write_key(out, "run_id", &mut first)?;
    write_str(out, run_id)?;
    write_payload_fields(out, payload, payload.object(), &mut first)?;
    out.write_all(b"}")
}

fn write_payload_fields<W: std::io::Write>(
    out: &mut W,
    payload: &Payload,
    object: &Object,
    first: &mut bool,
) -> std::io::Result<()> {
    for field in &object.fields {
        write_key(out, field.key.as_ref(), first)?;
        write_payload_value(out, payload, &field.value)?;
    }
    for section in &object.sections {
        write_key(out, section.name.as_ref(), first)?;
        write_payload_object(out, payload, section.object.as_ref())?;
    }
    for array in &object.arrays {
        write_key(out, array.name.as_ref(), first)?;
        write_payload_array(out, payload, array)?;
    }
    Ok(())
}

fn write_payload_object<W: std::io::Write>(
    out: &mut W,
    payload: &Payload,
    object: &Object,
) -> std::io::Result<()> {
    out.write_all(b"{")?;
    let mut first = true;
    write_payload_fields(out, payload, object, &mut first)?;
    out.write_all(b"}")
}

fn write_payload_array<W: std::io::Write>(
    out: &mut W,
    payload: &Payload,
    array: &causet_pooled_payload::Array,
) -> std::io::Result<()> {
    out.write_all(b"[")?;
    let mut first = true;
    for item in &array.items {
        if !first {
            out.write_all(b",")?;
        }
        first = false;
        match item {
            ArrayItem::Object(object) => {
                write_payload_object(out, payload, object.as_ref())?;
            }
            ArrayItem::Array(array) => {
                write_payload_array(out, payload, array.as_ref())?;
            }
            ArrayItem::Value(value) => {
                write_payload_value(out, payload, value)?;
            }
        }
    }
    out.write_all(b"]")
}

fn write_payload_value<W: std::io::Write>(
    out: &mut W,
    payload: &Payload,
    value: &Value,
) -> std::io::Result<()> {
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

fn write_key<W: std::io::Write>(out: &mut W, key: &str, first: &mut bool) -> std::io::Result<()> {
    if !*first {
        out.write_all(b",")?;
    }
    *first = false;
    write_str(out, key)?;
    out.write_all(b":")
}

fn write_str<W: std::io::Write>(out: &mut W, value: &str) -> std::io::Result<()> {
    write_escaped_str(out, value)
}

fn write_escaped_str<W: std::io::Write>(out: &mut W, value: &str) -> std::io::Result<()> {
    out.write_all(b"\"")?;
    for byte in value.as_bytes() {
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
            _ => out.write_all(&[*byte])?,
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

fn write_fmt<W: std::io::Write>(out: &mut W, args: std::fmt::Arguments<'_>) -> std::io::Result<()> {
    struct FmtWriter<'a, W: std::io::Write> {
        out: &'a mut W,
    }

    impl<'a, W: std::io::Write> std::fmt::Write for FmtWriter<'a, W> {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            self.out
                .write_all(s.as_bytes())
                .map_err(|_| std::fmt::Error)
        }
    }

    let mut writer = FmtWriter { out };
    std::fmt::write(&mut writer, args).map_err(|_| std::io::Error::other("formatting failed"))
}
