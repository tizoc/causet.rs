use std::mem::size_of;

use crate::{DedupeKey, Payload, Record, Report, ReportKind, Source};

const MAX_PAYLOAD_BYTES: usize = 16;
const MAX_REPORT_BYTES: usize = 64;
const MAX_RECORD_BYTES: usize = 96;
const MAX_KIND_BYTES: usize = 1;
const MAX_SOURCE_BYTES: usize = 16;
const MAX_DEDUPE_KEY_BYTES: usize = 24;

const _: () = {
    assert!(size_of::<Payload>() <= MAX_PAYLOAD_BYTES);
    assert!(size_of::<Report>() <= MAX_REPORT_BYTES);
    assert!(size_of::<Record>() <= MAX_RECORD_BYTES);
    assert!(size_of::<ReportKind>() <= MAX_KIND_BYTES);
    assert!(size_of::<Source>() <= MAX_SOURCE_BYTES);
    assert!(size_of::<DedupeKey>() <= MAX_DEDUPE_KEY_BYTES);
};

#[test]
fn report_sizes_are_small() {
    assert!(
        size_of::<Payload>() <= MAX_PAYLOAD_BYTES,
        "Payload is too large"
    );
    assert!(
        size_of::<Report>() <= MAX_REPORT_BYTES,
        "Report is too large"
    );
    assert!(
        size_of::<Record>() <= MAX_RECORD_BYTES,
        "Record is too large"
    );
    assert!(
        size_of::<ReportKind>() <= MAX_KIND_BYTES,
        "ReportKind is too large"
    );
    assert!(
        size_of::<Source>() <= MAX_SOURCE_BYTES,
        "Source is too large"
    );
    assert!(
        size_of::<DedupeKey>() <= MAX_DEDUPE_KEY_BYTES,
        "DedupeKey is too large"
    );
}
