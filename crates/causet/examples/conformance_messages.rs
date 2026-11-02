//! How messages print, one per line, for a port in another language to
//! compare against: each message's type, kind, level, `Debug` form and
//! logged fields. They cover what a port's own names can hide: a kind given
//! in full on a variant whose name it does not tell, names of more than one
//! word, a nested message, and a struct of one kind.
//!
//! `cargo run -p causet --example conformance_messages > messages.txt`

// The messages' fields are read by `Debug` alone.
#![allow(dead_code)]

use std::fmt::{Debug, Display, Write};

use causet::observers::FieldVisitor;
use causet::{Action, Event, Message};

/// What the kettle can be asked to do.
#[derive(Debug, Action)]
#[causet(type = "kettle.action")]
enum KettleAction {
    #[causet(log(level = "info", fields(target_celsius, display(label))))]
    SwitchOn {
        target_celsius: u32,
        keep_warm: bool,
        label: String,
    },
    #[causet(kind = "kettle.emptied")]
    Clear,
    #[causet(nested)]
    Heater(HeaterAction),
}

/// What its heater can be asked to do.
#[derive(Debug, Action)]
#[causet(type = "kettle.heater", prefix = "kettle.heater")]
enum HeaterAction {
    #[causet(log(fields(max_secs)))]
    Warm {
        max_secs: u64,
        last_reading: Option<i64>,
    },
    #[causet(kind = "kettle.heater.off")]
    Cut,
}

/// The thermometer's alarm: a struct of one kind.
#[derive(Debug, Event)]
#[causet(
    type = "kettle.alarm",
    kind = "kettle.dry",
    log(level = "warn", fields(dry_secs))
)]
struct DryAlarm {
    dry_secs: u32,
    recent_readings: Vec<i64>,
}

struct Fields(String);

impl FieldVisitor for Fields {
    fn debug(&mut self, name: &'static str, value: &dyn Debug) {
        let _ = write!(self.0, " {name}={value:?}");
    }
    fn display(&mut self, name: &'static str, value: &dyn Display) {
        let _ = write!(self.0, " {name}={value}");
    }
}

fn print<M: Message + Debug>(m: &M) {
    let mut fields = Fields(String::new());
    m.log_fields(&mut fields);
    let level = format!("{:?}", m.log_level()).to_lowercase();
    println!("{} {} {level} {m:?} |{}", M::TYPE, m.kind(), fields.0);
}

fn main() {
    print(&KettleAction::SwitchOn {
        target_celsius: 100,
        keep_warm: true,
        label: "green \"tea\"".into(),
    });
    print(&KettleAction::Clear);
    print(&KettleAction::Heater(HeaterAction::Warm {
        max_secs: 90,
        last_reading: Some(-4),
    }));
    print(&KettleAction::Heater(HeaterAction::Warm {
        max_secs: 5,
        last_reading: None,
    }));
    print(&KettleAction::Heater(HeaterAction::Cut));
    print(&DryAlarm {
        dry_secs: 3,
        recent_readings: vec![21, -4],
    });
}
