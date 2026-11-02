The causet guide's Rust examples for its messages page, run as doc tests
(`cargo test -p causet --doc`). The guide's pages are in its own
repository and show each block here by the name above it.

<!-- snippet: declare -->
```
use causet::{Action, Effect, Event, Message, Output};

/// What the kettle can be asked to do.
#[derive(Debug, Action)]
#[causet(type = "kettle.action")]
enum KettleAction {
    SwitchOn { target: u32 },
    SwitchOff,
}

/// What it asks of its heater.
#[derive(Debug, Effect)]
#[causet(type = "kettle.effect")]
enum KettleEffect {
    HeaterOn,
    HeaterOff,
}

/// The thermometer's answer.
#[derive(Debug, Event)]
#[causet(type = "kettle.temperature")]
struct Temperature {
    celsius: u32,
}

/// What it tells its parent.
#[derive(Debug, Output)]
#[causet(type = "kettle.output")]
enum KettleOutput {
    Boiled,
}

assert_eq!(KettleAction::TYPE, "kettle.action");
assert_eq!(KettleAction::SwitchOn { target: 100 }.kind(), "kettle.switch_on");
assert_eq!(KettleEffect::HeaterOff.kind(), "kettle.heater_off");
assert_eq!(Temperature { celsius: 20 }.kind(), "kettle.temperature");
assert_eq!(KettleOutput::Boiled.kind(), "kettle.boiled");
```

<!-- snippet: kinds -->
```
use causet::{Effect, Message, Output};

#[derive(Debug, Effect)]
#[causet(type = "kettle.heater", prefix = "kettle.heater")]
enum Heater {
    Warm, // the prefix and the variant
    #[causet(kind = "kettle.heater.off")]
    Cut, // given
}

/// A struct has one kind: its own, here, rather than the type's name.
#[derive(Debug, Output)]
#[causet(type = "kettle.alarm", kind = "kettle.dry")]
struct DryAlarm {
    dry_secs: u32,
}

assert_eq!(Heater::Warm.kind(), "kettle.heater.warm");
assert_eq!(Heater::Cut.kind(), "kettle.heater.off");
assert_eq!(DryAlarm { dry_secs: 3 }.kind(), "kettle.dry");
```

<!-- snippet: logging -->
```
use causet::{Action, Event, LogLevel, Message};

#[derive(Debug, Action)]
#[causet(type = "door.action", log(level = "info"))]
enum DoorAction {
    /// At info, the type's level, with no fields.
    Ring,
    /// At warn, with the person's name by `Display`, and never the code.
    #[causet(log(level = "warn", fields(who = display(name))))]
    Unlock { name: String, code: String },
}

/// At trace, with whether the door is ajar, by `Debug`.
#[derive(Debug, Event)]
#[causet(type = "door.sensor", log(level = "trace", fields(ajar)))]
struct DoorSensor {
    ajar: bool,
    battery: u8,
}

let unlock = DoorAction::Unlock { name: "Ada".into(), code: "1234".into() };
assert_eq!(DoorAction::Ring.log_level(), LogLevel::Info);
assert_eq!(unlock.log_level(), LogLevel::Warn);
assert_eq!(DoorSensor { ajar: true, battery: 80 }.log_level(), LogLevel::Trace);
```

<!-- snippet: nested -->
```
use causet::{Action, LogLevel, Message};

#[derive(Debug, Action)]
#[causet(type = "lamp.action")]
enum LampAction {
    #[causet(log(level = "info", fields(percent)))]
    Dim { percent: u8 },
    Unplug,
}

#[derive(Debug, Action)]
#[causet(type = "room.action")]
enum RoomAction {
    Vacate,
    #[causet(nested)]
    Lamp(LampAction),
}

let dim = RoomAction::Lamp(LampAction::Dim { percent: 40 });
assert_eq!(RoomAction::TYPE, "room.action");
assert_eq!(RoomAction::Vacate.kind(), "room.vacate");
// The inner message's kind, level and fields.
assert_eq!(dim.kind(), "lamp.dim");
assert_eq!(dim.log_level(), LogLevel::Info);
assert_eq!(format!("{dim:?}"), "Lamp(Dim { percent: 40 })");
```

<!-- snippet: by-hand -->
```
use causet::{Event, LogLevel, Message};

/// A thermometer tick, too frequent to log above trace.
#[derive(Debug)]
struct Tick {
    reading: u32,
}

impl Message for Tick {
    const TYPE: &'static str = "kettle.tick";
    fn kind(&self) -> &'static str {
        Self::TYPE
    }
    fn log_level(&self) -> LogLevel {
        LogLevel::Trace
    }
}

impl Event for Tick {}

let tick = Tick { reading: 21 };
assert_eq!(tick.kind(), "kettle.tick");
assert_eq!(tick.log_level(), LogLevel::Trace);
assert_eq!(format!("{tick:?}"), "Tick { reading: 21 }");
```

<!-- snippet: refused-no-type -->
```compile_fail
// error: missing `#[causet(type = "...")]`: the type's name
#[derive(Debug, causet::Action)]
enum KettleAction {
    SwitchOff,
}
```

<!-- snippet: refused-nested -->
```compile_fail
#[derive(Debug, causet::Action)]
#[causet(type = "lamp.action")]
enum LampAction {
    Unplug,
}

// error: a `nested` variant holds exactly one unnamed field: the inner message
#[derive(Debug, causet::Action)]
#[causet(type = "room.action")]
enum RoomAction {
    #[causet(nested)]
    Lamp { lamp: LampAction },
}
```
