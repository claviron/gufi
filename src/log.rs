//! `claviron.LogHandler`: Python's standard `logging` — Django's, FastAPI's,
//! any application's — on a log channel `logging.yaml` declares.
//!
//! Python's own output already goes to the server's log; what an application
//! adds is a handler, the way Laravel adds the `claviron` log driver. No
//! separate logging API is offered: the two functions below are the
//! handler's, named with an underscore.
//!
//! The record goes into this process's lane of the fleet's log ring and the
//! call returns; the fleet's one writer formats and writes it.

use claviron::logging::pipeline::{ChannelId, Level, Record, Value, installed};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyString};

/// The Python half: `claviron.LogHandler`, a `logging.Handler`.
pub(crate) const MODULE_SOURCE: &str = include_str!("log.py");

/// The id of a declared channel, or None: undeclared, or no pipeline.
#[pyfunction]
#[pyo3(name = "_log_channel")]
pub(crate) fn log_channel(name: &str) -> Option<u16> {
    installed()?.channel(name).map(ChannelId::raw)
}

/// Publish one record; False when it could not be (an undeclared channel, no
/// pipeline, a ring write that failed).
#[pyfunction]
#[pyo3(name = "_log", signature = (channel, level, message, target = None, fields = None))]
pub(crate) fn log(
    channel: u16,
    level: &str,
    message: &str,
    target: Option<&str>,
    fields: Option<&Bound<'_, PyDict>>,
) -> PyResult<bool> {
    let Some(channels) = installed() else { return Ok(false) };
    let mut owned: Vec<(String, Owned)> = Vec::new();
    if let Some(fields) = fields {
        for (key, value) in fields.iter() {
            owned.push((key.str()?.to_string(), Owned::from_python(&value)?));
        }
    }
    let name = level;
    let (level, lossy) = map_level(name);
    if lossy {
        owned.push((
            "psr_level".to_owned(),
            Owned::Str(name.to_ascii_lowercase()),
        ));
    }
    let fields: Vec<(&str, Value<'_>)> = owned
        .iter()
        .map(|(key, value)| (key.as_str(), value.borrow()))
        .collect();
    let record = Record {
        level,
        target: target.unwrap_or(""),
        message,
        fields: &fields,
    };
    Ok(channels.emit(ChannelId::from_raw(channel), &record).is_ok())
}

/// Python's levels onto the pipeline's five; `critical` merges into error
/// and keeps its name.
fn map_level(name: &str) -> (Level, bool) {
    match name.to_ascii_lowercase().as_str() {
        "debug" => (Level::Debug, false),
        "info" => (Level::Info, false),
        "warning" | "warn" => (Level::Warn, false),
        "error" => (Level::Error, false),
        "critical" | "fatal" => (Level::Error, true),
        _ => (Level::Info, true),
    }
}

enum Owned {
    Str(String),
    I64(i64),
    F64(f64),
    Bool(bool),
}

impl Owned {
    /// Typed where the value is a scalar; anything else as its `str()`.
    fn from_python(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        // `bool` before `int`: in Python a bool is an int.
        Ok(if value.is_instance_of::<PyBool>() {
            Self::Bool(value.extract()?)
        } else if value.is_instance_of::<PyInt>() {
            match value.extract::<i64>() {
                Ok(number) => Self::I64(number),
                Err(_) => Self::Str(value.str()?.to_string()),
            }
        } else if value.is_instance_of::<PyFloat>() {
            Self::F64(value.extract()?)
        } else if value.is_instance_of::<PyString>() {
            Self::Str(value.extract()?)
        } else if value.is_none() {
            Self::Str(String::new())
        } else {
            Self::Str(value.str()?.to_string())
        })
    }

    fn borrow(&self) -> Value<'_> {
        match self {
            Self::Str(text) => Value::Str(text),
            Self::I64(number) => Value::I64(*number),
            Self::F64(number) => Value::F64(*number),
            Self::Bool(flag) => Value::Bool(*flag),
        }
    }
}
