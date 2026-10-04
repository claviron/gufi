//! `claviron.counter(key)`: the fleet's keyed counters, from Python.
//!
//! The table is Orbit's (`orbit-counter`, through Claviron's
//! `CounterProvider`): one signed 64-bit value per key, in shared memory, the
//! same for every worker of the application and for its Rust routes. A worker
//! adds with an atomic, no lock and no message, so a count kept here is the
//! application's, not one process's — which a Python global cannot be once
//! there is more than one worker.
//!
//! ```python
//! import claviron
//!
//! hits = claviron.counter("hits")
//! hits.increment()        # the value after, across every worker
//! hits.get()              # None until the key has been touched
//! ```

use std::sync::Arc;

use claviron::orbit::Counter;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

/// One key of the fleet's counter table.
#[pyclass(name = "Counter", module = "claviron", frozen)]
pub(crate) struct PythonCounter {
    table: Arc<Counter>,
    key: String,
}

impl PythonCounter {
    pub(crate) fn new(
        table: Arc<Counter>,
        key: String,
    ) -> Self {
        Self { table, key }
    }
}

#[pymethods]
impl PythonCounter {
    #[getter]
    fn key(&self) -> &str {
        &self.key
    }

    /// The value now, or `None` for a key nothing has touched yet.
    fn get(&self) -> PyResult<Option<i64>> {
        self.table.get(&self.key).map_err(error)
    }

    /// Add `by` and return the value after.
    #[pyo3(signature = (by = 1))]
    fn increment(
        &self,
        by: i64,
    ) -> PyResult<i64> {
        self.table.increment(&self.key, by).map_err(error)
    }

    /// Subtract `by` and return the value after.
    #[pyo3(signature = (by = 1))]
    fn decrement(
        &self,
        by: i64,
    ) -> PyResult<i64> {
        self.table.decrement(&self.key, by).map_err(error)
    }

    /// Set the key to zero.
    fn reset(&self) -> PyResult<()> {
        self.table.reset(&self.key).map_err(error)
    }

    fn __repr__(&self) -> String {
        match self.table.get(&self.key) {
            Ok(Some(value)) => {
                format!("claviron.counter({:?}) = {value}", self.key)
            }
            _ => format!("claviron.counter({:?})", self.key),
        }
    }
}

/// A wrong key or amount is the caller's mistake; a full table or an I/O
/// failure is the runtime's.
fn error(error: claviron::orbit::counter::Error) -> PyErr {
    use claviron::orbit::counter::Error;
    match error {
        Error::KeyEmpty
        | Error::KeyTooLarge { .. }
        | Error::NegativeAmount(_) => PyValueError::new_err(error.to_string()),
        _ => PyRuntimeError::new_err(error.to_string()),
    }
}

/// What `claviron.counter` is bound to: the table, handing out keys.
#[pyclass(frozen)]
pub(crate) struct Counters {
    table: Arc<Counter>,
}

impl Counters {
    pub(crate) fn new(table: Arc<Counter>) -> Self {
        Self { table }
    }
}

#[pymethods]
impl Counters {
    /// The counter under `key`. A key the table would refuse is refused here,
    /// not at its first use.
    fn counter(
        &self,
        key: String,
    ) -> PyResult<PythonCounter> {
        self.table.get(&key).map_err(error)?;
        Ok(PythonCounter::new(Arc::clone(&self.table), key))
    }
}
