//! Django Channels' channel layer over Claviron's broadcast groups.
//!
//! `claviron.channels.OrbitChannelLayer` is the layer a Django project names
//! in `CHANNEL_LAYERS`, in place of `channels_redis`. Boxes, groups and their
//! fan-out across the fleet are the broadcast service's
//! ([`GroupLayer`]); this module hands them to Python and resolves a
//! consumer's wait for its next message the way a WebSocket's is resolved:
//! the message's arrival rings the application loop's doorbell with the
//! wait's id, and the loop takes it on its own thread. No other thread
//! touches Python for it.
//!
//! What a message holds is Python's: the layer packs it with msgpack, as
//! `channels_redis` does, and Rust carries bytes.

use std::sync::Arc;
use std::time::Duration;

use claviron::broadcast::GroupLayer;
use claviron::broadcast::groups::GroupError;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::options::Channels;

/// The Python layer, a file of its own so tools that read Python can.
pub(crate) const MODULE_SOURCE: &str = include_str!("channels.py");

/// Rings the application loop for one wait, by id.
pub(crate) type Ring = Arc<dyn Fn(u64) + Send + Sync>;

/// The layer as the application process holds it: the groups, the box
/// limits from `broadcasting.channels`, and the way to ring its loop.
#[derive(Clone)]
pub(crate) struct ChannelLayer {
    groups: GroupLayer,
    capacity: Option<usize>,
    expiry: Option<Duration>
}

impl ChannelLayer {
    pub(crate) fn new(
        groups: GroupLayer,
        settings: &Channels
    ) -> Self {
        Self { groups, capacity: settings.capacity, expiry: settings.expiry }
    }
}

/// `claviron._channels`: what `channels.py` calls.
#[pyclass(name = "ChannelCore", module = "claviron", frozen)]
pub(crate) struct ChannelCore {
    layer: ChannelLayer,
    ring: Ring,
    next_id: Arc<dyn Fn() -> u64 + Send + Sync>
}

impl ChannelCore {
    pub(crate) fn new(
        layer: ChannelLayer,
        ring: Ring,
        next_id: Arc<dyn Fn() -> u64 + Send + Sync>
    ) -> Self {
        Self { layer, ring, next_id }
    }
}

#[pymethods]
impl ChannelCore {
    /// A new box in this process, and its name.
    fn open(
        &self,
        prefix: &str
    ) -> PyResult<String> {
        self.layer.groups.open(prefix, self.layer.capacity, self.layer.expiry).map_err(error)
    }

    fn close(
        &self,
        channel: &str
    ) {
        self.layer.groups.close(channel);
    }

    /// Send to one box; `False` when it is full.
    fn send(
        &self,
        channel: &str,
        message: &[u8]
    ) -> PyResult<bool> {
        match self.layer.groups.send(channel, bytes::Bytes::copy_from_slice(message)) {
            Ok(()) => Ok(true),
            Err(GroupError::Full) => Ok(false),
            Err(other) => Err(error(other))
        }
    }

    /// The next message for a box of this process, or a wait the loop
    /// resolves when one arrives.
    fn receive<'py>(
        &self,
        py: Python<'py>,
        channel: String
    ) -> PyResult<Bound<'py, PyAny>> {
        let id = (self.next_id)();
        match take_or_wake(&self.layer.groups, &self.ring, &channel, id)? {
            Some(message) => Ok(PyBytes::new(py, &message).into_any()),
            None => Ok(Bound::new(
                py,
                ChannelWait {
                    id,
                    channel,
                    groups: self.layer.groups.clone(),
                    ring: self.ring.clone()
                }
            )?
            .into_any())
        }
    }

    fn group_add(
        &self,
        group: &str,
        channel: &str
    ) -> PyResult<()> {
        self.layer.groups.add(group, channel).map_err(error)
    }

    fn group_discard(
        &self,
        group: &str,
        channel: &str
    ) {
        self.layer.groups.discard(group, channel);
    }

    /// Send to every member across the fleet; a full member is passed over.
    fn group_send(
        &self,
        group: &str,
        message: &[u8]
    ) -> PyResult<usize> {
        self.layer.groups.group_send(group, bytes::Bytes::copy_from_slice(message)).map_err(error)
    }
}

/// A receive waiting for its box, in the loop's table of waits by `id`.
#[pyclass(name = "ChannelWait", module = "claviron", frozen)]
pub(crate) struct ChannelWait {
    #[pyo3(get)]
    id: u64,
    channel: String,
    groups: GroupLayer,
    ring: Ring
}

#[pymethods]
impl ChannelWait {
    /// The message, once the doorbell has named this wait; `None` — and
    /// waiting again — when another receive took it first.
    fn try_receive<'py>(
        &self,
        py: Python<'py>
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        Ok(take_or_wake(&self.groups, &self.ring, &self.channel, self.id)?
            .map(|message| PyBytes::new(py, &message)))
    }
}

fn take_or_wake(
    groups: &GroupLayer,
    ring: &Ring,
    channel: &str,
    id: u64
) -> PyResult<Option<bytes::Bytes>> {
    let ring = Arc::clone(ring);
    groups.receive_or_wake(channel, Box::new(move || ring(id))).map_err(error)
}

fn error(error: GroupError) -> PyErr {
    match error {
        GroupError::UnknownChannel => PyKeyError::new_err(error.to_string()),
        GroupError::NotProcessSpecific => PyValueError::new_err(error.to_string()),
        _ => PyRuntimeError::new_err(error.to_string())
    }
}
