//! What Django gets from gufi beyond ASGI, which Django already speaks.
//!
//! Django's own capabilities stay Django's; what is added here is where a
//! Django deployment would otherwise reach for another server — Channels'
//! channel layer, which is Redis in most deployments and the fleet's shared
//! memory here.

pub(crate) mod channels;
