//! Optional Django Channels consumer limits.

/// `broadcasting.channels:` — the limits of every consumer's box. Unwritten
/// means no limit, as Channels' own `CONFIG` would otherwise decide.
#[cfg(feature = "django")]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Channels {
    /// Messages a box holds before a direct send to it is refused
    /// (`ChannelFull`) and a group send passes it over.
    pub capacity: Option<usize>,
    /// How old a message may be when it is received; older ones are dropped
    /// then. Written as a duration, `"60s"`.
    #[serde(
        serialize_with = "serialize_expiry",
        deserialize_with = "claviron::helpers::de::deserialize_optional_duration"
    )]
    pub expiry: Option<std::time::Duration>,
}

/// In milliseconds, which the reader parses back without loss.
#[cfg(feature = "django")]
fn serialize_expiry<S: serde::Serializer>(
    value: &Option<std::time::Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Some(expiry) => {
            serializer.collect_str(&format_args!("{}ms", expiry.as_millis()))
        }
        None => serializer.serialize_none(),
    }
}
