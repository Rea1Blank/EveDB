// SPDX-License-Identifier: AGPL-3.0-only

//! Logical decoded allocation charges, separate from encoded WAL admission.

use crate::{
    Entity, Event, Fields, Result, Value, codec::Decoder, error::corrupt,
    resources::MemoryReservation,
};
use std::ops::Deref;

pub(crate) struct Accounted<T> {
    pub value: T,
    pub memory: Option<MemoryReservation>,
}
impl<T> Accounted<T> {
    #[cfg(test)]
    pub fn into_inner(self) -> T {
        self.value
    }
    pub fn bytes(&self) -> usize {
        self.memory.as_ref().map_or(0, |memory| memory.bytes)
    }
}
impl<T> Deref for Accounted<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

/// Includes a conservative node/slot allowance per field; not an allocator/RSS metric.
pub(crate) const FIELD_BYTES: usize = 128;
pub(crate) fn value_bytes(value: &Value) -> usize {
    match value {
        Value::Text(text) => text.capacity(),
        Value::Bytes(bytes) => bytes.capacity(),
        _ => 0,
    }
}
pub(crate) fn fields_bytes(fields: &Fields) -> usize {
    fields
        .values()
        .map(|value| FIELD_BYTES + value_bytes(value))
        .sum()
}
pub(crate) fn entity_bytes(entity: &Entity) -> usize {
    std::mem::size_of::<Entity>() + fields_bytes(&entity.fields)
}
pub(crate) fn event_bytes(event: &Event) -> usize {
    std::mem::size_of::<Accounted<Event>>() + fields_bytes(&event.fields)
}
pub(crate) fn merged_sizes(current: &Fields, changes: &Fields) -> (usize, usize) {
    let values = current
        .iter()
        .map(|(id, value)| changes.get(id).unwrap_or(value))
        .chain(
            changes
                .iter()
                .filter(|(id, _)| !current.contains_key(id))
                .map(|(_, value)| value),
        );
    let mut decoded = std::mem::size_of::<Entity>();
    let mut encoded = 25 + 8;
    for value in values {
        decoded += FIELD_BYTES + value_bytes(value);
        encoded += 5 + match value {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Text(text) => 8 + text.len(),
            Value::Bytes(bytes) => 8 + bytes.len(),
            _ => 8,
        };
    }
    (decoded, encoded)
}
/// Reads lengths and field tags before allocating decoded values.
pub(crate) fn record_bytes(bytes: &[u8], overhead: usize) -> Result<usize> {
    let mut d = Decoder::new(bytes);
    d.u64()?;
    d.u64()?;
    d.u64()?;
    d.u8()?;
    let count = d.count(5)?;
    let mut size = overhead
        .checked_add(
            count
                .checked_mul(FIELD_BYTES)
                .ok_or_else(|| corrupt("decoded size overflow"))?,
        )
        .ok_or_else(|| corrupt("decoded size overflow"))?;
    for _ in 0..count {
        d.u32()?;
        let variable = match d.u8()? {
            0 => 0,
            1 => {
                d.u8()?;
                0
            }
            2..=4 => {
                d.u64()?;
                0
            }
            5 | 6 => d.bytes()?.len(),
            _ => return Err(corrupt("unknown value tag")),
        };
        size = size
            .checked_add(variable)
            .ok_or_else(|| corrupt("decoded size overflow"))?;
    }
    d.finish()?;
    Ok(size)
}
