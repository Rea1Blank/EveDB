// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    Error, Result,
    codec::{Decoder, Encoder, MAX_RECORD},
    error::corrupt,
};
use std::collections::{BTreeMap, BTreeSet};

/// A stable table identifier.
pub type TableId = u64;
/// A caller-assigned entity identifier, scoped to one table.
pub type EntityId = u64;
/// Assignments keyed by stable field identifier.
pub type Fields = BTreeMap<u32, Value>;

/// A field's declared type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataType {
    /// A boolean.
    Bool,
    /// A signed 64-bit integer.
    Int64,
    /// An unsigned 64-bit integer.
    UInt64,
    /// A finite IEEE 754 double.
    Float64,
    /// UTF-8 text.
    Text,
    /// Arbitrary bytes.
    Bytes,
}

/// A typed field value. Null is distinct from an omitted assignment.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// Explicit null.
    Null,
    /// A boolean.
    Bool(bool),
    /// A signed integer.
    Int64(i64),
    /// An unsigned integer.
    UInt64(u64),
    /// A finite floating-point number.
    Float64(f64),
    /// UTF-8 text.
    Text(String),
    /// Arbitrary bytes.
    Bytes(Vec<u8>),
}

/// A field definition. IDs remain stable across schema versions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    /// A nonzero stable identifier.
    pub id: u32,
    /// A human-readable name.
    pub name: String,
    /// The declared value type.
    pub data_type: DataType,
    /// Whether explicit or default null is permitted.
    pub nullable: bool,
}

/// One version of a table's schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    /// Monotonically increasing version, starting at one.
    pub version: u64,
    /// Fields in logical schema order.
    pub fields: Vec<Field>,
}

impl Schema {
    /// Creates and validates version one of a schema.
    pub fn new(fields: Vec<Field>) -> Result<Self> {
        let schema = Self { version: 1, fields };
        schema.validate()?;
        Ok(schema)
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.version == 0 || self.fields.is_empty() || self.fields.len() > 4096 {
            return Err(Error::Invalid(
                "a schema needs a version and 1..=4096 fields".into(),
            ));
        }
        let mut ids = BTreeSet::new();
        let mut names = BTreeSet::new();
        for field in &self.fields {
            validate_name(&field.name)?;
            if field.id == 0 || !ids.insert(field.id) || !names.insert(&field.name) {
                return Err(Error::Invalid(
                    "field IDs and names must be unique and nonzero".into(),
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn normalize(&self, fields: &mut Fields, complete: bool) -> Result<()> {
        for (&id, value) in fields.iter() {
            let field = self
                .fields
                .iter()
                .find(|f| f.id == id)
                .ok_or_else(|| Error::Invalid(format!("unknown field {id}")))?;
            let valid = match (value, field.data_type) {
                (Value::Null, _) => field.nullable,
                (Value::Bool(_), DataType::Bool)
                | (Value::Int64(_), DataType::Int64)
                | (Value::UInt64(_), DataType::UInt64)
                | (Value::Text(_), DataType::Text)
                | (Value::Bytes(_), DataType::Bytes) => true,
                (Value::Float64(v), DataType::Float64) => v.is_finite(),
                _ => false,
            };
            if !valid {
                return Err(Error::Invalid(format!("wrong type or null for field {id}")));
            }
        }
        if complete {
            for field in &self.fields {
                if let std::collections::btree_map::Entry::Vacant(entry) = fields.entry(field.id) {
                    if !field.nullable {
                        return Err(Error::Invalid(format!("missing field {}", field.id)));
                    }
                    entry.insert(Value::Null);
                }
            }
        }
        let mut encoded = Encoder::default();
        encode_fields(fields, &mut encoded);
        if encoded.0.len() > MAX_RECORD / 2 {
            return Err(Error::Invalid("row exceeds 8 MiB".into()));
        }
        Ok(())
    }
}

pub(crate) fn validate_name(name: &str) -> Result<()> {
    if name.trim().is_empty() || name.len() > 255 || name.chars().any(char::is_control) {
        return Err(Error::Invalid(
            "names must contain 1..=255 UTF-8 bytes without control characters".into(),
        ));
    }
    Ok(())
}

/// A table definition with all schema versions needed by its history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    /// Stable table ID, independent of its name.
    pub id: TableId,
    /// Current table name.
    pub name: String,
    /// Schema history in increasing version order.
    pub schemas: Vec<Schema>,
}
impl Table {
    /// Returns the latest schema.
    pub fn schema(&self) -> &Schema {
        self.schemas.last().expect("validated schema history")
    }
    pub(crate) fn version(&self, version: u64) -> Result<&Schema> {
        self.schemas
            .iter()
            .find(|s| s.version == version)
            .ok_or_else(|| corrupt("unknown schema version"))
    }
}

/// An entity state at one version, including a deletion tombstone.
#[derive(Clone, Debug, PartialEq)]
pub struct Entity {
    /// Stable entity ID.
    pub id: EntityId,
    /// Entity version; creation establishes version zero.
    pub version: u64,
    /// Schema used to interpret these fields.
    pub schema_version: u64,
    /// Whether this state represents a deleted entity.
    pub deleted: bool,
    /// Complete typed state.
    pub fields: Fields,
}

/// The kind of a retained event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventKind {
    /// Assign new field values.
    Apply,
    /// End the entity's active life.
    Delete,
}

/// One committed step in an entity's history.
#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    /// Entity version produced by this event.
    pub version: u64,
    /// The shared transaction sequence number.
    pub transaction: u64,
    /// Schema version at the time of this event.
    pub schema_version: u64,
    /// Assignment or deletion.
    pub kind: EventKind,
    /// Assigned values; omitted fields retain their values.
    pub fields: Fields,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct EntityData {
    pub current: Entity,
    pub base: Entity,
    pub events: Vec<Event>,
    pub snapshots: BTreeMap<u64, Entity>,
}
impl EntityData {
    pub fn at(&self, table: &Table, version: u64, use_snapshots: bool) -> Result<Entity> {
        if version < self.base.version || version > self.current.version {
            return Err(Error::VersionUnavailable {
                requested: version,
                first: self.base.version,
                last: self.current.version,
            });
        }
        let mut state = if use_snapshots {
            self.snapshots
                .range(..=version)
                .next_back()
                .map(|(_, s)| s)
                .unwrap_or(&self.base)
                .clone()
        } else {
            self.base.clone()
        };
        for event in self.events.iter().filter(|e| e.version <= version) {
            if event.version > state.version {
                apply_event(&mut state, event, table)?;
            }
        }
        if state.version != version {
            return Err(corrupt("gap in entity history"));
        }
        Ok(state)
    }
    pub fn retain(&mut self, table: &Table, count: usize) -> Result<()> {
        let remove = self.events.len().saturating_sub(count);
        if remove == 0 {
            return Ok(());
        }
        let version = self.events[remove - 1].version;
        self.base = self.at(table, version, false)?;
        self.events.drain(..remove);
        self.snapshots.retain(|&v, _| v >= version);
        Ok(())
    }
}

pub(crate) fn apply_event(state: &mut Entity, event: &Event, table: &Table) -> Result<()> {
    if state.deleted || state.version.checked_add(1) != Some(event.version) {
        return Err(corrupt("invalid entity version or event after deletion"));
    }
    let schema = table.version(event.schema_version)?;
    let mut fields = state.fields.clone();
    fields.extend(event.fields.clone());
    schema.normalize(&mut fields, true)?;
    state.fields = fields;
    state.schema_version = event.schema_version;
    state.version = event.version;
    state.deleted = event.kind == EventKind::Delete;
    Ok(())
}

pub(crate) fn fields_encoded_len(fields: &Fields) -> usize {
    8 + fields
        .values()
        .map(|value| {
            5 + match value {
                Value::Null => 0,
                Value::Bool(_) => 1,
                Value::Text(text) => 8 + text.len(),
                Value::Bytes(bytes) => 8 + bytes.len(),
                _ => 8,
            }
        })
        .sum::<usize>()
}
pub(crate) fn encode_fields(fields: &Fields, e: &mut Encoder) {
    e.u64(fields.len() as u64);
    for (&id, value) in fields {
        e.u32(id);
        match value {
            Value::Null => e.u8(0),
            Value::Bool(v) => {
                e.u8(1);
                e.u8(u8::from(*v));
            }
            Value::Int64(v) => {
                e.u8(2);
                e.u64(*v as u64);
            }
            Value::UInt64(v) => {
                e.u8(3);
                e.u64(*v);
            }
            Value::Float64(v) => {
                e.u8(4);
                e.u64(v.to_bits());
            }
            Value::Text(v) => {
                e.u8(5);
                e.string(v);
            }
            Value::Bytes(v) => {
                e.u8(6);
                e.bytes(v);
            }
        }
    }
}
pub(crate) fn decode_fields(d: &mut Decoder<'_>) -> Result<Fields> {
    let count = d.count(5)?;
    if count > 4096 {
        return Err(corrupt("too many fields"));
    }
    let mut fields = Fields::new();
    for _ in 0..count {
        let id = d.u32()?;
        let value = match d.u8()? {
            0 => Value::Null,
            1 => match d.u8()? {
                0 => Value::Bool(false),
                1 => Value::Bool(true),
                _ => return Err(corrupt("invalid boolean")),
            },
            2 => Value::Int64(d.u64()? as i64),
            3 => Value::UInt64(d.u64()?),
            4 => {
                let v = f64::from_bits(d.u64()?);
                if !v.is_finite() {
                    return Err(corrupt("non-finite float"));
                }
                Value::Float64(v)
            }
            5 => Value::Text(d.string()?),
            6 => Value::Bytes(d.bytes()?.to_vec()),
            _ => return Err(corrupt("unknown value type")),
        };
        if fields.insert(id, value).is_some() {
            return Err(corrupt("duplicate field"));
        }
    }
    Ok(fields)
}
pub(crate) fn encode_entity(value: &Entity) -> Vec<u8> {
    let mut e = Encoder::default();
    e.u64(value.id);
    e.u64(value.version);
    e.u64(value.schema_version);
    e.u8(u8::from(value.deleted));
    encode_fields(&value.fields, &mut e);
    e.0
}
pub(crate) fn decode_entity(bytes: &[u8]) -> Result<Entity> {
    let mut d = Decoder::new(bytes);
    let id = d.u64()?;
    let version = d.u64()?;
    let schema_version = d.u64()?;
    let deleted = match d.u8()? {
        0 => false,
        1 => true,
        _ => return Err(corrupt("invalid tombstone")),
    };
    let fields = decode_fields(&mut d)?;
    d.finish()?;
    Ok(Entity {
        id,
        version,
        schema_version,
        deleted,
        fields,
    })
}
pub(crate) fn encode_event(value: &Event) -> Vec<u8> {
    let mut e = Encoder::default();
    e.u64(value.version);
    e.u64(value.transaction);
    e.u64(value.schema_version);
    e.u8(match value.kind {
        EventKind::Apply => 0,
        EventKind::Delete => 1,
    });
    encode_fields(&value.fields, &mut e);
    e.0
}
pub(crate) fn decode_event(bytes: &[u8]) -> Result<Event> {
    let mut d = Decoder::new(bytes);
    let version = d.u64()?;
    let transaction = d.u64()?;
    let schema_version = d.u64()?;
    let kind = match d.u8()? {
        0 => EventKind::Apply,
        1 => EventKind::Delete,
        _ => return Err(corrupt("invalid event kind")),
    };
    let fields = decode_fields(&mut d)?;
    d.finish()?;
    Ok(Event {
        version,
        transaction,
        schema_version,
        kind,
        fields,
    })
}
pub(crate) fn encode_table(table: &Table, e: &mut Encoder) {
    e.u64(table.id);
    e.string(&table.name);
    e.u64(table.schemas.len() as u64);
    for schema in &table.schemas {
        e.u64(schema.version);
        e.u64(schema.fields.len() as u64);
        for field in &schema.fields {
            e.u32(field.id);
            e.string(&field.name);
            e.u8(match field.data_type {
                DataType::Bool => 0,
                DataType::Int64 => 1,
                DataType::UInt64 => 2,
                DataType::Float64 => 3,
                DataType::Text => 4,
                DataType::Bytes => 5,
            });
            e.u8(u8::from(field.nullable));
        }
    }
}
pub(crate) fn decode_table(d: &mut Decoder<'_>) -> Result<Table> {
    let id = d.u64()?;
    let name = d.string()?;
    let count = d.count(16)?;
    let mut schemas = Vec::new();
    for _ in 0..count {
        let version = d.u64()?;
        let n = d.count(14)?;
        let mut fields = Vec::new();
        for _ in 0..n {
            let id = d.u32()?;
            let name = d.string()?;
            let data_type = match d.u8()? {
                0 => DataType::Bool,
                1 => DataType::Int64,
                2 => DataType::UInt64,
                3 => DataType::Float64,
                4 => DataType::Text,
                5 => DataType::Bytes,
                _ => return Err(corrupt("invalid field type")),
            };
            let nullable = match d.u8()? {
                0 => false,
                1 => true,
                _ => return Err(corrupt("invalid nullability")),
            };
            fields.push(Field {
                id,
                name,
                data_type,
                nullable,
            });
        }
        let schema = Schema { version, fields };
        schema.validate().map_err(|e| corrupt(e.to_string()))?;
        if version != schemas.len() as u64 + 1 {
            return Err(corrupt("nonsequential schema versions"));
        }
        schemas.push(schema);
    }
    if id == 0 || schemas.is_empty() {
        return Err(corrupt("invalid table"));
    }
    validate_name(&name).map_err(|e| corrupt(e.to_string()))?;
    Ok(Table { id, name, schemas })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_codec_preserves_extreme_values_null_and_omission() {
        let fields: Fields = [
            (1, Value::Bool(true)),
            (2, Value::Int64(i64::MIN)),
            (3, Value::UInt64(u64::MAX)),
            (4, Value::Float64(-1.25)),
            (5, Value::Text("EveDB \u{1f4be}".into())),
            (6, Value::Bytes(vec![0, 255, 0])),
            (7, Value::Null),
        ]
        .into();
        let entity = Entity {
            id: u64::MAX,
            version: 3,
            schema_version: 2,
            deleted: false,
            fields: fields.clone(),
        };
        assert_eq!(decode_entity(&encode_entity(&entity)).unwrap(), entity);
        let event = Event {
            version: 4,
            transaction: 9,
            schema_version: 2,
            kind: EventKind::Apply,
            fields: [(7, Value::Null)].into(),
        };
        let decoded = decode_event(&encode_event(&event)).unwrap();
        assert_eq!(decoded, event);
        assert!(!decoded.fields.contains_key(&1));
        let mut encoded = encode_entity(&entity);
        encoded.pop();
        assert!(decode_entity(&encoded).is_err());
    }

    #[test]
    fn schema_codec_and_validation_reject_incompatible_values() {
        let schema = Schema::new(vec![
            Field {
                id: 5,
                name: "name".into(),
                data_type: DataType::Text,
                nullable: false,
            },
            Field {
                id: 9,
                name: "score".into(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ])
        .unwrap();
        let table = Table {
            id: 1,
            name: "items".into(),
            schemas: vec![schema.clone()],
        };
        let mut e = Encoder::default();
        encode_table(&table, &mut e);
        let mut d = Decoder::new(&e.0);
        assert_eq!(decode_table(&mut d).unwrap(), table);
        d.finish().unwrap();
        let mut fields = [(5, Value::Text("item".into()))].into();
        schema.normalize(&mut fields, true).unwrap();
        assert_eq!(fields[&9], Value::Null);
        for bad in [Value::Null, Value::Int64(3)] {
            assert!(schema.normalize(&mut [(5, bad)].into(), false).is_err());
        }
        assert!(
            schema
                .normalize(&mut [(9, Value::Float64(f64::NAN))].into(), false)
                .is_err()
        );
        assert!(Schema::new(vec![schema.fields[0].clone(), schema.fields[0].clone()]).is_err());
    }
}
