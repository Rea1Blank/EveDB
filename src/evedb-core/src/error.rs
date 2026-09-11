// SPDX-License-Identifier: AGPL-3.0-only

use std::{fmt, io};

/// An engine, validation, or storage failure.
#[derive(Debug)]
pub enum Error {
    /// The operating system rejected an I/O operation.
    Io(io::Error),
    /// A stored structure failed validation.
    Corrupt(String),
    /// A caller supplied an invalid schema or operation.
    Invalid(String),
    /// A table or entity does not exist.
    NotFound(String),
    /// An identifier or name already exists.
    AlreadyExists(String),
    /// Another database handle owns the data directory.
    Locked,
    /// The operation exceeded its deadline before commit crossed the WAL boundary.
    DeadlineExceeded,
    /// A transaction or read snapshot expired and released its retained resources.
    TransactionExpired,
    /// An admission limit was reached before the requested write was accepted.
    LimitExceeded {
        /// The bounded resource.
        resource: &'static str,
        /// Configured maximum (bytes or item count, according to resource).
        limit: usize,
    },
    /// A requested isolation level is not implemented; no transaction was started.
    UnsupportedIsolation(crate::IsolationLevel),
    /// A concurrent commit invalidated this transaction. Retry the whole transaction.
    Conflict {
        /// Conflicting table, or None for a catalog change.
        table: Option<crate::TableId>,
        /// Conflicting entity, or None for a catalog change.
        entity: Option<crate::EntityId>,
    },
    /// The requested version is outside the locally retained range.
    VersionUnavailable {
        /// The requested entity version.
        requested: u64,
        /// The first available version.
        first: u64,
        /// The last available version.
        last: u64,
    },
    /// A write may have committed; reopen before inspecting or retrying it.
    CommitUnknown(io::Error),
    /// A previous uncertain write requires reopening this handle.
    NeedsRecovery,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "storage I/O failed: {e}"),
            Self::Corrupt(s) => write!(f, "corrupt storage: {s}"),
            Self::Invalid(s) => write!(f, "invalid operation: {s}"),
            Self::NotFound(s) => write!(f, "not found: {s}"),
            Self::AlreadyExists(s) => write!(f, "already exists: {s}"),
            Self::Locked => f.write_str("the data directory is already open"),
            Self::DeadlineExceeded => f.write_str("operation deadline exceeded"),
            Self::TransactionExpired => f.write_str("transaction or snapshot expired"),
            Self::LimitExceeded { resource, limit } => {
                write!(f, "limit exceeded: {resource} (maximum {limit})")
            }
            Self::UnsupportedIsolation(level) => {
                write!(f, "unsupported isolation level: {level:?}")
            }
            Self::Conflict { table, entity } => write!(
                f,
                "transaction conflict at table {table:?}, entity {entity:?}; retry the whole transaction"
            ),
            Self::VersionUnavailable {
                requested,
                first,
                last,
            } => {
                write!(
                    f,
                    "version {requested} is unavailable; retained range is {first}..={last}"
                )
            }
            Self::CommitUnknown(e) => {
                write!(f, "commit outcome is unknown; reopen the database: {e}")
            }
            Self::NeedsRecovery => {
                f.write_str("reopen the database after the previous storage failure")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) | Self::CommitUnknown(e) => Some(e),
            _ => None,
        }
    }
}

impl Error {
    pub(crate) fn duplicate(&self) -> Self {
        match self {
            Self::Io(e) => Self::Io(io::Error::new(e.kind(), e.to_string())),
            Self::CommitUnknown(e) => Self::CommitUnknown(io::Error::new(e.kind(), e.to_string())),
            Self::Corrupt(s) => Self::Corrupt(s.clone()),
            Self::Invalid(s) => Self::Invalid(s.clone()),
            Self::NotFound(s) => Self::NotFound(s.clone()),
            Self::AlreadyExists(s) => Self::AlreadyExists(s.clone()),
            Self::Locked => Self::Locked,
            Self::DeadlineExceeded => Self::DeadlineExceeded,
            Self::TransactionExpired => Self::TransactionExpired,
            Self::LimitExceeded { resource, limit } => Self::LimitExceeded {
                resource,
                limit: *limit,
            },
            Self::UnsupportedIsolation(level) => Self::UnsupportedIsolation(*level),
            Self::Conflict { table, entity } => Self::Conflict {
                table: *table,
                entity: *entity,
            },
            Self::VersionUnavailable {
                requested,
                first,
                last,
            } => Self::VersionUnavailable {
                requested: *requested,
                first: *first,
                last: *last,
            },
            Self::NeedsRecovery => Self::NeedsRecovery,
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<crate::storage::PageError> for Error {
    fn from(value: crate::storage::PageError) -> Self {
        Self::Corrupt(value.to_string())
    }
}

/// The result of an EveDB operation.
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn corrupt(message: impl Into<String>) -> Error {
    Error::Corrupt(message.into())
}
