// SPDX-License-Identifier: AGPL-3.0-only

//! Page primitives used by the experimental local storage engine.
//!
//! These APIs and byte formats may change before a stable storage release.

pub(crate) mod frame;
pub(crate) mod heap;
pub(crate) mod index;
mod page;
pub(crate) mod pager;

pub use page::{MAX_RECORD_SIZE, PAGE_SIZE, PageError, SlottedPage};
