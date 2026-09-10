// SPDX-License-Identifier: AGPL-3.0-only

//! Experimental storage primitives. Byte formats may change before a stable release.

mod page;

pub use page::{MAX_RECORD_SIZE, PAGE_SIZE, PageError, SlottedPage};
