//! Market time helpers, from the shared `nem-lens-aemo` crate, plus the
//! system clock (which the pure crate deliberately does not read).

use chrono::{NaiveDateTime, Utc};
pub use nem_lens_aemo::time::*;

pub fn now_market() -> NaiveDateTime {
    from_utc(Utc::now())
}
