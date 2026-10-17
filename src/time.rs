//! Market time helpers. NEMWeb timestamps are market time (AEST, UTC+10, no
//! daylight saving). SETTLEMENTDATE marks the END of a 5-minute interval.

use chrono::{DateTime, Duration, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;

pub fn market_offset() -> FixedOffset {
    FixedOffset::east_opt(10 * 3600).expect("valid offset")
}

pub fn to_utc(market: NaiveDateTime) -> DateTime<Utc> {
    market_offset()
        .from_local_datetime(&market)
        .single()
        .expect("fixed offset is unambiguous")
        .with_timezone(&Utc)
}

pub fn now_market() -> NaiveDateTime {
    Utc::now().with_timezone(&market_offset()).naive_local()
}

/// ISO 8601 with the market offset, e.g. 2026-10-12T14:35:00+10:00.
pub fn iso_market(t: NaiveDateTime) -> String {
    format!("{}+10:00", t.format("%Y-%m-%dT%H:%M:%S"))
}

pub fn parse_market(s: &str) -> Option<NaiveDateTime> {
    let s = s.trim().trim_matches('"');
    NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(s.trim_end_matches("+10:00"), "%Y-%m-%dT%H:%M:%S")
        })
        .ok()
}

/// The NEM trading day runs 04:00 to 04:00. The interval ending 04:00 belongs
/// to the previous day.
pub fn trading_day(interval_end: NaiveDateTime) -> NaiveDate {
    let four = NaiveTime::from_hms_opt(4, 0, 0).expect("valid time");
    if interval_end.time() <= four {
        interval_end.date() - Duration::days(1)
    } else {
        interval_end.date()
    }
}

pub fn region_tz(region: &str) -> Tz {
    match region {
        "NSW1" => chrono_tz::Australia::Sydney,
        "VIC1" => chrono_tz::Australia::Melbourne,
        "SA1" => chrono_tz::Australia::Adelaide,
        "TAS1" => chrono_tz::Australia::Hobart,
        _ => chrono_tz::Australia::Brisbane,
    }
}

pub fn region_name(region: &str) -> &str {
    match region {
        "NSW1" => "NSW",
        "VIC1" => "Victoria",
        "QLD1" => "Queensland",
        "SA1" => "South Australia",
        "TAS1" => "Tasmania",
        other => other,
    }
}

/// Local clock time for the public, e.g. "3:35pm AEDT".
pub fn local_label(t: NaiveDateTime, region: &str) -> String {
    let local = to_utc(t).with_timezone(&region_tz(region));
    local.format("%-I:%M%P %Z").to_string()
}

pub fn local_date_label(t: NaiveDateTime, region: &str) -> String {
    let local = to_utc(t).with_timezone(&region_tz(region));
    local.format("%a %-d %b %Y").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> NaiveDateTime {
        parse_market(s).unwrap()
    }

    #[test]
    fn trading_day_boundary() {
        assert_eq!(
            trading_day(t("2026/10/13 04:00:00")).to_string(),
            "2026-10-12"
        );
        assert_eq!(
            trading_day(t("2026/10/13 04:05:00")).to_string(),
            "2026-10-13"
        );
        assert_eq!(
            trading_day(t("2026/10/12 15:35:00")).to_string(),
            "2026-10-12"
        );
    }

    #[test]
    fn local_time_uses_daylight_saving() {
        // 14:30 market time on 12 Oct 2026 is 3:30pm AEDT in Sydney, 2:30pm in Brisbane.
        assert_eq!(local_label(t("2026/10/12 14:30:00"), "NSW1"), "3:30pm AEDT");
        assert_eq!(local_label(t("2026/10/12 14:30:00"), "QLD1"), "2:30pm AEST");
        assert_eq!(local_label(t("2026/10/12 14:30:00"), "SA1"), "3:00pm ACDT");
    }
}
