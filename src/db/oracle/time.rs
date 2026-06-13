//! Shared timestamp and vector-literal helpers for the Oracle backend.
//!
//! These were previously copy-pasted (with subtly drifting signatures) across
//! every `src/db/oracle/*.rs` store. Consolidating them here keeps timestamp
//! parsing/formatting and vector serialization consistent across all stores.

use chrono::{DateTime, Utc};

/// Parse an ISO-8601 / Oracle TIMESTAMP string into `DateTime<Utc>`.
///
/// Tries RFC-3339 first, then the two naive formats Oracle's `TO_CHAR`
/// produces; falls back to the Unix epoch if none match (timestamps are always
/// written as ISO strings via [`fmt_ts`], so the fallback should be unreachable
/// in practice).
pub fn parse_ts(s: &str) -> DateTime<Utc> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return dt.with_timezone(&Utc);
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return ndt.and_utc();
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return ndt.and_utc();
    }
    DateTime::UNIX_EPOCH
}

/// Parse an optional timestamp string: `Some(s)` parses, `None` stays `None`.
///
/// Note: a present-but-unparseable value yields `Some(UNIX_EPOCH)` (it does not
/// collapse to `None`); use [`parse_opt_ts_str`] when empty/epoch values should
/// be treated as absent.
pub fn parse_opt_ts(s: Option<&str>) -> Option<DateTime<Utc>> {
    s.map(parse_ts)
}

/// Parse a possibly-empty timestamp string, treating empty or epoch-fallback
/// values as absent (`None`).
pub fn parse_opt_ts_str(s: &str) -> Option<DateTime<Utc>> {
    if s.is_empty() {
        return None;
    }
    let ts = parse_ts(s);
    if ts == DateTime::UNIX_EPOCH {
        None
    } else {
        Some(ts)
    }
}

/// Format a `DateTime<Utc>` as an RFC-3339 string with millisecond precision.
pub fn fmt_ts(dt: &DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Serialize an embedding into the `"[f1,f2,...]"` literal Oracle AI Vector
/// Search expects for `VECTOR` columns / `VECTOR_DISTANCE()`.
pub fn vec_literal(emb: &[f32]) -> String {
    format!(
        "[{}]",
        emb.iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(",")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_then_parse_roundtrips() {
        let now = Utc::now();
        let s = fmt_ts(&now);
        let parsed = parse_ts(&s);
        // RFC-3339 with millisecond precision: equal to the millisecond.
        assert_eq!(
            parsed.timestamp_millis(),
            now.timestamp_millis(),
            "round-trip lost precision"
        );
    }

    #[test]
    fn parse_ts_accepts_naive_formats() {
        let with_frac = parse_ts("2024-01-02 03:04:05.678");
        assert_eq!(with_frac.timestamp_millis(), 1704164645678);
        let no_frac = parse_ts("2024-01-02 03:04:05");
        assert_eq!(no_frac.timestamp(), 1704164645);
    }

    #[test]
    fn parse_ts_unknown_falls_back_to_epoch() {
        assert_eq!(parse_ts("not a timestamp"), DateTime::UNIX_EPOCH);
    }

    #[test]
    fn parse_opt_ts_maps_present_only() {
        assert_eq!(parse_opt_ts(None), None);
        let s = "2024-01-02T03:04:05.678Z";
        assert_eq!(parse_opt_ts(Some(s)), Some(parse_ts(s)));
        // present-but-unparseable stays Some(epoch)
        assert_eq!(parse_opt_ts(Some("garbage")), Some(DateTime::UNIX_EPOCH));
    }

    #[test]
    fn parse_opt_ts_str_filters_empty_and_epoch() {
        assert_eq!(parse_opt_ts_str(""), None);
        assert_eq!(parse_opt_ts_str("garbage"), None);
        let s = "2024-01-02T03:04:05.678Z";
        assert_eq!(parse_opt_ts_str(s), Some(parse_ts(s)));
    }

    #[test]
    fn vec_literal_formats_brackets_and_commas() {
        assert_eq!(vec_literal(&[1.0, 2.5, -3.0]), "[1,2.5,-3]");
        assert_eq!(vec_literal(&[]), "[]");
    }
}
