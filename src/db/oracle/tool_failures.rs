//! Oracle implementation of ToolFailureStore.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use super::OracleBackend;
use crate::agent::BrokenTool;
use crate::db::ToolFailureStore;
use crate::error::DatabaseError;

/// Parse an ISO-8601 / Oracle TIMESTAMP string into DateTime<Utc>.
fn parse_ts(s: &str) -> DateTime<Utc> {
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

fn fmt_ts(dt: &DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[async_trait]
impl ToolFailureStore for OracleBackend {
    async fn record_tool_failure(
        &self,
        tool_name: &str,
        error_message: &str,
    ) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let tool_name = tool_name.to_string();
        let error_message = error_message.to_string();
        let now = fmt_ts(&Utc::now());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            // MERGE INTO for upsert: insert new or increment failure_count
            conn.execute(
                "MERGE INTO IRON_TOOL_FAILURES t
                USING (SELECT :1 AS tool_name FROM DUAL) src
                ON (t.tool_name = src.tool_name)
                WHEN MATCHED THEN
                    UPDATE SET
                        error_message = :2,
                        failure_count = t.failure_count + 1,
                        last_failure = TO_TIMESTAMP(:3, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"')
                WHEN NOT MATCHED THEN
                    INSERT (tool_name, error_message, failure_count, first_failure, last_failure)
                    VALUES (:1, :2, 1,
                        TO_TIMESTAMP(:3, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),
                        TO_TIMESTAMP(:3, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"')
                    )",
                &[&tool_name, &error_message, &now],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))??;
        Ok(())
    }

    async fn get_broken_tools(&self, threshold: i32) -> Result<Vec<BrokenTool>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let threshold = threshold as i64;

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let rows = conn
                .query(
                    "SELECT tool_name, error_message, failure_count,
                        TO_CHAR(first_failure, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS first_failure,
                        TO_CHAR(last_failure, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS last_failure,
                        repair_attempts
                    FROM IRON_TOOL_FAILURES
                    WHERE failure_count >= :1 AND (repaired = 0 OR repaired IS NULL)
                    ORDER BY failure_count DESC",
                    &[&threshold],
                )
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            let mut tools = Vec::new();
            for row_result in rows {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let name: String = row.get(0).unwrap_or_default();
                let last_error: Option<String> = row.get(1).unwrap_or(None);
                let failure_count: i64 = row.get(2).unwrap_or(0);
                let first_failure_str: String = row.get(3).unwrap_or_default();
                let last_failure_str: String = row.get(4).unwrap_or_default();
                let repair_attempts: i64 = row.get(5).unwrap_or(0);

                tools.push(BrokenTool {
                    name,
                    last_error,
                    failure_count: failure_count as u32,
                    first_failure: parse_ts(&first_failure_str),
                    last_failure: parse_ts(&last_failure_str),
                    last_build_result: None, // Not stored in IRON_TOOL_FAILURES schema
                    repair_attempts: repair_attempts as u32,
                });
            }
            Ok(tools)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn mark_tool_repaired(&self, tool_name: &str) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let tool_name = tool_name.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "UPDATE IRON_TOOL_FAILURES SET repaired = 1, failure_count = 0 WHERE tool_name = :1",
                &[&tool_name],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))??;
        Ok(())
    }

    async fn increment_repair_attempts(&self, tool_name: &str) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let tool_name = tool_name.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "UPDATE IRON_TOOL_FAILURES SET repair_attempts = repair_attempts + 1 WHERE tool_name = :1",
                &[&tool_name],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))??;
        Ok(())
    }
}
