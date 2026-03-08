//! Oracle implementation of SettingsStore.

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use super::OracleBackend;
use crate::db::SettingsStore;
use crate::error::DatabaseError;
use crate::history::SettingRow;

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
impl SettingsStore for OracleBackend {
    async fn get_setting(
        &self,
        user_id: &str,
        key: &str,
    ) -> Result<Option<serde_json::Value>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();
        let key = key.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let rows = conn
                .query(
                    "SELECT value FROM IRON_SETTINGS WHERE user_id = :1 AND key = :2",
                    &[&user_id, &key],
                )
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let value_str: String = row.get(0).unwrap_or_default();
                let value: serde_json::Value =
                    serde_json::from_str(&value_str).unwrap_or(serde_json::Value::Null);
                return Ok(Some(value));
            }
            Ok(None)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn get_setting_full(
        &self,
        user_id: &str,
        key: &str,
    ) -> Result<Option<SettingRow>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();
        let key = key.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let rows = conn
                .query(
                    "SELECT key, value, TO_CHAR(updated_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS updated_at
                     FROM IRON_SETTINGS WHERE user_id = :1 AND key = :2",
                    &[&user_id, &key],
                )
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let key: String = row.get(0).unwrap_or_default();
                let value_str: String = row.get(1).unwrap_or_default();
                let updated_at_str: String = row.get(2).unwrap_or_default();
                return Ok(Some(SettingRow {
                    key,
                    value: serde_json::from_str(&value_str)
                        .unwrap_or(serde_json::Value::Null),
                    updated_at: parse_ts(&updated_at_str),
                }));
            }
            Ok(None)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn set_setting(
        &self,
        user_id: &str,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();
        let key = key.to_string();
        let value_str = value.to_string();
        let now = fmt_ts(&Utc::now());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            // MERGE INTO for upsert
            conn.execute(
                "MERGE INTO IRON_SETTINGS t
                USING (SELECT :1 AS user_id, :2 AS key FROM DUAL) src
                ON (t.user_id = src.user_id AND t.key = src.key)
                WHEN MATCHED THEN
                    UPDATE SET value = :3, updated_at = TO_TIMESTAMP(:4, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"')
                WHEN NOT MATCHED THEN
                    INSERT (user_id, key, value, updated_at)
                    VALUES (:1, :2, :3, TO_TIMESTAMP(:4, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))",
                &[&user_id, &key, &value_str, &now],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))??;
        Ok(())
    }

    async fn delete_setting(&self, user_id: &str, key: &str) -> Result<bool, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();
        let key = key.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let stmt = conn
                .execute(
                    "DELETE FROM IRON_SETTINGS WHERE user_id = :1 AND key = :2",
                    &[&user_id, &key],
                )
                .map_err(|e| DatabaseError::Query(e.to_string()))?;
            let count = stmt.row_count().map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(count > 0)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn list_settings(&self, user_id: &str) -> Result<Vec<SettingRow>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let rows = conn
                .query(
                    "SELECT key, value, TO_CHAR(updated_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS updated_at
                     FROM IRON_SETTINGS WHERE user_id = :1 ORDER BY key",
                    &[&user_id],
                )
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            let mut settings = Vec::new();
            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let key: String = row.get(0).unwrap_or_default();
                let value_str: String = row.get(1).unwrap_or_default();
                let updated_at_str: String = row.get(2).unwrap_or_default();
                settings.push(SettingRow {
                    key,
                    value: serde_json::from_str(&value_str)
                        .unwrap_or(serde_json::Value::Null),
                    updated_at: parse_ts(&updated_at_str),
                });
            }
            Ok(settings)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn get_all_settings(
        &self,
        user_id: &str,
    ) -> Result<HashMap<String, serde_json::Value>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let rows = conn
                .query(
                    "SELECT key, value FROM IRON_SETTINGS WHERE user_id = :1",
                    &[&user_id],
                )
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            let mut map = HashMap::new();
            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let key: String = row.get(0).unwrap_or_default();
                let value_str: String = row.get(1).unwrap_or_default();
                let value: serde_json::Value =
                    serde_json::from_str(&value_str).unwrap_or(serde_json::Value::Null);
                map.insert(key, value);
            }
            Ok(map)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn set_all_settings(
        &self,
        user_id: &str,
        settings: &HashMap<String, serde_json::Value>,
    ) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();
        // Clone the map to own the data for the blocking closure
        let settings: Vec<(String, String)> = settings
            .iter()
            .map(|(k, v)| (k.clone(), v.to_string()))
            .collect();
        let now = fmt_ts(&Utc::now());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;

            for (key, value_str) in &settings {
                conn.execute(
                    "MERGE INTO IRON_SETTINGS t
                    USING (SELECT :1 AS user_id, :2 AS key FROM DUAL) src
                    ON (t.user_id = src.user_id AND t.key = src.key)
                    WHEN MATCHED THEN
                        UPDATE SET value = :3, updated_at = TO_TIMESTAMP(:4, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"')
                    WHEN NOT MATCHED THEN
                        INSERT (user_id, key, value, updated_at)
                        VALUES (:1, :2, :3, TO_TIMESTAMP(:4, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))",
                    &[&user_id as &dyn oracle::sql_type::ToSql, key, value_str, &now],
                )
                .map_err(|e| DatabaseError::Query(e.to_string()))?;
            }

            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))??;
        Ok(())
    }

    async fn has_settings(&self, user_id: &str) -> Result<bool, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let rows = conn
                .query(
                    "SELECT COUNT(*) FROM IRON_SETTINGS WHERE user_id = :1",
                    &[&user_id],
                )
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let count: i64 = row.get(0).unwrap_or(0);
                return Ok(count > 0);
            }
            Ok(false)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }
}
