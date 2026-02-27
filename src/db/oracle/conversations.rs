//! Oracle implementation of ConversationStore.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::OracleBackend;
use crate::db::ConversationStore;
use crate::error::DatabaseError;
use crate::history::{ConversationMessage, ConversationSummary};

/// Parse an ISO-8601 / Oracle TIMESTAMP string into DateTime<Utc>.
fn parse_ts(s: &str) -> DateTime<Utc> {
    // Try RFC-3339 first, then common Oracle formats
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return dt.with_timezone(&Utc);
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return ndt.and_utc();
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return ndt.and_utc();
    }
    // Oracle TIMESTAMP default format: DD-MON-RR HH.MI.SS.FF AM
    // We won't encounter it if we always write ISO strings; fall back to epoch.
    DateTime::UNIX_EPOCH
}

fn fmt_ts(dt: &DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[async_trait]
impl ConversationStore for OracleBackend {
    async fn create_conversation(
        &self,
        channel: &str,
        user_id: &str,
        thread_id: Option<&str>,
    ) -> Result<Uuid, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id = Uuid::new_v4();
        let id_str = id.to_string();
        let channel = channel.to_string();
        let user_id = user_id.to_string();
        let thread_id = thread_id.map(String::from);

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "INSERT INTO IRON_CONVERSATIONS (id, channel, user_id, thread_id) VALUES (:1, :2, :3, :4)",
                &[&id_str, &channel, &user_id, &thread_id as &dyn oracle::sql_type::ToSql],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))??;
        Ok(id)
    }

    async fn touch_conversation(&self, id: Uuid) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();
        let now = fmt_ts(&Utc::now());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "UPDATE IRON_CONVERSATIONS SET last_activity = TO_TIMESTAMP(:2, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') WHERE id = :1",
                &[&id_str, &now],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn add_conversation_message(
        &self,
        conversation_id: Uuid,
        role: &str,
        content: &str,
    ) -> Result<Uuid, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id = Uuid::new_v4();
        let id_str = id.to_string();
        let conv_id = conversation_id.to_string();
        let role = role.to_string();
        let content = content.to_string();
        let now = fmt_ts(&Utc::now());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "INSERT INTO IRON_MESSAGES (id, conversation_id, role, content, created_at) \
                 VALUES (:1, :2, :3, :4, TO_TIMESTAMP(:5, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))",
                &[&id_str, &conv_id, &role, &content, &now],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            // Also touch conversation
            conn.execute(
                "UPDATE IRON_CONVERSATIONS SET last_activity = TO_TIMESTAMP(:2, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') WHERE id = :1",
                &[&conv_id, &now],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))??;
        Ok(id)
    }

    async fn ensure_conversation(
        &self,
        id: Uuid,
        channel: &str,
        user_id: &str,
        thread_id: Option<&str>,
    ) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();
        let channel = channel.to_string();
        let user_id = user_id.to_string();
        let thread_id = thread_id.map(String::from);
        let now = fmt_ts(&Utc::now());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "MERGE INTO IRON_CONVERSATIONS c \
                 USING (SELECT :1 AS id FROM DUAL) src \
                 ON (c.id = src.id) \
                 WHEN NOT MATCHED THEN \
                     INSERT (id, channel, user_id, thread_id) VALUES (:1, :2, :3, :4) \
                 WHEN MATCHED THEN \
                     UPDATE SET c.last_activity = TO_TIMESTAMP(:5, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"')",
                &[&id_str, &channel, &user_id, &thread_id as &dyn oracle::sql_type::ToSql, &now],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn list_conversations_with_preview(
        &self,
        user_id: &str,
        channel: &str,
        limit: i64,
    ) -> Result<Vec<ConversationSummary>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();
        let channel = channel.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = "SELECT \
                c.id, \
                TO_CHAR(c.started_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS started_at, \
                TO_CHAR(c.last_activity, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS last_activity, \
                c.metadata, \
                (SELECT COUNT(*) FROM IRON_MESSAGES m WHERE m.conversation_id = c.id AND m.role = 'user') AS message_count, \
                (SELECT SUBSTR(m2.content, 1, 100) FROM IRON_MESSAGES m2 \
                 WHERE m2.conversation_id = c.id AND m2.role = 'user' \
                 ORDER BY m2.created_at ASC FETCH FIRST 1 ROW ONLY) AS title \
                FROM IRON_CONVERSATIONS c \
                WHERE c.user_id = :1 AND c.channel = :2 \
                ORDER BY c.last_activity DESC \
                FETCH FIRST :3 ROWS ONLY";

            let rows = conn.query(sql, &[&user_id, &channel, &limit])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            let mut results = Vec::new();
            for row_result in rows {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let id_str: String = row.get(0).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let started_at_str: String = row.get(1).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let last_activity_str: String = row.get(2).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let metadata_str: Option<String> = row.get(3).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let message_count: i64 = row.get(4).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let title: Option<String> = row.get(5).map_err(|e| DatabaseError::Query(e.to_string()))?;

                let metadata: serde_json::Value = metadata_str
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or(serde_json::Value::Null);
                let thread_type = metadata
                    .get("thread_type")
                    .and_then(|v| v.as_str())
                    .map(String::from);

                results.push(ConversationSummary {
                    id: id_str.parse().unwrap_or_default(),
                    started_at: parse_ts(&started_at_str),
                    last_activity: parse_ts(&last_activity_str),
                    message_count,
                    title,
                    thread_type,
                });
            }
            Ok::<_, DatabaseError>(results)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn get_or_create_assistant_conversation(
        &self,
        user_id: &str,
        channel: &str,
    ) -> Result<Uuid, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();
        let channel = channel.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;

            // Try to find existing assistant conversation
            let sql = "SELECT id FROM IRON_CONVERSATIONS \
                       WHERE user_id = :1 AND channel = :2 \
                       AND JSON_VALUE(metadata, '$.thread_type') = 'assistant' \
                       FETCH FIRST 1 ROW ONLY";
            let rows = conn.query(sql, &[&user_id, &channel])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;
            for row_result in rows {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let id_str: String = row.get(0).map_err(|e| DatabaseError::Query(e.to_string()))?;
                return id_str.parse()
                    .map_err(|_| DatabaseError::Serialization("Invalid UUID".to_string()));
            }

            // Create new
            let id = Uuid::new_v4();
            let id_str = id.to_string();
            let metadata = serde_json::json!({"thread_type": "assistant", "title": "Assistant"}).to_string();
            conn.execute(
                "INSERT INTO IRON_CONVERSATIONS (id, channel, user_id, metadata) VALUES (:1, :2, :3, :4)",
                &[&id_str, &channel, &user_id, &metadata],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(id)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn create_conversation_with_metadata(
        &self,
        channel: &str,
        user_id: &str,
        metadata: &serde_json::Value,
    ) -> Result<Uuid, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id = Uuid::new_v4();
        let id_str = id.to_string();
        let channel = channel.to_string();
        let user_id = user_id.to_string();
        let metadata_str = metadata.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "INSERT INTO IRON_CONVERSATIONS (id, channel, user_id, metadata) VALUES (:1, :2, :3, :4)",
                &[&id_str, &channel, &user_id, &metadata_str],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))??;
        Ok(id)
    }

    async fn list_conversation_messages_paginated(
        &self,
        conversation_id: Uuid,
        before: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<(Vec<ConversationMessage>, bool), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let cid = conversation_id.to_string();
        let before_str = before.map(|dt| fmt_ts(&dt));
        let fetch_limit = limit + 1;

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;

            let rows = if let Some(ref before_ts) = before_str {
                let sql = "SELECT id, role, content, TO_CHAR(created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS created_at \
                           FROM IRON_MESSAGES \
                           WHERE conversation_id = :1 AND created_at < TO_TIMESTAMP(:2, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') \
                           ORDER BY created_at DESC \
                           FETCH FIRST :3 ROWS ONLY";
                conn.query(sql, &[&cid, before_ts, &fetch_limit])
                    .map_err(|e| DatabaseError::Query(e.to_string()))?
            } else {
                let sql = "SELECT id, role, content, TO_CHAR(created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS created_at \
                           FROM IRON_MESSAGES \
                           WHERE conversation_id = :1 \
                           ORDER BY created_at DESC \
                           FETCH FIRST :2 ROWS ONLY";
                conn.query(sql, &[&cid, &fetch_limit])
                    .map_err(|e| DatabaseError::Query(e.to_string()))?
            };

            let mut all = Vec::new();
            for row_result in rows {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let id_str: String = row.get(0).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let role: String = row.get(1).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let content: String = row.get(2).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let ts_str: String = row.get(3).map_err(|e| DatabaseError::Query(e.to_string()))?;
                all.push(ConversationMessage {
                    id: id_str.parse().unwrap_or_default(),
                    role,
                    content,
                    created_at: parse_ts(&ts_str),
                });
            }

            let has_more = all.len() as i64 > limit;
            all.truncate(limit as usize);
            all.reverse(); // oldest first
            Ok::<_, DatabaseError>((all, has_more))
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn update_conversation_metadata_field(
        &self,
        id: Uuid,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();
        let key = key.to_string();
        let value = value.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;

            // Read current metadata, merge the key, write back
            let sql = "SELECT metadata FROM IRON_CONVERSATIONS WHERE id = :1";
            let rows = conn.query(sql, &[&id_str])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;
            let mut current: serde_json::Value = serde_json::json!({});
            for row_result in rows {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let meta_str: Option<String> = row.get(0).map_err(|e| DatabaseError::Query(e.to_string()))?;
                if let Some(s) = meta_str {
                    current = serde_json::from_str(&s).unwrap_or(serde_json::json!({}));
                }
            }
            if let serde_json::Value::Object(ref mut map) = current {
                map.insert(key, value);
            }
            let new_meta = current.to_string();
            conn.execute(
                "UPDATE IRON_CONVERSATIONS SET metadata = :2 WHERE id = :1",
                &[&id_str, &new_meta],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn get_conversation_metadata(
        &self,
        id: Uuid,
    ) -> Result<Option<serde_json::Value>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = "SELECT metadata FROM IRON_CONVERSATIONS WHERE id = :1";
            let rows = conn.query(sql, &[&id_str])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;
            for row_result in rows {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let meta_str: Option<String> = row.get(0).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let val = meta_str
                    .and_then(|s| serde_json::from_str(&s).ok())
                    .unwrap_or(serde_json::Value::Null);
                return Ok(Some(val));
            }
            Ok::<_, DatabaseError>(None)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn list_conversation_messages(
        &self,
        conversation_id: Uuid,
    ) -> Result<Vec<ConversationMessage>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let cid = conversation_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = "SELECT id, role, content, TO_CHAR(created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS created_at \
                       FROM IRON_MESSAGES \
                       WHERE conversation_id = :1 \
                       ORDER BY created_at ASC";
            let rows = conn.query(sql, &[&cid])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            let mut messages = Vec::new();
            for row_result in rows {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let id_str: String = row.get(0).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let role: String = row.get(1).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let content: String = row.get(2).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let ts_str: String = row.get(3).map_err(|e| DatabaseError::Query(e.to_string()))?;
                messages.push(ConversationMessage {
                    id: id_str.parse().unwrap_or_default(),
                    role,
                    content,
                    created_at: parse_ts(&ts_str),
                });
            }
            Ok::<_, DatabaseError>(messages)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn conversation_belongs_to_user(
        &self,
        conversation_id: Uuid,
        user_id: &str,
    ) -> Result<bool, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let cid = conversation_id.to_string();
        let user_id = user_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = "SELECT COUNT(*) FROM IRON_CONVERSATIONS WHERE id = :1 AND user_id = :2";
            let rows = conn.query(sql, &[&cid, &user_id])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;
            for row_result in rows {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let count: i64 = row.get(0).map_err(|e| DatabaseError::Query(e.to_string()))?;
                return Ok(count > 0);
            }
            Ok::<_, DatabaseError>(false)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }
}
