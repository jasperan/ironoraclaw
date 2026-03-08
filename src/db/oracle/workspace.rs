//! Oracle implementation of WorkspaceStore.

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::OracleBackend;
use crate::db::WorkspaceStore;
use crate::error::WorkspaceError;
use crate::workspace::{
    MemoryChunk, MemoryDocument, RankedResult, SearchConfig, SearchResult, WorkspaceEntry,
    reciprocal_rank_fusion,
};

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

fn parse_opt_ts(s: &str) -> Option<DateTime<Utc>> {
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

fn fmt_ts(dt: &DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Convert an Oracle row to MemoryDocument.
/// Expected column order: id, user_id, agent_id, path, content, created_at, updated_at, metadata
fn row_to_memory_document(row: &oracle::Row) -> MemoryDocument {
    let id_str: String = row.get(0).unwrap_or_default();
    let user_id: String = row.get(1).unwrap_or_default();
    let agent_id_str: Option<String> = row.get(2).unwrap_or(None);
    let path: String = row.get(3).unwrap_or_default();
    let content: String = row.get(4).unwrap_or_default();
    let created_at_str: String = row.get(5).unwrap_or_default();
    let updated_at_str: String = row.get(6).unwrap_or_default();
    let metadata_str: String = row.get(7).unwrap_or_else(|_| "{}".to_string());

    MemoryDocument {
        id: id_str.parse().unwrap_or_default(),
        user_id,
        agent_id: agent_id_str.and_then(|s| s.parse().ok()),
        path,
        content,
        created_at: parse_ts(&created_at_str),
        updated_at: parse_ts(&updated_at_str),
        metadata: serde_json::from_str(&metadata_str)
            .unwrap_or(serde_json::Value::Object(Default::default())),
    }
}

/// Common SELECT columns for memory documents with timestamps formatted.
const DOC_COLUMNS: &str = "\
    id, user_id, agent_id, path, content, \
    TO_CHAR(created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS created_at, \
    TO_CHAR(updated_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS updated_at, \
    metadata";

#[async_trait]
impl WorkspaceStore for OracleBackend {
    async fn get_document_by_path(
        &self,
        user_id: &str,
        agent_id: Option<Uuid>,
        path: &str,
    ) -> Result<MemoryDocument, WorkspaceError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();
        let agent_id_str = agent_id.map(|id| id.to_string());
        let path = path.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| WorkspaceError::SearchFailed {
                reason: format!("Lock error: {e}"),
            })?;

            // Build query based on whether agent_id is Some or None
            let (sql, result) = if let Some(ref aid) = agent_id_str {
                let sql = format!(
                    "SELECT {} FROM IRON_MEMORIES WHERE user_id = :1 AND agent_id = :2 AND path = :3",
                    DOC_COLUMNS
                );
                let rows = conn.query(&sql, &[&user_id, aid, &path]).map_err(|e| {
                    WorkspaceError::SearchFailed {
                        reason: format!("Query failed: {e}"),
                    }
                })?;
                (sql, rows)
            } else {
                let sql = format!(
                    "SELECT {} FROM IRON_MEMORIES WHERE user_id = :1 AND agent_id IS NULL AND path = :2",
                    DOC_COLUMNS
                );
                let rows = conn.query(&sql, &[&user_id, &path]).map_err(|e| {
                    WorkspaceError::SearchFailed {
                        reason: format!("Query failed: {e}"),
                    }
                })?;
                (sql, rows)
            };

            let _ = sql; // suppress unused warning
            if let Some(row_result) = result.into_iter().next() {
                let row = row_result.map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Row error: {e}"),
                })?;
                return Ok(row_to_memory_document(&row));
            }
            Err(WorkspaceError::DocumentNotFound {
                doc_type: path,
                user_id,
            })
        })
        .await
        .map_err(|e| WorkspaceError::SearchFailed {
            reason: format!("Spawn error: {e}"),
        })?
    }

    async fn get_document_by_id(&self, id: Uuid) -> Result<MemoryDocument, WorkspaceError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| WorkspaceError::SearchFailed {
                reason: format!("Lock error: {e}"),
            })?;
            let sql = format!(
                "SELECT {} FROM IRON_MEMORIES WHERE id = :1",
                DOC_COLUMNS
            );
            let rows = conn.query(&sql, &[&id_str]).map_err(|e| {
                WorkspaceError::SearchFailed {
                    reason: format!("Query failed: {e}"),
                }
            })?;

            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Row error: {e}"),
                })?;
                return Ok(row_to_memory_document(&row));
            }
            Err(WorkspaceError::DocumentNotFound {
                doc_type: "unknown".to_string(),
                user_id: "unknown".to_string(),
            })
        })
        .await
        .map_err(|e| WorkspaceError::SearchFailed {
            reason: format!("Spawn error: {e}"),
        })?
    }

    async fn get_or_create_document_by_path(
        &self,
        user_id: &str,
        agent_id: Option<Uuid>,
        path: &str,
    ) -> Result<MemoryDocument, WorkspaceError> {
        // Try to get first
        match self.get_document_by_path(user_id, agent_id, path).await {
            Ok(doc) => return Ok(doc),
            Err(WorkspaceError::DocumentNotFound { .. }) => {}
            Err(e) => return Err(e),
        }

        // Create new document
        let conn_mgr = self.conn_mgr.clone();
        let id = Uuid::new_v4();
        let id_str = id.to_string();
        let user_id_owned = user_id.to_string();
        let agent_id_str = agent_id.map(|id| id.to_string());
        let path_owned = path.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| WorkspaceError::SearchFailed {
                reason: format!("Lock error: {e}"),
            })?;

            // Use MERGE to avoid race conditions (concurrent creates)
            conn.execute(
                "MERGE INTO IRON_MEMORIES t
                USING (SELECT :1 AS user_id, :2 AS agent_id, :3 AS path FROM DUAL) src
                ON (t.user_id = src.user_id AND (t.agent_id = src.agent_id OR (t.agent_id IS NULL AND src.agent_id IS NULL)) AND t.path = src.path)
                WHEN NOT MATCHED THEN
                    INSERT (id, user_id, agent_id, path, content, metadata)
                    VALUES (:4, :1, :2, :3, '', '{}')",
                &[
                    &user_id_owned,
                    &agent_id_str as &dyn oracle::sql_type::ToSql,
                    &path_owned,
                    &id_str,
                ],
            )
            .map_err(|e| WorkspaceError::SearchFailed {
                reason: format!("Insert failed: {e}"),
            })?;
            conn.commit().map_err(|e| WorkspaceError::SearchFailed {
                reason: format!("Commit failed: {e}"),
            })?;
            Ok::<_, WorkspaceError>(())
        })
        .await
        .map_err(|e| WorkspaceError::SearchFailed {
            reason: format!("Spawn error: {e}"),
        })??;

        // Fetch the document (either ours or the winner of the race)
        self.get_document_by_path(user_id, agent_id, path).await
    }

    async fn update_document(&self, id: Uuid, content: &str) -> Result<(), WorkspaceError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();
        let content = content.to_string();
        let now = fmt_ts(&Utc::now());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| WorkspaceError::SearchFailed {
                reason: format!("Lock error: {e}"),
            })?;
            conn.execute(
                "UPDATE IRON_MEMORIES SET content = :2, updated_at = TO_TIMESTAMP(:3, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') WHERE id = :1",
                &[&id_str, &content, &now],
            )
            .map_err(|e| WorkspaceError::SearchFailed {
                reason: format!("Update failed: {e}"),
            })?;
            conn.commit().map_err(|e| WorkspaceError::SearchFailed {
                reason: format!("Commit failed: {e}"),
            })?;
            Ok::<_, WorkspaceError>(())
        })
        .await
        .map_err(|e| WorkspaceError::SearchFailed {
            reason: format!("Spawn error: {e}"),
        })?
    }

    async fn delete_document_by_path(
        &self,
        user_id: &str,
        agent_id: Option<Uuid>,
        path: &str,
    ) -> Result<(), WorkspaceError> {
        // Get the doc to find its ID (for deleting chunks)
        let doc = self.get_document_by_path(user_id, agent_id, path).await?;
        self.delete_chunks(doc.id).await?;

        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();
        let agent_id_str = agent_id.map(|id| id.to_string());
        let path = path.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| WorkspaceError::SearchFailed {
                reason: format!("Lock error: {e}"),
            })?;

            if let Some(ref aid) = agent_id_str {
                conn.execute(
                    "DELETE FROM IRON_MEMORIES WHERE user_id = :1 AND agent_id = :2 AND path = :3",
                    &[&user_id, aid, &path],
                )
                .map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Delete failed: {e}"),
                })?;
            } else {
                conn.execute(
                    "DELETE FROM IRON_MEMORIES WHERE user_id = :1 AND agent_id IS NULL AND path = :2",
                    &[&user_id, &path],
                )
                .map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Delete failed: {e}"),
                })?;
            }
            conn.commit().map_err(|e| WorkspaceError::SearchFailed {
                reason: format!("Commit failed: {e}"),
            })?;
            Ok::<_, WorkspaceError>(())
        })
        .await
        .map_err(|e| WorkspaceError::SearchFailed {
            reason: format!("Spawn error: {e}"),
        })?
    }

    async fn list_directory(
        &self,
        user_id: &str,
        agent_id: Option<Uuid>,
        directory: &str,
    ) -> Result<Vec<WorkspaceEntry>, WorkspaceError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();
        let agent_id_str = agent_id.map(|id| id.to_string());
        let dir = if !directory.is_empty() && !directory.ends_with('/') {
            format!("{}/", directory)
        } else {
            directory.to_string()
        };

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| WorkspaceError::SearchFailed {
                reason: format!("Lock error: {e}"),
            })?;

            let pattern = if dir.is_empty() {
                "%".to_string()
            } else {
                format!("{}%", dir)
            };

            let rows = if let Some(ref aid) = agent_id_str {
                conn.query(
                    "SELECT path, TO_CHAR(updated_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS updated_at, SUBSTR(content, 1, 200) AS content_preview
                     FROM IRON_MEMORIES
                     WHERE user_id = :1 AND agent_id = :2
                       AND (:3 = '%' OR path LIKE :3)
                     ORDER BY path",
                    &[&user_id, aid, &pattern],
                )
                .map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Query failed: {e}"),
                })?
            } else {
                conn.query(
                    "SELECT path, TO_CHAR(updated_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS updated_at, SUBSTR(content, 1, 200) AS content_preview
                     FROM IRON_MEMORIES
                     WHERE user_id = :1 AND agent_id IS NULL
                       AND (:2 = '%' OR path LIKE :2)
                     ORDER BY path",
                    &[&user_id, &pattern],
                )
                .map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Query failed: {e}"),
                })?
            };

            let mut entries_map: HashMap<String, WorkspaceEntry> = HashMap::new();

            for row_result in rows {
                let row = row_result.map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Row error: {e}"),
                })?;

                let full_path: String = row.get(0).unwrap_or_default();
                let updated_at_str: Option<String> = row.get(1).unwrap_or(None);
                let content_preview: Option<String> = row.get(2).unwrap_or(None);

                let updated_at = updated_at_str.and_then(|s| parse_opt_ts(&s));

                let relative = if dir.is_empty() {
                    full_path.as_str()
                } else if let Some(stripped) = full_path.strip_prefix(&dir) {
                    stripped
                } else {
                    continue;
                };

                let child_name = if let Some(slash_pos) = relative.find('/') {
                    &relative[..slash_pos]
                } else {
                    relative
                };

                if child_name.is_empty() {
                    continue;
                }

                let is_dir = relative.contains('/');
                let entry_path = if dir.is_empty() {
                    child_name.to_string()
                } else {
                    format!("{}{}", dir, child_name)
                };

                entries_map
                    .entry(child_name.to_string())
                    .and_modify(|e| {
                        if is_dir {
                            e.is_directory = true;
                            e.content_preview = None;
                        }
                        if let (Some(existing), Some(new)) = (&e.updated_at, &updated_at)
                            && new > existing
                        {
                            e.updated_at = Some(*new);
                        }
                    })
                    .or_insert(WorkspaceEntry {
                        path: entry_path,
                        is_directory: is_dir,
                        updated_at,
                        content_preview: if is_dir { None } else { content_preview },
                    });
            }

            let mut entries: Vec<WorkspaceEntry> = entries_map.into_values().collect();
            entries.sort_by(|a, b| a.path.cmp(&b.path));
            Ok(entries)
        })
        .await
        .map_err(|e| WorkspaceError::SearchFailed {
            reason: format!("Spawn error: {e}"),
        })?
    }

    async fn list_all_paths(
        &self,
        user_id: &str,
        agent_id: Option<Uuid>,
    ) -> Result<Vec<String>, WorkspaceError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();
        let agent_id_str = agent_id.map(|id| id.to_string());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| WorkspaceError::SearchFailed {
                reason: format!("Lock error: {e}"),
            })?;

            let rows = if let Some(ref aid) = agent_id_str {
                conn.query(
                    "SELECT path FROM IRON_MEMORIES WHERE user_id = :1 AND agent_id = :2 ORDER BY path",
                    &[&user_id, aid],
                )
                .map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Query failed: {e}"),
                })?
            } else {
                conn.query(
                    "SELECT path FROM IRON_MEMORIES WHERE user_id = :1 AND agent_id IS NULL ORDER BY path",
                    &[&user_id],
                )
                .map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Query failed: {e}"),
                })?
            };

            let mut paths = Vec::new();
            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Row error: {e}"),
                })?;
                let path: String = row.get(0).unwrap_or_default();
                paths.push(path);
            }
            Ok(paths)
        })
        .await
        .map_err(|e| WorkspaceError::SearchFailed {
            reason: format!("Spawn error: {e}"),
        })?
    }

    async fn list_documents(
        &self,
        user_id: &str,
        agent_id: Option<Uuid>,
    ) -> Result<Vec<MemoryDocument>, WorkspaceError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();
        let agent_id_str = agent_id.map(|id| id.to_string());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| WorkspaceError::SearchFailed {
                reason: format!("Lock error: {e}"),
            })?;

            let rows = if let Some(ref aid) = agent_id_str {
                conn.query(
                    &format!(
                        "SELECT {} FROM IRON_MEMORIES WHERE user_id = :1 AND agent_id = :2 ORDER BY updated_at DESC",
                        DOC_COLUMNS
                    ),
                    &[&user_id, aid],
                )
                .map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Query failed: {e}"),
                })?
            } else {
                conn.query(
                    &format!(
                        "SELECT {} FROM IRON_MEMORIES WHERE user_id = :1 AND agent_id IS NULL ORDER BY updated_at DESC",
                        DOC_COLUMNS
                    ),
                    &[&user_id],
                )
                .map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Query failed: {e}"),
                })?
            };

            let mut docs = Vec::new();
            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Row error: {e}"),
                })?;
                docs.push(row_to_memory_document(&row));
            }
            Ok(docs)
        })
        .await
        .map_err(|e| WorkspaceError::SearchFailed {
            reason: format!("Spawn error: {e}"),
        })?
    }

    async fn delete_chunks(&self, document_id: Uuid) -> Result<(), WorkspaceError> {
        let conn_mgr = self.conn_mgr.clone();
        let doc_id = document_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| WorkspaceError::ChunkingFailed {
                reason: format!("Lock error: {e}"),
            })?;
            conn.execute(
                "DELETE FROM IRON_CHUNKS WHERE document_id = :1",
                &[&doc_id],
            )
            .map_err(|e| WorkspaceError::ChunkingFailed {
                reason: format!("Delete failed: {e}"),
            })?;
            conn.commit().map_err(|e| WorkspaceError::ChunkingFailed {
                reason: format!("Commit failed: {e}"),
            })?;
            Ok::<_, WorkspaceError>(())
        })
        .await
        .map_err(|e| WorkspaceError::ChunkingFailed {
            reason: format!("Spawn error: {e}"),
        })?
    }

    async fn insert_chunk(
        &self,
        document_id: Uuid,
        chunk_index: i32,
        content: &str,
        embedding: Option<&[f32]>,
    ) -> Result<Uuid, WorkspaceError> {
        let conn_mgr = self.conn_mgr.clone();
        let id = Uuid::new_v4();
        let id_str = id.to_string();
        let doc_id = document_id.to_string();
        let chunk_index = chunk_index as i64;
        let content = content.to_string();
        let embedding_str = embedding.map(|e| {
            format!(
                "[{}]",
                e.iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            )
        });

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| WorkspaceError::ChunkingFailed {
                reason: format!("Lock error: {e}"),
            })?;

            if let Some(ref emb_str) = embedding_str {
                conn.execute(
                    "INSERT INTO IRON_CHUNKS (id, document_id, chunk_index, content, embedding)
                     VALUES (:1, :2, :3, :4, TO_VECTOR(:5))",
                    &[&id_str, &doc_id, &chunk_index, &content, emb_str],
                )
                .map_err(|e| WorkspaceError::ChunkingFailed {
                    reason: format!("Insert failed: {e}"),
                })?;
            } else {
                conn.execute(
                    "INSERT INTO IRON_CHUNKS (id, document_id, chunk_index, content)
                     VALUES (:1, :2, :3, :4)",
                    &[&id_str, &doc_id, &chunk_index, &content],
                )
                .map_err(|e| WorkspaceError::ChunkingFailed {
                    reason: format!("Insert failed: {e}"),
                })?;
            }

            conn.commit().map_err(|e| WorkspaceError::ChunkingFailed {
                reason: format!("Commit failed: {e}"),
            })?;
            Ok::<_, WorkspaceError>(id)
        })
        .await
        .map_err(|e| WorkspaceError::ChunkingFailed {
            reason: format!("Spawn error: {e}"),
        })?
    }

    async fn update_chunk_embedding(
        &self,
        chunk_id: Uuid,
        embedding: &[f32],
    ) -> Result<(), WorkspaceError> {
        let conn_mgr = self.conn_mgr.clone();
        let chunk_id_str = chunk_id.to_string();
        let embedding_str = format!(
            "[{}]",
            embedding
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| WorkspaceError::EmbeddingFailed {
                reason: format!("Lock error: {e}"),
            })?;
            conn.execute(
                "UPDATE IRON_CHUNKS SET embedding = TO_VECTOR(:2) WHERE id = :1",
                &[&chunk_id_str, &embedding_str],
            )
            .map_err(|e| WorkspaceError::EmbeddingFailed {
                reason: format!("Update failed: {e}"),
            })?;
            conn.commit().map_err(|e| WorkspaceError::EmbeddingFailed {
                reason: format!("Commit failed: {e}"),
            })?;
            Ok::<_, WorkspaceError>(())
        })
        .await
        .map_err(|e| WorkspaceError::EmbeddingFailed {
            reason: format!("Spawn error: {e}"),
        })?
    }

    async fn get_chunks_without_embeddings(
        &self,
        user_id: &str,
        agent_id: Option<Uuid>,
        limit: usize,
    ) -> Result<Vec<MemoryChunk>, WorkspaceError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();
        let agent_id_str = agent_id.map(|id| id.to_string());
        let limit = limit as i64;

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| WorkspaceError::SearchFailed {
                reason: format!("Lock error: {e}"),
            })?;

            let rows = if let Some(ref aid) = agent_id_str {
                conn.query(
                    "SELECT c.id, c.document_id, c.chunk_index, c.content,
                            TO_CHAR(c.created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS created_at
                     FROM IRON_CHUNKS c
                     JOIN IRON_MEMORIES d ON d.id = c.document_id
                     WHERE d.user_id = :1 AND d.agent_id = :2
                       AND c.embedding IS NULL
                     FETCH FIRST :3 ROWS ONLY",
                    &[&user_id, aid, &limit],
                )
                .map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Query failed: {e}"),
                })?
            } else {
                conn.query(
                    "SELECT c.id, c.document_id, c.chunk_index, c.content,
                            TO_CHAR(c.created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS created_at
                     FROM IRON_CHUNKS c
                     JOIN IRON_MEMORIES d ON d.id = c.document_id
                     WHERE d.user_id = :1 AND d.agent_id IS NULL
                       AND c.embedding IS NULL
                     FETCH FIRST :2 ROWS ONLY",
                    &[&user_id, &limit],
                )
                .map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Query failed: {e}"),
                })?
            };

            let mut chunks = Vec::new();
            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| WorkspaceError::SearchFailed {
                    reason: format!("Row error: {e}"),
                })?;
                let id_str: String = row.get(0).unwrap_or_default();
                let doc_id_str: String = row.get(1).unwrap_or_default();
                let chunk_index: i64 = row.get(2).unwrap_or(0);
                let content: String = row.get(3).unwrap_or_default();
                let created_at_str: String = row.get(4).unwrap_or_default();

                chunks.push(MemoryChunk {
                    id: id_str.parse().unwrap_or_default(),
                    document_id: doc_id_str.parse().unwrap_or_default(),
                    chunk_index: chunk_index as i32,
                    content,
                    embedding: None,
                    created_at: parse_ts(&created_at_str),
                });
            }
            Ok(chunks)
        })
        .await
        .map_err(|e| WorkspaceError::SearchFailed {
            reason: format!("Spawn error: {e}"),
        })?
    }

    async fn hybrid_search(
        &self,
        user_id: &str,
        agent_id: Option<Uuid>,
        query: &str,
        embedding: Option<&[f32]>,
        config: &SearchConfig,
    ) -> Result<Vec<SearchResult>, WorkspaceError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();
        let agent_id_str = agent_id.map(|id| id.to_string());
        let query = query.to_string();
        let embedding_vec = embedding.map(|e| e.to_vec());
        let pre_limit = config.pre_fusion_limit as i64;
        let use_fts = config.use_fts;
        let use_vector = config.use_vector;
        let config_clone = config.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| WorkspaceError::SearchFailed {
                reason: format!("Lock error: {e}"),
            })?;

            // --- FTS results via Oracle CONTAINS (Oracle Text) or LIKE fallback ---
            let fts_results = if use_fts {
                // Use a simple LIKE-based text search as fallback.
                // Oracle Text (CONTAINS) requires a domain index which may not be set up.
                // The LIKE approach is simpler and always works.
                // Escape LIKE wildcards in user input to prevent unintended matching
                let escaped_query: String = query
                    .chars()
                    .flat_map(|c| match c {
                        '\\' => vec!['\\', '\\'],
                        '%' => vec!['\\', '%'],
                        '_' => vec!['\\', '_'],
                        other => vec![other],
                    })
                    .collect();
                let like_pattern = format!("%{escaped_query}%");

                let rows = if let Some(ref aid) = agent_id_str {
                    conn.query(
                        "SELECT c.id, c.document_id, c.content
                         FROM IRON_CHUNKS c
                         JOIN IRON_MEMORIES d ON d.id = c.document_id
                         WHERE d.user_id = :1 AND d.agent_id = :2
                           AND LOWER(c.content) LIKE LOWER(:3) ESCAPE '\\'
                         FETCH FIRST :4 ROWS ONLY",
                        &[&user_id, aid, &like_pattern, &pre_limit],
                    )
                    .map_err(|e| WorkspaceError::SearchFailed {
                        reason: format!("FTS query failed: {e}"),
                    })?
                } else {
                    conn.query(
                        "SELECT c.id, c.document_id, c.content
                         FROM IRON_CHUNKS c
                         JOIN IRON_MEMORIES d ON d.id = c.document_id
                         WHERE d.user_id = :1 AND d.agent_id IS NULL
                           AND LOWER(c.content) LIKE LOWER(:2) ESCAPE '\\'
                         FETCH FIRST :3 ROWS ONLY",
                        &[&user_id, &like_pattern, &pre_limit],
                    )
                    .map_err(|e| WorkspaceError::SearchFailed {
                        reason: format!("FTS query failed: {e}"),
                    })?
                };

                let mut results = Vec::new();
                if let Some(row_result) = rows.into_iter().next() {
                    let row = row_result.map_err(|e| WorkspaceError::SearchFailed {
                        reason: format!("FTS row error: {e}"),
                    })?;
                    let chunk_id_str: String = row.get(0).unwrap_or_default();
                    let doc_id_str: String = row.get(1).unwrap_or_default();
                    let content: String = row.get(2).unwrap_or_default();
                    results.push(RankedResult {
                        chunk_id: chunk_id_str.parse().unwrap_or_default(),
                        document_id: doc_id_str.parse().unwrap_or_default(),
                        content,
                        rank: results.len() as u32 + 1,
                    });
                }
                results
            } else {
                Vec::new()
            };

            // --- Vector results via VECTOR_DISTANCE with COSINE ---
            let vector_results = if use_vector {
                if let Some(ref emb) = embedding_vec {
                    let vector_str = format!(
                        "[{}]",
                        emb.iter()
                            .map(|f| f.to_string())
                            .collect::<Vec<_>>()
                            .join(",")
                    );

                    let rows = if let Some(ref aid) = agent_id_str {
                        conn.query(
                            "SELECT c.id, c.document_id, c.content
                             FROM IRON_CHUNKS c
                             JOIN IRON_MEMORIES d ON d.id = c.document_id
                             WHERE d.user_id = :1 AND d.agent_id = :2
                               AND c.embedding IS NOT NULL
                             ORDER BY VECTOR_DISTANCE(c.embedding, TO_VECTOR(:3), COSINE)
                             FETCH FIRST :4 ROWS ONLY",
                            &[&user_id, aid, &vector_str, &pre_limit],
                        )
                        .map_err(|e| WorkspaceError::SearchFailed {
                            reason: format!("Vector query failed: {e}"),
                        })?
                    } else {
                        conn.query(
                            "SELECT c.id, c.document_id, c.content
                             FROM IRON_CHUNKS c
                             JOIN IRON_MEMORIES d ON d.id = c.document_id
                             WHERE d.user_id = :1 AND d.agent_id IS NULL
                               AND c.embedding IS NOT NULL
                             ORDER BY VECTOR_DISTANCE(c.embedding, TO_VECTOR(:2), COSINE)
                             FETCH FIRST :3 ROWS ONLY",
                            &[&user_id, &vector_str, &pre_limit],
                        )
                        .map_err(|e| WorkspaceError::SearchFailed {
                            reason: format!("Vector query failed: {e}"),
                        })?
                    };

                    let mut results = Vec::new();
                    if let Some(row_result) = rows.into_iter().next() {
                        let row = row_result.map_err(|e| WorkspaceError::SearchFailed {
                            reason: format!("Vector row error: {e}"),
                        })?;
                        let chunk_id_str: String = row.get(0).unwrap_or_default();
                        let doc_id_str: String = row.get(1).unwrap_or_default();
                        let content: String = row.get(2).unwrap_or_default();
                        results.push(RankedResult {
                            chunk_id: chunk_id_str.parse().unwrap_or_default(),
                            document_id: doc_id_str.parse().unwrap_or_default(),
                            content,
                            rank: results.len() as u32 + 1,
                        });
                    }
                    results
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };

            if embedding_vec.is_some() && !use_vector {
                tracing::warn!(
                    "Embedding provided but vector search is disabled in config; using FTS-only results"
                );
            }

            Ok(reciprocal_rank_fusion(fts_results, vector_results, &config_clone))
        })
        .await
        .map_err(|e| WorkspaceError::SearchFailed {
            reason: format!("Spawn error: {e}"),
        })?
    }
}
