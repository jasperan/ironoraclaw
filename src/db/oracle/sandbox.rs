//! Oracle implementation of SandboxStore.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::OracleBackend;
use crate::db::SandboxStore;
use crate::error::DatabaseError;
use crate::history::{JobEventRecord, SandboxJobRecord, SandboxJobSummary};

fn fmt_ts(dt: &DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

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

fn parse_opt_ts(s: &Option<String>) -> Option<DateTime<Utc>> {
    s.as_ref().map(|s| parse_ts(s))
}

/// Read a SandboxJobRecord from a row whose columns are:
/// 0:id, 1:tool_name(task), 2:message(cred_json), 3:status, 4:user_id, 5:job_id(project_dir),
/// 6:success, 7:failure_reason(message), 8:created_at, 9:started_at, 10:completed_at
fn row_to_sandbox_job(row: &oracle::Row) -> Result<SandboxJobRecord, DatabaseError> {
    let success_val: Option<i64> = row.get(6).map_err(|e| DatabaseError::Query(e.to_string()))?;
    let created_str: String = row.get(8).map_err(|e| DatabaseError::Query(e.to_string()))?;
    let started_str: Option<String> = row.get(9).map_err(|e| DatabaseError::Query(e.to_string()))?;
    let completed_str: Option<String> = row.get(10).map_err(|e| DatabaseError::Query(e.to_string()))?;

    Ok(SandboxJobRecord {
        id: row.get::<usize, String>(0).map_err(|e| DatabaseError::Query(e.to_string()))?.parse().unwrap_or_default(),
        task: row.get::<usize, Option<String>>(1).map_err(|e| DatabaseError::Query(e.to_string()))?.unwrap_or_default(),
        credential_grants_json: row.get::<usize, Option<String>>(2).map_err(|e| DatabaseError::Query(e.to_string()))?.unwrap_or_default(),
        status: row.get(3).map_err(|e| DatabaseError::Query(e.to_string()))?,
        user_id: row.get::<usize, Option<String>>(4).map_err(|e| DatabaseError::Query(e.to_string()))?.unwrap_or_default(),
        project_dir: row.get::<usize, Option<String>>(5).map_err(|e| DatabaseError::Query(e.to_string()))?.unwrap_or_default(),
        success: success_val.map(|v| v != 0),
        failure_reason: row.get(7).map_err(|e| DatabaseError::Query(e.to_string()))?,
        created_at: parse_ts(&created_str),
        started_at: parse_opt_ts(&started_str),
        completed_at: parse_opt_ts(&completed_str),
    })
}

const SANDBOX_SELECT: &str = "\
    SELECT id, tool_name, message, status, user_id, job_id, \
    success, message AS failure_reason, \
    TO_CHAR(created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS created_at, \
    TO_CHAR(started_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS started_at, \
    TO_CHAR(completed_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS completed_at \
    FROM IRON_SANDBOX_JOBS";

#[async_trait]
impl SandboxStore for OracleBackend {
    async fn save_sandbox_job(&self, job: &SandboxJobRecord) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = job.id.to_string();
        let task = job.task.clone();
        let cred_json = job.credential_grants_json.clone();
        let status = job.status.clone();
        let user_id = job.user_id.clone();
        let project_dir = job.project_dir.clone();
        let success = job.success.map(|b| if b { 1i64 } else { 0i64 });
        let failure_reason = job.failure_reason.clone();
        let created_at = fmt_ts(&job.created_at);
        let started_at = job.started_at.map(|dt| fmt_ts(&dt));
        let completed_at = job.completed_at.map(|dt| fmt_ts(&dt));

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = "\
                MERGE INTO IRON_SANDBOX_JOBS s \
                USING (SELECT :1 AS id FROM DUAL) src \
                ON (s.id = src.id) \
                WHEN NOT MATCHED THEN \
                    INSERT (id, tool_name, message, status, user_id, job_id, \
                            success, \
                            created_at, started_at, completed_at) \
                    VALUES (:1, :2, :3, :4, :5, :6, :7, \
                            TO_TIMESTAMP(:8, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'), \
                            TO_TIMESTAMP(:9, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'), \
                            TO_TIMESTAMP(:10, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"')) \
                WHEN MATCHED THEN \
                    UPDATE SET s.status = :4, s.success = :7, \
                               s.message = COALESCE(:11, s.message), \
                               s.started_at = COALESCE(TO_TIMESTAMP(:9, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'), s.started_at), \
                               s.completed_at = COALESCE(TO_TIMESTAMP(:10, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'), s.completed_at)";
            conn.execute(sql, &[
                &id_str,                                               // :1
                &task,                                                 // :2 tool_name
                &cred_json,                                            // :3 message (stores cred_grants_json)
                &status,                                               // :4
                &user_id,                                              // :5
                &project_dir,                                          // :6 job_id col
                &success as &dyn oracle::sql_type::ToSql,              // :7
                &created_at,                                           // :8
                &started_at as &dyn oracle::sql_type::ToSql,           // :9
                &completed_at as &dyn oracle::sql_type::ToSql,         // :10
                &failure_reason as &dyn oracle::sql_type::ToSql,       // :11
            ])
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn get_sandbox_job(&self, id: Uuid) -> Result<Option<SandboxJobRecord>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = format!("{} WHERE id = :1", SANDBOX_SELECT);
            let rows = conn.query(&sql, &[&id_str])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;
            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                return Ok(Some(row_to_sandbox_job(&row)?));
            }
            Ok::<_, DatabaseError>(None)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn list_sandbox_jobs(&self) -> Result<Vec<SandboxJobRecord>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = format!("{} ORDER BY created_at DESC", SANDBOX_SELECT);
            let rows = conn.query(&sql, &[])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;
            let mut jobs = Vec::new();
            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                jobs.push(row_to_sandbox_job(&row)?);
            }
            Ok::<_, DatabaseError>(jobs)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn update_sandbox_job_status(
        &self,
        id: Uuid,
        status: &str,
        success: Option<bool>,
        message: Option<&str>,
        started_at: Option<DateTime<Utc>>,
        completed_at: Option<DateTime<Utc>>,
    ) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();
        let status = status.to_string();
        let success_val = success.map(|b| if b { 1i64 } else { 0i64 });
        let message = message.map(String::from);
        let started = started_at.map(|dt| fmt_ts(&dt));
        let completed = completed_at.map(|dt| fmt_ts(&dt));

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "UPDATE IRON_SANDBOX_JOBS SET \
                    status = :2, \
                    success = COALESCE(:3, success), \
                    message = COALESCE(:4, message), \
                    started_at = COALESCE(TO_TIMESTAMP(:5, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'), started_at), \
                    completed_at = COALESCE(TO_TIMESTAMP(:6, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'), completed_at) \
                WHERE id = :1",
                &[
                    &id_str,
                    &status,
                    &success_val as &dyn oracle::sql_type::ToSql,
                    &message as &dyn oracle::sql_type::ToSql,
                    &started as &dyn oracle::sql_type::ToSql,
                    &completed as &dyn oracle::sql_type::ToSql,
                ],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn cleanup_stale_sandbox_jobs(&self) -> Result<u64, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let now = fmt_ts(&Utc::now());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let stmt = conn.execute(
                "UPDATE IRON_SANDBOX_JOBS SET \
                    status = 'interrupted', \
                    message = 'Process restarted', \
                    completed_at = TO_TIMESTAMP(:1, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') \
                WHERE status IN ('running', 'creating')",
                &[&now],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            let count = stmt.row_count().map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            if count > 0 {
                tracing::info!("Marked {} stale sandbox jobs as interrupted", count);
            }
            Ok::<_, DatabaseError>(count as u64)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn sandbox_job_summary(&self) -> Result<SandboxJobSummary, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let rows = conn.query(
                "SELECT status, COUNT(*) AS cnt FROM IRON_SANDBOX_JOBS GROUP BY status",
                &[],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;

            let mut summary = SandboxJobSummary::default();
            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let status: String = row.get(0).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let count: i64 = row.get(1).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let count = count as usize;
                summary.total += count;
                match status.as_str() {
                    "creating" => summary.creating += count,
                    "running" => summary.running += count,
                    "completed" => summary.completed += count,
                    "failed" => summary.failed += count,
                    "interrupted" => summary.interrupted += count,
                    _ => {}
                }
            }
            Ok::<_, DatabaseError>(summary)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn list_sandbox_jobs_for_user(
        &self,
        user_id: &str,
    ) -> Result<Vec<SandboxJobRecord>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = format!("{} WHERE user_id = :1 ORDER BY created_at DESC", SANDBOX_SELECT);
            let rows = conn.query(&sql, &[&user_id])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;
            let mut jobs = Vec::new();
            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                jobs.push(row_to_sandbox_job(&row)?);
            }
            Ok::<_, DatabaseError>(jobs)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn sandbox_job_summary_for_user(
        &self,
        user_id: &str,
    ) -> Result<SandboxJobSummary, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let rows = conn.query(
                "SELECT status, COUNT(*) AS cnt FROM IRON_SANDBOX_JOBS WHERE user_id = :1 GROUP BY status",
                &[&user_id],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;

            let mut summary = SandboxJobSummary::default();
            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let status: String = row.get(0).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let count: i64 = row.get(1).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let count = count as usize;
                summary.total += count;
                match status.as_str() {
                    "creating" => summary.creating += count,
                    "running" => summary.running += count,
                    "completed" => summary.completed += count,
                    "failed" => summary.failed += count,
                    "interrupted" => summary.interrupted += count,
                    _ => {}
                }
            }
            Ok::<_, DatabaseError>(summary)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn sandbox_job_belongs_to_user(
        &self,
        job_id: Uuid,
        user_id: &str,
    ) -> Result<bool, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = job_id.to_string();
        let user_id = user_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let rows = conn.query(
                "SELECT COUNT(*) FROM IRON_SANDBOX_JOBS WHERE id = :1 AND user_id = :2",
                &[&id_str, &user_id],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let count: i64 = row.get(0).map_err(|e| DatabaseError::Query(e.to_string()))?;
                return Ok(count > 0);
            }
            Ok::<_, DatabaseError>(false)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn update_sandbox_job_mode(&self, id: Uuid, mode: &str) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();
        let mode = mode.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "UPDATE IRON_SANDBOX_JOBS SET mode = :2 WHERE id = :1",
                &[&id_str, &mode],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn get_sandbox_job_mode(&self, id: Uuid) -> Result<Option<String>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let rows = conn.query(
                "SELECT mode FROM IRON_SANDBOX_JOBS WHERE id = :1",
                &[&id_str],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let mode: Option<String> = row.get(0).map_err(|e| DatabaseError::Query(e.to_string()))?;
                return Ok(mode);
            }
            Ok::<_, DatabaseError>(None)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn save_job_event(
        &self,
        job_id: Uuid,
        event_type: &str,
        data: &serde_json::Value,
    ) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let job_id = job_id.to_string();
        let event_type = event_type.to_string();
        let data_str = data.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "INSERT INTO IRON_JOB_EVENTS (job_id, event_type, data) VALUES (:1, :2, :3)",
                &[&job_id, &event_type, &data_str],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn list_job_events(
        &self,
        job_id: Uuid,
        limit: Option<i64>,
    ) -> Result<Vec<JobEventRecord>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let job_id = job_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;

            let rows = if let Some(n) = limit {
                let sql = "SELECT * FROM (\
                    SELECT id, job_id, event_type, data, \
                    TO_CHAR(created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS created_at \
                    FROM IRON_JOB_EVENTS WHERE job_id = :1 \
                    ORDER BY id DESC FETCH FIRST :2 ROWS ONLY\
                    ) ORDER BY id ASC";
                conn.query(sql, &[&job_id, &n])
                    .map_err(|e| DatabaseError::Query(e.to_string()))?
            } else {
                let sql = "SELECT id, job_id, event_type, data, \
                    TO_CHAR(created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS created_at \
                    FROM IRON_JOB_EVENTS WHERE job_id = :1 ORDER BY id ASC";
                conn.query(sql, &[&job_id])
                    .map_err(|e| DatabaseError::Query(e.to_string()))?
            };

            let mut events = Vec::new();
            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let data_str: Option<String> = row.get(3).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let ts_str: String = row.get(4).map_err(|e| DatabaseError::Query(e.to_string()))?;
                events.push(JobEventRecord {
                    id: row.get::<usize, i64>(0).map_err(|e| DatabaseError::Query(e.to_string()))?,
                    job_id: row.get::<usize, String>(1).map_err(|e| DatabaseError::Query(e.to_string()))?.parse().unwrap_or_default(),
                    event_type: row.get(2).map_err(|e| DatabaseError::Query(e.to_string()))?,
                    data: data_str
                        .and_then(|s| serde_json::from_str(&s).ok())
                        .unwrap_or(serde_json::Value::Null),
                    created_at: parse_ts(&ts_str),
                });
            }
            Ok::<_, DatabaseError>(events)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }
}
