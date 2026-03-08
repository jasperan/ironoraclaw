//! Oracle implementation of JobStore.

use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use uuid::Uuid;

use super::OracleBackend;
use crate::context::{ActionRecord, JobContext, JobState};
use crate::db::JobStore;
use crate::error::DatabaseError;
use crate::history::LlmCallRecord;

fn fmt_ts(dt: &chrono::DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn parse_ts(s: &str) -> chrono::DateTime<Utc> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return dt.with_timezone(&Utc);
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return ndt.and_utc();
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return ndt.and_utc();
    }
    chrono::DateTime::UNIX_EPOCH
}

fn parse_opt_ts(s: &Option<String>) -> Option<chrono::DateTime<Utc>> {
    s.as_ref().map(|s| parse_ts(s))
}

fn parse_job_state(s: &str) -> JobState {
    match s {
        "pending" => JobState::Pending,
        "in_progress" => JobState::InProgress,
        "completed" => JobState::Completed,
        "submitted" => JobState::Submitted,
        "accepted" => JobState::Accepted,
        "failed" => JobState::Failed,
        "stuck" => JobState::Stuck,
        "cancelled" => JobState::Cancelled,
        _ => JobState::Pending,
    }
}

#[async_trait]
impl JobStore for OracleBackend {
    async fn save_job(&self, ctx: &JobContext) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let job_id = ctx.job_id.to_string();
        let conv_id = ctx.conversation_id.map(|id| id.to_string());
        let title = ctx.title.clone();
        let description = ctx.description.clone();
        let category = ctx.category.clone();
        let status = ctx.state.to_string();
        let budget_amount = ctx.budget.map(|d| d.to_string());
        let budget_token = ctx.budget_token.clone();
        let bid_amount = ctx.bid_amount.map(|d| d.to_string());
        let estimated_cost = ctx.estimated_cost.map(|d| d.to_string());
        let estimated_time_secs = ctx.estimated_duration.map(|d| d.as_secs() as i64);
        let actual_cost = ctx.actual_cost.to_string();
        let repair_attempts = ctx.repair_attempts as i64;
        let created_at = fmt_ts(&ctx.created_at);
        let started_at = ctx.started_at.map(|dt| fmt_ts(&dt));
        let completed_at = ctx.completed_at.map(|dt| fmt_ts(&dt));
        let user_id = ctx.user_id.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;

            let sql = "\
                MERGE INTO IRON_JOBS j \
                USING (SELECT :1 AS id FROM DUAL) src \
                ON (j.id = src.id) \
                WHEN NOT MATCHED THEN \
                    INSERT (id, conversation_id, title, description, category, status, source, \
                            budget_amount, budget_token, bid_amount, estimated_cost, estimated_time_secs, \
                            actual_cost, repair_attempts, user_id, \
                            created_at, started_at, completed_at) \
                    VALUES (:1, :2, :3, :4, :5, :6, 'direct', \
                            TO_NUMBER(:7), :8, TO_NUMBER(:9), TO_NUMBER(:10), :11, \
                            TO_NUMBER(:12), :13, :14, \
                            TO_TIMESTAMP(:15, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'), \
                            TO_TIMESTAMP(:16, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'), \
                            TO_TIMESTAMP(:17, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"')) \
                WHEN MATCHED THEN \
                    UPDATE SET j.title = :3, j.description = :4, j.category = :5, j.status = :6, \
                               j.estimated_cost = TO_NUMBER(:10), j.estimated_time_secs = :11, \
                               j.actual_cost = TO_NUMBER(:12), j.repair_attempts = :13, \
                               j.started_at = TO_TIMESTAMP(:16, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'), \
                               j.completed_at = TO_TIMESTAMP(:17, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"')";

            conn.execute(sql, &[
                &job_id,
                &conv_id as &dyn oracle::sql_type::ToSql,
                &title,
                &description,
                &category as &dyn oracle::sql_type::ToSql,
                &status,
                &budget_amount as &dyn oracle::sql_type::ToSql,
                &budget_token as &dyn oracle::sql_type::ToSql,
                &bid_amount as &dyn oracle::sql_type::ToSql,
                &estimated_cost as &dyn oracle::sql_type::ToSql,
                &estimated_time_secs as &dyn oracle::sql_type::ToSql,
                &actual_cost,
                &repair_attempts,
                &user_id,
                &created_at,
                &started_at as &dyn oracle::sql_type::ToSql,
                &completed_at as &dyn oracle::sql_type::ToSql,
            ])
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn get_job(&self, id: Uuid) -> Result<Option<JobContext>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;

            let sql = "SELECT id, conversation_id, title, description, category, status, user_id, \
                       TO_CHAR(budget_amount) AS budget_amount, budget_token, \
                       TO_CHAR(bid_amount) AS bid_amount, TO_CHAR(estimated_cost) AS estimated_cost, \
                       estimated_time_secs, TO_CHAR(actual_cost) AS actual_cost, repair_attempts, \
                       TO_CHAR(created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS created_at, \
                       TO_CHAR(started_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS started_at, \
                       TO_CHAR(completed_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS completed_at \
                       FROM IRON_JOBS WHERE id = :1";

            let rows = conn.query(sql, &[&id_str])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let status_str: String = row.get(5).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let est_time: Option<i64> = row.get(11).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let created_str: String = row.get(14).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let started_str: Option<String> = row.get(15).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let completed_str: Option<String> = row.get(16).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let budget_str: Option<String> = row.get(7).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let bid_str: Option<String> = row.get(9).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let est_cost_str: Option<String> = row.get(10).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let actual_cost_str: Option<String> = row.get(12).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let repair: i64 = row.get(13).map_err(|e| DatabaseError::Query(e.to_string()))?;

                return Ok(Some(JobContext {
                    job_id: row.get::<usize, String>(0).map_err(|e| DatabaseError::Query(e.to_string()))?.parse().unwrap_or_default(),
                    state: parse_job_state(&status_str),
                    user_id: row.get(6).map_err(|e| DatabaseError::Query(e.to_string()))?,
                    conversation_id: row.get::<usize, Option<String>>(1)
                        .map_err(|e| DatabaseError::Query(e.to_string()))?
                        .and_then(|s| s.parse().ok()),
                    title: row.get(2).map_err(|e| DatabaseError::Query(e.to_string()))?,
                    description: row.get(3).map_err(|e| DatabaseError::Query(e.to_string()))?,
                    category: row.get(4).map_err(|e| DatabaseError::Query(e.to_string()))?,
                    budget: budget_str.and_then(|s| s.trim().parse().ok()),
                    budget_token: row.get(8).map_err(|e| DatabaseError::Query(e.to_string()))?,
                    bid_amount: bid_str.and_then(|s| s.trim().parse().ok()),
                    estimated_cost: est_cost_str.and_then(|s| s.trim().parse().ok()),
                    estimated_duration: est_time.map(|s| std::time::Duration::from_secs(s as u64)),
                    actual_cost: actual_cost_str
                        .and_then(|s| s.trim().parse().ok())
                        .unwrap_or(Decimal::ZERO),
                    total_tokens_used: 0,
                    max_tokens: 0,
                    repair_attempts: repair as u32,
                    created_at: parse_ts(&created_str),
                    started_at: parse_opt_ts(&started_str),
                    completed_at: parse_opt_ts(&completed_str),
                    transitions: Vec::new(),
                    metadata: serde_json::Value::Null,
                    extra_env: std::sync::Arc::new(std::collections::HashMap::new()),
                }));
            }
            Ok::<_, DatabaseError>(None)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn update_job_status(
        &self,
        id: Uuid,
        status: JobState,
        failure_reason: Option<&str>,
    ) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();
        let status_str = status.to_string();
        let failure = failure_reason.map(String::from);

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "UPDATE IRON_JOBS SET status = :2, failure_reason = :3 WHERE id = :1",
                &[&id_str, &status_str, &failure as &dyn oracle::sql_type::ToSql],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn mark_job_stuck(&self, id: Uuid) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();
        let now = fmt_ts(&Utc::now());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "UPDATE IRON_JOBS SET status = 'stuck', stuck_since = TO_TIMESTAMP(:2, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') WHERE id = :1",
                &[&id_str, &now],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn get_stuck_jobs(&self) -> Result<Vec<Uuid>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let rows = conn.query("SELECT id FROM IRON_JOBS WHERE status = 'stuck'", &[])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            let mut ids = Vec::new();
            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let id_str: String = row.get(0).map_err(|e| DatabaseError::Query(e.to_string()))?;
                if let Ok(id) = id_str.parse() {
                    ids.push(id);
                }
            }
            Ok::<_, DatabaseError>(ids)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn save_action(&self, job_id: Uuid, action: &ActionRecord) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let action_id = action.id.to_string();
        let job_id = job_id.to_string();
        let sequence = action.sequence as i64;
        let tool_name = action.tool_name.clone();
        let input = action.input.to_string();
        let output_raw = action.output_raw.clone();
        let output_sanitized = action.output_sanitized.as_ref().map(|v| v.to_string());
        let warnings = serde_json::to_string(&action.sanitization_warnings)
            .map_err(|e| DatabaseError::Serialization(e.to_string()))?;
        let cost = action.cost.map(|d| d.to_string());
        let duration_ms = action.duration.as_millis() as i64;
        let success = if action.success { 1i64 } else { 0i64 };
        let error = action.error.clone();
        let executed_at = fmt_ts(&action.executed_at);

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = "INSERT INTO IRON_ACTIONS \
                (id, job_id, sequence_num, tool_name, input, output_raw, output_sanitized, \
                 sanitization_warnings, cost, duration_ms, success, error_message, created_at) \
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, TO_NUMBER(:9), :10, :11, :12, \
                        TO_TIMESTAMP(:13, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))";
            conn.execute(sql, &[
                &action_id,
                &job_id,
                &sequence,
                &tool_name,
                &input,
                &output_raw as &dyn oracle::sql_type::ToSql,
                &output_sanitized as &dyn oracle::sql_type::ToSql,
                &warnings,
                &cost as &dyn oracle::sql_type::ToSql,
                &duration_ms,
                &success,
                &error as &dyn oracle::sql_type::ToSql,
                &executed_at,
            ])
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn get_job_actions(&self, job_id: Uuid) -> Result<Vec<ActionRecord>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let job_id = job_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = "SELECT id, sequence_num, tool_name, input, output_raw, output_sanitized, \
                       sanitization_warnings, TO_CHAR(cost) AS cost, duration_ms, success, error_message, \
                       TO_CHAR(created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS created_at \
                       FROM IRON_ACTIONS WHERE job_id = :1 ORDER BY sequence_num";
            let rows = conn.query(sql, &[&job_id])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            let mut actions = Vec::new();
            if let Some(row_result) = rows.into_iter().next() {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let warnings_str: String = row.get::<usize, Option<String>>(6)
                    .map_err(|e| DatabaseError::Query(e.to_string()))?
                    .unwrap_or_else(|| "[]".to_string());
                let warnings: Vec<String> = serde_json::from_str(&warnings_str).unwrap_or_default();
                let cost_str: Option<String> = row.get(7).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let success_val: i64 = row.get(9).map_err(|e| DatabaseError::Query(e.to_string()))?;
                let output_san_str: Option<String> = row.get(5).map_err(|e| DatabaseError::Query(e.to_string()))?;

                actions.push(ActionRecord {
                    id: row.get::<usize, String>(0).map_err(|e| DatabaseError::Query(e.to_string()))?.parse().unwrap_or_default(),
                    sequence: row.get::<usize, i64>(1).map_err(|e| DatabaseError::Query(e.to_string()))? as u32,
                    tool_name: row.get(2).map_err(|e| DatabaseError::Query(e.to_string()))?,
                    input: serde_json::from_str(&row.get::<usize, String>(3).map_err(|e| DatabaseError::Query(e.to_string()))?)
                        .unwrap_or(serde_json::Value::Null),
                    output_raw: row.get(4).map_err(|e| DatabaseError::Query(e.to_string()))?,
                    output_sanitized: output_san_str.and_then(|s| serde_json::from_str(&s).ok()),
                    sanitization_warnings: warnings,
                    cost: cost_str.and_then(|s| s.trim().parse().ok()),
                    duration: std::time::Duration::from_millis(
                        row.get::<usize, i64>(8).map_err(|e| DatabaseError::Query(e.to_string()))? as u64
                    ),
                    success: success_val != 0,
                    error: row.get(10).map_err(|e| DatabaseError::Query(e.to_string()))?,
                    executed_at: parse_ts(&row.get::<usize, String>(11).map_err(|e| DatabaseError::Query(e.to_string()))?),
                });
            }
            Ok::<_, DatabaseError>(actions)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn record_llm_call(&self, record: &LlmCallRecord<'_>) -> Result<Uuid, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id = Uuid::new_v4();
        let id_str = id.to_string();
        let job_id = record.job_id.map(|id| id.to_string());
        let conv_id = record.conversation_id.map(|id| id.to_string());
        let provider = record.provider.to_string();
        let model = record.model.to_string();
        let input_tokens = record.input_tokens as i64;
        let output_tokens = record.output_tokens as i64;
        let cost_str = record.cost.to_string();
        let purpose = record.purpose.map(String::from);

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = "INSERT INTO IRON_LLM_CALLS \
                (id, job_id, conversation_id, provider, model, input_tokens, output_tokens, cost, purpose) \
                VALUES (:1, :2, :3, :4, :5, :6, :7, TO_NUMBER(:8), :9)";
            conn.execute(sql, &[
                &id_str,
                &job_id as &dyn oracle::sql_type::ToSql,
                &conv_id as &dyn oracle::sql_type::ToSql,
                &provider,
                &model,
                &input_tokens,
                &output_tokens,
                &cost_str,
                &purpose as &dyn oracle::sql_type::ToSql,
            ])
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))??;
        Ok(id)
    }

    async fn save_estimation_snapshot(
        &self,
        job_id: Uuid,
        category: &str,
        tool_names: &[String],
        estimated_cost: Decimal,
        estimated_time_secs: i32,
        estimated_value: Decimal,
    ) -> Result<Uuid, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id = Uuid::new_v4();
        let id_str = id.to_string();
        let job_id = job_id.to_string();
        let category = category.to_string();
        let tools_json = serde_json::to_string(tool_names)
            .map_err(|e| DatabaseError::Serialization(e.to_string()))?;
        let est_cost = estimated_cost.to_string();
        let est_time = estimated_time_secs as i64;
        let est_value = estimated_value.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = "INSERT INTO IRON_ESTIMATES \
                (id, job_id, category, tool_names, estimated_cost, estimated_time_secs, estimated_value) \
                VALUES (:1, :2, :3, :4, TO_NUMBER(:5), :6, TO_NUMBER(:7))";
            conn.execute(sql, &[
                &id_str, &job_id, &category, &tools_json, &est_cost, &est_time, &est_value,
            ])
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))??;
        Ok(id)
    }

    async fn update_estimation_actuals(
        &self,
        id: Uuid,
        actual_cost: Decimal,
        actual_time_secs: i32,
        actual_value: Option<Decimal>,
    ) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();
        let cost_str = actual_cost.to_string();
        let time_secs = actual_time_secs as i64;
        let value_str = actual_value.map(|d| d.to_string());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "UPDATE IRON_ESTIMATES SET actual_cost = TO_NUMBER(:2), actual_time_secs = :3, actual_value = TO_NUMBER(:4) WHERE id = :1",
                &[&id_str, &cost_str, &time_secs, &value_str as &dyn oracle::sql_type::ToSql],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }
}
