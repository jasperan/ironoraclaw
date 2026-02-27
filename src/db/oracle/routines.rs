//! Oracle implementation of RoutineStore.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::OracleBackend;
use crate::agent::routine::{
    NotifyConfig, Routine, RoutineAction, RoutineGuardrails, RoutineRun, RunStatus, Trigger,
};
use crate::db::RoutineStore;
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

/// Column list for IRON_ROUTINES (matches positional access in `row_to_routine`).
const ROUTINE_COLUMNS: &str = "\
    TO_CHAR(id) AS id, name, description, user_id, enabled, \
    trigger_type, trigger_config, action_type, action_config, \
    cooldown_secs, max_concurrent, dedup_window_secs, \
    notify_channel, notify_user, notify_on_success, notify_on_failure, notify_on_attention, \
    state, \
    TO_CHAR(last_run_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS last_run_at, \
    TO_CHAR(next_fire_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS next_fire_at, \
    run_count, consecutive_failures, \
    TO_CHAR(created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS created_at, \
    TO_CHAR(updated_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS updated_at";

/// Column list for IRON_ROUTINE_RUNS.
const ROUTINE_RUN_COLUMNS: &str = "\
    TO_CHAR(id) AS id, TO_CHAR(routine_id) AS routine_id, trigger_type, trigger_detail, \
    TO_CHAR(started_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS started_at, \
    status, \
    TO_CHAR(completed_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS completed_at, \
    result_summary, tokens_used, TO_CHAR(job_id) AS job_id, \
    TO_CHAR(created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AS created_at";

/// Convert an Oracle row to a Routine struct.
fn row_to_routine(row: &oracle::Row) -> Result<Routine, DatabaseError> {
    let id_str: String = row.get(0).unwrap_or_default();
    let name: String = row.get(1).unwrap_or_default();
    let description: String = row.get(2).unwrap_or_default();
    let user_id: String = row.get(3).unwrap_or_default();
    let enabled: i32 = row.get(4).unwrap_or(1);

    let trigger_type: String = row.get(5).unwrap_or_default();
    let trigger_config_str: String = row.get(6).unwrap_or_default();
    let action_type: String = row.get(7).unwrap_or_default();
    let action_config_str: String = row.get(8).unwrap_or_default();

    let cooldown_secs: i64 = row.get(9).unwrap_or(300);
    let max_concurrent: i64 = row.get(10).unwrap_or(1);
    let dedup_window_secs: Option<i64> = row.get(11).unwrap_or(None);

    let notify_channel: Option<String> = row.get(12).unwrap_or(None);
    let notify_user: String = row.get(13).unwrap_or_else(|_| "default".to_string());
    let notify_on_success: i32 = row.get(14).unwrap_or(0);
    let notify_on_failure: i32 = row.get(15).unwrap_or(1);
    let notify_on_attention: i32 = row.get(16).unwrap_or(1);

    let state_str: String = row.get(17).unwrap_or_else(|_| "{}".to_string());
    let last_run_at_str: Option<String> = row.get(18).unwrap_or(None);
    let next_fire_at_str: Option<String> = row.get(19).unwrap_or(None);
    let run_count: i64 = row.get(20).unwrap_or(0);
    let consecutive_failures: i64 = row.get(21).unwrap_or(0);
    let created_at_str: String = row.get(22).unwrap_or_default();
    let updated_at_str: String = row.get(23).unwrap_or_default();

    let trigger_config: serde_json::Value =
        serde_json::from_str(&trigger_config_str).unwrap_or(serde_json::Value::Object(Default::default()));
    let action_config: serde_json::Value =
        serde_json::from_str(&action_config_str).unwrap_or(serde_json::Value::Object(Default::default()));

    let trigger = Trigger::from_db(&trigger_type, trigger_config)
        .map_err(|e| DatabaseError::Serialization(e.to_string()))?;
    let action = RoutineAction::from_db(&action_type, action_config)
        .map_err(|e| DatabaseError::Serialization(e.to_string()))?;

    Ok(Routine {
        id: id_str.parse().unwrap_or_default(),
        name,
        description,
        user_id,
        enabled: enabled != 0,
        trigger,
        action,
        guardrails: RoutineGuardrails {
            cooldown: Duration::from_secs(cooldown_secs as u64),
            max_concurrent: max_concurrent as u32,
            dedup_window: dedup_window_secs.map(|s| Duration::from_secs(s as u64)),
        },
        notify: NotifyConfig {
            channel: notify_channel,
            user: notify_user,
            on_success: notify_on_success != 0,
            on_failure: notify_on_failure != 0,
            on_attention: notify_on_attention != 0,
        },
        state: serde_json::from_str(&state_str).unwrap_or(serde_json::Value::Object(Default::default())),
        last_run_at: last_run_at_str.and_then(|s| parse_opt_ts(&s)),
        next_fire_at: next_fire_at_str.and_then(|s| parse_opt_ts(&s)),
        run_count: run_count as u64,
        consecutive_failures: consecutive_failures as u32,
        created_at: parse_ts(&created_at_str),
        updated_at: parse_ts(&updated_at_str),
    })
}

/// Convert an Oracle row to a RoutineRun struct.
fn row_to_routine_run(row: &oracle::Row) -> Result<RoutineRun, DatabaseError> {
    let id_str: String = row.get(0).unwrap_or_default();
    let routine_id_str: String = row.get(1).unwrap_or_default();
    let trigger_type: String = row.get(2).unwrap_or_default();
    let trigger_detail: Option<String> = row.get(3).unwrap_or(None);
    let started_at_str: String = row.get(4).unwrap_or_default();
    let status_str: String = row.get(5).unwrap_or_else(|_| "running".to_string());
    let completed_at_str: Option<String> = row.get(6).unwrap_or(None);
    let result_summary: Option<String> = row.get(7).unwrap_or(None);
    let tokens_used: Option<i64> = row.get(8).unwrap_or(None);
    let job_id_str: Option<String> = row.get(9).unwrap_or(None);
    let created_at_str: String = row.get(10).unwrap_or_default();

    let status: RunStatus = status_str
        .parse()
        .map_err(|e: crate::error::RoutineError| DatabaseError::Serialization(e.to_string()))?;

    Ok(RoutineRun {
        id: id_str.parse().unwrap_or_default(),
        routine_id: routine_id_str.parse().unwrap_or_default(),
        trigger_type,
        trigger_detail,
        started_at: parse_ts(&started_at_str),
        completed_at: completed_at_str.and_then(|s| parse_opt_ts(&s)),
        status,
        result_summary,
        tokens_used: tokens_used.map(|v| v as i32),
        job_id: job_id_str.and_then(|s| s.parse().ok()),
        created_at: parse_ts(&created_at_str),
    })
}

#[async_trait]
impl RoutineStore for OracleBackend {
    async fn create_routine(&self, routine: &Routine) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id = routine.id.to_string();
        let name = routine.name.clone();
        let description = routine.description.clone();
        let user_id = routine.user_id.clone();
        let enabled = routine.enabled as i32;
        let trigger_type = routine.trigger.type_tag().to_string();
        let trigger_config = routine.trigger.to_config_json().to_string();
        let action_type = routine.action.type_tag().to_string();
        let action_config = routine.action.to_config_json().to_string();
        let cooldown_secs = routine.guardrails.cooldown.as_secs() as i64;
        let max_concurrent = routine.guardrails.max_concurrent as i64;
        let dedup_window_secs: Option<i64> =
            routine.guardrails.dedup_window.map(|d| d.as_secs() as i64);
        let notify_channel = routine.notify.channel.clone();
        let notify_user = routine.notify.user.clone();
        let notify_on_success = routine.notify.on_success as i32;
        let notify_on_failure = routine.notify.on_failure as i32;
        let notify_on_attention = routine.notify.on_attention as i32;
        let state = routine.state.to_string();
        let next_fire_at = routine.next_fire_at.map(|dt| fmt_ts(&dt));
        let created_at = fmt_ts(&routine.created_at);
        let updated_at = fmt_ts(&routine.updated_at);

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "INSERT INTO IRON_ROUTINES (
                    id, name, description, user_id, enabled,
                    trigger_type, trigger_config, action_type, action_config,
                    cooldown_secs, max_concurrent, dedup_window_secs,
                    notify_channel, notify_user, notify_on_success, notify_on_failure, notify_on_attention,
                    state, next_fire_at, created_at, updated_at
                ) VALUES (
                    :1, :2, :3, :4, :5,
                    :6, :7, :8, :9,
                    :10, :11, :12,
                    :13, :14, :15, :16, :17,
                    :18,
                    CASE WHEN :19 IS NOT NULL THEN TO_TIMESTAMP(:19, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') ELSE NULL END,
                    TO_TIMESTAMP(:20, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),
                    TO_TIMESTAMP(:21, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"')
                )",
                &[
                    &id, &name, &description, &user_id, &enabled,
                    &trigger_type, &trigger_config, &action_type, &action_config,
                    &cooldown_secs, &max_concurrent,
                    &dedup_window_secs as &dyn oracle::sql_type::ToSql,
                    &notify_channel as &dyn oracle::sql_type::ToSql,
                    &notify_user, &notify_on_success, &notify_on_failure, &notify_on_attention,
                    &state,
                    &next_fire_at as &dyn oracle::sql_type::ToSql,
                    &created_at,
                    &updated_at,
                ],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))??;
        Ok(())
    }

    async fn get_routine(&self, id: Uuid) -> Result<Option<Routine>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = format!(
                "SELECT {} FROM IRON_ROUTINES WHERE id = :1",
                ROUTINE_COLUMNS
            );
            let rows = conn
                .query(&sql, &[&id_str])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            for row_result in rows {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                return Ok(Some(row_to_routine(&row)?));
            }
            Ok(None)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn get_routine_by_name(
        &self,
        user_id: &str,
        name: &str,
    ) -> Result<Option<Routine>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();
        let name = name.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = format!(
                "SELECT {} FROM IRON_ROUTINES WHERE user_id = :1 AND name = :2",
                ROUTINE_COLUMNS
            );
            let rows = conn
                .query(&sql, &[&user_id, &name])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            for row_result in rows {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                return Ok(Some(row_to_routine(&row)?));
            }
            Ok(None)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn list_routines(&self, user_id: &str) -> Result<Vec<Routine>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let user_id = user_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = format!(
                "SELECT {} FROM IRON_ROUTINES WHERE user_id = :1 ORDER BY name",
                ROUTINE_COLUMNS
            );
            let rows = conn
                .query(&sql, &[&user_id])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            let mut routines = Vec::new();
            for row_result in rows {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                routines.push(row_to_routine(&row)?);
            }
            Ok(routines)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn list_event_routines(&self) -> Result<Vec<Routine>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = format!(
                "SELECT {} FROM IRON_ROUTINES WHERE enabled = 1 AND trigger_type = 'event'",
                ROUTINE_COLUMNS
            );
            let rows = conn
                .query(&sql, &[] as &[&dyn oracle::sql_type::ToSql])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            let mut routines = Vec::new();
            for row_result in rows {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                routines.push(row_to_routine(&row)?);
            }
            Ok(routines)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn list_due_cron_routines(&self) -> Result<Vec<Routine>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = format!(
                "SELECT {} FROM IRON_ROUTINES WHERE enabled = 1 AND trigger_type = 'cron' AND next_fire_at IS NOT NULL AND next_fire_at <= CURRENT_TIMESTAMP",
                ROUTINE_COLUMNS
            );
            let rows = conn
                .query(&sql, &[] as &[&dyn oracle::sql_type::ToSql])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            let mut routines = Vec::new();
            for row_result in rows {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                routines.push(row_to_routine(&row)?);
            }
            Ok(routines)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn update_routine(&self, routine: &Routine) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id = routine.id.to_string();
        let name = routine.name.clone();
        let description = routine.description.clone();
        let enabled = routine.enabled as i32;
        let trigger_type = routine.trigger.type_tag().to_string();
        let trigger_config = routine.trigger.to_config_json().to_string();
        let action_type = routine.action.type_tag().to_string();
        let action_config = routine.action.to_config_json().to_string();
        let cooldown_secs = routine.guardrails.cooldown.as_secs() as i64;
        let max_concurrent = routine.guardrails.max_concurrent as i64;
        let dedup_window_secs: Option<i64> =
            routine.guardrails.dedup_window.map(|d| d.as_secs() as i64);
        let notify_channel = routine.notify.channel.clone();
        let notify_user = routine.notify.user.clone();
        let notify_on_success = routine.notify.on_success as i32;
        let notify_on_failure = routine.notify.on_failure as i32;
        let notify_on_attention = routine.notify.on_attention as i32;
        let state = routine.state.to_string();
        let next_fire_at = routine.next_fire_at.map(|dt| fmt_ts(&dt));
        let now = fmt_ts(&Utc::now());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "UPDATE IRON_ROUTINES SET
                    name = :2, description = :3, enabled = :4,
                    trigger_type = :5, trigger_config = :6,
                    action_type = :7, action_config = :8,
                    cooldown_secs = :9, max_concurrent = :10, dedup_window_secs = :11,
                    notify_channel = :12, notify_user = :13,
                    notify_on_success = :14, notify_on_failure = :15, notify_on_attention = :16,
                    state = :17,
                    next_fire_at = CASE WHEN :18 IS NOT NULL THEN TO_TIMESTAMP(:18, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') ELSE NULL END,
                    updated_at = TO_TIMESTAMP(:19, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"')
                WHERE id = :1",
                &[
                    &id,
                    &name, &description, &enabled,
                    &trigger_type, &trigger_config,
                    &action_type, &action_config,
                    &cooldown_secs, &max_concurrent,
                    &dedup_window_secs as &dyn oracle::sql_type::ToSql,
                    &notify_channel as &dyn oracle::sql_type::ToSql,
                    &notify_user,
                    &notify_on_success, &notify_on_failure, &notify_on_attention,
                    &state,
                    &next_fire_at as &dyn oracle::sql_type::ToSql,
                    &now,
                ],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))??;
        Ok(())
    }

    async fn update_routine_runtime(
        &self,
        id: Uuid,
        last_run_at: DateTime<Utc>,
        next_fire_at: Option<DateTime<Utc>>,
        run_count: u64,
        consecutive_failures: u32,
        state: &serde_json::Value,
    ) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();
        let last_run_at_str = fmt_ts(&last_run_at);
        let next_fire_at_str = next_fire_at.map(|dt| fmt_ts(&dt));
        let run_count = run_count as i64;
        let consecutive_failures = consecutive_failures as i64;
        let state_str = state.to_string();
        let now = fmt_ts(&Utc::now());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "UPDATE IRON_ROUTINES SET
                    last_run_at = TO_TIMESTAMP(:2, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),
                    next_fire_at = CASE WHEN :3 IS NOT NULL THEN TO_TIMESTAMP(:3, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') ELSE NULL END,
                    run_count = :4, consecutive_failures = :5,
                    state = :6,
                    updated_at = TO_TIMESTAMP(:7, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"')
                WHERE id = :1",
                &[
                    &id_str,
                    &last_run_at_str,
                    &next_fire_at_str as &dyn oracle::sql_type::ToSql,
                    &run_count,
                    &consecutive_failures,
                    &state_str,
                    &now,
                ],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))??;
        Ok(())
    }

    async fn delete_routine(&self, id: Uuid) -> Result<bool, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let stmt = conn
                .execute(
                    "DELETE FROM IRON_ROUTINES WHERE id = :1",
                    &[&id_str],
                )
                .map_err(|e| DatabaseError::Query(e.to_string()))?;
            let count = stmt.row_count().map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(count > 0)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn create_routine_run(&self, run: &RoutineRun) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id = run.id.to_string();
        let routine_id = run.routine_id.to_string();
        let trigger_type = run.trigger_type.clone();
        let trigger_detail = run.trigger_detail.clone();
        let started_at = fmt_ts(&run.started_at);
        let status = run.status.to_string();
        let job_id = run.job_id.map(|id| id.to_string());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "INSERT INTO IRON_ROUTINE_RUNS (
                    id, routine_id, trigger_type, trigger_detail,
                    started_at, status, job_id
                ) VALUES (
                    :1, :2, :3, :4,
                    TO_TIMESTAMP(:5, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),
                    :6, :7
                )",
                &[
                    &id,
                    &routine_id,
                    &trigger_type,
                    &trigger_detail as &dyn oracle::sql_type::ToSql,
                    &started_at,
                    &status,
                    &job_id as &dyn oracle::sql_type::ToSql,
                ],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))??;
        Ok(())
    }

    async fn complete_routine_run(
        &self,
        id: Uuid,
        status: RunStatus,
        result_summary: Option<&str>,
        tokens_used: Option<i32>,
    ) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let id_str = id.to_string();
        let status_str = status.to_string();
        let result_summary = result_summary.map(String::from);
        let tokens_used = tokens_used.map(|t| t as i64);
        let now = fmt_ts(&Utc::now());

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "UPDATE IRON_ROUTINE_RUNS SET
                    completed_at = TO_TIMESTAMP(:5, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),
                    status = :2,
                    result_summary = :3,
                    tokens_used = :4
                WHERE id = :1",
                &[
                    &id_str,
                    &status_str,
                    &result_summary as &dyn oracle::sql_type::ToSql,
                    &tokens_used as &dyn oracle::sql_type::ToSql,
                    &now,
                ],
            )
            .map_err(|e| DatabaseError::Query(e.to_string()))?;
            conn.commit().map_err(|e| DatabaseError::Query(e.to_string()))?;
            Ok::<_, DatabaseError>(())
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))??;
        Ok(())
    }

    async fn list_routine_runs(
        &self,
        routine_id: Uuid,
        limit: i64,
    ) -> Result<Vec<RoutineRun>, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let routine_id_str = routine_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let sql = format!(
                "SELECT {} FROM IRON_ROUTINE_RUNS WHERE routine_id = :1 ORDER BY started_at DESC FETCH FIRST :2 ROWS ONLY",
                ROUTINE_RUN_COLUMNS
            );
            let rows = conn
                .query(&sql, &[&routine_id_str, &limit])
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            let mut runs = Vec::new();
            for row_result in rows {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                runs.push(row_to_routine_run(&row)?);
            }
            Ok(runs)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn count_running_routine_runs(&self, routine_id: Uuid) -> Result<i64, DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let routine_id_str = routine_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            let rows = conn
                .query(
                    "SELECT COUNT(*) FROM IRON_ROUTINE_RUNS WHERE routine_id = :1 AND status = 'running'",
                    &[&routine_id_str],
                )
                .map_err(|e| DatabaseError::Query(e.to_string()))?;

            for row_result in rows {
                let row = row_result.map_err(|e| DatabaseError::Query(e.to_string()))?;
                let count: i64 = row.get(0).unwrap_or(0);
                return Ok(count);
            }
            Ok(0)
        })
        .await
        .map_err(|e| DatabaseError::Query(format!("Spawn error: {e}")))?
    }

    async fn link_routine_run_to_job(
        &self,
        run_id: Uuid,
        job_id: Uuid,
    ) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        let job_id_str = job_id.to_string();
        let run_id_str = run_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn.lock().map_err(|e| DatabaseError::Query(format!("Lock error: {e}")))?;
            conn.execute(
                "UPDATE IRON_ROUTINE_RUNS SET job_id = :1 WHERE id = :2",
                &[&job_id_str, &run_id_str],
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
