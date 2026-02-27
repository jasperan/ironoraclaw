//! Oracle schema initialization and migration.
//!
//! Creates the 15 `IRON_*` tables, regular indexes, and vector indexes
//! required by IronOraClaw.  All DDL is idempotent -- existing objects
//! are silently skipped via ORA-00955 / ORA-01408 error handling.

use oracle::Connection;
use tracing::{debug, info};

// -- ORA error codes we intentionally ignore ----------------------------
/// ORA-00955: name is already used by an existing object
const ORA_NAME_ALREADY_USED: i32 = 955;
/// ORA-01408: such column list already indexed
const ORA_COLUMN_ALREADY_INDEXED: i32 = 1408;

// ======================================================================
// Table DDL
// ======================================================================

const CREATE_IRON_META: &str = "
CREATE TABLE IRON_META (
    agent_id        VARCHAR2(128)   NOT NULL,
    schema_version  NUMBER(10)      DEFAULT 1 NOT NULL,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_iron_meta PRIMARY KEY (agent_id)
)";

const CREATE_IRON_CONVERSATIONS: &str = "
CREATE TABLE IRON_CONVERSATIONS (
    id              VARCHAR2(36)    NOT NULL,
    channel         VARCHAR2(256)   NOT NULL,
    user_id         VARCHAR2(256)   NOT NULL,
    thread_id       VARCHAR2(256),
    started_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    last_activity   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    metadata        CLOB            DEFAULT '{}',
    CONSTRAINT pk_iron_conversations PRIMARY KEY (id)
)";

const CREATE_IRON_MESSAGES: &str = "
CREATE TABLE IRON_MESSAGES (
    id              VARCHAR2(36)    NOT NULL,
    conversation_id VARCHAR2(36)    NOT NULL,
    role            VARCHAR2(64)    NOT NULL,
    content         CLOB            NOT NULL,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_iron_messages PRIMARY KEY (id),
    CONSTRAINT fk_iron_messages_conv
        FOREIGN KEY (conversation_id) REFERENCES IRON_CONVERSATIONS(id) ON DELETE CASCADE
)";

const CREATE_IRON_JOBS: &str = "
CREATE TABLE IRON_JOBS (
    id                  VARCHAR2(36)    NOT NULL,
    marketplace_job_id  VARCHAR2(36),
    conversation_id     VARCHAR2(36),
    title               VARCHAR2(4000)  NOT NULL,
    description         CLOB            NOT NULL,
    category            VARCHAR2(256),
    status              VARCHAR2(64)    NOT NULL,
    source              VARCHAR2(256)   NOT NULL,
    budget_amount       NUMBER(19,2),
    budget_token        VARCHAR2(128),
    bid_amount          NUMBER(19,2),
    estimated_cost      NUMBER(19,2),
    estimated_time_secs NUMBER,
    estimated_value     NUMBER(19,2),
    actual_cost         NUMBER(19,2),
    actual_time_secs    NUMBER,
    success             NUMBER(1),
    failure_reason      CLOB,
    stuck_since         TIMESTAMP,
    repair_attempts     NUMBER          DEFAULT 0 NOT NULL,
    project_dir         VARCHAR2(4000),
    user_id             VARCHAR2(256)   DEFAULT 'default' NOT NULL,
    job_mode            VARCHAR2(64)    DEFAULT 'worker' NOT NULL,
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    started_at          TIMESTAMP,
    completed_at        TIMESTAMP,
    CONSTRAINT pk_iron_jobs PRIMARY KEY (id)
)";

const CREATE_IRON_ACTIONS: &str = "
CREATE TABLE IRON_ACTIONS (
    id                      VARCHAR2(36)    NOT NULL,
    job_id                  VARCHAR2(36)    NOT NULL,
    sequence_num            NUMBER          NOT NULL,
    tool_name               VARCHAR2(512)   NOT NULL,
    input                   CLOB            NOT NULL,
    output_raw              CLOB,
    output_sanitized        CLOB,
    sanitization_warnings   CLOB,
    cost                    NUMBER(19,4),
    duration_ms             NUMBER,
    success                 NUMBER(1)       NOT NULL,
    error_message           CLOB,
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_iron_actions PRIMARY KEY (id),
    CONSTRAINT fk_iron_actions_job
        FOREIGN KEY (job_id) REFERENCES IRON_JOBS(id) ON DELETE CASCADE,
    CONSTRAINT uq_iron_actions_seq UNIQUE (job_id, sequence_num)
)";

const CREATE_IRON_LLM_CALLS: &str = "
CREATE TABLE IRON_LLM_CALLS (
    id                  VARCHAR2(36)    NOT NULL,
    job_id              VARCHAR2(36),
    conversation_id     VARCHAR2(36),
    provider            VARCHAR2(256)   NOT NULL,
    model               VARCHAR2(256)   NOT NULL,
    input_tokens        NUMBER          NOT NULL,
    output_tokens       NUMBER          NOT NULL,
    cost                NUMBER(19,4)    NOT NULL,
    purpose             VARCHAR2(512),
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_iron_llm_calls PRIMARY KEY (id)
)";

const CREATE_IRON_MEMORIES: &str = "
CREATE TABLE IRON_MEMORIES (
    id              VARCHAR2(36)    NOT NULL,
    user_id         VARCHAR2(256)   NOT NULL,
    agent_id        VARCHAR2(36),
    path            VARCHAR2(4000)  NOT NULL,
    content         CLOB            NOT NULL,
    metadata        CLOB            DEFAULT '{}',
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_iron_memories PRIMARY KEY (id),
    CONSTRAINT uq_iron_memories_path UNIQUE (user_id, agent_id, path)
)";

const CREATE_IRON_CHUNKS: &str = "
CREATE TABLE IRON_CHUNKS (
    id              VARCHAR2(36)    NOT NULL,
    document_id     VARCHAR2(36)    NOT NULL,
    chunk_index     NUMBER          NOT NULL,
    content         CLOB            NOT NULL,
    embedding       VECTOR(384, FLOAT32),
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_iron_chunks PRIMARY KEY (id),
    CONSTRAINT fk_iron_chunks_doc
        FOREIGN KEY (document_id) REFERENCES IRON_MEMORIES(id) ON DELETE CASCADE,
    CONSTRAINT uq_iron_chunks_idx UNIQUE (document_id, chunk_index)
)";

const CREATE_IRON_SANDBOX_JOBS: &str = "
CREATE TABLE IRON_SANDBOX_JOBS (
    id              VARCHAR2(36)    NOT NULL,
    job_id          VARCHAR2(36),
    user_id         VARCHAR2(256),
    tool_name       VARCHAR2(512),
    status          VARCHAR2(64)    NOT NULL,
    success         NUMBER(1),
    message         CLOB,
    mode            VARCHAR2(64),
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_iron_sandbox_jobs PRIMARY KEY (id)
)";

const CREATE_IRON_JOB_EVENTS: &str = "
CREATE TABLE IRON_JOB_EVENTS (
    id              NUMBER          GENERATED ALWAYS AS IDENTITY,
    job_id          VARCHAR2(36)    NOT NULL,
    event_type      VARCHAR2(256)   NOT NULL,
    data            CLOB            NOT NULL,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_iron_job_events PRIMARY KEY (id)
)";

const CREATE_IRON_ROUTINES: &str = "
CREATE TABLE IRON_ROUTINES (
    id                      VARCHAR2(36)    NOT NULL,
    name                    VARCHAR2(512)   NOT NULL,
    description             CLOB            DEFAULT '',
    user_id                 VARCHAR2(256)   NOT NULL,
    enabled                 NUMBER(1)       DEFAULT 1 NOT NULL,
    trigger_type            VARCHAR2(64)    NOT NULL,
    trigger_config          CLOB            NOT NULL,
    action_type             VARCHAR2(64)    NOT NULL,
    action_config           CLOB            NOT NULL,
    cooldown_secs           NUMBER          DEFAULT 300 NOT NULL,
    max_concurrent          NUMBER          DEFAULT 1 NOT NULL,
    dedup_window_secs       NUMBER,
    notify_channel          VARCHAR2(256),
    notify_user             VARCHAR2(256)   DEFAULT 'default' NOT NULL,
    notify_on_success       NUMBER(1)       DEFAULT 0 NOT NULL,
    notify_on_failure       NUMBER(1)       DEFAULT 1 NOT NULL,
    notify_on_attention     NUMBER(1)       DEFAULT 1 NOT NULL,
    state                   CLOB            DEFAULT '{}' NOT NULL,
    last_run_at             TIMESTAMP,
    next_fire_at            TIMESTAMP,
    run_count               NUMBER          DEFAULT 0 NOT NULL,
    consecutive_failures    NUMBER          DEFAULT 0 NOT NULL,
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_iron_routines PRIMARY KEY (id),
    CONSTRAINT uq_iron_routines_name UNIQUE (user_id, name)
)";

const CREATE_IRON_ROUTINE_RUNS: &str = "
CREATE TABLE IRON_ROUTINE_RUNS (
    id              VARCHAR2(36)    NOT NULL,
    routine_id      VARCHAR2(36)    NOT NULL,
    trigger_type    VARCHAR2(64)    NOT NULL,
    trigger_detail  VARCHAR2(4000),
    started_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    completed_at    TIMESTAMP,
    status          VARCHAR2(64)    DEFAULT 'running' NOT NULL,
    result_summary  CLOB,
    tokens_used     NUMBER,
    job_id          VARCHAR2(36),
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_iron_routine_runs PRIMARY KEY (id),
    CONSTRAINT fk_iron_routine_runs_routine
        FOREIGN KEY (routine_id) REFERENCES IRON_ROUTINES(id) ON DELETE CASCADE
)";

const CREATE_IRON_TOOL_FAILURES: &str = "
CREATE TABLE IRON_TOOL_FAILURES (
    tool_name       VARCHAR2(512)   NOT NULL,
    error_message   CLOB,
    failure_count   NUMBER          DEFAULT 1 NOT NULL,
    repair_attempts NUMBER          DEFAULT 0 NOT NULL,
    first_failure   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    last_failure    TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    repaired        NUMBER(1)       DEFAULT 0,
    CONSTRAINT pk_iron_tool_failures PRIMARY KEY (tool_name)
)";

const CREATE_IRON_SETTINGS: &str = "
CREATE TABLE IRON_SETTINGS (
    user_id     VARCHAR2(256)   NOT NULL,
    key         VARCHAR2(512)   NOT NULL,
    value       CLOB            NOT NULL,
    updated_at  TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_iron_settings PRIMARY KEY (user_id, key)
)";

const CREATE_IRON_ESTIMATES: &str = "
CREATE TABLE IRON_ESTIMATES (
    id                  VARCHAR2(36)    NOT NULL,
    job_id              VARCHAR2(36)    NOT NULL,
    category            VARCHAR2(256)   NOT NULL,
    tool_names          CLOB            NOT NULL,
    estimated_cost      NUMBER(19,2)    NOT NULL,
    actual_cost         NUMBER(19,2),
    estimated_time_secs NUMBER          NOT NULL,
    actual_time_secs    NUMBER,
    estimated_value     NUMBER(19,2)    NOT NULL,
    actual_value        NUMBER(19,2),
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_iron_estimates PRIMARY KEY (id),
    CONSTRAINT fk_iron_estimates_job
        FOREIGN KEY (job_id) REFERENCES IRON_JOBS(id) ON DELETE CASCADE
)";

// ======================================================================
// Regular index DDL
// ======================================================================

const INDEXES: &[&str] = &[
    // IRON_CONVERSATIONS
    "CREATE INDEX idx_iron_conv_channel ON IRON_CONVERSATIONS(channel)",
    "CREATE INDEX idx_iron_conv_user ON IRON_CONVERSATIONS(user_id)",
    "CREATE INDEX idx_iron_conv_activity ON IRON_CONVERSATIONS(last_activity)",
    // IRON_MESSAGES
    "CREATE INDEX idx_iron_msg_conv ON IRON_MESSAGES(conversation_id)",
    // IRON_JOBS
    "CREATE INDEX idx_iron_jobs_status ON IRON_JOBS(status)",
    "CREATE INDEX idx_iron_jobs_marketplace ON IRON_JOBS(marketplace_job_id)",
    "CREATE INDEX idx_iron_jobs_conv ON IRON_JOBS(conversation_id)",
    "CREATE INDEX idx_iron_jobs_user ON IRON_JOBS(user_id)",
    "CREATE INDEX idx_iron_jobs_source ON IRON_JOBS(source)",
    "CREATE INDEX idx_iron_jobs_created ON IRON_JOBS(created_at)",
    // IRON_ACTIONS
    "CREATE INDEX idx_iron_actions_job ON IRON_ACTIONS(job_id)",
    "CREATE INDEX idx_iron_actions_tool ON IRON_ACTIONS(tool_name)",
    // IRON_LLM_CALLS
    "CREATE INDEX idx_iron_llm_job ON IRON_LLM_CALLS(job_id)",
    "CREATE INDEX idx_iron_llm_conv ON IRON_LLM_CALLS(conversation_id)",
    "CREATE INDEX idx_iron_llm_provider ON IRON_LLM_CALLS(provider)",
    // IRON_MEMORIES
    "CREATE INDEX idx_iron_mem_user ON IRON_MEMORIES(user_id)",
    "CREATE INDEX idx_iron_mem_path ON IRON_MEMORIES(user_id, path)",
    "CREATE INDEX idx_iron_mem_updated ON IRON_MEMORIES(updated_at)",
    // IRON_CHUNKS
    "CREATE INDEX idx_iron_chunks_doc ON IRON_CHUNKS(document_id)",
    // IRON_SANDBOX_JOBS
    "CREATE INDEX idx_iron_sandbox_status ON IRON_SANDBOX_JOBS(status)",
    "CREATE INDEX idx_iron_sandbox_user ON IRON_SANDBOX_JOBS(user_id)",
    "CREATE INDEX idx_iron_sandbox_job ON IRON_SANDBOX_JOBS(job_id)",
    // IRON_JOB_EVENTS
    "CREATE INDEX idx_iron_events_job ON IRON_JOB_EVENTS(job_id)",
    // IRON_ROUTINES
    "CREATE INDEX idx_iron_routines_user ON IRON_ROUTINES(user_id)",
    "CREATE INDEX idx_iron_routines_fire ON IRON_ROUTINES(next_fire_at)",
    // IRON_ROUTINE_RUNS
    "CREATE INDEX idx_iron_runs_routine ON IRON_ROUTINE_RUNS(routine_id)",
    "CREATE INDEX idx_iron_runs_status ON IRON_ROUTINE_RUNS(status)",
    // IRON_SETTINGS
    "CREATE INDEX idx_iron_settings_user ON IRON_SETTINGS(user_id)",
    // IRON_ESTIMATES
    "CREATE INDEX idx_iron_est_job ON IRON_ESTIMATES(job_id)",
    "CREATE INDEX idx_iron_est_category ON IRON_ESTIMATES(category)",
];

// ======================================================================
// Vector index DDL
// ======================================================================

const VECTOR_INDEXES: &[&str] = &[
    "CREATE VECTOR INDEX vidx_iron_chunks_emb ON IRON_CHUNKS(embedding)
     ORGANIZATION NEIGHBOR PARTITIONS
     DISTANCE COSINE
     WITH TARGET ACCURACY 95",
];

// ======================================================================
// Helpers
// ======================================================================

/// Execute DDL, silently ignoring "already exists" errors.
fn exec_ddl_idempotent(conn: &Connection, sql: &str, ignore_codes: &[i32]) -> anyhow::Result<()> {
    match conn.execute(sql, &[]) {
        Ok(_) => Ok(()),
        Err(ref e) => {
            if let Some(db_err) = e.db_error() {
                if ignore_codes.contains(&db_err.code()) {
                    debug!(
                        "DDL skipped (ORA-{}): {}",
                        db_err.code(),
                        db_err.message().trim()
                    );
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("DDL failed: {e}\nSQL: {sql}"))
                }
            } else {
                Err(anyhow::anyhow!("DDL failed: {e}\nSQL: {sql}"))
            }
        }
    }
}

/// Seed an IRON_META row for this agent (MERGE = upsert).
fn seed_meta(conn: &Connection, agent_id: &str) -> anyhow::Result<()> {
    let sql = "
        MERGE INTO IRON_META m
        USING (SELECT :1 AS agent_id FROM DUAL) src
        ON (m.agent_id = src.agent_id)
        WHEN NOT MATCHED THEN
            INSERT (agent_id, schema_version, created_at, updated_at)
            VALUES (src.agent_id, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        WHEN MATCHED THEN
            UPDATE SET m.updated_at = CURRENT_TIMESTAMP
    ";
    conn.execute(sql, &[&agent_id])?;
    Ok(())
}

// ======================================================================
// Public API
// ======================================================================

/// Initialise the full IronOraClaw schema idempotently.
///
/// The caller must hold the `Mutex<Connection>` lock and pass the
/// inner `&Connection`.  This function commits on success.
pub fn init_schema(conn: &Connection, agent_id: &str) -> anyhow::Result<()> {
    info!("Initialising Oracle schema for agent '{agent_id}'...");

    // 1. Create tables (ignore ORA-00955 "name already used")
    let table_stmts = [
        ("IRON_META", CREATE_IRON_META),
        ("IRON_CONVERSATIONS", CREATE_IRON_CONVERSATIONS),
        ("IRON_MESSAGES", CREATE_IRON_MESSAGES),
        ("IRON_JOBS", CREATE_IRON_JOBS),
        ("IRON_ACTIONS", CREATE_IRON_ACTIONS),
        ("IRON_LLM_CALLS", CREATE_IRON_LLM_CALLS),
        ("IRON_MEMORIES", CREATE_IRON_MEMORIES),
        ("IRON_CHUNKS", CREATE_IRON_CHUNKS),
        ("IRON_SANDBOX_JOBS", CREATE_IRON_SANDBOX_JOBS),
        ("IRON_JOB_EVENTS", CREATE_IRON_JOB_EVENTS),
        ("IRON_ROUTINES", CREATE_IRON_ROUTINES),
        ("IRON_ROUTINE_RUNS", CREATE_IRON_ROUTINE_RUNS),
        ("IRON_TOOL_FAILURES", CREATE_IRON_TOOL_FAILURES),
        ("IRON_SETTINGS", CREATE_IRON_SETTINGS),
        ("IRON_ESTIMATES", CREATE_IRON_ESTIMATES),
    ];

    for (name, ddl) in &table_stmts {
        debug!("Creating table {name}...");
        exec_ddl_idempotent(conn, ddl, &[ORA_NAME_ALREADY_USED])?;
    }

    // 2. Create regular indexes (ignore ORA-00955 / ORA-01408)
    for idx_ddl in INDEXES {
        exec_ddl_idempotent(conn, idx_ddl, &[ORA_NAME_ALREADY_USED, ORA_COLUMN_ALREADY_INDEXED])?;
    }

    // 3. Create vector indexes (ignore ORA-00955 / ORA-01408)
    for vidx_ddl in VECTOR_INDEXES {
        exec_ddl_idempotent(
            conn,
            vidx_ddl,
            &[ORA_NAME_ALREADY_USED, ORA_COLUMN_ALREADY_INDEXED],
        )?;
    }

    // 4. Seed meta row for this agent
    seed_meta(conn, agent_id)?;

    // 5. Commit the transaction
    conn.commit()?;
    info!("Oracle schema ready (agent '{agent_id}')");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_ddl_contains_primary_keys() {
        assert!(CREATE_IRON_META.contains("pk_iron_meta"));
        assert!(CREATE_IRON_CONVERSATIONS.contains("pk_iron_conversations"));
        assert!(CREATE_IRON_MESSAGES.contains("pk_iron_messages"));
        assert!(CREATE_IRON_JOBS.contains("pk_iron_jobs"));
        assert!(CREATE_IRON_ACTIONS.contains("pk_iron_actions"));
        assert!(CREATE_IRON_LLM_CALLS.contains("pk_iron_llm_calls"));
        assert!(CREATE_IRON_MEMORIES.contains("pk_iron_memories"));
        assert!(CREATE_IRON_CHUNKS.contains("pk_iron_chunks"));
        assert!(CREATE_IRON_SANDBOX_JOBS.contains("pk_iron_sandbox_jobs"));
        assert!(CREATE_IRON_JOB_EVENTS.contains("pk_iron_job_events"));
        assert!(CREATE_IRON_ROUTINES.contains("pk_iron_routines"));
        assert!(CREATE_IRON_ROUTINE_RUNS.contains("pk_iron_routine_runs"));
        assert!(CREATE_IRON_TOOL_FAILURES.contains("pk_iron_tool_failures"));
        assert!(CREATE_IRON_SETTINGS.contains("pk_iron_settings"));
        assert!(CREATE_IRON_ESTIMATES.contains("pk_iron_estimates"));
    }

    #[test]
    fn table_count_is_correct() {
        // 15 tables total
        let table_count = 15;
        let tables = [
            CREATE_IRON_META,
            CREATE_IRON_CONVERSATIONS,
            CREATE_IRON_MESSAGES,
            CREATE_IRON_JOBS,
            CREATE_IRON_ACTIONS,
            CREATE_IRON_LLM_CALLS,
            CREATE_IRON_MEMORIES,
            CREATE_IRON_CHUNKS,
            CREATE_IRON_SANDBOX_JOBS,
            CREATE_IRON_JOB_EVENTS,
            CREATE_IRON_ROUTINES,
            CREATE_IRON_ROUTINE_RUNS,
            CREATE_IRON_TOOL_FAILURES,
            CREATE_IRON_SETTINGS,
            CREATE_IRON_ESTIMATES,
        ];
        assert_eq!(tables.len(), table_count);
    }

    #[test]
    fn table_ddl_contains_vector_column() {
        assert!(CREATE_IRON_CHUNKS.contains("VECTOR(384, FLOAT32)"));
    }

    #[test]
    fn index_count_is_correct() {
        // 3 conv + 1 msg + 6 jobs + 2 actions + 3 llm + 3 mem + 1 chunks
        // + 3 sandbox + 1 events + 2 routines + 2 runs + 1 settings + 2 estimates = 30
        assert_eq!(INDEXES.len(), 30);
    }

    #[test]
    fn vector_index_count_is_correct() {
        assert_eq!(VECTOR_INDEXES.len(), 1);
    }

    #[test]
    fn vector_indexes_use_cosine_distance() {
        for vidx in VECTOR_INDEXES {
            assert!(vidx.contains("COSINE"), "Vector index missing COSINE: {vidx}");
            assert!(
                vidx.contains("TARGET ACCURACY 95"),
                "Vector index missing TARGET ACCURACY: {vidx}"
            );
        }
    }
}
