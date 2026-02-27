//! Oracle AI Database backend for the Database trait.
//!
//! All Oracle operations are synchronous (the `oracle` crate does not support
//! async).  Every blocking call is wrapped in `tokio::task::spawn_blocking`
//! so the Tokio runtime stays responsive.

pub mod connection;
pub mod schema;

// Sub-trait implementations (one file per Database sub-trait).
pub mod conversations;
pub mod jobs;
pub mod sandbox;
pub mod routines;
pub mod tool_failures;
pub mod settings;
pub mod workspace;

use std::sync::Arc;

use async_trait::async_trait;

use crate::db::Database;
use crate::error::DatabaseError;
use crate::config::DatabaseConfig;

pub use connection::OracleConnectionManager;

/// Oracle database backend.
///
/// Wraps an `OracleConnectionManager` to implement the unified `Database`
/// trait.  All synchronous Oracle calls are dispatched via
/// `tokio::task::spawn_blocking`.
pub struct OracleBackend {
    pub conn_mgr: Arc<OracleConnectionManager>,
}

impl OracleBackend {
    /// Create a new Oracle backend from configuration.
    ///
    /// Establishes the connection (blocking) inside `spawn_blocking`, then
    /// returns the backend ready for use.
    pub async fn new(config: &DatabaseConfig) -> Result<Self, anyhow::Error> {
        let config = config.clone();
        let conn_mgr = tokio::task::spawn_blocking(move || {
            OracleConnectionManager::new(&config)
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking join error: {e}"))??;

        Ok(Self {
            conn_mgr: Arc::new(conn_mgr),
        })
    }
}

// ==================== Database (supertrait) ====================

#[async_trait]
impl Database for OracleBackend {
    async fn run_migrations(&self) -> Result<(), DatabaseError> {
        let conn_mgr = self.conn_mgr.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn_mgr.conn();
            let conn = conn
                .lock()
                .map_err(|e| DatabaseError::Migration(format!("Mutex poisoned: {e}")))?;
            schema::init_schema(&conn, conn_mgr.agent_id())
                .map_err(|e| DatabaseError::Migration(e.to_string()))
        })
        .await
        .map_err(|e| DatabaseError::Migration(format!("spawn_blocking join error: {e}")))?
    }
}
