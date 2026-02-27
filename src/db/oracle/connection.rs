//! Oracle AI Database connection manager.
//!
//! Supports two connection modes:
//! - **FreePDB**: Standard `//host:port/service` connection (Oracle Database Free container)
//! - **ADB**: Autonomous Database with DSN (wallet-less TLS or mTLS with wallet)

use crate::config::DatabaseConfig;
use oracle::{Connection, Connector};
use secrecy::ExposeSecret;
use std::sync::{Arc, Mutex};
use tracing::info;

/// Manages Oracle database connections with FreePDB and ADB support.
pub struct OracleConnectionManager {
    config: DatabaseConfig,
    conn: Arc<Mutex<Connection>>,
}

impl OracleConnectionManager {
    /// Create a new connection manager and establish connection.
    pub fn new(config: &DatabaseConfig) -> anyhow::Result<Self> {
        let conn = match config.oracle_mode.as_str() {
            "adb" => {
                info!("Connecting to Oracle Autonomous Database...");
                Self::connect_adb(config)?
            }
            _ => {
                info!(
                    "Connecting to Oracle FreePDB at {}:{}/{}...",
                    config.oracle_host, config.oracle_port, config.oracle_service
                );
                Self::connect_freepdb(config)?
            }
        };

        info!("Oracle connection established");
        Ok(Self {
            config: config.clone(),
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn connect_freepdb(config: &DatabaseConfig) -> anyhow::Result<Connection> {
        let connect_string = format!(
            "//{}:{}/{}",
            config.oracle_host, config.oracle_port, config.oracle_service
        );
        let conn = Connector::new(
            &config.oracle_user,
            config.oracle_password.expose_secret(),
            &connect_string,
        )
        .connect()?;
        Ok(conn)
    }

    fn connect_adb(config: &DatabaseConfig) -> anyhow::Result<Connection> {
        let dsn = config
            .oracle_dsn
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("ADB mode requires IRONORACLAW_ORACLE_DSN"))?;
        let conn = Connector::new(
            &config.oracle_user,
            config.oracle_password.expose_secret(),
            dsn,
        )
        .connect()?;
        Ok(conn)
    }

    /// Get a shared reference to the connection.
    pub fn conn(&self) -> Arc<Mutex<Connection>> {
        self.conn.clone()
    }

    /// Get the agent ID from config.
    pub fn agent_id(&self) -> &str {
        &self.config.oracle_agent_id
    }

    /// Get the ONNX model name from config.
    pub fn onnx_model(&self) -> &str {
        &self.config.oracle_onnx_model
    }

    /// Get a reference to the database config.
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Check if the connection is alive.
    pub fn ping(&self) -> bool {
        self.conn
            .lock()
            .map_or(false, |conn| conn.ping().is_ok())
    }
}
