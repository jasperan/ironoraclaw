use std::path::PathBuf;

use secrecy::{ExposeSecret, SecretString};

use crate::bootstrap::ironclaw_base_dir;
use crate::config::helpers::{optional_env, parse_optional_env};
use crate::error::ConfigError;

/// Which database backend to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DatabaseBackend {
    /// Oracle AI Database (default for IronOraClaw).
    #[default]
    Oracle,
    /// PostgreSQL via deadpool-postgres.
    Postgres,
    /// libSQL/Turso embedded database.
    LibSql,
}

impl std::fmt::Display for DatabaseBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Oracle => write!(f, "oracle"),
            Self::Postgres => write!(f, "postgres"),
            Self::LibSql => write!(f, "libsql"),
        }
    }
}

impl std::str::FromStr for DatabaseBackend {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "oracle" | "oracledb" => Ok(Self::Oracle),
            "postgres" | "postgresql" | "pg" => Ok(Self::Postgres),
            "libsql" | "turso" | "sqlite" => Ok(Self::LibSql),
            _ => Err(format!(
                "invalid database backend '{}', expected 'oracle', 'postgres', or 'libsql'",
                s
            )),
        }
    }
}

/// Database configuration.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Which backend to use (default: Oracle).
    pub backend: DatabaseBackend,

    // -- PostgreSQL fields --
    pub url: SecretString,
    pub pool_size: usize,

    // -- libSQL fields --
    /// Path to local libSQL database file (default: ~/.ironclaw/ironclaw.db).
    pub libsql_path: Option<PathBuf>,
    /// Turso cloud URL for remote sync (optional).
    pub libsql_url: Option<String>,
    /// Turso auth token (required when libsql_url is set).
    pub libsql_auth_token: Option<SecretString>,

    // -- Oracle fields --
    /// Oracle host (default: localhost).
    pub oracle_host: String,
    /// Oracle listener port (default: 1521).
    pub oracle_port: u16,
    /// Oracle service name (default: FREEPDB1).
    pub oracle_service: String,
    /// Oracle database user (default: ironoraclaw).
    pub oracle_user: String,
    /// Oracle database password.
    pub oracle_password: SecretString,
    /// Connection mode: "freepdb" for local container, "adb" for Autonomous Database.
    pub oracle_mode: String,
    /// DSN for ADB mode (required when oracle_mode = "adb").
    pub oracle_dsn: Option<String>,
    /// ONNX embedding model name (default: ALL_MINILM_L12_V2).
    pub oracle_onnx_model: String,
    /// Agent ID for multi-agent isolation (default: "default").
    pub oracle_agent_id: String,
}

impl DatabaseConfig {
    pub(crate) fn resolve() -> Result<Self, ConfigError> {
        let backend: DatabaseBackend = if let Some(b) = optional_env("DATABASE_BACKEND")? {
            b.parse().map_err(|e| ConfigError::InvalidValue {
                key: "DATABASE_BACKEND".to_string(),
                message: e,
            })?
        } else {
            DatabaseBackend::default()
        };

        // PostgreSQL URL is required only when using the postgres backend.
        // For libsql/oracle backends, default to an empty placeholder.
        let url = optional_env("DATABASE_URL")?
            .or_else(|| {
                if backend != DatabaseBackend::Postgres {
                    Some("unused://placeholder".to_string())
                } else {
                    None
                }
            })
            .ok_or_else(|| ConfigError::MissingRequired {
                key: "DATABASE_URL".to_string(),
                hint: "Run 'ironoraclaw onboard' or set DATABASE_URL environment variable"
                    .to_string(),
            })?;

        let pool_size = parse_optional_env("DATABASE_POOL_SIZE", 10)?;

        let libsql_path = optional_env("LIBSQL_PATH")?.map(PathBuf::from).or_else(|| {
            if backend == DatabaseBackend::LibSql {
                Some(default_libsql_path())
            } else {
                None
            }
        });

        let libsql_url = optional_env("LIBSQL_URL")?;
        let libsql_auth_token = optional_env("LIBSQL_AUTH_TOKEN")?.map(SecretString::from);

        if libsql_url.is_some() && libsql_auth_token.is_none() {
            return Err(ConfigError::MissingRequired {
                key: "LIBSQL_AUTH_TOKEN".to_string(),
                hint: "LIBSQL_AUTH_TOKEN is required when LIBSQL_URL is set".to_string(),
            });
        }

        // Oracle configuration
        let oracle_host = optional_env("IRONORACLAW_ORACLE_HOST")?
            .unwrap_or_else(|| "localhost".to_string());
        let oracle_port = parse_optional_env("IRONORACLAW_ORACLE_PORT", 1521)?;
        let oracle_service = optional_env("IRONORACLAW_ORACLE_SERVICE")?
            .unwrap_or_else(|| "FREEPDB1".to_string());
        let oracle_user = optional_env("IRONORACLAW_ORACLE_USER")?
            .unwrap_or_else(|| "ironoraclaw".to_string());
        let oracle_password = SecretString::from(
            optional_env("IRONORACLAW_ORACLE_PASSWORD")?
                .unwrap_or_else(|| "IronOraClaw2026".to_string()),
        );
        let oracle_mode = optional_env("IRONORACLAW_ORACLE_MODE")?
            .unwrap_or_else(|| "freepdb".to_string());
        let oracle_dsn = optional_env("IRONORACLAW_ORACLE_DSN")?;
        let oracle_onnx_model = optional_env("IRONORACLAW_ORACLE_ONNX_MODEL")?
            .unwrap_or_else(|| "ALL_MINILM_L12_V2".to_string());
        let oracle_agent_id = optional_env("IRONORACLAW_ORACLE_AGENT_ID")?
            .unwrap_or_else(|| "default".to_string());

        if backend == DatabaseBackend::Oracle
            && oracle_mode == "adb"
            && oracle_dsn.is_none()
        {
            return Err(ConfigError::MissingRequired {
                key: "IRONORACLAW_ORACLE_DSN".to_string(),
                hint: "IRONORACLAW_ORACLE_DSN is required when IRONORACLAW_ORACLE_MODE=adb"
                    .to_string(),
            });
        }

        Ok(Self {
            backend,
            url: SecretString::from(url),
            pool_size,
            libsql_path,
            libsql_url,
            libsql_auth_token,
            oracle_host,
            oracle_port,
            oracle_service,
            oracle_user,
            oracle_password,
            oracle_mode,
            oracle_dsn,
            oracle_onnx_model,
            oracle_agent_id,
        })
    }

    /// Get the database URL (exposes the secret).
    pub fn url(&self) -> &str {
        self.url.expose_secret()
    }

    /// Get the Oracle password (exposes the secret).
    pub fn oracle_password(&self) -> &str {
        self.oracle_password.expose_secret()
    }
}

/// Default libSQL database path (~/.ironclaw/ironclaw.db).
pub fn default_libsql_path() -> PathBuf {
    ironclaw_base_dir().join("ironclaw.db")
}
