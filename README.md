<p align="center">
  <img src="ironoraclaw.png?v=2" alt="IronOraClaw" width="200"/>
</p>

<h1 align="center">IronOraClaw</h1>

<p align="center">
  <strong>Oracle AI Database-powered secure personal AI assistant.</strong> Iron-clad security. Oracle-grade persistence.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Rust-1.92+-DEA584?style=for-the-badge&logo=rust&logoColor=white" alt="Rust 1.92+" />
  <img src="https://img.shields.io/badge/Oracle_AI_Database-26ai_Free-F80000?style=for-the-badge&logo=oracle&logoColor=white" alt="Oracle AI Database 26ai Free" />
  <img src="https://img.shields.io/badge/backend-Ollama-black?style=for-the-badge" alt="Ollama" />
  <a href="https://docs.oracle.com/en-us/iaas/Content/generative-ai/home.htm"><img src="https://img.shields.io/badge/OCI-GenAI-F80000.svg?style=for-the-badge&logo=oracle&logoColor=white" alt="OCI GenAI" /></a>
  <a href="LICENSE-APACHE"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache%202.0-blue.svg?style=for-the-badge" alt="License: MIT OR Apache-2.0" /></a>
</p>

<p align="center">
  <a href="https://cloud.oracle.com/resourcemanager/stacks/create?zipUrl=https://github.com/jasperan/ironoraclaw/raw/main/deploy/oci/orm/ironoraclaw-orm.zip">
    <img src="https://oci-resourcemanager-plugin.plugins.oci.oraclecloud.com/latest/deploy-to-oracle-cloud.svg" alt="Deploy to Oracle Cloud"/>
  </a>
</p>

---

IronOraClaw is a fork of [IronClaw](https://github.com/nearai/ironclaw) that replaces **ALL** storage backends with **Oracle AI Database** as the exclusive persistence layer. Every byte of memory, conversation, job state, and embedding lives in Oracle -- no PostgreSQL, no SQLite, no files.

## Why Oracle AI Database?

- **In-Database ONNX Embeddings**: Generate 384-dim vectors with `VECTOR_EMBEDDING()` -- zero API calls, zero latency
- **AI Vector Search**: Semantic recall via `VECTOR_DISTANCE()` with COSINE similarity
- **ACID Transactions**: No data loss on crash, ever
- **Multi-Agent Isolation**: Each agent gets its own namespace via `agent_id`
- **Enterprise-Grade**: Connection pooling, automatic indexing, audit trails

## Features

### From IronClaw

- **WASM Sandbox** -- Untrusted tools run in isolated WebAssembly containers with capability-based permissions
- **Multi-Channel** -- REPL, HTTP webhooks, Telegram, Slack, Signal, Discord, and web gateway
- **Routines** -- Cron schedules, event triggers, webhook handlers for background automation
- **Self-Repair** -- Automatic detection and recovery of stuck operations
- **Hybrid Search** -- Full-text + vector search using Reciprocal Rank Fusion
- **Dynamic Tool Generation** -- Describe what you need, and IronOraClaw builds it as a WASM tool
- **Parallel Jobs** -- Handle multiple requests concurrently with isolated contexts
- **Docker Sandbox** -- Isolated container execution with per-job tokens and orchestrator/worker pattern

### Oracle AI Database Additions

- Oracle AI Database as **exclusive storage** (no PostgreSQL, no SQLite, no files)
- In-database ONNX embeddings (`ALL_MINILM_L12_V2`)
- **15 IRON_* tables** with vector indexes
- `setup-oracle` CLI for one-command database setup
- **Default: [Oracle AI Database 26ai Free](https://www.oracle.com/database/free/) container** for local development
- **Optional: [Oracle Autonomous Database](https://www.oracle.com/autonomous-database/)** for managed cloud deployment via the Deploy to Oracle Cloud button

## Quick Start

<!-- one-command-install -->
> **One-command install** — clone, configure, and run in a single step:
>
> ```bash
> curl -fsSL https://raw.githubusercontent.com/jasperan/ironoraclaw/main/install.sh | bash
> ```
>
> <details><summary>Advanced options</summary>
>
> Override install location:
> ```bash
> PROJECT_DIR=/opt/myapp curl -fsSL https://raw.githubusercontent.com/jasperan/ironoraclaw/main/install.sh | bash
> ```
>
> Or install manually:
> ```bash
> git clone https://github.com/jasperan/ironoraclaw.git
> cd ironoraclaw
> # See below for setup instructions
> ```
> </details>


### Prerequisites

- Rust 1.92+
- [Oracle AI Database 26ai Free](https://www.oracle.com/database/free/) (Docker) -- the default backend
- Oracle Instant Client (for building)

### 1. Build

```bash
git clone https://github.com/jasperan/ironoraclaw.git
cd ironoraclaw
cargo build --release
```

### 2. Start Oracle Database

```bash
./scripts/setup-oracle.sh
# Or manually:
docker compose up oracle-db -d
```

### 3. Initialize

```bash
./target/release/ironoraclaw setup-oracle
./target/release/ironoraclaw onboard
```

### 4. Chat

```bash
./target/release/ironoraclaw agent -m "Hello! Remember that I love Rust."
./target/release/ironoraclaw agent -m "What programming language do I like?"
```

### 5. Inspect

```bash
./target/release/ironoraclaw oracle-inspect
./target/release/ironoraclaw oracle-inspect memories --search "programming"
```

## Docker Compose

```bash
# Full stack: Oracle AI Database 26ai Free + IronOraClaw
docker compose up -d

# With custom API key
API_KEY=sk-... docker compose up -d

# Oracle AI Database 26ai Free only (for local development)
docker compose up oracle-db -d
```

The Oracle AI Database 26ai Free container takes approximately 2 minutes to initialize on first start. The `ironoraclaw` service will wait for it to become healthy before starting.

## OCI Generative AI (Optional)

IronOraClaw can optionally use **OCI Generative AI** as an LLM backend via the `oci-openai` Python library. This is **not required** -- Ollama remains the default and recommended LLM backend.

### Why OCI GenAI?

- **Enterprise models** -- Access xAI Grok, Meta Llama, Cohere, and other models through OCI
- **OCI-native auth** -- Uses your existing `~/.oci/config` profile (no separate API keys)
- **Same region as your database** -- Run inference and storage in the same OCI region

### Setup

1. **Install the OCI GenAI proxy:**
   ```bash
   cd oci-genai
   pip install -r requirements.txt
   ```

2. **Configure OCI credentials** (`~/.oci/config`):
   ```ini
   [DEFAULT]
   user=ocid1.user.oc1..aaaaaaaaexample
   fingerprint=aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99
   tenancy=ocid1.tenancy.oc1..aaaaaaaaexample
   region=us-chicago-1
   key_file=~/.oci/oci_api_key.pem
   ```

3. **Set environment variables:**
   ```bash
   export OCI_PROFILE=DEFAULT
   export OCI_REGION=us-chicago-1
   export OCI_COMPARTMENT_ID=ocid1.compartment.oc1..your-compartment-ocid
   ```

4. **Start the OCI GenAI proxy:**
   ```bash
   cd oci-genai
   python proxy.py
   # Proxy runs at http://localhost:9999/v1
   ```

5. **Configure IronOraClaw** (`.env` or environment):
   ```bash
   LLM_BACKEND=openai_compatible
   LLM_BASE_URL=http://localhost:9999/v1
   LLM_API_KEY=oci-genai
   LLM_MODEL=meta.llama-3.3-70b-instruct
   ```

See [`oci-genai/README.md`](oci-genai/README.md) for full documentation.

## Oracle Schema

| Table | Purpose | Key Feature |
|---|---|---|
| IRON_META | Schema version | Single row per agent |
| IRON_CONVERSATIONS | Chat conversations | UUID PK, metadata CLOB |
| IRON_MESSAGES | Conversation messages | FK cascade delete |
| IRON_JOBS | Agent job tracking | Budget + cost tracking |
| IRON_ACTIONS | Tool executions | Sequence ordering |
| IRON_LLM_CALLS | LLM provider log | Token + cost tracking |
| IRON_MEMORIES | Memory documents | Path-based workspace |
| IRON_CHUNKS | Memory chunks | VECTOR(384) + COSINE index |
| IRON_SANDBOX_JOBS | Sandbox execution | WASM isolation tracking |
| IRON_JOB_EVENTS | Event audit log | IDENTITY sequence PK |
| IRON_ROUTINES | Scheduled jobs | Cron/event/webhook triggers |
| IRON_ROUTINE_RUNS | Execution history | Status + token tracking |
| IRON_TOOL_FAILURES | Self-repair tracking | Failure count threshold |
| IRON_SETTINGS | User settings | K-V store |
| IRON_ESTIMATES | Cost estimation | Predicted vs actual |

## Configuration

```toml
# ~/.ironoraclaw/config.toml

[oracle]
mode = "freepdb"           # "freepdb" for 26ai Free container (default) | "adb" for Autonomous DB (cloud)
host = "localhost"
port = 1521
service = "FREEPDB1"
user = "ironoraclaw"
password = "IronOraClaw2026"
onnx_model = "ALL_MINILM_L12_V2"
agent_id = "default"
```

### Environment Variables

Oracle connection settings can also be configured via environment variables:

| Variable | Description | Default |
|---|---|---|
| `IRONORACLAW_ORACLE_HOST` | Database hostname | `localhost` |
| `IRONORACLAW_ORACLE_PORT` | Listener port | `1521` |
| `IRONORACLAW_ORACLE_SERVICE` | Service name | `FREEPDB1` |
| `IRONORACLAW_ORACLE_USER` | Database user | `ironoraclaw` |
| `IRONORACLAW_ORACLE_PASSWORD` | Database password | -- |
| `API_KEY` | LLM provider API key | -- |
| `PROVIDER` | LLM provider name | `ollama` |

## Architecture

```
ironoraclaw
  src/
    db/
      oracle/              # Oracle AI Database integration
        connection.rs      # Connection pooling and lifecycle
        schema.rs          # 15 IRON_* table DDL + ONNX model loading
        conversations.rs   # Conversation + message persistence
        jobs.rs            # Job tracking with budget/cost
        workspace.rs       # Memory workspace with vector search
        routines.rs        # Routine scheduling persistence
        sandbox.rs         # WASM sandbox job tracking
        settings.rs        # User settings K-V store
        tool_failures.rs   # Self-repair failure tracking
        mod.rs             # Module exports
    agent/                 # Agent runtime loop
    cli/                   # CLI commands (setup-oracle, oracle-inspect)
    sandbox/               # WASM sandbox runtime
    channels/              # Multi-channel support (Telegram, Slack, etc.)
    worker/                # Parallel job execution
    orchestrator/          # Docker sandbox orchestration
    safety/                # Prompt injection defense
    workspace/             # Memory with hybrid search
    ...
```

## Deploy to Oracle Cloud (One-Click)

[![Deploy to Oracle Cloud](https://oci-resourcemanager-plugin.plugins.oci.oraclecloud.com/latest/deploy-to-oracle-cloud.svg)](https://cloud.oracle.com/resourcemanager/stacks/create?zipUrl=https://github.com/jasperan/ironoraclaw/raw/main/deploy/oci/orm/ironoraclaw-orm.zip)

This deploys a fully configured IronOraClaw instance on OCI with:

- **Oracle Linux 9** compute instance (ARM A1.Flex -- Always Free eligible)
- **Ollama** with gemma3:270m model pre-installed
- **Oracle AI Database 26ai Free** container by default (or optional Autonomous AI Database when toggled)
- **IronOraClaw** built from source with Oracle schema initialized
- **Gateway** running as a systemd service

After deployment, check the Terraform outputs for your instance IP and run:

```bash
# Watch setup progress (~10 min for Rust build + Oracle init)
ssh opc@<instance-ip> -t 'tail -f /var/log/ironoraclaw-setup.log'

# Start chatting
ssh opc@<instance-ip> -t ironoraclaw agent

# Check gateway health
curl http://<instance-ip>:42617/health
```

## Sister Projects

- [PicoOraClaw](https://github.com/jasperan/picooraclaw) -- Go-based, same Oracle pattern
- [ZeroOraClaw](https://github.com/jasperan/zerooraclaw) -- Rust-based, zero-overhead agent
- [OracLaw](https://github.com/jasperan/oraclaw) -- TypeScript + Python sidecar
- [TinyOraClaw](https://github.com/jasperan/tinyoraclaw) -- TypeScript multi-agent

## Credits

- [IronClaw](https://github.com/nearai/ironclaw) -- the secure Rust AI assistant this project is forked from
- [Oracle AI Database](https://www.oracle.com/database/) -- the exclusive storage backbone

## License

MIT OR Apache-2.0

---

<div align="center">

[![GitHub](https://img.shields.io/badge/GitHub-jasperan-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/jasperan)&nbsp;
[![LinkedIn](https://img.shields.io/badge/LinkedIn-jasperan-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/jasperan/)&nbsp;
[![Oracle](https://img.shields.io/badge/Oracle_AI_Database-26ai_Free-F80000?style=for-the-badge&logo=oracle&logoColor=white)](https://www.oracle.com/database/free/)

</div>
