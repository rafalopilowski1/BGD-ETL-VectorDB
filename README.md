# BGD-ETL-VectorDB

ETL pipeline that ingests arXiv paper abstracts, cleans them, and generates
embedding vectors at multiple dimensions using Matryoshka Representation
Learning (MRL). Stores results in PostgreSQL with pgvector.

## Architecture

The pipeline follows a medallion (bronze/silver/gold) architecture:

```
.jsonl file arrives in data/incoming/
  -> Bronze: raw JSON records loaded into PostgreSQL as JSONB
  -> Silver: abstracts cleaned (LaTeX to text, whitespace normalized), fields parsed
  -> Gold:   embeddings generated at 384, 192, 96, 48 dimensions
  -> File moved to data/processed/
```

Two ingestion modes are available:
- **Direct watcher** -- watchdog monitors `data/incoming/` and runs the pipeline directly
- **Kafka** -- a producer watches `data/incoming/` and publishes file paths to a Kafka
  topic; a separate consumer reads the topic and runs the pipeline

### Project Structure

```
core/
  config.py         # All configuration (DB, model, Kafka, paths)
  db.py             # PostgreSQL connection helpers
  watcher.py        # Direct file watcher + pipeline orchestration
  producer.py       # Kafka producer (file watcher -> topic)
  consumer.py       # Kafka consumer (topic -> pipeline)
pipeline/
  bronze.py         # Raw JSONL -> bronze.arxiv_raw
  cleaning.py       # LaTeX-to-text, whitespace normalization
  silver.py         # bronze -> silver.arxiv_abstracts
  embedding.py      # SentenceTransformer wrapper, MRL truncation
  gold.py           # silver -> gold.arxiv_embeddings
scripts/
  split_jsonl.py    # Utility to split large JSONL files
sql/
  00_init.sql       # Schema and table definitions
data/
  incoming/         # Drop .jsonl files here to trigger pipeline
  processed/        # Files moved here after successful processing
```

### Database Schema

| Table | Schema | Purpose |
|---|---|---|
| `bronze.arxiv_raw` | `arxiv_id`, `raw_data` (JSONB), `source_file`, `ingested_at` | Raw landing |
| `silver.arxiv_abstracts` | `arxiv_id`, `title`, `abstract_clean`, `authors`, `categories`, `doi`, `journal_ref` | Cleaned records |
| `gold.arxiv_embeddings` | `arxiv_id`, `embedding_384`, `embedding_192`, `embedding_96`, `embedding_48`, `model_name` | Matryoshka embeddings |

### Embedding Model

Uses [mixedbread-ai/mxbai-embed-xsmall-v1](https://huggingface.co/mixedbread-ai/mxbai-embed-xsmall-v1)
via `sentence-transformers`. The model produces 384-dimensional embeddings. Smaller
dimensions (192, 96, 48) are obtained by truncating the full vector and L2-normalizing
the result -- this is how MRL is designed to work and avoids redundant model calls.

## Prerequisites

- Python 3.13
- [uv](https://docs.astral.sh/uv/) (package manager)
- Docker / Podman with Compose
- [just](https://github.com/casey/just) (command runner)

## Setup

```bash
# Install Python dependencies
uv sync

# Start all services (PostgreSQL + pgvector, Kafka, Redpanda Console)
just up

# Initialize database (create schemas, tables, indexes)
just db-init
```

## Usage

### Option A: Direct Watcher (no Kafka)

```bash
# Watch data/incoming/ for new .jsonl files (Ctrl+C to stop)
just watch

# Or process a specific file directly
just run data/incoming/some_file.jsonl
```

### Option B: Kafka Pipeline

Run the producer and consumer in separate terminals:

```bash
# Terminal 1: watch for files and publish to Kafka
just kafka-producer

# Terminal 2: consume from Kafka and run pipeline
just kafka-consumer
```

Then drop a `.jsonl` file into `data/incoming/`.

### Dev Sample

Extract a small 200-record sample from the full dataset for testing:

```bash
just dev-sample    # creates data/incoming/dev_sample.jsonl
```

## Data

The pipeline expects JSONL files where each line is a JSON object with at least
an `id` and `abstract` field. The arXiv abstracts dataset (`arxiv-abstracts.jsonl`,
~2M records) is the primary data source. It is not included in the repository.

To split the full dataset into smaller parts:

```bash
just split-data
```

## Justfile Commands

| Command | Description |
|---|---|
| `just up` | Start Docker services |
| `just db-init` | Initialize database schemas and tables |
| `just db-reset` | Drop all schemas and reinitialize |
| `just db-shell` | Open interactive psql session |
| `just db-counts` | Show row counts for all tables |
| `just db-peek` | Show first 2 rows from each table |
| `just watch` | Start direct file watcher |
| `just run <file>` | Process a single file |
| `just kafka-producer` | Start Kafka producer (file watcher) |
| `just kafka-consumer` | Start Kafka consumer (pipeline runner) |
| `just dev-sample` | Extract 200-record test sample |
| `just split-data` | Split full dataset into parts |

## Configuration

All config is in `core/config.py` with environment variable overrides:

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql://bgd:bgd@localhost:5432/bgd` | PostgreSQL connection string |
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | `localhost`, `5432`, `bgd`, `bgd`, `bgd` | Individual DB connection params (used if `DATABASE_URL` not set) |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `bgd-incoming-files` | Topic for file notifications |
| `KAFKA_GROUP_ID` | `bgd-pipeline` | Consumer group ID |

## Services

Started via `docker compose up -d`:

| Service | Port | Description |
|---|---|---|
| PostgreSQL + pgvector | 5432 | Vector database |
| Kafka (KRaft) | 9092 | Message broker |
| Redpanda Console | 8080 | Kafka UI at http://localhost:8080 |
