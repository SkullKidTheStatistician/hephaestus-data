# Hephaestus

A local-first data lake for your personal computer, written in Rust.

Hephaestus turns a plain `datalake/` directory into an automated pipeline that
ingests, cleans, and organizes your files without manual intervention. Drop
data in, and the forge does the rest.

## Why

Personal data piles up fast: exports, logs, CSVs, JSON dumps, downloads.
Hephaestus gives it a clear structure and a repeatable workflow, with no
cloud, no heavy infrastructure, and no Python environment to maintain.

## Directory layout

```text
crates/
├── hephaestus-merger/        # immutable source data, organized by date (YYYY/MM/DD)
├── hephaestus-reader/    # cleaned data awaiting validation
└── hephaestus-writer/    # analysis-ready data

datalake/
├── raw/        # immutable source data, organized by date (YYYY/MM/DD)
├── staging/    # cleaned data awaiting validation
├── curated/    # analysis-ready data
├── archive/    # old or unused data
├── logs/       # pipeline.log
├── config/     # datalake.conf
└── README.md
```

## How it works

Data flows one way through four stages, inspired by the medallion
architecture (bronze, silver, gold).

| Stage | Directory | What happens |
|-------|-----------|--------------|
| Ingest | `raw/` | New files are detected and copied in, partitioned by date. Raw files are never modified. |
| Transform | `staging/` | Data is cleaned, normalized, and converted to Parquet. |
| Validate | `curated/` | Schema, null, and duplicate checks gate promotion to the curated layer. |
| Archive | `archive/` | Stale data is moved out based on configurable retention rules. |

Every step writes to `logs/pipeline.log`. The whole pipeline runs with a
single command or on a schedule, and it is idempotent: re-running it is always
safe.

## Principles

- **Raw is immutable.** Source data is never edited, so any result can be
  rebuilt from scratch.
- **One direction.** Data only moves forward: raw, staging, curated, archive.
- **Validate before promoting.** Nothing reaches `curated/` unless it passes
  the checks.
- **Everything is logged.** Each run leaves a trace you can inspect.
- **Local first.** No network, no account, no external service required.

## Features

- Single static binary, fast and dependency-free
- Date-partitioned raw layer (`YYYY/MM/DD`)
- Parquet output for staging and curated layers
- Configurable validation and retention rules
- Structured logging
- Idempotent, repeatable runs

## Installation

```bash
git clone https://github.com/<your-user>/hephaestus
cd hephaestus
cargo install --path .
```

## Quick start

```bash
# Create the directory structure
hephaestus init ~/datalake

# Run the full pipeline
hephaestus run

# Run a single stage
hephaestus run --stage transform
```

To run it automatically, add a cron entry (or a systemd timer):

```cron
0 * * * * hephaestus run --config ~/datalake/config/datalake.conf
```

## Configuration

Settings live in `config/datalake.conf`:

```toml
[paths]
root = "~/datalake"

[ingest]
watch = ["~/Downloads", "~/Documents/exports"]
extensions = ["csv", "json", "jsonl", "parquet"]

[validate]
fail_on_nulls = false
deduplicate = true

[archive]
after_days = 180
```

## Roadmap

- [ ] `init` command and directory scaffolding
- [ ] Ingest with date partitioning
- [ ] CSV and JSON to Parquet transforms
- [ ] Validation rules
- [ ] Retention and archiving
- [ ] File watcher mode
- [ ] SQL queries over curated data (DataFusion)

## Contributing

Early development. Issues, ideas, and pull requests are welcome.

## License

Apache-2.0
