# mboxer - not usuable yet

Almost done, just need to hook in local AI 

Create **NotebookLM-ready Markdown source packs** from **Gmail MBOX exports**, with local SQLite, JSONL, and CSV outputs for search, RAG, archive review, and LLM workflows.

`mboxer` is a local-first email archive processor designed around a common problem:

```text
You can export Gmail as an MBOX file,
but a raw MBOX archive is not useful for NotebookLM, RAG, review, or analysis.
```

`mboxer` turns that raw archive into organized, structured, reusable knowledge assets.

```text
Gmail / Google Takeout
  → MBOX file
  → local SQLite index
  → organized Markdown source packs
  → NotebookLM, RAG, search, review, JSONL, CSV, and future tools
```

## Why this exists

Gmail archives often contain years of valuable personal, professional, legal, financial, operational, project, and organizational history.

Google Takeout makes it possible to export that history as an `.mbox` file, but the exported file is not immediately useful for modern AI workflows.

NotebookLM works best with readable, focused, well-organized source documents.

RAG systems work best with structured, chunkable records.

Spreadsheets work best with clean rows and metadata.

Local review works best when everything is inspectable before anything is uploaded.

`mboxer` bridges that gap.

## Primary use case: Gmail MBOX to NotebookLM

The main selling point of `mboxer` is converting Gmail MBOX exports into clean, category-organized Markdown files that can be used as NotebookLM sources.

Instead of uploading one giant raw archive, `mboxer` is designed to create structured source packs like:

```text
exports/notebooklm/
  finance/
    invoices/
      2024/
        finance-invoices-2024-001.md
  legal/
    contracts/
      2023-2024/
        legal-contracts-2023-2024-001.md
  projects/
    product-launch/
      2026/
        projects-product-launch-2026-001.md
  operations/
    vendor-correspondence/
      2025/
        operations-vendor-correspondence-2025-001.md
```

The goal is to make exported Gmail content easier to:

- upload into NotebookLM
- organize by topic or category
- review before upload
- split into useful source packs
- preserve context from email threads
- exclude sensitive or irrelevant material
- reuse later for RAG, search, or analysis

## What `mboxer` produces

`mboxer` is designed to turn Gmail MBOX archives into multiple useful formats.

### NotebookLM Markdown source packs

Markdown is the primary output format.

Markdown exports should be readable, organized, and useful as NotebookLM source material.

Each exported file can preserve useful email context such as:

- subject
- sender
- recipients
- date
- thread hints
- category
- source account
- cleaned body text
- attachment references when available

### SQLite database

SQLite is the durable local project index.

It allows `mboxer` to support features beyond one-time conversion, including:

- resumable ingest
- deduplication
- multi-account separation
- classification state
- category review
- export tracking
- attachment tracking
- security review
- future search tools
- future local web UI
- future incremental workflows

### JSONL exports

JSONL is intended for RAG pipelines, embeddings, local LLM tools, and structured downstream processing.

Each line can represent a message, thread, chunk, or export unit depending on the selected export mode.

### CSV exports

CSV is intended for spreadsheet review, filtering, auditing, lightweight analysis, and manual cleanup.

Useful CSV columns may include:

- message id
- account label
- source name
- subject
- sender
- recipients
- date
- category
- classification confidence
- attachment count
- security flags
- export status

## What you can use it for

`mboxer` is intended for workflows like:

- creating NotebookLM-ready source packs from Gmail exports
- preparing Gmail history for local LLM analysis
- organizing years of email by topic, project, sender, thread, or category
- creating searchable Markdown archives
- exporting structured JSONL for RAG systems
- exporting CSV for spreadsheet review
- building a durable local SQLite database over email history
- preserving data locally before deciding what is safe to upload elsewhere
- creating a foundation for future tools that work with email archives

## Project identity

```text
Project name:      mboxer
Python package:    mboxer
CLI command:       mboxer
Default database:  var/mboxer.sqlite
```

## Current project status

This repository is an early scaffold and design baseline.

It currently defines the intended architecture, workflow, configuration shape, schema direction, and export strategy. The next implementation passes should continue wiring the CLI, ingest pipeline, classification, security scanning, scrubbing, and exporters into the SQLite-backed project model.

Current design baseline includes:

- local-first SQLite storage
- support for one or more MBOX files
- multi-account archive separation
- config-driven export profiles
- NotebookLM-oriented Markdown source packs
- JSONL export planning for RAG workflows
- CSV export planning for spreadsheet workflows
- category-directory export conventions
- attachment tracking model
- ingest-run checkpoint model
- deterministic classification rules
- optional local LLM classification
- security scanning and scrubbing roadmap
- CLI shape and config loading utilities

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate

pip install -e .

mboxer --help
```

Copy and customize the example config:

```bash
cp config/mboxer.example.yaml config/mboxer.yaml
```

## First run

Complete walkthrough from a fresh checkout to a dry-run export.

**1. Initialize the database**

```bash
mboxer init-db --config config/mboxer.yaml
```

**2. Register your account**

```bash
mboxer account add primary-gmail \
  --display-name "Primary Gmail" \
  --email user@example.com \
  --config config/mboxer.yaml
```

**3. Verify the account was registered**

```bash
mboxer account list --config config/mboxer.yaml
```

**4. Ingest a small test archive first** (see warning below)

```bash
mboxer ingest data/mboxes/primary-gmail/sample.mbox \
  --config config/mboxer.yaml \
  --account primary-gmail \
  --source-name "Sample" \
  --extract-attachments \
  --resume
```

**5. Classify with rules**

```bash
mboxer classify \
  --config config/mboxer.yaml \
  --account primary-gmail
```

**6. Dry-run export to verify output shape**

```bash
mboxer export notebooklm \
  --config config/mboxer.yaml \
  --account primary-gmail \
  --profile ultra_safe \
  --dry-run
```

**7. Real export when ready**

```bash
mboxer export notebooklm \
  --config config/mboxer.yaml \
  --account primary-gmail \
  --profile ultra_safe \
  --out exports/notebooklm
```

> **Warning: test with a small MBOX before ingesting large archives.**
>
> Gmail MBOX exports can exceed several gigabytes for long-lived accounts.
> Before ingesting a full archive:
>
> 1. Extract a small slice of messages into a separate `.mbox` file and ingest that first.
> 2. Run `mboxer export notebooklm --dry-run` to verify the output shape.
> 3. Review the generated exports locally before uploading anything to a cloud service.
>
> `--resume` makes ingest restartable, but a full ingest of a large archive still takes
> significant time and disk space. Running `--dry-run` on exports is free and fast.

## Getting a Gmail MBOX file

You can export Gmail data from Google Takeout / Google Data Request.

The typical flow is:

1. Request an export of your Gmail data.
2. Download the archive from Google.
3. Extract the downloaded archive locally.
4. Locate the `.mbox` file.
5. Ingest the `.mbox` file with `mboxer`.
6. Export organized Markdown files for NotebookLM.

Example:

```bash
mboxer ingest data/mboxes/archive.mbox \
  --config config/mboxer.yaml \
  --source-name "Primary Gmail Archive" \
  --account-label "primary-account" \
  --extract-attachments \
  --resume
```

## Intended workflow

```bash
mboxer ingest data/mboxes/archive.mbox \
  --config config/mboxer.yaml \
  --source-name "Primary Gmail Archive" \
  --account-label "primary-account" \
  --extract-attachments \
  --resume

mboxer classify \
  --config config/mboxer.yaml \
  --model llama3.1:8b \
  --level thread

mboxer review-categories \
  --config config/mboxer.yaml

mboxer security-scan \
  --config config/mboxer.yaml

mboxer export notebooklm \
  --config config/mboxer.yaml \
  --profile ultra_safe \
  --out exports/notebooklm

mboxer export jsonl \
  --config config/mboxer.yaml \
  --out exports/rag/messages.jsonl

mboxer export csv \
  --config config/mboxer.yaml \
  --out exports/csv/messages.csv
```

## Multi-account support

`mboxer` should support multiple separate Gmail accounts and archives.

Each ingested source should be trackable by account, source name, import run, and original MBOX identity.

Example account labels:

```text
primary-account
work-account
business-archive
organization-archive
project-archive
```

The goal is to allow multiple archives to live in the same local project without losing separation between accounts or import sources.

## NotebookLM source-pack strategy

NotebookLM exports should be Markdown-first and organized by category directories.

Filenames should remain meaningful even if the folder hierarchy is flattened during upload.

A good exported source file should be understandable on its own:

```text
category-topic-year-sequence.md
```

Examples:

```text
finance-invoices-2024-001.md
legal-contracts-2023-2024-001.md
projects-product-launch-2026-001.md
operations-vendor-correspondence-2025-001.md
research-literature-review-2024-001.md
support-customer-requests-2025-001.md
```

## Export profiles

NotebookLM export limits live in `config/mboxer.example.yaml`.

Planned profiles:

- `standard`
- `plus`
- `pro`
- `ultra`
- `ultra_safe`

Use `ultra_safe` as the default for large NotebookLM-oriented workflows where you want to preserve headroom for manual sources, attachments, PDFs, and later additions.

## Security stance

`mboxer` assumes mail archives contain sensitive material.

Raw exports should be local-only.

Cloud-oriented exports should use reviewed, scrubbed, or metadata-only profiles.

Planned security pipeline:

```text
ingest
  → normalize
  → classify
  → security-scan
  → scrub
  → review
  → export
```

Security-sensitive workflows should prefer:

- local SQLite processing
- local Markdown review before upload
- explicit export profiles
- scrubbed cloud exports
- metadata-only exports when appropriate
- attachment review before inclusion
- local LLMs where possible

## Classification strategy

Classification should support deterministic rules first, with optional local LLM assistance later.

The system should be able to organize messages by:

- account
- source archive
- sender
- recipient
- subject
- date range
- thread
- category
- project
- topic
- sensitivity level
- security flags

LLM-based classification should be optional, configurable, and local-first by default.

## Design goals

`mboxer` should be:

- NotebookLM-friendly
- Gmail MBOX-focused
- local-first
- privacy-conscious
- resumable
- inspectable
- useful without a cloud service
- useful with local LLMs
- useful with future RAG systems
- safe for sensitive archives
- flexible enough for multiple Gmail accounts
- structured enough to support future application features

## Non-goals

`mboxer` is not intended to be:

- a Gmail client
- a replacement for Gmail search
- a hosted SaaS product
- a tool that uploads raw email archives by default
- a black-box AI classifier
- a cloud-first archive processor

## Development direction

Near-term implementation priorities:

1. Complete MBOX ingest into SQLite.
2. Preserve account/source separation.
3. Add resumable ingest checkpoints.
4. Normalize message metadata and body text.
5. Add NotebookLM Markdown export.
6. Add JSONL export.
7. Add CSV export.
8. Add deterministic classification rules.
9. Add category review workflows.
10. Add security scan and scrub hooks.
11. Add optional local LLM classification.
12. Add thread-level classification and export modes.

## License

MIT
