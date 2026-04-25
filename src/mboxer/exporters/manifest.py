"""Manifest generation for NotebookLM and JSONL exports."""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

MANIFEST_FIELDS = [
    "account_key",
    "account_display_name",
    "account_email_address",
    "source_file",
    "source_path",
    "category_path",
    "source_pack",
    "message_count",
    "thread_count",
    "date_min",
    "date_max",
    "word_count",
    "byte_count",
    "export_profile",
    "security_profile",
    "contains_scrubbed_content",
    "created_at",
]


def build_notebooklm_manifest_rows(
    file_stats: list[dict[str, Any]],
    *,
    account_key: str,
    account_display_name: str | None,
    account_email_address: str | None,
    export_profile: str | None,
    security_profile: str | None,
    created_at: str,
) -> list[dict[str, Any]]:
    rows = []
    for stat in file_stats:
        fpath: Path = stat["path"]
        rows.append({
            "account_key": account_key,
            "account_display_name": account_display_name or "",
            "account_email_address": account_email_address or "",
            "source_file": fpath.name,
            "source_path": str(fpath),
            "category_path": stat.get("category_path", ""),
            "source_pack": fpath.name,
            "message_count": stat.get("message_count", 0),
            "thread_count": stat.get("thread_count", 0),
            "date_min": stat.get("date_min") or "",
            "date_max": stat.get("date_max") or "",
            "word_count": stat.get("word_count", 0),
            "byte_count": stat.get("byte_count", 0),
            "export_profile": export_profile or "",
            "security_profile": security_profile or "",
            "contains_scrubbed_content": bool(stat.get("contains_scrubbed_content", False)),
            "created_at": created_at,
        })
    return rows


def build_jsonl_manifest_rows(
    *,
    account_key: str,
    account_display_name: str | None,
    account_email_address: str | None,
    out_path: Path,
    message_count: int,
    thread_count: int,
    date_min: str | None,
    date_max: str | None,
    word_count: int,
    byte_count: int,
    export_profile: str | None,
    security_profile: str | None,
    contains_scrubbed_content: bool = False,
    created_at: str = "",
) -> list[dict[str, Any]]:
    return [{
        "account_key": account_key,
        "account_display_name": account_display_name or "",
        "account_email_address": account_email_address or "",
        "source_file": out_path.name,
        "source_path": str(out_path),
        "category_path": "",
        "source_pack": out_path.name,
        "message_count": message_count,
        "thread_count": thread_count,
        "date_min": date_min or "",
        "date_max": date_max or "",
        "word_count": word_count,
        "byte_count": byte_count,
        "export_profile": export_profile or "",
        "security_profile": security_profile or "",
        "contains_scrubbed_content": contains_scrubbed_content,
        "created_at": created_at,
    }]


def write_notebooklm_manifest(
    out_dir: Path,
    account_key: str,
    rows: list[dict[str, Any]],
) -> tuple[Path, Path]:
    """Write manifest.csv and manifest.json under out_dir/<account_key>/."""
    acct_dir = out_dir / account_key
    acct_dir.mkdir(parents=True, exist_ok=True)
    csv_path = acct_dir / "manifest.csv"
    json_path = acct_dir / "manifest.json"

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    json_path.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")

    return csv_path, json_path


def write_jsonl_manifest(
    out_path: Path,
    rows: list[dict[str, Any]],
) -> Path:
    """Write <stem>.manifest.json alongside the JSONL output file."""
    manifest_path = out_path.with_suffix("").with_suffix(".manifest.json")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
    return manifest_path
