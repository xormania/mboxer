from __future__ import annotations

import hashlib
import json
import mailbox
import sqlite3
from pathlib import Path
from typing import Any

from .accounts import AccountError, create_account, get_account
from .attachments import extract_attachments
from .config import deep_get, ensure_parent_dir
from .db import init_db
from .naming import slugify
from .normalize import normalize_message, parse_gmail_labels


def _file_sha256(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while data := f.read(chunk):
            h.update(data)
    return h.hexdigest()


def _get_or_create_source(
    conn: sqlite3.Connection,
    file_path: Path,
    source_name: str,
    account_id: int,
) -> int:
    source_slug = slugify(source_name)
    row = conn.execute(
        "SELECT id FROM mbox_sources WHERE account_id = ? AND file_path = ?",
        (account_id, str(file_path)),
    ).fetchone()
    if row:
        return row[0]

    stat = file_path.stat()
    conn.execute(
        """
        INSERT INTO mbox_sources (account_id, source_name, source_slug, file_path, file_size, source_mtime)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (account_id, source_name, source_slug, str(file_path), stat.st_size, stat.st_mtime),
    )
    conn.commit()
    return conn.execute(
        "SELECT id FROM mbox_sources WHERE account_id = ? AND file_path = ?",
        (account_id, str(file_path)),
    ).fetchone()[0]


def _create_run(conn: sqlite3.Connection, source_id: int, account_id: int) -> int:
    conn.execute(
        "INSERT INTO ingest_runs (account_id, source_id, status) VALUES (?, ?, 'running')",
        (account_id, source_id),
    )
    conn.commit()
    return conn.execute(
        "SELECT id FROM ingest_runs WHERE source_id = ? ORDER BY id DESC LIMIT 1",
        (source_id,),
    ).fetchone()[0]


def _get_resume_run(conn: sqlite3.Connection, source_id: int) -> tuple[int, str | None] | None:
    row = conn.execute(
        """
        SELECT id, last_mbox_key FROM ingest_runs
        WHERE source_id = ? AND status IN ('running', 'interrupted')
        ORDER BY id DESC LIMIT 1
        """,
        (source_id,),
    ).fetchone()
    return (row[0], row[1]) if row else None


def _update_run(conn: sqlite3.Connection, run_id: int, **kwargs: Any) -> None:
    sets = ", ".join(f"{k} = :{k}" for k in kwargs)
    conn.execute(f"UPDATE ingest_runs SET {sets} WHERE id = :_id", {"_id": run_id, **kwargs})


def _upsert_thread(
    conn: sqlite3.Connection,
    account_id: int,
    source_id: int,
    thread_key: str,
    subject: str | None,
    date_utc: str | None,
    participants: list[str],
) -> None:
    existing = conn.execute(
        "SELECT id, message_count, first_date_utc, last_date_utc FROM threads "
        "WHERE account_id = ? AND thread_key = ? AND source_id = ?",
        (account_id, thread_key, source_id),
    ).fetchone()

    if existing:
        tid, mc, first_date, last_date = existing
        new_first = min(filter(None, [first_date, date_utc])) if (first_date or date_utc) else None
        new_last = max(filter(None, [last_date, date_utc])) if (last_date or date_utc) else None
        conn.execute(
            "UPDATE threads SET message_count = ?, first_date_utc = ?, last_date_utc = ?, "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (mc + 1, new_first, new_last, tid),
        )
    else:
        conn.execute(
            """
            INSERT INTO threads
              (account_id, source_id, thread_key, subject, message_count, first_date_utc, last_date_utc, participants_json)
            VALUES (?, ?, ?, ?, 1, ?, ?, ?)
            """,
            (account_id, source_id, thread_key, subject, date_utc, date_utc, json.dumps(participants[:20])),
        )


def _delete_message_dependents(conn: sqlite3.Connection, msg_db_id: int) -> None:
    """Delete all dependent rows for a message and the message itself.

    Thread-level classifications (target_type='thread') are left intact;
    re-running classify will refresh them.
    """
    thread_info = conn.execute(
        "SELECT thread_key, account_id, source_id FROM messages WHERE id = ?",
        (msg_db_id,),
    ).fetchone()

    conn.execute("DELETE FROM export_items WHERE message_db_id = ?", (msg_db_id,))
    conn.execute("DELETE FROM security_findings WHERE message_db_id = ?", (msg_db_id,))
    conn.execute(
        "DELETE FROM classifications WHERE message_db_id = ? AND target_type = 'message'",
        (msg_db_id,),
    )
    conn.execute("DELETE FROM message_labels WHERE message_db_id = ?", (msg_db_id,))
    conn.execute("DELETE FROM attachments WHERE message_db_id = ?", (msg_db_id,))
    conn.execute("DELETE FROM messages WHERE id = ?", (msg_db_id,))

    if thread_info:
        thread_key, account_id, source_id = thread_info
        if thread_key:
            conn.execute(
                "UPDATE threads SET message_count = MAX(0, message_count - 1) "
                "WHERE account_id = ? AND thread_key = ? AND source_id = ?",
                (account_id, thread_key, source_id),
            )


def _store_labels(
    conn: sqlite3.Connection,
    account_id: int,
    msg_db_id: int,
    labels: list[str],
) -> None:
    for label_name in labels:
        normalized = label_name.lower().replace(" ", "-")
        conn.execute(
            "INSERT OR IGNORE INTO labels (account_id, label_name, normalized_name) VALUES (?, ?, ?)",
            (account_id, label_name, normalized),
        )
        label_id = conn.execute(
            "SELECT id FROM labels WHERE account_id = ? AND label_name = ?",
            (account_id, label_name),
        ).fetchone()[0]
        conn.execute(
            "INSERT OR IGNORE INTO message_labels (account_id, message_db_id, label_id) VALUES (?, ?, ?)",
            (account_id, msg_db_id, label_id),
        )


def ingest_mbox(
    mbox_path: str | Path,
    *,
    config: dict[str, Any],
    db_path: Path,
    account_key: str,
    source_name: str | None = None,
    resume: bool = False,
    extract_attachments_flag: bool = False,
    force: bool = False,
    create_account_if_missing: bool = False,
) -> dict[str, Any]:
    mbox_path = Path(mbox_path).resolve()
    if not mbox_path.exists():
        raise FileNotFoundError(f"MBOX file not found: {mbox_path}")

    ensure_parent_dir(db_path)
    init_db(db_path)

    if source_name is None:
        source_name = mbox_path.stem

    batch_size = int(deep_get(config, "ingest.batch_commit_size", 500))
    attachments_dir = Path(deep_get(config, "paths.attachments_dir", "data/attachments"))
    store_body_html = bool(deep_get(config, "ingest.store_body_html", False))
    max_body_chars = int(deep_get(config, "ingest.max_body_chars", 50000))

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")

    try:
        account = get_account(conn, account_key)
        if not account:
            if create_account_if_missing:
                from .accounts import create_account as _create_account
                _create_account(conn, account_key)
                account = get_account(conn, account_key)
                print(f"Created account: {account_key}")
            else:
                raise AccountError(
                    f"Account '{account_key}' not found.\n"
                    f"Run: mboxer account add {account_key}\n"
                    "Or pass --create-account to create it automatically."
                )

        account_id: int = account["id"]  # type: ignore[index]
        source_id = _get_or_create_source(conn, mbox_path, source_name, account_id)

        if force:
            print(
                f"Warning: --force enabled; existing messages from this account/source may be replaced."
            )

        resume_run_id: int | None = None
        resume_key: str | None = None

        if resume:
            existing = _get_resume_run(conn, source_id)
            if existing:
                resume_run_id, resume_key = existing
                print(f"Resuming run {resume_run_id} from key {resume_key!r}")

        if resume_run_id is not None:
            run_id = resume_run_id
            _update_run(conn, run_id, status="running")
            conn.commit()
        else:
            if not resume:
                conn.execute(
                    "UPDATE ingest_runs SET status = 'interrupted' "
                    "WHERE source_id = ? AND status = 'running'",
                    (source_id,),
                )
                conn.commit()
            run_id = _create_run(conn, source_id, account_id)

        counts = {"seen": 0, "inserted": 0, "skipped": 0, "replaced": 0, "errors": 0}
        last_key_processed: str | None = resume_key
        past_resume_key = resume_key is None

        mbox = mailbox.mbox(str(mbox_path), factory=None, create=False)
        try:
            keys = mbox.keys()
        except Exception as exc:
            _update_run(conn, run_id, status="failed")
            conn.commit()
            raise RuntimeError(f"Failed to open MBOX: {exc}") from exc

        try:
            for mbox_key in keys:
                str_key = str(mbox_key)

                if not past_resume_key:
                    if str_key == resume_key:
                        past_resume_key = True
                    counts["skipped"] += 1
                    continue

                counts["seen"] += 1

                try:
                    raw_msg = mbox.get_message(mbox_key)
                except Exception as exc:
                    counts["errors"] += 1
                    conn.execute(
                        "INSERT INTO ingest_errors (account_id, ingest_run_id, source_id, mbox_key, error_type, error_message) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (account_id, run_id, source_id, str_key, type(exc).__name__, str(exc)),
                    )
                    continue

                try:
                    record = normalize_message(raw_msg, source_id, str_key, account_id)
                except Exception as exc:
                    counts["errors"] += 1
                    conn.execute(
                        "INSERT INTO ingest_errors (account_id, ingest_run_id, source_id, mbox_key, error_type, error_message) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (account_id, run_id, source_id, str_key, type(exc).__name__, str(exc)),
                    )
                    continue

                if not store_body_html:
                    record["body_html"] = None
                if record.get("body_text") and len(record["body_text"]) > max_body_chars:
                    record["body_text"] = record["body_text"][:max_body_chars]

                gmail_labels = record.pop("gmail_labels", [])

                try:
                    if force:
                        existing = conn.execute(
                            "SELECT id FROM messages WHERE account_id = ? AND source_id = ? AND mbox_key = ?",
                            (record["account_id"], record["source_id"], record["mbox_key"]),
                        ).fetchone()
                        if existing:
                            _delete_message_dependents(conn, existing[0])
                            counts["replaced"] += 1

                    cursor = conn.execute(
                        """
                        INSERT OR IGNORE INTO messages
                          (account_id, source_id, mbox_key, message_id, thread_key, subject, sender,
                           recipients_json, cc_json, bcc_json, date_header, date_utc,
                           body_text, body_html, body_hash, body_chars, body_word_count,
                           attachment_count, raw_headers_json)
                        VALUES
                          (:account_id, :source_id, :mbox_key, :message_id, :thread_key, :subject, :sender,
                           :recipients_json, :cc_json, :bcc_json, :date_header, :date_utc,
                           :body_text, :body_html, :body_hash, :body_chars, :body_word_count,
                           :attachment_count, :raw_headers_json)
                        """,
                        record,
                    )
                    if cursor.rowcount > 0:
                        counts["inserted"] += 1
                        msg_db_id = cursor.lastrowid

                        if record.get("thread_key"):
                            participants = json.loads(record.get("recipients_json") or "[]")
                            if record.get("sender"):
                                participants = [record["sender"]] + participants
                            _upsert_thread(
                                conn, account_id, source_id,
                                record["thread_key"], record.get("subject"),
                                record.get("date_utc"), participants,
                            )

                        if gmail_labels:
                            _store_labels(conn, account_id, msg_db_id, gmail_labels)

                        if extract_attachments_flag and record.get("attachment_count", 0) > 0:
                            try:
                                extract_attachments(
                                    raw_msg, msg_db_id, source_id,
                                    account_id=account_id,
                                    account_key=account_key,
                                    date_utc=record.get("date_utc"),
                                    message_id=record.get("message_id") or str_key,
                                    attachments_dir=attachments_dir,
                                    conn=conn,
                                    extract_to_disk=True,
                                )
                            except Exception as exc:
                                conn.execute(
                                    "INSERT INTO ingest_errors (account_id, ingest_run_id, source_id, mbox_key, error_type, error_message) "
                                    "VALUES (?, ?, ?, ?, ?, ?)",
                                    (account_id, run_id, source_id, str_key, "AttachmentError", str(exc)),
                                )
                    else:
                        counts["skipped"] += 1

                except Exception as exc:
                    counts["errors"] += 1
                    conn.execute(
                        "INSERT INTO ingest_errors (account_id, ingest_run_id, source_id, mbox_key, error_type, error_message) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (account_id, run_id, source_id, str_key, type(exc).__name__, str(exc)[:500]),
                    )
                    continue

                last_key_processed = str_key
                total_done = counts["inserted"] + counts["skipped"] + counts["errors"]
                if total_done % batch_size == 0:
                    _update_run(
                        conn, run_id,
                        last_mbox_key=last_key_processed,
                        messages_seen=counts["seen"],
                        messages_inserted=counts["inserted"],
                        messages_skipped=counts["skipped"],
                        errors_count=counts["errors"],
                    )
                    conn.commit()

        except KeyboardInterrupt:
            _update_run(
                conn, run_id, status="interrupted",
                last_mbox_key=last_key_processed,
                messages_seen=counts["seen"],
                messages_inserted=counts["inserted"],
                messages_skipped=counts["skipped"],
                errors_count=counts["errors"],
            )
            conn.commit()
            print("\nInterrupted. Run with --resume to continue.")
            return counts

        _update_run(
            conn, run_id, status="completed",
            last_mbox_key=last_key_processed,
            messages_seen=counts["seen"],
            messages_inserted=counts["inserted"],
            messages_skipped=counts["skipped"],
            errors_count=counts["errors"],
        )
        conn.execute(
            "UPDATE ingest_runs SET finished_at = CURRENT_TIMESTAMP WHERE id = ?", (run_id,)
        )
        conn.commit()

        replaced_note = f" ({counts['replaced']} replaced)" if force and counts["replaced"] else ""
        print(
            f"Ingest complete [{account_key}]: {counts['inserted']} inserted{replaced_note}, "
            f"{counts['skipped']} skipped, {counts['errors']} errors"
        )
        return counts

    finally:
        conn.close()
