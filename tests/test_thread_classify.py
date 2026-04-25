"""Tests for thread-level rule classification."""
from __future__ import annotations

import json
import mailbox
import sqlite3
from pathlib import Path

import pytest

from mboxer.accounts import create_account
from mboxer.classify import _build_thread_input, _select_excerpts, run_rule_classification
from mboxer.db import init_db
from mboxer.ingest import ingest_mbox


# ── Synthetic messages ────────────────────────────────────────────────────────
# Three USPS messages that share a thread (MSG_2 and MSG_3 reply to MSG_1).

USPS_MSG_1 = """\
From: noreply@usps.com
To: user@example.com
Subject: Your Informed Delivery Daily Digest
Date: Mon, 1 Jan 2024 08:00:00 +0000
Message-ID: <usps-t1-001@usps.com>
X-Gmail-Labels: Inbox

Today's digest: one package en route.
"""

USPS_MSG_2 = """\
From: noreply@usps.com
To: user@example.com
Subject: Re: Your Informed Delivery Daily Digest
Date: Tue, 2 Jan 2024 08:00:00 +0000
Message-ID: <usps-t1-002@usps.com>
In-Reply-To: <usps-t1-001@usps.com>
X-Gmail-Labels: Inbox

Tuesday digest: package out for delivery.
"""

USPS_MSG_3 = """\
From: noreply@usps.com
To: user@example.com
Subject: Re: Your Informed Delivery Daily Digest
Date: Wed, 3 Jan 2024 08:00:00 +0000
Message-ID: <usps-t1-003@usps.com>
In-Reply-To: <usps-t1-001@usps.com>
X-Gmail-Labels: Inbox

Wednesday digest: package delivered.
"""

UNMATCHED_MSG = """\
From: friend@example.com
To: user@example.com
Subject: Let us catch up
Date: Thu, 4 Jan 2024 10:00:00 +0000
Message-ID: <friend-001@example.com>
X-Gmail-Labels: Inbox

How are you doing?
"""

CONFIG = {
    "paths": {"attachments_dir": "/tmp/attachments"},
    "ingest": {"batch_commit_size": 10, "store_body_html": False, "max_body_chars": 50000},
    "taxonomy": {"locked_categories": ["postal/usps-informed-delivery"]},
    "rules": [
        {
            "name": "usps-informed-delivery",
            "match": {
                "from_contains": ["usps"],
                "subject_contains": ["informed delivery"],
            },
            "assign": {
                "category_path": "postal/usps-informed-delivery",
                "sensitivity": "medium",
                "export_profile": "metadata-only",
            },
        }
    ],
}

NO_RULES_CONFIG = {
    "paths": {"attachments_dir": "/tmp/attachments"},
    "ingest": {"batch_commit_size": 10, "store_body_html": False, "max_body_chars": 50000},
    "rules": [],
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_mbox(path: Path, messages: list[str]) -> None:
    mbox = mailbox.mbox(str(path), create=True)
    for raw in messages:
        mbox.add(mailbox.mboxMessage(raw))
    mbox.flush()
    mbox.close()


def _setup(
    tmp_path: Path,
    messages: list[str] | None = None,
    account_key: str = "test-gmail",
) -> tuple[Path, int]:
    """Create a DB, ingest messages, and return (db_path, account_id)."""
    if messages is None:
        messages = [USPS_MSG_1, USPS_MSG_2, USPS_MSG_3, UNMATCHED_MSG]
    db_path = tmp_path / "test.sqlite"
    mbox_path = tmp_path / "test.mbox"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    create_account(conn, account_key)
    conn.close()
    _make_mbox(mbox_path, messages)
    ingest_mbox(mbox_path, config=CONFIG, db_path=db_path, account_key=account_key)
    conn = sqlite3.connect(db_path)
    account_id = conn.execute(
        "SELECT id FROM accounts WHERE account_key = ?", (account_key,)
    ).fetchone()[0]
    conn.close()
    return db_path, account_id


# ── Unit tests for helpers ─────────────────────────────────────────────────────

def test_select_excerpts_empty():
    assert _select_excerpts([]) == []
    assert _select_excerpts(["", "  "]) == []


def test_select_excerpts_single():
    result = _select_excerpts(["hello world"])
    assert result == ["hello world"]


def test_select_excerpts_two():
    result = _select_excerpts(["first", "last"])
    assert result == ["first", "last"]


def test_select_excerpts_three():
    result = _select_excerpts(["a", "b", "c"])
    assert len(result) == 3
    assert result[0] == "a"
    assert result[-1] == "c"


def test_select_excerpts_max_chars():
    long_body = "x" * 1000
    result = _select_excerpts([long_body])
    assert len(result[0]) == 500


def test_build_thread_input_aggregates_participants():
    messages = [
        {"subject": "Test", "sender": "a@example.com", "recipients_json": '["b@example.com"]',
         "body_text": "hello", "date_utc": "2024-01-01T00:00:00"},
        {"subject": "Re: Test", "sender": "b@example.com", "recipients_json": '["a@example.com"]',
         "body_text": "world", "date_utc": "2024-01-02T00:00:00"},
    ]
    result = _build_thread_input("tk1", messages)
    assert result["subject"] == "Test"
    assert result["sender"] == "a@example.com"
    all_participants = result["_participants"]
    assert "a@example.com" in all_participants
    assert "b@example.com" in all_participants


def test_build_thread_input_strips_re_prefix():
    messages = [
        {"subject": "Re: Re: Important notice", "sender": "s@x.com",
         "recipients_json": "[]", "body_text": "", "date_utc": None},
    ]
    result = _build_thread_input("tk2", messages)
    assert result["subject"] == "Important notice"


def test_build_thread_input_empty_messages():
    result = _build_thread_input("tk3", [])
    assert result["subject"] == ""
    assert result["sender"] == ""


# ── Thread classification integration tests ───────────────────────────────────

def test_thread_classification_creates_one_row_per_thread(tmp_path):
    """Thread-level classify stores exactly one target_type='thread' row per matching thread."""
    db_path, account_id = _setup(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        result = run_rule_classification(conn, CONFIG, level="thread", account_id=account_id)
        # The 3 USPS messages share a thread_key; the unmatched message is its own thread.
        # Only the USPS thread matches the rule.
        assert result["classified"] == 1
        assert result["level"] == "thread"

        thread_rows = conn.execute(
            "SELECT thread_key, category_path, classifier_type FROM classifications "
            "WHERE target_type = 'thread' AND account_id = ?",
            (account_id,),
        ).fetchall()
        assert len(thread_rows) == 1
        assert thread_rows[0][1] == "postal/usps-informed-delivery"
        assert thread_rows[0][2] == "rule"
    finally:
        conn.close()


def test_thread_messages_inherit_classification(tmp_path):
    """All messages in the matching thread receive rule_inherited classifications."""
    db_path, account_id = _setup(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        run_rule_classification(conn, CONFIG, level="thread", account_id=account_id)

        inherited = conn.execute(
            "SELECT message_db_id FROM classifications "
            "WHERE classifier_type = 'rule_inherited' AND account_id = ?",
            (account_id,),
        ).fetchall()
        # All 3 USPS messages should be inherited
        assert len(inherited) == 3

        # Each inherited row has the correct category
        cats = conn.execute(
            "SELECT DISTINCT category_path FROM classifications "
            "WHERE classifier_type = 'rule_inherited' AND account_id = ?",
            (account_id,),
        ).fetchall()
        assert len(cats) == 1
        assert cats[0][0] == "postal/usps-informed-delivery"
    finally:
        conn.close()


def test_unmatched_thread_not_classified(tmp_path):
    """Messages in unmatched threads receive no classification."""
    db_path, account_id = _setup(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        run_rule_classification(conn, CONFIG, level="thread", account_id=account_id)

        # The unmatched message's message_id is <friend-001@example.com>
        friend_id = conn.execute(
            "SELECT id FROM messages WHERE message_id = '<friend-001@example.com>'",
        ).fetchone()[0]
        count = conn.execute(
            "SELECT COUNT(*) FROM classifications WHERE message_db_id = ?",
            (friend_id,),
        ).fetchone()[0]
        assert count == 0
    finally:
        conn.close()


def test_thread_classification_stored_fields(tmp_path):
    """Thread classification row has summary and raw_output_json populated."""
    db_path, account_id = _setup(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        run_rule_classification(conn, CONFIG, level="thread", account_id=account_id)
        row = conn.execute(
            "SELECT summary, raw_output_json, prompt_version, sensitivity, export_profile "
            "FROM classifications WHERE target_type = 'thread' AND account_id = ?",
            (account_id,),
        ).fetchone()
        assert row is not None
        assert "usps-informed-delivery" in (row[0] or "")
        raw = json.loads(row[1])
        assert "message_count" in raw
        assert raw["message_count"] == 3
        assert row[2] == "rules-v1"
        assert row[3] == "medium"
        assert row[4] == "metadata-only"
    finally:
        conn.close()


def test_higher_confidence_message_classification_preserved(tmp_path):
    """A message with an explicit rule classification is not overwritten by inheritance."""
    db_path, account_id = _setup(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        # Determine the thread_key and message IDs
        usps_msgs = conn.execute(
            "SELECT id, thread_key FROM messages WHERE sender = 'noreply@usps.com' "
            "ORDER BY id",
        ).fetchall()
        assert len(usps_msgs) == 3
        msg1_id = usps_msgs[0][0]
        thread_key = usps_msgs[0][1]

        # Pre-insert an explicit rule classification for the first USPS message
        conn.execute(
            """
            INSERT INTO classifications
              (account_id, target_type, message_db_id, thread_key, category_path,
               classifier_type, classifier_name, confidence)
            VALUES (?, 'message', ?, ?, 'postal/usps-informed-delivery',
                    'rule', 'manual-pre', 1.0)
            """,
            (account_id, msg1_id, thread_key),
        )
        conn.commit()

        run_rule_classification(conn, CONFIG, level="thread", account_id=account_id)

        # msg1 should still have exactly 1 classification (the explicit one, not inherited)
        rows = conn.execute(
            "SELECT classifier_type FROM classifications "
            "WHERE message_db_id = ? AND account_id = ?",
            (msg1_id, account_id),
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "rule"  # not replaced by rule_inherited

        # The other two USPS messages should be inherited
        other_inherited = conn.execute(
            "SELECT COUNT(*) FROM classifications "
            "WHERE classifier_type = 'rule_inherited' AND account_id = ?",
            (account_id,),
        ).fetchone()[0]
        assert other_inherited == 2
    finally:
        conn.close()


def test_thread_classification_account_scoped(tmp_path):
    """Thread classification for account A does not affect account B."""
    db_path_a = tmp_path / "a.sqlite"
    db_path_b = tmp_path / "b.sqlite"
    mbox_path = tmp_path / "test.mbox"
    _make_mbox(mbox_path, [USPS_MSG_1, USPS_MSG_2, USPS_MSG_3])

    for db_path, key in [(db_path_a, "acc-a"), (db_path_b, "acc-b")]:
        init_db(db_path)
        conn = sqlite3.connect(db_path)
        create_account(conn, key)
        conn.close()
        ingest_mbox(mbox_path, config=CONFIG, db_path=db_path, account_key=key)

    conn_a = sqlite3.connect(db_path_a)
    conn_b = sqlite3.connect(db_path_b)
    try:
        acc_a_id = conn_a.execute("SELECT id FROM accounts WHERE account_key = 'acc-a'").fetchone()[0]
        run_rule_classification(conn_a, CONFIG, level="thread", account_id=acc_a_id)

        # Account A: classified
        a_thread_count = conn_a.execute(
            "SELECT COUNT(*) FROM classifications WHERE target_type = 'thread'"
        ).fetchone()[0]
        assert a_thread_count == 1

        # Account B: untouched
        b_any = conn_b.execute("SELECT COUNT(*) FROM classifications").fetchone()[0]
        assert b_any == 0
    finally:
        conn_a.close()
        conn_b.close()


def test_same_thread_key_different_accounts_no_collision(tmp_path):
    """Same thread_key shared between two accounts classifies each independently."""
    db_path = tmp_path / "shared.sqlite"
    # Use distinct paths so the file-path uniqueness constraint is satisfied;
    # both files have identical content, producing the same thread_key values.
    mbox_path_x = tmp_path / "test_x.mbox"
    mbox_path_y = tmp_path / "test_y.mbox"
    _make_mbox(mbox_path_x, [USPS_MSG_1, USPS_MSG_2, USPS_MSG_3])
    _make_mbox(mbox_path_y, [USPS_MSG_1, USPS_MSG_2, USPS_MSG_3])

    init_db(db_path)
    conn = sqlite3.connect(db_path)
    create_account(conn, "acc-x")
    create_account(conn, "acc-y")
    conn.close()

    ingest_mbox(mbox_path_x, config=CONFIG, db_path=db_path, account_key="acc-x")
    ingest_mbox(mbox_path_y, config=CONFIG, db_path=db_path, account_key="acc-y")

    conn = sqlite3.connect(db_path)
    try:
        acc_x_id = conn.execute("SELECT id FROM accounts WHERE account_key = 'acc-x'").fetchone()[0]
        acc_y_id = conn.execute("SELECT id FROM accounts WHERE account_key = 'acc-y'").fetchone()[0]

        run_rule_classification(conn, CONFIG, level="thread", account_id=acc_x_id)
        run_rule_classification(conn, CONFIG, level="thread", account_id=acc_y_id)

        x_count = conn.execute(
            "SELECT COUNT(*) FROM classifications WHERE target_type = 'thread' AND account_id = ?",
            (acc_x_id,),
        ).fetchone()[0]
        y_count = conn.execute(
            "SELECT COUNT(*) FROM classifications WHERE target_type = 'thread' AND account_id = ?",
            (acc_y_id,),
        ).fetchone()[0]
        assert x_count == 1
        assert y_count == 1
    finally:
        conn.close()


def test_message_level_classification_still_works(tmp_path):
    """level='message' still classifies individual messages as before."""
    db_path, account_id = _setup(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        result = run_rule_classification(conn, CONFIG, level="message", account_id=account_id)
        assert result["classified"] == 3  # all 3 USPS messages matched
        assert result["skipped"] == 1     # unmatched friend message

        # Only message-level rows; no thread rows
        thread_count = conn.execute(
            "SELECT COUNT(*) FROM classifications WHERE target_type = 'thread'"
        ).fetchone()[0]
        assert thread_count == 0

        msg_count = conn.execute(
            "SELECT COUNT(*) FROM classifications WHERE target_type = 'message'"
        ).fetchone()[0]
        assert msg_count == 3
    finally:
        conn.close()


def test_thread_classification_idempotent(tmp_path):
    """Running thread classification twice produces the same rows, no duplicates."""
    db_path, account_id = _setup(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        run_rule_classification(conn, CONFIG, level="thread", account_id=account_id)
        result2 = run_rule_classification(conn, CONFIG, level="thread", account_id=account_id)

        # Second run: all threads already classified, classified=0
        assert result2["classified"] == 0

        total = conn.execute("SELECT COUNT(*) FROM classifications").fetchone()[0]
        # Still only 1 thread row + 3 inherited message rows
        assert total == 4
    finally:
        conn.close()


def test_thread_classification_stable_category(tmp_path):
    """USPS thread always maps to postal/usps-informed-delivery category."""
    db_path, account_id = _setup(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        run_rule_classification(conn, CONFIG, level="thread", account_id=account_id)

        cats = conn.execute(
            "SELECT DISTINCT category_path FROM classifications WHERE account_id = ?",
            (account_id,),
        ).fetchall()
        paths = {r[0] for r in cats}
        assert "postal/usps-informed-delivery" in paths
        # No other categories should appear
        assert paths == {"postal/usps-informed-delivery"}
    finally:
        conn.close()


def test_thread_classification_no_rules_returns_early(tmp_path):
    """Thread classification with no rules returns zero classified without error."""
    db_path, account_id = _setup(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        result = run_rule_classification(conn, NO_RULES_CONFIG, level="thread", account_id=account_id)
        assert result["classified"] == 0
        assert result["skipped"] == 0
        assert result["level"] == "thread"
    finally:
        conn.close()


def test_export_uses_inherited_thread_classification(tmp_path):
    """NotebookLM export picks up messages classified via thread inheritance."""
    from pathlib import Path
    from mboxer.exporters.notebooklm import export_notebooklm
    from mboxer.limits import NotebookLMLimits

    db_path, account_id = _setup(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        run_rule_classification(conn, CONFIG, level="thread", account_id=account_id)

        # Build a minimal config for export (raw profile so metadata-only doesn't hide everything)
        export_config = {
            "security": {"default_export_profile": "raw", "scrub_enabled": False},
        }
        limits = NotebookLMLimits(
            profile_name="standard",
            max_sources=300, reserved_sources=50, target_sources=200,
            max_words_per_source=500000, target_words_per_source=200000,
            max_bytes_per_source=200_000_000, target_bytes_per_source=100_000_000,
            max_messages_per_source=2000,
        )
        out_dir = tmp_path / "exports"

        stats = export_notebooklm(
            conn,
            export_config,
            limits,
            out_dir,
            account_id=account_id,
            account_key="test-gmail",
            db_path=str(db_path),
            include_unclassified=False,
        )

        # Only the 3 USPS messages (via inherited classification) should appear
        assert stats["messages_exported"] == 3
        assert stats["files_written"] >= 1
    finally:
        conn.close()


def test_jsonl_export_uses_inherited_thread_classification(tmp_path):
    """JSONL export picks up messages classified via thread inheritance."""
    from mboxer.exporters.jsonl import export_jsonl

    db_path, account_id = _setup(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        run_rule_classification(conn, CONFIG, level="thread", account_id=account_id)

        export_config = {
            "security": {"default_export_profile": "raw", "scrub_enabled": False},
            "exports": {"jsonl": {"include_classification": True}},
        }
        out_path = tmp_path / "export.jsonl"
        result = export_jsonl(conn, export_config, out_path, account_id=account_id)

        # All 4 messages (3 USPS + 1 unmatched) appear in raw JSONL
        assert result["messages_written"] == 4

        # Verify the USPS messages have classification info populated
        lines = out_path.read_text().splitlines()
        classified_count = sum(
            1 for line in lines
            if json.loads(line).get("classification", {}).get("classifier_type") == "rule_inherited"
        )
        assert classified_count == 3
    finally:
        conn.close()
