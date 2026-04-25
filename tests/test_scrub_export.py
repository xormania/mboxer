"""Export-profile and scrubbing tests.

Covers:
  raw / scrubbed / reviewed / metadata-only / exclude
  for both NotebookLM and JSONL exporters.
"""
from __future__ import annotations

import json
import sqlite3
import mailbox
from pathlib import Path
from typing import Any

import pytest

from mboxer.accounts import create_account
from mboxer.config import load_config
from mboxer.db import init_db
from mboxer.exporters.jsonl import export_jsonl
from mboxer.exporters.notebooklm import export_notebooklm
from mboxer.ingest import ingest_mbox
from mboxer.limits import resolve_notebooklm_limits

# ── Shared test data ──────────────────────────────────────────────────────────

# Message bodies with deliberate PII patterns
PHONE_BODY = "Call us at 555-867-5309 if you have questions."
SSN_BODY = "Your SSN on file is 123-45-6789."
CARD_BODY = "Charge to card 4111 1111 1111 1111 is confirmed."
CLEAN_BODY = "Thank you for your recent purchase. No sensitive info here."
EMAIL_BODY = "Reply to support@example.org for help."

RAW_MESSAGES = [
    (
        "From: sender@example.com\n"
        "To: user@example.com\n"
        "Subject: Phone test\n"
        "Date: Mon, 01 Jan 2024 10:00:00 +0000\n"
        "Message-ID: <scrub-phone-001@example.com>\n"
        "\n" + PHONE_BODY
    ),
    (
        "From: sender@example.com\n"
        "To: user@example.com\n"
        "Subject: SSN test\n"
        "Date: Tue, 02 Jan 2024 10:00:00 +0000\n"
        "Message-ID: <scrub-ssn-001@example.com>\n"
        "\n" + SSN_BODY
    ),
    (
        "From: sender@example.com\n"
        "To: user@example.com\n"
        "Subject: Card test\n"
        "Date: Wed, 03 Jan 2024 10:00:00 +0000\n"
        "Message-ID: <scrub-card-001@example.com>\n"
        "\n" + CARD_BODY
    ),
    (
        "From: sender@example.com\n"
        "To: user@example.com\n"
        "Subject: Clean message\n"
        "Date: Thu, 04 Jan 2024 10:00:00 +0000\n"
        "Message-ID: <scrub-clean-001@example.com>\n"
        "\n" + CLEAN_BODY
    ),
    (
        "From: sender@example.com\n"
        "To: user@example.com\n"
        "Subject: Email in body test\n"
        "Date: Fri, 05 Jan 2024 10:00:00 +0000\n"
        "Message-ID: <scrub-email-001@example.com>\n"
        "\n" + EMAIL_BODY
    ),
]

BASE_CONFIG: dict[str, Any] = {
    "paths": {"attachments_dir": "/tmp/test-scrub-export"},
    "ingest": {"batch_commit_size": 10, "store_body_html": False, "max_body_chars": 50000},
}

SCRUB_CONFIG: dict[str, Any] = {
    **BASE_CONFIG,
    "security": {
        "default_export_profile": "scrubbed",
        "scrub_enabled": True,
        "redact_email_addresses": False,
        "redact_phone_numbers": True,
        "redact_ssn_like_numbers": True,
        "redact_credit_card_like_numbers": True,
    },
}

RAW_CONFIG: dict[str, Any] = {
    **BASE_CONFIG,
    "security": {"default_export_profile": "raw", "scrub_enabled": True},
}

META_CONFIG: dict[str, Any] = {
    **BASE_CONFIG,
    "security": {"default_export_profile": "metadata-only", "scrub_enabled": True},
}

EMAIL_REDACT_CONFIG: dict[str, Any] = {
    **BASE_CONFIG,
    "security": {
        "default_export_profile": "scrubbed",
        "scrub_enabled": True,
        "redact_email_addresses": True,
        "redact_phone_numbers": False,
        "redact_ssn_like_numbers": False,
        "redact_credit_card_like_numbers": False,
    },
}


def _make_mbox(path: Path, messages: list[str]) -> None:
    mbox = mailbox.mbox(str(path), create=True)
    for raw in messages:
        mbox.add(mailbox.mboxMessage(raw))
    mbox.flush()
    mbox.close()


@pytest.fixture()
def db_pii(tmp_path):
    """DB with a single account and 5 messages containing PII in their bodies."""
    db_path = tmp_path / "mboxer.sqlite"
    mbox_path = tmp_path / "pii.mbox"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    create_account(conn, "test-account", display_name="Test Account", email_address="user@example.com")
    conn.close()
    _make_mbox(mbox_path, RAW_MESSAGES)
    ingest_mbox(mbox_path, config=BASE_CONFIG, db_path=db_path, account_key="test-account")
    return db_path


def _get_account_id(db_path: Path, account_key: str = "test-account") -> int:
    conn = sqlite3.connect(db_path)
    aid = conn.execute(
        "SELECT id FROM accounts WHERE account_key = ?", (account_key,)
    ).fetchone()[0]
    conn.close()
    return aid


def _do_nlm_export(db_path, out_dir, config, account_key="test-account", export_profile=None):
    example_config = load_config("config/mboxer.example.yaml")
    limits = resolve_notebooklm_limits(example_config, "ultra_safe")
    account_id = _get_account_id(db_path, account_key)
    conn = sqlite3.connect(db_path)
    try:
        return export_notebooklm(
            conn, config, limits, out_dir,
            account_id=account_id,
            account_key=account_key,
            account_display_name="Test Account",
            export_profile=export_profile,
            dry_run=False,
            db_path=str(db_path),
        )
    finally:
        conn.close()


def _do_jsonl_export(db_path, out_path, config, account_key="test-account", export_profile=None):
    account_id = _get_account_id(db_path, account_key)
    conn = sqlite3.connect(db_path)
    try:
        return export_jsonl(
            conn, config, out_path,
            account_id=account_id,
            account_key=account_key,
            account_display_name="Test Account",
            export_profile=export_profile,
        )
    finally:
        conn.close()


def _read_md_bodies(out_dir: Path) -> str:
    return "\n".join(f.read_text() for f in out_dir.rglob("*.md"))


def _read_jsonl_bodies(out_path: Path) -> list[str | None]:
    return [json.loads(line).get("body_text") for line in out_path.read_text().splitlines()]


# ── NotebookLM: raw ───────────────────────────────────────────────────────────

def test_nlm_raw_preserves_body(tmp_path, db_pii):
    _do_nlm_export(db_pii, tmp_path / "out", RAW_CONFIG)
    bodies = _read_md_bodies(tmp_path / "out")
    assert "555-867-5309" in bodies
    assert "123-45-6789" in bodies
    assert "4111 1111 1111 1111" in bodies


def test_nlm_raw_no_redaction_markers(tmp_path, db_pii):
    _do_nlm_export(db_pii, tmp_path / "out", RAW_CONFIG)
    bodies = _read_md_bodies(tmp_path / "out")
    assert "[PHONE REDACTED]" not in bodies
    assert "[SSN REDACTED]" not in bodies
    assert "[CARD REDACTED]" not in bodies


# ── NotebookLM: scrubbed ──────────────────────────────────────────────────────

def test_nlm_scrubbed_redacts_phone(tmp_path, db_pii):
    _do_nlm_export(db_pii, tmp_path / "out", SCRUB_CONFIG)
    bodies = _read_md_bodies(tmp_path / "out")
    assert "555-867-5309" not in bodies
    assert "[PHONE REDACTED]" in bodies


def test_nlm_scrubbed_redacts_ssn(tmp_path, db_pii):
    _do_nlm_export(db_pii, tmp_path / "out", SCRUB_CONFIG)
    bodies = _read_md_bodies(tmp_path / "out")
    assert "123-45-6789" not in bodies
    assert "[SSN REDACTED]" in bodies


def test_nlm_scrubbed_redacts_credit_card(tmp_path, db_pii):
    _do_nlm_export(db_pii, tmp_path / "out", SCRUB_CONFIG)
    bodies = _read_md_bodies(tmp_path / "out")
    assert "4111 1111 1111 1111" not in bodies
    assert "[CARD REDACTED]" in bodies


def test_nlm_scrubbed_preserves_clean_content(tmp_path, db_pii):
    _do_nlm_export(db_pii, tmp_path / "out", SCRUB_CONFIG)
    bodies = _read_md_bodies(tmp_path / "out")
    assert CLEAN_BODY in bodies


# ── NotebookLM: reviewed behaves like scrubbed ────────────────────────────────

def test_nlm_reviewed_redacts_like_scrubbed(tmp_path, db_pii):
    reviewed_config = {**SCRUB_CONFIG, "security": {**SCRUB_CONFIG["security"], "default_export_profile": "reviewed"}}
    _do_nlm_export(db_pii, tmp_path / "out", reviewed_config)
    bodies = _read_md_bodies(tmp_path / "out")
    assert "555-867-5309" not in bodies
    assert "[PHONE REDACTED]" in bodies


# ── NotebookLM: metadata-only ─────────────────────────────────────────────────

def test_nlm_metadata_only_omits_body(tmp_path, db_pii):
    _do_nlm_export(db_pii, tmp_path / "out", META_CONFIG)
    bodies = _read_md_bodies(tmp_path / "out")
    # PII must not appear
    assert "555-867-5309" not in bodies
    assert "123-45-6789" not in bodies
    assert CLEAN_BODY not in bodies
    # Subjects must still appear (in the frontmatter)
    assert "Phone test" in bodies


def test_nlm_metadata_only_shows_no_body_marker(tmp_path, db_pii):
    _do_nlm_export(db_pii, tmp_path / "out", META_CONFIG)
    bodies = _read_md_bodies(tmp_path / "out")
    assert "*(no body)*" in bodies


# ── NotebookLM: exclude ───────────────────────────────────────────────────────

def test_nlm_exclude_omits_record(tmp_path, db_pii):
    """Messages classified with export_profile='exclude' must not appear in output."""
    account_id = _get_account_id(db_pii)
    conn = sqlite3.connect(db_pii)
    # Manually classify one message as 'exclude'
    msg_id = conn.execute(
        "SELECT id FROM messages WHERE subject = 'Phone test'"
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO classifications (account_id, message_db_id, target_type, category_path, "
        "export_profile, sensitivity, classifier_type, confidence) "
        "VALUES (?, ?, 'message', 'test', 'exclude', 'high', 'rule', 1.0)",
        (account_id, msg_id),
    )
    conn.commit()
    conn.close()

    _do_nlm_export(db_pii, tmp_path / "out", RAW_CONFIG)
    bodies = _read_md_bodies(tmp_path / "out")
    assert "Phone test" not in bodies
    # Other messages still present
    assert "Clean message" in bodies


# ── NotebookLM: email redaction config ───────────────────────────────────────

def test_nlm_email_not_redacted_by_default(tmp_path, db_pii):
    _do_nlm_export(db_pii, tmp_path / "out", SCRUB_CONFIG)
    bodies = _read_md_bodies(tmp_path / "out")
    assert "support@example.org" in bodies


def test_nlm_email_redacted_when_enabled(tmp_path, db_pii):
    _do_nlm_export(db_pii, tmp_path / "out", EMAIL_REDACT_CONFIG)
    bodies = _read_md_bodies(tmp_path / "out")
    assert "support@example.org" not in bodies
    assert "[EMAIL REDACTED]" in bodies


# ── NotebookLM: export_profile override flag ─────────────────────────────────

def test_nlm_override_profile_scrubbed(tmp_path, db_pii):
    """CLI --export-profile scrubbed overrides the config's default_export_profile.

    The override says *which* profile to apply; the config says *which patterns* to redact.
    Both are needed: config must have the redact flags set, profile must say scrubbed.
    """
    override_config = {
        **BASE_CONFIG,
        "security": {
            "default_export_profile": "raw",  # config default is raw
            "scrub_enabled": True,
            "redact_phone_numbers": True,
            "redact_ssn_like_numbers": True,
            "redact_credit_card_like_numbers": True,
        },
    }
    _do_nlm_export(db_pii, tmp_path / "out", override_config, export_profile="scrubbed")
    bodies = _read_md_bodies(tmp_path / "out")
    assert "555-867-5309" not in bodies
    assert "[PHONE REDACTED]" in bodies


def test_nlm_override_profile_metadata_only(tmp_path, db_pii):
    _do_nlm_export(db_pii, tmp_path / "out", RAW_CONFIG, export_profile="metadata-only")
    bodies = _read_md_bodies(tmp_path / "out")
    assert CLEAN_BODY not in bodies
    assert "*(no body)*" in bodies


# ── NotebookLM: manifest contains_scrubbed_content ───────────────────────────

def test_nlm_manifest_scrubbed_content_true(tmp_path, db_pii):
    stats = _do_nlm_export(db_pii, tmp_path / "out", SCRUB_CONFIG)
    data = json.loads(Path(stats["manifest_json"]).read_text())
    # At least one source file must report scrubbed content
    assert any(row["contains_scrubbed_content"] for row in data)


def test_nlm_manifest_scrubbed_content_false_for_raw(tmp_path, db_pii):
    stats = _do_nlm_export(db_pii, tmp_path / "out", RAW_CONFIG)
    data = json.loads(Path(stats["manifest_json"]).read_text())
    assert all(not row["contains_scrubbed_content"] for row in data)


def test_nlm_manifest_scrubbed_content_false_for_metadata_only(tmp_path, db_pii):
    stats = _do_nlm_export(db_pii, tmp_path / "out", META_CONFIG)
    data = json.loads(Path(stats["manifest_json"]).read_text())
    assert all(not row["contains_scrubbed_content"] for row in data)


def test_nlm_manifest_no_scrubbed_when_no_pii_matches(tmp_path, db_pii):
    """Scrubbed profile but no PII patterns fire → contains_scrubbed_content is False."""
    db_path = tmp_path / "nopii.sqlite"
    mbox_path = tmp_path / "nopii.mbox"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    create_account(conn, "test-account")
    conn.close()
    _make_mbox(mbox_path, [
        "From: a@example.com\nTo: b@example.com\nSubject: Clean\n"
        "Date: Mon, 01 Jan 2024 10:00:00 +0000\nMessage-ID: <clean@example.com>\n\n"
        "No PII here whatsoever."
    ])
    ingest_mbox(mbox_path, config=BASE_CONFIG, db_path=db_path, account_key="test-account")
    stats = _do_nlm_export(db_path, tmp_path / "out", SCRUB_CONFIG)
    data = json.loads(Path(stats["manifest_json"]).read_text())
    assert all(not row["contains_scrubbed_content"] for row in data)


# ── JSONL: raw ────────────────────────────────────────────────────────────────

def test_jsonl_raw_preserves_body(tmp_path, db_pii):
    out = tmp_path / "test-account" / "messages.jsonl"
    _do_jsonl_export(db_pii, out, RAW_CONFIG)
    bodies = _read_jsonl_bodies(out)
    texts = " ".join(b for b in bodies if b)
    assert "555-867-5309" in texts
    assert "123-45-6789" in texts


def test_jsonl_raw_no_redaction_markers(tmp_path, db_pii):
    out = tmp_path / "test-account" / "messages.jsonl"
    _do_jsonl_export(db_pii, out, RAW_CONFIG)
    bodies = _read_jsonl_bodies(out)
    texts = " ".join(b for b in bodies if b)
    assert "[PHONE REDACTED]" not in texts


# ── JSONL: scrubbed ───────────────────────────────────────────────────────────

def test_jsonl_scrubbed_redacts_phone(tmp_path, db_pii):
    out = tmp_path / "test-account" / "messages.jsonl"
    _do_jsonl_export(db_pii, out, SCRUB_CONFIG)
    bodies = _read_jsonl_bodies(out)
    texts = " ".join(b for b in bodies if b)
    assert "555-867-5309" not in texts
    assert "[PHONE REDACTED]" in texts


def test_jsonl_scrubbed_redacts_ssn(tmp_path, db_pii):
    out = tmp_path / "test-account" / "messages.jsonl"
    _do_jsonl_export(db_pii, out, SCRUB_CONFIG)
    bodies = _read_jsonl_bodies(out)
    texts = " ".join(b for b in bodies if b)
    assert "123-45-6789" not in texts
    assert "[SSN REDACTED]" in texts


def test_jsonl_scrubbed_redacts_credit_card(tmp_path, db_pii):
    out = tmp_path / "test-account" / "messages.jsonl"
    _do_jsonl_export(db_pii, out, SCRUB_CONFIG)
    bodies = _read_jsonl_bodies(out)
    texts = " ".join(b for b in bodies if b)
    assert "4111 1111 1111 1111" not in texts
    assert "[CARD REDACTED]" in texts


# ── JSONL: reviewed behaves like scrubbed ────────────────────────────────────

def test_jsonl_reviewed_redacts_like_scrubbed(tmp_path, db_pii):
    reviewed_config = {
        **SCRUB_CONFIG,
        "security": {**SCRUB_CONFIG["security"], "default_export_profile": "reviewed"},
    }
    out = tmp_path / "test-account" / "messages.jsonl"
    _do_jsonl_export(db_pii, out, reviewed_config)
    bodies = _read_jsonl_bodies(out)
    texts = " ".join(b for b in bodies if b)
    assert "555-867-5309" not in texts
    assert "[PHONE REDACTED]" in texts


# ── JSONL: metadata-only ─────────────────────────────────────────────────────

def test_jsonl_metadata_only_omits_body(tmp_path, db_pii):
    out = tmp_path / "test-account" / "messages.jsonl"
    _do_jsonl_export(db_pii, out, META_CONFIG)
    bodies = _read_jsonl_bodies(out)
    assert all(b is None for b in bodies)


def test_jsonl_metadata_only_preserves_subject(tmp_path, db_pii):
    out = tmp_path / "test-account" / "messages.jsonl"
    _do_jsonl_export(db_pii, out, META_CONFIG)
    records = [json.loads(line) for line in out.read_text().splitlines()]
    subjects = {r.get("subject") for r in records}
    assert "Phone test" in subjects
    assert "Clean message" in subjects


# ── JSONL: exclude ────────────────────────────────────────────────────────────

def test_jsonl_exclude_omits_record(tmp_path, db_pii):
    account_id = _get_account_id(db_pii)
    conn = sqlite3.connect(db_pii)
    msg_id = conn.execute(
        "SELECT id FROM messages WHERE subject = 'Phone test'"
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO classifications (account_id, message_db_id, target_type, category_path, "
        "export_profile, sensitivity, classifier_type, confidence) "
        "VALUES (?, ?, 'message', 'test', 'exclude', 'high', 'rule', 1.0)",
        (account_id, msg_id),
    )
    conn.commit()
    conn.close()

    out = tmp_path / "test-account" / "messages.jsonl"
    _do_jsonl_export(db_pii, out, RAW_CONFIG)
    records = [json.loads(line) for line in out.read_text().splitlines()]
    subjects = {r.get("subject") for r in records}
    assert "Phone test" not in subjects
    assert "Clean message" in subjects


# ── JSONL: manifest contains_scrubbed_content ─────────────────────────────────

def test_jsonl_manifest_scrubbed_content_true(tmp_path, db_pii):
    out = tmp_path / "test-account" / "messages.jsonl"
    result = _do_jsonl_export(db_pii, out, SCRUB_CONFIG)
    assert result["contains_scrubbed_content"] is True
    manifest = json.loads(Path(result["manifest_path"]).read_text())
    assert manifest[0]["contains_scrubbed_content"] is True


def test_jsonl_manifest_scrubbed_content_false_for_raw(tmp_path, db_pii):
    out = tmp_path / "test-account" / "messages.jsonl"
    result = _do_jsonl_export(db_pii, out, RAW_CONFIG)
    assert result["contains_scrubbed_content"] is False
    manifest = json.loads(Path(result["manifest_path"]).read_text())
    assert manifest[0]["contains_scrubbed_content"] is False


def test_jsonl_manifest_scrubbed_content_false_for_metadata_only(tmp_path, db_pii):
    out = tmp_path / "test-account" / "messages.jsonl"
    result = _do_jsonl_export(db_pii, out, META_CONFIG)
    assert result["contains_scrubbed_content"] is False


# ── JSONL: manifest export/security profile recorded ─────────────────────────

def test_jsonl_manifest_security_profile_recorded(tmp_path, db_pii):
    out = tmp_path / "test-account" / "messages.jsonl"
    result = _do_jsonl_export(db_pii, out, SCRUB_CONFIG)
    manifest = json.loads(Path(result["manifest_path"]).read_text())
    assert manifest[0]["security_profile"] == "scrubbed"


# ── Account scoping ───────────────────────────────────────────────────────────

def test_scrub_is_account_scoped(tmp_path):
    """Scrubbing applied to account A's messages must not affect account B's export."""
    db_path = tmp_path / "multi.sqlite"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    create_account(conn, "account-a")
    create_account(conn, "account-b")
    conn.close()

    mbox_a = tmp_path / "a.mbox"
    mbox_b = tmp_path / "b.mbox"
    _make_mbox(mbox_a, [RAW_MESSAGES[0]])  # phone PII
    _make_mbox(mbox_b, [RAW_MESSAGES[3]])  # clean

    ingest_mbox(mbox_a, config=BASE_CONFIG, db_path=db_path, account_key="account-a")
    ingest_mbox(mbox_b, config=BASE_CONFIG, db_path=db_path, account_key="account-b")

    example_config = load_config("config/mboxer.example.yaml")
    limits = resolve_notebooklm_limits(example_config, "ultra_safe")

    conn = sqlite3.connect(db_path)
    try:
        aid_a = conn.execute("SELECT id FROM accounts WHERE account_key='account-a'").fetchone()[0]
        aid_b = conn.execute("SELECT id FROM accounts WHERE account_key='account-b'").fetchone()[0]
        stats_a = export_notebooklm(
            conn, SCRUB_CONFIG, limits, tmp_path / "out",
            account_id=aid_a, account_key="account-a",
            dry_run=False, db_path=str(db_path),
        )
        stats_b = export_notebooklm(
            conn, RAW_CONFIG, limits, tmp_path / "out",
            account_id=aid_b, account_key="account-b",
            dry_run=False, db_path=str(db_path),
        )
    finally:
        conn.close()

    bodies_a = _read_md_bodies(tmp_path / "out" / "account-a")
    bodies_b = _read_md_bodies(tmp_path / "out" / "account-b")

    assert "555-867-5309" not in bodies_a  # scrubbed
    assert "[PHONE REDACTED]" in bodies_a
    assert CLEAN_BODY in bodies_b           # raw, preserved
    assert "[PHONE REDACTED]" not in bodies_b
