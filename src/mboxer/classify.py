from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from .naming import normalize_category_path


def _load_rules(config: dict[str, Any]) -> list[dict[str, Any]]:
    return config.get("rules", [])


def _match_rule(rule: dict[str, Any], record: dict[str, Any]) -> bool:
    match = rule.get("match", {})
    sender = (record.get("sender") or "").lower()
    subject = (record.get("subject") or "").lower()
    try:
        recipients = json.loads(record.get("recipients_json") or "[]")
    except Exception:
        recipients = []
    all_addrs = [sender] + [r.lower() for r in recipients]

    for domain in match.get("from_domain", []):
        if any(addr.endswith(f"@{domain.lower()}") for addr in all_addrs):
            return True

    for fragment in match.get("from_contains", []):
        if any(fragment.lower() in addr for addr in all_addrs):
            return True

    for phrase in match.get("subject_contains", []):
        if phrase.lower() in subject:
            return True

    return False


def _apply_assignment(
    conn: sqlite3.Connection,
    record: dict[str, Any],
    rule: dict[str, Any],
    assign_key: str,
    account_id: int | None,
) -> None:
    assign = rule.get(assign_key, {})
    if not assign:
        return
    category_path = assign.get("category_path")
    if not category_path:
        return
    category_path = normalize_category_path(category_path)
    classifier_type = "rule" if assign_key == "assign" else "rule_hint"

    conn.execute(
        """
        INSERT OR IGNORE INTO classifications
          (account_id, target_type, message_db_id, thread_key, category_path,
           sensitivity, notebooklm_priority, export_profile,
           classifier_type, classifier_name, confidence)
        VALUES
          (:account_id, 'message', :msg_id, :thread_key, :category_path,
           :sensitivity, :notebooklm_priority, :export_profile,
           :classifier_type, :classifier_name, :confidence)
        """,
        {
            "account_id": account_id,
            "msg_id": record["id"],
            "thread_key": record.get("thread_key"),
            "category_path": category_path,
            "sensitivity": assign.get("sensitivity"),
            "notebooklm_priority": assign.get("notebooklm_priority"),
            "export_profile": assign.get("export_profile"),
            "classifier_type": classifier_type,
            "classifier_name": rule.get("name"),
            "confidence": 1.0 if assign_key == "assign" else 0.75,
        },
    )


def _select_excerpts(bodies: list[str], max_chars: int = 500) -> list[str]:
    """Pick up to 4 representative body excerpts: first, middle(s), last."""
    non_empty = [(i, b.strip()) for i, b in enumerate(bodies) if b and b.strip()]
    if not non_empty:
        return []
    if len(non_empty) == 1:
        return [non_empty[0][1][:max_chars]]

    selected_idxs: list[int] = [non_empty[0][0], non_empty[-1][0]]
    if len(non_empty) > 2:
        mid = non_empty[len(non_empty) // 2][0]
        if mid not in selected_idxs:
            selected_idxs.insert(1, mid)
    if len(non_empty) > 4:
        q3 = non_empty[(3 * len(non_empty)) // 4][0]
        if q3 not in selected_idxs:
            selected_idxs.insert(-1, q3)

    seen: set[int] = set()
    result: list[str] = []
    for idx, body in sorted(non_empty, key=lambda x: x[0]):
        if idx in selected_idxs and idx not in seen:
            seen.add(idx)
            result.append(body[:max_chars])
    return result


def _build_thread_input(
    thread_key: str,
    messages: list[dict[str, Any]],
) -> dict[str, Any]:
    """Aggregate thread messages into a single record suitable for rule matching."""
    if not messages:
        return {"thread_key": thread_key, "subject": "", "sender": "", "recipients_json": "[]"}

    subject = ""
    for m in messages:
        s = (m.get("subject") or "").strip()
        if s:
            # Strip all leading Re:/Fwd: prefixes
            while True:
                stripped = re.sub(r"^(?:re|fwd?)\s*:\s*", "", s, flags=re.IGNORECASE).strip()
                if stripped == s:
                    break
                s = stripped
            subject = s
            break

    # Collect all unique participant addresses across every message in the thread.
    # Putting all of them into recipients_json ensures _match_rule's all_addrs covers
    # every sender domain, not just the first message's sender.
    all_addrs: set[str] = set()
    for m in messages:
        if m.get("sender"):
            all_addrs.add(m["sender"].lower())
        try:
            for r in json.loads(m.get("recipients_json") or "[]"):
                all_addrs.add(r.lower())
        except Exception:
            pass

    sender = messages[0].get("sender") or ""
    other_addrs = sorted(all_addrs - {sender.lower()})

    bodies = [m.get("body_text") or "" for m in messages]
    excerpts = _select_excerpts(bodies)
    dates = sorted(d for m in messages if (d := m.get("date_utc")))

    return {
        "thread_key": thread_key,
        "subject": subject,
        "sender": sender,
        "recipients_json": json.dumps(other_addrs),
        "_participants": sorted(all_addrs),
        "_excerpts": excerpts,
        "_message_count": len(messages),
        "_date_min": dates[0] if dates else None,
        "_date_max": dates[-1] if dates else None,
    }


def _store_thread_classification(
    conn: sqlite3.Connection,
    thread_input: dict[str, Any],
    rule: dict[str, Any],
    assign_key: str,
    account_id: int | None,
) -> None:
    assign = rule.get(assign_key, {})
    if not assign:
        return
    category_path = assign.get("category_path")
    if not category_path:
        return
    category_path = normalize_category_path(category_path)
    classifier_type = "rule" if assign_key == "assign" else "rule_hint"
    confidence = 1.0 if assign_key == "assign" else 0.75

    raw = {
        "participants": thread_input.get("_participants", []),
        "date_min": thread_input.get("_date_min"),
        "date_max": thread_input.get("_date_max"),
        "message_count": thread_input.get("_message_count", 0),
        "excerpts": thread_input.get("_excerpts", []),
    }

    conn.execute(
        """
        INSERT INTO classifications
          (account_id, target_type, thread_key, category_path,
           sensitivity, notebooklm_priority, export_profile,
           classifier_type, classifier_name, confidence,
           prompt_version, summary, raw_output_json)
        VALUES
          (:account_id, 'thread', :thread_key, :category_path,
           :sensitivity, :notebooklm_priority, :export_profile,
           :classifier_type, :classifier_name, :confidence,
           'rules-v1', :summary, :raw_output_json)
        """,
        {
            "account_id": account_id,
            "thread_key": thread_input["thread_key"],
            "category_path": category_path,
            "sensitivity": assign.get("sensitivity"),
            "notebooklm_priority": assign.get("notebooklm_priority"),
            "export_profile": assign.get("export_profile"),
            "classifier_type": classifier_type,
            "classifier_name": rule.get("name"),
            "confidence": confidence,
            "summary": f"Thread classified by rule: {rule.get('name', '')}",
            "raw_output_json": json.dumps(raw),
        },
    )


def _inherit_to_messages(
    conn: sqlite3.Connection,
    thread_key: str,
    account_id: int | None,
    rule: dict[str, Any],
    assign_key: str,
    message_ids: list[int],
) -> int:
    assign = rule.get(assign_key, {})
    if not assign:
        return 0
    category_path = assign.get("category_path")
    if not category_path:
        return 0
    category_path = normalize_category_path(category_path)
    thread_confidence = 1.0 if assign_key == "assign" else 0.75

    inherited = 0
    for msg_id in message_ids:
        # Preserve explicit message-level classifications with equal or higher confidence.
        row = conn.execute(
            """
            SELECT MAX(confidence) FROM classifications
            WHERE message_db_id = ? AND account_id IS ?
              AND classifier_type IN ('rule', 'rule_hint')
            """,
            (msg_id, account_id),
        ).fetchone()
        existing = row[0] if row else None
        if existing is not None and existing >= thread_confidence:
            continue

        # Idempotency: skip if this message already has an inherited classification.
        if conn.execute(
            "SELECT id FROM classifications WHERE message_db_id = ? AND account_id IS ?"
            " AND classifier_type = 'rule_inherited'",
            (msg_id, account_id),
        ).fetchone():
            continue

        conn.execute(
            """
            INSERT INTO classifications
              (account_id, target_type, message_db_id, thread_key, category_path,
               sensitivity, notebooklm_priority, export_profile,
               classifier_type, classifier_name, confidence, prompt_version)
            VALUES
              (:account_id, 'message', :msg_id, :thread_key, :category_path,
               :sensitivity, :notebooklm_priority, :export_profile,
               'rule_inherited', :classifier_name, :confidence, 'rules-v1')
            """,
            {
                "account_id": account_id,
                "msg_id": msg_id,
                "thread_key": thread_key,
                "category_path": category_path,
                "sensitivity": assign.get("sensitivity"),
                "notebooklm_priority": assign.get("notebooklm_priority"),
                "export_profile": assign.get("export_profile"),
                "classifier_name": rule.get("name"),
                "confidence": thread_confidence,
            },
        )
        inherited += 1

    return inherited


def _run_rule_classification_thread(
    conn: sqlite3.Connection,
    config: dict[str, Any],
    *,
    account_id: int | None = None,
) -> dict[str, int]:
    rules = _load_rules(config)
    if not rules:
        print("No rules defined in config.")
        return {"classified": 0, "skipped": 0, "level": "thread"}

    # Find distinct thread_keys that don't yet have a thread-level rule classification.
    # Use IS for null-safe account_id comparison in the join.
    if account_id is not None:
        thread_rows = conn.execute(
            """
            SELECT DISTINCT m.thread_key
            FROM messages m
            LEFT JOIN classifications c
              ON c.thread_key = m.thread_key
              AND c.account_id IS m.account_id
              AND c.target_type = 'thread'
              AND c.classifier_type IN ('rule', 'rule_hint')
            WHERE m.account_id = ? AND m.thread_key IS NOT NULL AND c.id IS NULL
            """,
            (account_id,),
        ).fetchall()
    else:
        thread_rows = conn.execute(
            """
            SELECT DISTINCT m.thread_key
            FROM messages m
            LEFT JOIN classifications c
              ON c.thread_key = m.thread_key
              AND c.target_type = 'thread'
              AND c.classifier_type IN ('rule', 'rule_hint')
            WHERE m.thread_key IS NOT NULL AND c.id IS NULL
            """
        ).fetchall()

    thread_keys = [r[0] for r in thread_rows]
    classified = 0
    skipped = 0

    for thread_key in thread_keys:
        if account_id is not None:
            msg_rows = conn.execute(
                """
                SELECT id, subject, sender, recipients_json, date_utc, body_text,
                       thread_key, account_id
                FROM messages
                WHERE thread_key = ? AND account_id = ?
                ORDER BY date_utc NULLS LAST, id
                """,
                (thread_key, account_id),
            ).fetchall()
        else:
            msg_rows = conn.execute(
                """
                SELECT id, subject, sender, recipients_json, date_utc, body_text,
                       thread_key, account_id
                FROM messages
                WHERE thread_key = ?
                ORDER BY date_utc NULLS LAST, id
                """,
                (thread_key,),
            ).fetchall()

        cols = [
            "id", "subject", "sender", "recipients_json", "date_utc",
            "body_text", "thread_key", "account_id",
        ]
        messages = [dict(zip(cols, row)) for row in msg_rows]
        if not messages:
            continue

        thread_input = _build_thread_input(thread_key, messages)
        rec_account_id = messages[0].get("account_id") if account_id is None else account_id

        matched = False
        for rule in rules:
            if _match_rule(rule, thread_input):
                assign_key = "assign" if "assign" in rule else (
                    "assign_hint" if "assign_hint" in rule else None
                )
                if assign_key:
                    _store_thread_classification(
                        conn, thread_input, rule, assign_key, rec_account_id
                    )
                    msg_ids = [m["id"] for m in messages]
                    _inherit_to_messages(
                        conn, thread_key, rec_account_id, rule, assign_key, msg_ids
                    )
                    matched = True
                    break

        if matched:
            classified += 1
        else:
            skipped += 1

    conn.commit()
    return {"classified": classified, "skipped": skipped, "level": "thread"}


def run_rule_classification(
    conn: sqlite3.Connection,
    config: dict[str, Any],
    *,
    level: str = "message",
    account_id: int | None = None,
) -> dict[str, int]:
    if level == "thread":
        return _run_rule_classification_thread(conn, config, account_id=account_id)

    rules = _load_rules(config)
    if not rules:
        print("No rules defined in config.")
        return {"classified": 0, "skipped": 0}

    query = """
        SELECT m.id, m.account_id, m.source_id, m.mbox_key, m.message_id, m.thread_key,
               m.subject, m.sender, m.recipients_json, m.date_utc
        FROM messages m
        LEFT JOIN classifications c
          ON c.message_db_id = m.id AND c.classifier_type IN ('rule', 'rule_hint')
        WHERE c.id IS NULL
    """
    params: list[Any] = []
    if account_id is not None:
        query += " AND m.account_id = ?"
        params.append(account_id)

    rows = conn.execute(query, params).fetchall()
    cols = [
        "id", "account_id", "source_id", "mbox_key", "message_id", "thread_key",
        "subject", "sender", "recipients_json", "date_utc",
    ]
    records = [dict(zip(cols, row)) for row in rows]

    classified = 0
    skipped = 0

    for record in records:
        rec_account_id = record.get("account_id")
        matched = False
        for rule in rules:
            if _match_rule(rule, record):
                if "assign" in rule:
                    _apply_assignment(conn, record, rule, "assign", rec_account_id)
                    matched = True
                    break
                elif "assign_hint" in rule:
                    _apply_assignment(conn, record, rule, "assign_hint", rec_account_id)
                    matched = True
                    break
        if matched:
            classified += 1
        else:
            skipped += 1

    conn.commit()
    return {"classified": classified, "skipped": skipped}
