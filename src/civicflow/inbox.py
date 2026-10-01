"""带来源序号的消息接入。"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, ValidationError
from .identifiers import require_safe
from .jsonutil import canonical_json, digest_json
from .timeutil import Clock, canonical_instant, parse_instant


def variant_summary(existing_json: str, incoming: object) -> dict:
    """仅依据字段名生成异文摘要,不带出任何载荷取值。"""
    try:
        existing = json.loads(existing_json)
    except json.JSONDecodeError:
        existing = None
    if not isinstance(existing, dict) or not isinstance(incoming, dict):
        return {"changed": [], "added": [], "removed": []}
    existing_keys = set(existing)
    incoming_keys = set(incoming)
    changed = sorted(key for key in existing_keys & incoming_keys if canonical_json(existing[key]) != canonical_json(incoming[key]))
    return {
        "changed": changed,
        "added": sorted(incoming_keys - existing_keys),
        "removed": sorted(existing_keys - incoming_keys),
    }


def public_conflict(row: dict) -> dict:
    """冲突记录的对外形态:剔除异文原文,摘要解析为对象。"""
    record = {"kind": "conflict", **row}
    record.pop("incoming_payload_json", None)
    record["summary"] = json.loads(record.pop("summary_json") or "{}")
    return record


@dataclass(frozen=True)
class Inbox:
    database: Database
    clock: Clock
    audit: AuditLog

    def receive(self, *, source: str, source_key: str, sequence: int, payload: dict, occurred_at: str) -> dict:
        require_safe(source, "来源"); require_safe(source_key, "来源标识")
        if sequence < 0:
            raise ValidationError("来源序号不能为负数")
        occurred_at = canonical_instant(occurred_at); digest = digest_json(payload)
        received_at = self.clock.now()
        actor = f"inbox:{source}"
        entity_id = f"{source}/{source_key}/{sequence}"
        with self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM inbox_messages WHERE source=? AND source_key=? AND sequence=?", (source, source_key, sequence)).fetchone()
            if row is None:
                connection.execute("INSERT INTO inbox_messages(source,source_key,sequence,payload_digest,payload_json,occurred_at,received_at,status) VALUES(?,?,?,?,?,?,?,?)", (source, source_key, sequence, digest, canonical_json(payload), occurred_at, received_at, "accepted"))
                self.audit.append(connection, actor_id=actor, action="inbox.accept", entity_type="inbox_messages", entity_id=entity_id, version=1, detail={"source": source, "source_key": source_key, "sequence": sequence, "payload_digest": digest, "occurred_at": occurred_at, "received_at": received_at})
                return {"status": "accepted", "digest": digest}
            if row["payload_digest"] == digest:
                self.audit.append(connection, actor_id=actor, action="inbox.duplicate", entity_type="inbox_messages", entity_id=entity_id, version=1, detail={"source": source, "source_key": source_key, "sequence": sequence, "payload_digest": digest, "received_at": received_at})
                return {"status": "duplicate", "digest": digest}
            conflict_id = self._record_conflict(connection, row=row, actor=actor, source=source, source_key=source_key, sequence=sequence, digest=digest, payload=payload, received_at=received_at)
        # 冲突记录与审计已随事务提交,再向调用方抛出明确的冲突结果。
        raise ConflictError(f"相同来源序号出现不同内容，冲突编号 {conflict_id}")

    def timeline(self, source: str, source_key: str) -> list[dict]:
        """按来源回放完整链路:已收消息与异文冲突按序号和接收时间交错排列。"""
        with self.database.connect() as connection:
            messages = [dict(row, kind="message") for row in connection.execute("SELECT * FROM inbox_messages WHERE source=? AND source_key=?", (source, source_key))]
            conflicts = [public_conflict(dict(row)) for row in connection.execute("SELECT * FROM inbox_conflicts WHERE source=? AND source_key=?", (source, source_key))]
        events = messages + conflicts
        # 旧表迁移可能带入非规范化时间串,按解析后的时刻排序以保证回放顺序稳定。
        events.sort(key=lambda event: (event["sequence"], parse_instant(event["received_at"]), 0 if event["kind"] == "message" else 1, event.get("conflict_id", 0)))
        return events

    def chain(self, source: str, source_key: str, sequence: int) -> dict:
        """按来源、编号和序号回放单个环节:原始消息保持不变,冲突按编号排序。"""
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM inbox_messages WHERE source=? AND source_key=? AND sequence=?", (source, source_key, sequence)).fetchone()
            conflicts = connection.execute("SELECT * FROM inbox_conflicts WHERE source=? AND source_key=? AND sequence=? ORDER BY conflict_id", (source, source_key, sequence)).fetchall()
        return {
            "message": dict(row, kind="message") if row else None,
            "conflicts": [public_conflict(dict(conflict)) for conflict in conflicts],
        }

    def _record_conflict(self, connection: sqlite3.Connection, *, row: sqlite3.Row, actor: str, source: str, source_key: str, sequence: int, digest: str, payload: dict, received_at: str) -> int:
        existing = connection.execute("SELECT conflict_id FROM inbox_conflicts WHERE source=? AND source_key=? AND sequence=? AND incoming_digest=?", (source, source_key, sequence, digest)).fetchone()
        if existing:
            conflict_id = int(existing["conflict_id"]); replayed = True
        else:
            summary = variant_summary(row["payload_json"], payload)
            cursor = connection.execute("INSERT INTO inbox_conflicts(source,source_key,sequence,existing_digest,incoming_digest,summary_json,incoming_payload_json,received_at,status) VALUES(?,?,?,?,?,?,?,?,?)", (source, source_key, sequence, row["payload_digest"], digest, canonical_json(summary), canonical_json(payload), received_at, "pending"))
            conflict_id = int(cursor.lastrowid); replayed = False
        self.audit.append(connection, actor_id=actor, action="inbox.conflict", entity_type="inbox_conflicts", entity_id=str(conflict_id), version=1, detail={"source": source, "source_key": source_key, "sequence": sequence, "existing_digest": row["payload_digest"], "incoming_digest": digest, "received_at": received_at, "replayed": replayed})
        return conflict_id
