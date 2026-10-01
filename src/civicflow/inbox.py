"""带来源序号的消息接入与异文冲突留痕。

冲突记录在与接收事务相互独立的事务中提交：接收方抛出的冲突异常
不再回滚留痕，值班人员事后可以按来源、编号和序号回放完整链路。
冲突表只保存双方指纹与字段级异文摘要，异文原文仅供复核岗位查看。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, InboxConflictError, NotFoundError, ValidationError
from .identifiers import require_safe
from .jsonutil import canonical_json, digest_json
from .security import AccessContext
from .timeutil import Clock, canonical_instant


ENTITY_TYPE = "inbox"
CONFLICT_STATES = ("pending", "resolved")
READ_PERMISSION = "read:inbox_conflicts"
REVIEW_PERMISSION = "review:inbox_conflicts"
CONFLICT_COLUMNS = "conflict_id,source,source_key,sequence,existing_digest,incoming_digest,diff_json,status,resolution,resolved_by,resolved_at,occurrences,received_at,last_received_at"

_MISSING = object()


def _diff_summary(existing: Any, incoming: Any, path: str = "") -> list[dict]:
    """字段级异文摘要：只记录路径与值指纹，不携带敏感原文。"""
    if isinstance(existing, dict) and isinstance(incoming, dict):
        changes: list[dict] = []
        for key in sorted(set(existing) | set(incoming)):
            child = f"{path}.{key}" if path else str(key)
            changes.extend(_diff_summary(existing.get(key, _MISSING), incoming.get(key, _MISSING), child))
        return changes
    if existing is _MISSING:
        return [{"path": path, "kind": "added", "incoming_digest": digest_json(incoming)}]
    if incoming is _MISSING:
        return [{"path": path, "kind": "removed", "existing_digest": digest_json(existing)}]
    if existing != incoming:
        return [{"path": path, "kind": "changed", "existing_digest": digest_json(existing), "incoming_digest": digest_json(incoming)}]
    return []


@dataclass(frozen=True)
class Inbox:
    database: Database
    clock: Clock
    audit: AuditLog

    def receive(self, *, source: str, source_key: str, sequence: int, payload: dict, occurred_at: str, actor: str | None = None) -> dict:
        require_safe(source, "来源"); require_safe(source_key, "来源标识")
        if sequence < 0:
            raise ValidationError("来源序号不能为负数")
        occurred_at = canonical_instant(occurred_at); digest = digest_json(payload)
        actor_id = actor or f"inbox:{source}"
        entity_id = f"{source}/{source_key}/{sequence}"
        with self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM inbox_messages WHERE source=? AND source_key=? AND sequence=?", (source, source_key, sequence)).fetchone()
            if row is None:
                connection.execute("INSERT INTO inbox_messages(source,source_key,sequence,payload_digest,payload_json,occurred_at,received_at,status) VALUES(?,?,?,?,?,?,?,?)", (source, source_key, sequence, digest, canonical_json(payload), occurred_at, self.clock.now(), "accepted"))
                self.audit.append(connection, actor_id=actor_id, action="inbox.accepted", entity_type=ENTITY_TYPE, entity_id=entity_id, version=0, detail={"source": source, "source_key": source_key, "sequence": sequence, "payload_digest": digest, "occurred_at": occurred_at})
                return {"status": "accepted", "digest": digest}
            if row["payload_digest"] == digest:
                self.audit.append(connection, actor_id=actor_id, action="inbox.duplicate", entity_type=ENTITY_TYPE, entity_id=entity_id, version=0, detail={"source": source, "source_key": source_key, "sequence": sequence, "payload_digest": digest})
                return {"status": "duplicate", "digest": digest}
            existing = dict(row)
        conflict = self._record_conflict(source=source, source_key=source_key, sequence=sequence, existing=existing, payload=payload, digest=digest, actor=actor_id, entity_id=entity_id)
        raise InboxConflictError(f"相同来源序号出现不同内容，已登记冲突 {conflict['conflict_id']} 等待复核", conflict=conflict)

    def _record_conflict(self, *, source: str, source_key: str, sequence: int, existing: dict, payload: dict, digest: str, actor: str, entity_id: str) -> dict:
        """在独立事务中持久化冲突，接收方随后的异常不会将其回滚。"""
        now = self.clock.now()
        changes = _diff_summary(json.loads(existing["payload_json"]), payload)
        with self.database.transaction() as connection:
            prior = connection.execute("SELECT conflict_id,occurrences FROM inbox_conflicts WHERE source=? AND source_key=? AND sequence=? AND incoming_digest=?", (source, source_key, sequence, digest)).fetchone()
            if prior:
                connection.execute("UPDATE inbox_conflicts SET occurrences=occurrences+1,last_received_at=? WHERE conflict_id=?", (now, prior["conflict_id"]))
                conflict_id = prior["conflict_id"]; occurrences = prior["occurrences"] + 1; action = "inbox.conflict_replayed"
            else:
                cursor = connection.execute("INSERT INTO inbox_conflicts(source,source_key,sequence,existing_digest,incoming_digest,diff_json,incoming_json,status,occurrences,received_at,last_received_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)", (source, source_key, sequence, existing["payload_digest"], digest, canonical_json({"changes": changes}), canonical_json(payload), "pending", 1, now, now))
                conflict_id = cursor.lastrowid; occurrences = 1; action = "inbox.conflict"
            self.audit.append(connection, actor_id=actor, action=action, entity_type=ENTITY_TYPE, entity_id=entity_id, version=0, detail={"conflict_id": conflict_id, "source": source, "source_key": source_key, "sequence": sequence, "existing_digest": existing["payload_digest"], "incoming_digest": digest, "changes": changes, "occurrences": occurrences})
        return {"conflict_id": conflict_id, "source": source, "source_key": source_key, "sequence": sequence, "existing_digest": existing["payload_digest"], "incoming_digest": digest, "changes": changes, "occurrences": occurrences, "received_at": now}

    def timeline(self, source: str, source_key: str) -> list[dict]:
        """按序号回放某来源编号的完整链路：已收消息与异文冲突事件。"""
        require_safe(source, "来源"); require_safe(source_key, "来源标识")
        with self.database.connect() as connection:
            messages = [dict(row) for row in connection.execute("SELECT * FROM inbox_messages WHERE source=? AND source_key=?", (source, source_key))]
            conflicts = [self._summary(dict(row)) for row in connection.execute(f"SELECT {CONFLICT_COLUMNS} FROM inbox_conflicts WHERE source=? AND source_key=?", (source, source_key))]
        events = [{"kind": "message", **row} for row in messages]
        events += [{"kind": "conflict", **row} for row in conflicts]
        events.sort(key=lambda event: (event["sequence"], event["received_at"], 0 if event["kind"] == "message" else 1, event.get("conflict_id", 0)))
        return events

    def list_conflicts(self, context: AccessContext, *, status: str | None = None, limit: int = 100) -> list[dict]:
        """冲突摘要列表，不包含异文原文。"""
        context.require(READ_PERMISSION)
        if limit < 1 or limit > 500:
            raise ValidationError("limit 必须在 1 到 500 之间")
        sql = f"SELECT {CONFLICT_COLUMNS} FROM inbox_conflicts"; params: list[object] = []
        if status is not None:
            if status not in CONFLICT_STATES:
                raise ValidationError("未知冲突状态")
            sql += " WHERE status=?"; params.append(status)
        sql += " ORDER BY conflict_id LIMIT ?"; params.append(limit)
        with self.database.connect() as connection:
            return [self._summary(dict(row)) for row in connection.execute(sql, params)]

    def reveal_conflict(self, context: AccessContext, conflict_id: int) -> dict:
        """复核岗位查看冲突双方原文。"""
        context.require(REVIEW_PERMISSION)
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM inbox_conflicts WHERE conflict_id=?", (conflict_id,)).fetchone()
            if not row:
                raise NotFoundError(f"冲突 {conflict_id} 不存在")
            record = self._summary(dict(row))
            record["incoming_payload"] = json.loads(row["incoming_json"]) if row["incoming_json"] else None
            message = connection.execute("SELECT payload_json FROM inbox_messages WHERE source=? AND source_key=? AND sequence=?", (row["source"], row["source_key"], row["sequence"])).fetchone()
            record["existing_payload"] = json.loads(message["payload_json"]) if message else None
            return record

    def resolve_conflict(self, context: AccessContext, conflict_id: int, *, resolution: str) -> dict:
        """人工裁决冲突；裁决结果进入统一审计。"""
        context.require(REVIEW_PERMISSION)
        if not resolution.strip():
            raise ValidationError("裁决说明不能为空")
        with self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM inbox_conflicts WHERE conflict_id=?", (conflict_id,)).fetchone()
            if not row:
                raise NotFoundError(f"冲突 {conflict_id} 不存在")
            if row["status"] != "pending":
                raise ConflictError(f"冲突 {conflict_id} 已裁决")
            now = self.clock.now()
            connection.execute("UPDATE inbox_conflicts SET status='resolved',resolution=?,resolved_by=?,resolved_at=? WHERE conflict_id=?", (resolution.strip(), context.actor_id, now, conflict_id))
            self.audit.append(connection, actor_id=context.actor_id, action="inbox.conflict_resolved", entity_type=ENTITY_TYPE, entity_id=f"{row['source']}/{row['source_key']}/{row['sequence']}", version=0, detail={"conflict_id": conflict_id, "resolution": resolution.strip(), "existing_digest": row["existing_digest"], "incoming_digest": row["incoming_digest"]})
        return {"conflict_id": conflict_id, "status": "resolved", "resolution": resolution.strip(), "resolved_by": context.actor_id, "resolved_at": now}

    @staticmethod
    def _summary(row: dict) -> dict:
        row.pop("incoming_json", None)
        row["diff"] = json.loads(row.pop("diff_json"))
        return row
