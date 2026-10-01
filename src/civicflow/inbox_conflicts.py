"""消息异文冲突的复核查看与人工裁决。"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .security import AccessContext
from .timeutil import Clock


READ_PERMISSION = "read:inbox_conflicts"
REVIEW_PERMISSION = "review:inbox_conflicts"
STATUSES = ("pending", "dismissed", "adopted")
DECISIONS = ("dismissed", "adopted")


@dataclass(frozen=True)
class InboxConflictService:
    """冲突记录默认最小化暴露,只有复核岗位能查看异文原文并作出裁决。"""

    database: Database
    clock: Clock
    audit: AuditLog

    def list(self, context: AccessContext, *, status: str | None = None, limit: int = 100) -> list[dict]:
        context.require(READ_PERMISSION)
        if status is not None and status not in STATUSES:
            raise ValidationError("未知状态")
        if limit < 1 or limit > 500:
            raise ValidationError("limit 必须在 1 到 500 之间")
        sql = "SELECT * FROM inbox_conflicts"
        params: list[object] = []
        if status is not None:
            sql += " WHERE status=?"; params.append(status)
        sql += " ORDER BY conflict_id LIMIT ?"; params.append(limit)
        with self.database.connect() as connection:
            return [self._present(dict(row), context) for row in connection.execute(sql, params)]

    def get(self, context: AccessContext, conflict_id: int) -> dict:
        context.require(READ_PERMISSION)
        with self.database.connect() as connection:
            return self._present(self._fetch(connection, conflict_id), context)

    def reveal_payload(self, context: AccessContext, conflict_id: int) -> dict:
        """仅复核岗位可查看异文原文与当前在册原文。"""
        context.require(REVIEW_PERMISSION)
        if not context.reveal_sensitive:
            raise PermissionDenied("查看异文原文需要复核岗位并开启敏感访问")
        with self.database.connect() as connection:
            row = self._fetch(connection, conflict_id)
            message = connection.execute("SELECT payload_json FROM inbox_messages WHERE source=? AND source_key=? AND sequence=?", (row["source"], row["source_key"], row["sequence"])).fetchone()
        return {
            "conflict_id": int(row["conflict_id"]),
            "incoming_payload": json.loads(row["incoming_payload_json"]) if row["incoming_payload_json"] else None,
            "existing_payload": json.loads(message["payload_json"]) if message else None,
        }

    def resolve(self, context: AccessContext, conflict_id: int, *, decision: str, reason: str) -> dict:
        """人工裁决:dismissed 维持在册原文,adopted 以异文替换在册内容。"""
        context.require(REVIEW_PERMISSION)
        if decision not in DECISIONS:
            raise ValidationError("未知裁决结果")
        reason = (reason or "").strip()
        if not reason:
            raise ValidationError("裁决必须说明原因")
        with self.database.transaction() as connection:
            row = self._fetch(connection, conflict_id)
            if row["status"] != "pending":
                raise ConflictError("冲突已裁决")
            if decision == "adopted":
                if not row["incoming_payload_json"]:
                    raise ValidationError("该冲突缺少异文原文，无法采用")
                connection.execute("UPDATE inbox_messages SET payload_digest=?, payload_json=? WHERE source=? AND source_key=? AND sequence=?", (row["incoming_digest"], row["incoming_payload_json"], row["source"], row["source_key"], row["sequence"]))
            now = self.clock.now()
            connection.execute("UPDATE inbox_conflicts SET status=?, resolved_at=?, resolved_by=?, resolution=? WHERE conflict_id=?", (decision, now, context.actor_id, reason, conflict_id))
            self.audit.append(connection, actor_id=context.actor_id, action="inbox.resolve", entity_type="inbox_conflicts", entity_id=str(conflict_id), version=2, detail={"source": row["source"], "source_key": row["source_key"], "sequence": row["sequence"], "decision": decision, "reason": reason, "existing_digest": row["existing_digest"], "incoming_digest": row["incoming_digest"]})
            return self._present(self._fetch(connection, conflict_id), context)

    @staticmethod
    def _fetch(connection: sqlite3.Connection, conflict_id: int) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM inbox_conflicts WHERE conflict_id=?", (conflict_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"inbox_conflicts/{conflict_id} 不存在")
        return row

    @staticmethod
    def _present(row: dict, context: AccessContext) -> dict:
        record = dict(row)
        record["summary"] = json.loads(record.pop("summary_json") or "{}")
        if not (context.reveal_sensitive and context.allows(REVIEW_PERMISSION)):
            record["incoming_payload_json"] = "***"
        return record
