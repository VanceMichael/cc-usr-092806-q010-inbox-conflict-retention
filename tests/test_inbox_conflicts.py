from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.errors import ConflictError, InboxConflictError, NotFoundError, PermissionDenied, ValidationError
from civicflow.security import AccessContext


class InboxConflictTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "test.sqlite3"
        self.app = CivicFlow.open(self.db_path, fixed_now="2026-10-01T08:00:00+08:00")
        self.reviewer = AccessContext(actor_id="duty-reviewer", permissions=frozenset({"read:inbox_conflicts", "review:inbox_conflicts"}))
        self.reader = AccessContext(actor_id="duty-reader", permissions=frozenset({"read:inbox_conflicts"}))

    def tearDown(self):
        self.temp.cleanup()

    def _receive(self, app=None, **overrides):
        params = {"source": "flood", "source_key": "th-2026-01", "sequence": 7, "payload": {"rainfall": 120, "road": "open"}, "occurred_at": "2026-10-01T07:00:00+08:00"}
        params.update(overrides)
        return (app or self.app).inbox.receive(**params)

    def _conflict_rows(self):
        with self.app.database.connect() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM inbox_conflicts ORDER BY conflict_id")]

    def _audit_actions(self):
        with self.app.database.connect() as connection:
            return [row["action"] for row in connection.execute("SELECT action FROM audit_entries ORDER BY audit_id")]

    def test_conflict_record_survives_rollback(self):
        self._receive()
        with self.assertRaises(InboxConflictError) as caught:
            self._receive(payload={"rainfall": 260, "road": "closed"})
        self.assertIsInstance(caught.exception, ConflictError)
        outcome = caught.exception.conflict
        rows = self._conflict_rows()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["conflict_id"], outcome["conflict_id"])
        self.assertEqual(row["existing_digest"], outcome["existing_digest"])
        self.assertEqual(row["incoming_digest"], outcome["incoming_digest"])
        self.assertEqual(row["received_at"], "2026-10-01T00:00:00Z")
        self.assertEqual(row["status"], "pending")
        diff = json.loads(row["diff_json"])["changes"]
        self.assertEqual({change["path"] for change in diff}, {"rainfall", "road"})
        self.assertNotIn("closed", row["diff_json"])
        # 原始消息保持不变
        with self.app.database.connect() as connection:
            message = connection.execute("SELECT * FROM inbox_messages WHERE source='flood' AND source_key='th-2026-01' AND sequence=7").fetchone()
        self.assertEqual(json.loads(message["payload_json"]), {"rainfall": 120, "road": "open"})
        self.assertEqual(message["payload_digest"], row["existing_digest"])

    def test_same_variant_replay_does_not_create_second_conflict(self):
        self._receive()
        variant = {"rainfall": 260, "road": "closed"}
        with self.assertRaises(InboxConflictError) as first:
            self._receive(payload=variant)
        with self.assertRaises(InboxConflictError) as second:
            self._receive(payload=variant)
        self.assertEqual(first.exception.conflict["conflict_id"], second.exception.conflict["conflict_id"])
        rows = self._conflict_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["occurrences"], 2)
        self.assertEqual(rows[0]["last_received_at"], "2026-10-01T00:00:00Z")

    def test_identical_replay_stays_duplicate_without_conflict(self):
        self.assertEqual(self._receive()["status"], "accepted")
        self.assertEqual(self._receive()["status"], "duplicate")
        with self.assertRaises(InboxConflictError):
            self._receive(payload={"rainfall": 260, "road": "closed"})
        self.assertEqual(self._receive()["status"], "duplicate")
        self.assertEqual(len(self._conflict_rows()), 1)

    def test_audit_trail_covers_accept_duplicate_conflict_and_resolution(self):
        self._receive()
        self._receive()
        with self.assertRaises(InboxConflictError) as caught:
            self._receive(payload={"rainfall": 260, "road": "closed"})
        self.app.inbox.resolve_conflict(self.reviewer, caught.exception.conflict["conflict_id"], resolution="以水利部门第二版为准")
        actions = self._audit_actions()
        self.assertEqual(actions, ["inbox.accepted", "inbox.duplicate", "inbox.conflict", "inbox.conflict_resolved"])
        self.assertGreaterEqual(self.app.verify()["audit_entries"], 4)

    def test_timeline_replays_full_chain_after_restart(self):
        self._receive(sequence=3, payload={"level": 1})
        self._receive(sequence=5, payload={"level": 2})
        with self.assertRaises(InboxConflictError):
            self._receive(sequence=5, payload={"level": 3})
        reopened = CivicFlow.open(self.db_path, fixed_now="2026-10-01T09:00:00+08:00")
        events = reopened.inbox.timeline("flood", "th-2026-01")
        self.assertEqual([(event["kind"], event["sequence"]) for event in events], [("message", 3), ("message", 5), ("conflict", 5)])
        conflict_event = events[2]
        self.assertEqual(conflict_event["status"], "pending")
        self.assertIn("diff", conflict_event)
        self.assertNotIn("incoming_json", conflict_event)
        self.assertNotIn("incoming_payload", conflict_event)

    def test_legacy_conflict_table_migrates(self):
        legacy_path = Path(self.temp.name) / "legacy.sqlite3"
        legacy = sqlite3.connect(legacy_path)
        legacy.execute("CREATE TABLE inbox_conflicts (conflict_id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, source_key TEXT NOT NULL, sequence INTEGER NOT NULL, existing_digest TEXT NOT NULL, incoming_digest TEXT NOT NULL, received_at TEXT NOT NULL)")
        legacy.execute("INSERT INTO inbox_conflicts(source,source_key,sequence,existing_digest,incoming_digest,received_at) VALUES('flood','th-2026-01',9,'aaa','bbb','2026-09-30T23:00:00Z')")
        legacy.commit(); legacy.close()
        migrated = CivicFlow.open(legacy_path, fixed_now="2026-10-01T10:00:00+08:00")
        with migrated.database.connect() as connection:
            columns = {row["name"] for row in connection.execute("PRAGMA table_info(inbox_conflicts)")}
        self.assertTrue({"diff_json", "incoming_json", "status", "resolution", "resolved_by", "resolved_at", "occurrences", "last_received_at"} <= columns)
        rows = migrated.inbox.list_conflicts(self.reviewer)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "pending")
        self.assertEqual(rows[0]["occurrences"], 1)
        self.assertEqual(rows[0]["last_received_at"], "2026-09-30T23:00:00Z")
        events = migrated.inbox.timeline("flood", "th-2026-01")
        self.assertEqual([(event["kind"], event["sequence"]) for event in events], [("conflict", 9)])
        revealed = migrated.inbox.reveal_conflict(self.reviewer, rows[0]["conflict_id"])
        self.assertIsNone(revealed["incoming_payload"])

    def test_conflict_payload_requires_reviewer_role(self):
        self._receive()
        with self.assertRaises(InboxConflictError) as caught:
            self._receive(payload={"rainfall": 260, "road": "closed", "hospital": "icu:full"})
        conflict_id = caught.exception.conflict["conflict_id"]
        listed = self.app.inbox.list_conflicts(self.reader)
        self.assertEqual(len(listed), 1)
        self.assertNotIn("incoming_payload", listed[0])
        self.assertNotIn("incoming_json", listed[0])
        with self.assertRaises(PermissionDenied):
            self.app.inbox.reveal_conflict(self.reader, conflict_id)
        revealed = self.app.inbox.reveal_conflict(self.reviewer, conflict_id)
        self.assertEqual(revealed["incoming_payload"], {"rainfall": 260, "road": "closed", "hospital": "icu:full"})
        self.assertEqual(revealed["existing_payload"], {"rainfall": 120, "road": "open"})
        with self.assertRaises(PermissionDenied):
            self.app.inbox.resolve_conflict(self.reader, conflict_id, resolution="驳回")
        with self.assertRaises(ValidationError):
            self.app.inbox.resolve_conflict(self.reviewer, conflict_id, resolution="  ")
        resolved = self.app.inbox.resolve_conflict(self.reviewer, conflict_id, resolution="以交通部门回执为准")
        self.assertEqual(resolved["status"], "resolved")
        self.assertEqual(resolved["resolved_by"], "duty-reviewer")
        with self.assertRaises(ConflictError):
            self.app.inbox.resolve_conflict(self.reviewer, conflict_id, resolution="重复裁决")
        with self.assertRaises(NotFoundError):
            self.app.inbox.reveal_conflict(self.reviewer, 9999)

    def test_list_conflicts_requires_read_permission(self):
        outsider = AccessContext(actor_id="outsider", permissions=frozenset())
        with self.assertRaises(PermissionDenied):
            self.app.inbox.list_conflicts(outsider)
        with self.assertRaises(ValidationError):
            self.app.inbox.list_conflicts(self.reader, status="unknown")

    def test_concurrent_variant_replay_creates_single_conflict(self):
        self._receive()
        variant = {"rainfall": 260, "road": "closed"}

        def replay(_):
            with self.assertRaises(InboxConflictError) as caught:
                self._receive(payload=variant)
            return caught.exception.conflict["conflict_id"]

        with ThreadPoolExecutor(max_workers=8) as pool:
            conflict_ids = list(pool.map(replay, range(8)))
        self.assertEqual(len(set(conflict_ids)), 1)
        rows = self._conflict_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["occurrences"], 8)

    def test_concurrent_identical_replay_single_accept(self):
        payload = {"rainfall": 120, "road": "open"}

        def send(_):
            return self._receive(payload=payload)["status"]

        with ThreadPoolExecutor(max_workers=8) as pool:
            statuses = list(pool.map(send, range(8)))
        self.assertEqual(statuses.count("accepted"), 1)
        self.assertEqual(statuses.count("duplicate"), 7)
        self.assertEqual(self._conflict_rows(), [])


if __name__ == "__main__":
    unittest.main()
