from __future__ import annotations

import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from civicflow.jsonutil import canonical_json, digest_json
from civicflow.security import AccessContext


NOW = "2026-09-28T12:00:00+08:00"
OLD_SCHEMA = """
CREATE TABLE inbox_messages (
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    payload_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY(source, source_key, sequence)
);
CREATE TABLE inbox_conflicts (
    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    existing_digest TEXT NOT NULL,
    incoming_digest TEXT NOT NULL,
    received_at TEXT NOT NULL
);
"""


class InboxConflictTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "test.sqlite3"
        self.app = CivicFlow.open(self.db_path, fixed_now=NOW)
        self.reader = AccessContext(actor_id="duty-reader", permissions=frozenset({"read:inbox_conflicts"}))
        self.reviewer = AccessContext(actor_id="duty-reviewer", permissions=frozenset({"read:inbox_conflicts", "review:inbox_conflicts"}), reveal_sensitive=True)

    def tearDown(self):
        self.temp.cleanup()

    def receive(self, payload, *, sequence=1, source="flood", source_key="case-9"):
        return self.app.inbox.receive(source=source, source_key=source_key, sequence=sequence, payload=payload, occurred_at=NOW)

    def make_conflict(self):
        first = self.receive({"rain": "50mm", "road": "open"})
        with self.assertRaises(ConflictError):
            self.receive({"rain": "80mm", "road": "closed"})
        return first

    def audit_actions(self):
        with self.app.database.connect() as connection:
            return [row["action"] for row in connection.execute("SELECT action FROM audit_entries ORDER BY audit_id")]

    def test_conflict_survives_rollback_and_keeps_original(self):
        first = self.make_conflict()
        chain = self.app.inbox.chain("flood", "case-9", 1)
        self.assertEqual(chain["message"]["payload_digest"], first["digest"])
        self.assertEqual(chain["message"]["payload_json"], canonical_json({"rain": "50mm", "road": "open"}))
        self.assertEqual(len(chain["conflicts"]), 1)
        conflict = chain["conflicts"][0]
        self.assertEqual(conflict["existing_digest"], digest_json({"rain": "50mm", "road": "open"}))
        self.assertEqual(conflict["incoming_digest"], digest_json({"rain": "80mm", "road": "closed"}))
        self.assertEqual(conflict["summary"], {"changed": ["rain", "road"], "added": [], "removed": []})
        self.assertEqual(conflict["received_at"], self.app.clock.now())
        self.assertEqual(conflict["status"], "pending")
        self.assertNotIn("incoming_payload_json", conflict)
        kinds = [event["kind"] for event in self.app.inbox.timeline("flood", "case-9")]
        self.assertEqual(kinds, ["message", "conflict"])
        self.assertEqual(self.app.verify()["inbox_conflicts"], 1)

    def test_duplicate_and_variant_replay_do_not_recreate_conflicts(self):
        first = self.receive({"rain": "50mm"})
        replay = self.receive({"rain": "50mm"})
        self.assertEqual(replay["status"], "duplicate")
        self.assertEqual(replay["digest"], first["digest"])
        self.assertEqual(self.app.verify()["inbox_conflicts"], 0)
        variant = {"rain": "80mm"}
        for _ in range(3):
            with self.assertRaises(ConflictError):
                self.receive(variant)
        conflicts = self.app.inbox.chain("flood", "case-9", 1)["conflicts"]
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["incoming_digest"], digest_json(variant))
        actions = self.audit_actions()
        self.assertEqual(actions.count("inbox.accept"), 1)
        self.assertEqual(actions.count("inbox.duplicate"), 1)
        self.assertEqual(actions.count("inbox.conflict"), 3)

    def test_concurrent_replay_is_serialized(self):
        payload = {"rain": "50mm"}
        variant = {"rain": "80mm"}
        other_variant = {"rain": "120mm"}

        def call(body):
            try:
                return self.receive(body)["status"]
            except ConflictError:
                return "conflict"

        with ThreadPoolExecutor(max_workers=8) as pool:
            first_round = list(pool.map(call, [payload] * 8))
        self.assertEqual(sorted(first_round), ["accepted"] + ["duplicate"] * 7)
        with ThreadPoolExecutor(max_workers=8) as pool:
            second_round = list(pool.map(call, [variant] * 8))
        self.assertEqual(second_round, ["conflict"] * 8)
        with ThreadPoolExecutor(max_workers=8) as pool:
            third_round = list(pool.map(call, [variant, other_variant] * 4))
        self.assertEqual(third_round, ["conflict"] * 8)
        conflicts = self.app.inbox.chain("flood", "case-9", 1)["conflicts"]
        self.assertEqual(len(conflicts), 2)
        self.assertEqual({item["incoming_digest"] for item in conflicts}, {digest_json(variant), digest_json(other_variant)})
        messages = [event for event in self.app.inbox.timeline("flood", "case-9") if event["kind"] == "message"]
        self.assertEqual(len(messages), 1)
        self.assertGreater(self.app.verify()["audit_entries"], 0)

    def test_migration_from_old_table(self):
        old_db = Path(self.temp.name) / "old.sqlite3"
        connection = sqlite3.connect(old_db)
        connection.executescript(OLD_SCHEMA)
        connection.execute("INSERT INTO inbox_messages VALUES(?,?,?,?,?,?,?,?)", ("flood", "case-1", 1, digest_json({"rain": "10mm"}), canonical_json({"rain": "10mm"}), NOW, NOW, "accepted"))
        connection.execute("INSERT INTO inbox_conflicts(source,source_key,sequence,existing_digest,incoming_digest,received_at) VALUES(?,?,?,?,?,?)", ("flood", "case-1", 1, digest_json({"rain": "10mm"}), digest_json({"rain": "20mm"}), NOW))
        connection.commit()
        connection.close()

        app = CivicFlow.open(old_db, fixed_now=NOW)
        with app.database.connect() as check:
            columns = {row["name"] for row in check.execute("PRAGMA table_info(inbox_conflicts)")}
        self.assertTrue({"summary_json", "incoming_payload_json", "status", "resolved_at", "resolved_by", "resolution"} <= columns)
        chain = app.inbox.chain("flood", "case-1", 1)
        self.assertEqual(chain["message"]["payload_digest"], digest_json({"rain": "10mm"}))
        self.assertEqual(len(chain["conflicts"]), 1)
        self.assertEqual(chain["conflicts"][0]["status"], "pending")
        self.assertEqual(chain["conflicts"][0]["summary"], {})
        with self.assertRaises(ConflictError):
            app.inbox.receive(source="flood", source_key="case-1", sequence=1, payload={"rain": "30mm"}, occurred_at=NOW)
        chain = app.inbox.chain("flood", "case-1", 1)
        self.assertEqual(len(chain["conflicts"]), 2)
        self.assertEqual(chain["conflicts"][1]["summary"]["changed"], ["rain"])
        self.assertEqual([event["kind"] for event in app.inbox.timeline("flood", "case-1")], ["message", "conflict", "conflict"])

        restarted = CivicFlow.open(old_db, fixed_now=NOW)
        self.assertEqual(len(restarted.inbox.chain("flood", "case-1", 1)["conflicts"]), 2)
        self.assertGreater(restarted.verify()["audit_entries"], 0)

    def test_payload_only_visible_to_reviewer(self):
        self.make_conflict()
        conflict_id = self.app.inbox.chain("flood", "case-9", 1)["conflicts"][0]["conflict_id"]
        service = self.app.inbox_conflicts

        masked = service.get(self.reader, conflict_id)
        self.assertEqual(masked["incoming_payload_json"], "***")
        self.assertEqual(masked["summary"]["changed"], ["rain", "road"])
        with self.assertRaises(PermissionDenied):
            service.reveal_payload(self.reader, conflict_id)
        with self.assertRaises(PermissionDenied):
            service.list(AccessContext(actor_id="outsider", permissions=frozenset({"read:cases"})))
        strict_reviewer = AccessContext(actor_id="duty-reviewer", permissions=frozenset({"review:inbox_conflicts"}))
        with self.assertRaises(PermissionDenied):
            service.reveal_payload(strict_reviewer, conflict_id)

        revealed = service.reveal_payload(self.reviewer, conflict_id)
        self.assertEqual(revealed["incoming_payload"], {"rain": "80mm", "road": "closed"})
        self.assertEqual(revealed["existing_payload"], {"rain": "50mm", "road": "open"})
        visible = service.get(self.reviewer, conflict_id)
        self.assertEqual(visible["incoming_payload_json"], canonical_json({"rain": "80mm", "road": "closed"}))

    def test_manual_adjudication_and_audit(self):
        self.make_conflict()
        conflict_id = self.app.inbox.chain("flood", "case-9", 1)["conflicts"][0]["conflict_id"]
        service = self.app.inbox_conflicts

        with self.assertRaises(PermissionDenied):
            service.resolve(self.reader, conflict_id, decision="adopted", reason="越权")
        with self.assertRaises(ValidationError):
            service.resolve(self.reviewer, conflict_id, decision="unknown", reason="无效")
        with self.assertRaises(ValidationError):
            service.resolve(self.reviewer, conflict_id, decision="adopted", reason="  ")
        with self.assertRaises(NotFoundError):
            service.resolve(self.reviewer, 9999, decision="adopted", reason="不存在")

        resolved = service.resolve(self.reviewer, conflict_id, decision="adopted", reason="值班复核确认以最新雨情为准")
        self.assertEqual(resolved["status"], "adopted")
        self.assertEqual(resolved["resolved_by"], "duty-reviewer")
        chain = self.app.inbox.chain("flood", "case-9", 1)
        self.assertEqual(chain["message"]["payload_digest"], digest_json({"rain": "80mm", "road": "closed"}))
        self.assertEqual(self.receive({"rain": "80mm", "road": "closed"})["status"], "duplicate")
        with self.assertRaises(ConflictError):
            service.resolve(self.reviewer, conflict_id, decision="dismissed", reason="重复裁决")

        for action in ("inbox.accept", "inbox.conflict", "inbox.resolve", "inbox.duplicate"):
            self.assertIn(action, self.audit_actions())
        self.assertGreaterEqual(self.app.verify()["audit_entries"], 4)


if __name__ == "__main__":
    unittest.main()
