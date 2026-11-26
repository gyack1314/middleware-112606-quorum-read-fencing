"""协调读服务的语义测试。

这些测试同时充当可重演的规格说明：多数派身份计票、任期权威、未确认分叉
截断、新鲜 qid 证据、target 应用前缀、节点级延迟隔离、截止失败与重启恢复。
"""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

from coordinator import (
    Coordinator,
    JsonFileStore,
    JsonMemoryStore,
    majority_size,
    quorum_watermark,
)

ROOT = Path(__file__).resolve().parent
EXAMPLES = ROOT / "examples"


def cluster(nodes=("n1", "n2", "n3"), leader="n1", term=1):
    coord = Coordinator()
    coord.process("members", {"nodes": list(nodes)}, at=0)
    coord.process("leader", {"node": leader, "term": term}, at=0)
    return coord


def kinds(events):
    return [e["type"] for e in events]


def only(events, event_type):
    matches = [e for e in events if e["type"] == event_type]
    assert matches, f"缺少事件 {event_type}；实际 {kinds(events)}"
    return matches[-1]


def feed_file(coord, name):
    path = EXAMPLES / name
    all_events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            all_events.extend(coord.handle(json.loads(line)))
    return all_events


class UtilTests(unittest.TestCase):
    def test_majority_and_watermark(self):
        self.assertEqual(majority_size(1), 1)
        self.assertEqual(majority_size(3), 2)
        self.assertEqual(majority_size(5), 3)
        self.assertEqual(quorum_watermark([1, 1, 2], 2), 1)
        self.assertEqual(quorum_watermark([1, 2, 2], 2), 2)
        self.assertEqual(quorum_watermark([3, 3, 5, 5, 5], 3), 5)
        self.assertIsNone(quorum_watermark([1], 2))


class WriteQuorumTests(unittest.TestCase):
    def test_duplicate_node_receipts_count_once(self):
        c = cluster()
        c.process("put", {"entry": "e1", "term": 1, "index": 1, "key": "k", "value": "v"}, 1)
        e1 = c.process("replica", {"node": "n1", "term": 1, "entry": "e1", "index": 1}, 2)
        self.assertEqual(only(e1, "replica_counted")["count"], 1)
        e2 = c.process("replica", {"node": "n1", "term": 1, "entry": "e1", "index": 1}, 2)
        self.assertEqual(only(e2, "replica_counted")["count"], 1)
        self.assertEqual(c.state["committed_index"], 0)
        e3 = c.process("replica", {"node": "n2", "term": 1, "entry": "e1", "index": 1}, 3)
        self.assertIn("write_committed", kinds(e3))
        self.assertEqual(c.state["committed_index"], 1)
        self.assertEqual(len(c.state["credentials"]), 1)

    def test_non_member_and_stale_term_do_not_count(self):
        c = cluster()
        c.process("put", {"entry": "e1", "term": 1, "index": 1, "key": "k", "value": "v"}, 1)
        out = c.process("replica", {"node": "x9", "term": 1, "entry": "e1", "index": 1}, 2)
        self.assertEqual(only(out, "replica_ignored")["reason"], "not_current_member")

        out = c.process("replica", {"node": "n2", "term": 0, "entry": "e1", "index": 1}, 2)
        self.assertEqual(only(out, "replica_ignored")["reason"], "stale_lower_term")

        # 只报最大位置、没有确切 entry 身份也不行。
        out = c.process("replica", {"node": "n3", "term": 1, "index": 1}, 2)
        self.assertEqual(only(out, "replica_ignored")["reason"], "missing_entry_identity")
        self.assertEqual(c.state["committed_index"], 0)

        c.process("replica", {"node": "n1", "term": 1, "entry": "e1", "index": 1}, 3)
        self.assertEqual(c.state["committed_index"], 0)
        c.process("replica", {"node": "n2", "term": 1, "entry": "e1", "index": 1}, 4)
        self.assertEqual(c.state["committed_index"], 1)

    def test_commit_is_a_prefix(self):
        c = cluster()
        for i in (1, 2):
            c.process("put", {"entry": f"e{i}", "term": 1, "index": i,
                              "key": "k", "value": i}, i)
        # index 2 先拿到多数，但 index 1 缺票，不能跳过位置提交。
        for n in ("n1", "n2"):
            c.process("replica", {"node": n, "term": 1, "entry": "e2", "index": 2}, 3)
        self.assertEqual(c.state["committed_index"], 0)
        for n in ("n1", "n2"):
            out = c.process("replica", {"node": n, "term": 1, "entry": "e1", "index": 1}, 4)
        self.assertIn("write_committed", kinds(out))
        self.assertEqual(c.state["committed_index"], 2)
        self.assertEqual([x["entry"] for x in c.state["credentials"]], ["e1", "e2"])

    def test_put_term_must_match_authority(self):
        c = cluster(term=3)
        out = c.process("put", {"entry": "e", "term": 2, "index": 1, "key": "k", "value": 1}, 1)
        self.assertEqual(only(out, "write_rejected")["reason"], "not_current_term")


class ForkAndTruncationTests(unittest.TestCase):
    def test_two_identities_at_same_index_are_not_mixed(self):
        c = cluster(leader="a", nodes=("a", "b", "c"), term=3)
        c.process("put", {"entry": "old", "term": 3, "index": 1,
                          "key": "run", "value": "pending"}, 1)
        c.process("replica", {"node": "a", "term": 3, "entry": "old", "index": 1}, 2)
        c.process("leader", {"node": "b", "term": 4}, 3)
        out = c.process("put", {"entry": "new", "term": 4, "index": 1,
                                "key": "run", "value": "complete"}, 4)
        self.assertIn("unconfirmed_tail_truncated", kinds(out))

        # a 的低任期 old 回报既过期又是被截断分叉，不能给 new 计票。
        out = c.process("replica", {"node": "a", "term": 3, "entry": "old", "index": 1}, 6)
        self.assertEqual(only(out, "replica_ignored")["reason"], "stale_lower_term")
        # 即便用当前任期重报 old，活跃身份仍是 new，依然不能混用。
        out = c.process("replica", {"node": "a", "term": 4, "entry": "old", "index": 1}, 7)
        self.assertEqual(only(out, "replica_ignored")["reason"], "unconfirmed_fork_truncated")

        c.process("replica", {"node": "b", "term": 4, "entry": "new", "index": 1}, 5)
        c.process("replica", {"node": "c", "term": 4, "entry": "new", "index": 1}, 8)
        self.assertEqual(c.state["committed_index"], 1)
        report = c.inspect_entry("old")
        self.assertEqual(report["status"], "truncated")
        self.assertIsNone(report["credential"])
        self.assertEqual([x["entry"] for x in c.state["credentials"]], ["new"])

    def test_committed_index_cannot_be_overwritten_and_credential_remains(self):
        c = cluster()
        c.process("put", {"entry": "e1", "term": 1, "index": 1, "key": "run", "value": "closed"}, 1)
        for n in ("n1", "n2"):
            c.process("replica", {"node": n, "term": 1, "entry": "e1", "index": 1}, 2)
        credential = c.state["credentials"][0]

        out = c.process("put", {"entry": "fake", "term": 1, "index": 1,
                                "key": "run", "value": "reopened"}, 3)
        self.assertEqual(only(out, "write_rejected")["reason"], "index_already_committed")
        self.assertEqual(c.state["log"]["1"]["entry"], "e1")
        self.assertEqual(c.state["credentials"], [credential])


class ApplyTests(unittest.TestCase):
    def _two_committed(self):
        c = cluster()
        for i, v in ((1, "a"), (2, "b")):
            c.process("put", {"entry": f"e{i}", "term": 1, "index": i,
                              "key": "k", "value": v}, 1)
        for i in (1, 2):
            for n in ("n1", "n2"):
                c.process("replica", {"node": n, "term": 1, "entry": f"e{i}", "index": i}, 2 + i)
        self.assertEqual(c.state["committed_index"], 2)
        return c

    def test_out_of_order_and_duplicate_apply(self):
        c = self._two_committed()
        out = c.process("apply", {"node": "n3", "entry": "e2", "index": 2}, 5)
        self.assertEqual(only(out, "apply_recorded")["applied_index"], 0)
        self.assertEqual(c.inspect_node("n3")["applied_indices"], [2])

        out = c.process("apply", {"node": "n3", "entry": "e1", "index": 1}, 6)
        self.assertEqual(only(out, "apply_recorded")["applied_index"], 2)

        out = c.process("apply", {"node": "n3", "entry": "e1", "index": 1}, 7)
        self.assertIn("apply_duplicate", kinds(out))

    def test_apply_identity_mismatch_rejected(self):
        c = self._two_committed()
        out = c.process("apply", {"node": "n3", "entry": "evil", "index": 1}, 5)
        self.assertIn("apply_identity_mismatch", kinds(out))
        self.assertEqual(c.state["applied"].get("n3", {}), {})

    def test_early_apply_claim_verified_after_commit(self):
        c = cluster()
        c.process("put", {"entry": "e1", "term": 1, "index": 1, "key": "k", "value": "v"}, 1)
        out = c.process("apply", {"node": "n3", "entry": "e1", "index": 1}, 2)
        self.assertIn("apply_pending_verification", kinds(out))
        for n in ("n1", "n2"):
            out = c.process("replica", {"node": n, "term": 1, "entry": "e1", "index": 1}, 3)
        self.assertIn("apply_verified", kinds(out))
        self.assertEqual(c.inspect_node("n3")["applied_index"], 1)


class ReadEvidenceTests(unittest.TestCase):
    def _ready(self, target="n3", apply_target=True, nodes=("n1", "n2", "n3")):
        c = cluster(nodes=nodes)
        c.process("put", {"entry": "e1", "term": 1, "index": 1,
                          "key": "run", "value": "closed"}, 1)
        for n in ("n1", "n2"):
            c.process("replica", {"node": n, "term": 1, "entry": "e1", "index": 1}, 2)
        if apply_target:
            c.process("apply", {"node": target, "entry": "e1", "index": 1}, 3)
        return c

    def test_witness_before_read_is_rejected(self):
        c = self._ready()
        out = c.process("witness", {"qid": "r", "node": "n1", "term": 1, "commit": 1}, 3)
        self.assertEqual(only(out, "evidence_ignored")["reason"], "unknown_qid")

    def test_fresh_qid_bound_evidence_and_answer(self):
        c = self._ready()
        c.process("read", {"qid": "r", "key": "run", "target": "n3",
                           "min_index": 1, "deadline": 20}, 4)
        c.process("witness", {"qid": "r", "node": "n1", "term": 1, "commit": 1}, 5)
        out = c.process("witness", {"qid": "r", "node": "n2", "term": 1, "commit": 1}, 6)
        ans = only(out, "read_answered")
        self.assertEqual(ans["entry"], "e1")
        self.assertEqual(ans["value"], "closed")
        self.assertEqual(ans["served_term"], 1)

        # 后来的读 qid 不同，不能复用 r 的旧确认。
        c.process("read", {"qid": "r2", "key": "run", "target": "n3",
                           "min_index": 1, "deadline": 20}, 7)
        report = c.inspect_read("r2")
        self.assertEqual(report["status"], "waiting_evidence")
        self.assertEqual(report["active_witnesses"], {})

    def test_same_term_duplicate_leader_notice_keeps_evidence(self):
        c = self._ready(apply_target=False)
        c.process("read", {"qid": "r", "key": "run", "target": "n3",
                           "min_index": 1, "deadline": 20}, 4)
        c.process("witness", {"qid": "r", "node": "n1", "term": 1, "commit": 1}, 5)
        c.process("witness", {"qid": "r", "node": "n2", "term": 1, "commit": 1}, 5)
        self.assertEqual(c.inspect_read("r")["status"], "waiting_apply")
        out = c.process("leader", {"node": "n1", "term": 1}, 6)
        self.assertIn("authority_unchanged", kinds(out))
        self.assertEqual(c.inspect_read("r")["guarantee_index"], 1)
        out = c.process("apply", {"node": "n3", "entry": "e1", "index": 1}, 7)
        self.assertIn("read_answered", kinds(out))

    def test_term_change_invalidates_secured_but_unapplied_evidence(self):
        c = self._ready(apply_target=False)
        c.process("read", {"qid": "r", "key": "run", "target": "n3",
                           "min_index": 1, "deadline": 20}, 3)
        c.process("witness", {"qid": "r", "node": "n1", "term": 1, "commit": 1}, 4)
        c.process("witness", {"qid": "r", "node": "n2", "term": 1, "commit": 1}, 4)
        self.assertEqual(c.inspect_read("r")["status"], "waiting_apply")

        c.process("leader", {"node": "n2", "term": 2}, 5)
        report = c.inspect_read("r")
        self.assertEqual(report["status"], "waiting_evidence")
        self.assertIsNone(report["guarantee_index"])
        self.assertEqual(report["generation"], 2)

        # 旧任期证人不能在新世代充数。
        out = c.process("witness", {"qid": "r", "node": "n3", "term": 1, "commit": 1}, 8)
        self.assertEqual(only(out, "evidence_ignored")["reason"], "stale_lower_term")
        self.assertEqual(c.inspect_read("r")["status"], "waiting_evidence")

        # 新任期新鲜多数 + target 已应用 -> 回答，且记录跨代轨迹。
        c.process("apply", {"node": "n3", "entry": "e1", "index": 1}, 6)
        c.process("witness", {"qid": "r", "node": "n2", "term": 2, "commit": 1}, 10)
        out = c.process("witness", {"qid": "r", "node": "n3", "term": 2, "commit": 1}, 10)
        ans = only(out, "read_answered")
        self.assertEqual((ans["generation"], ans["served_term"]), (2, 2))
        report = c.inspect_read("r")
        self.assertEqual([g["generation"] for g in report["generations"]], [1, 2])
        self.assertEqual(report["generations"][0]["end_reason"], "authority_changed")

    def test_target_own_witness_commit_is_its_promised_prefix(self):
        c = cluster()
        for i in (1, 2):
            c.process("put", {"entry": f"e{i}", "term": 1, "index": i,
                              "key": "run", "value": i}, 1)
        for i in (1, 2):
            for n in ("n1", "n2"):
                c.process("replica", {"node": n, "term": 1, "entry": f"e{i}", "index": i}, 2 + i)
        # target n3 只应用到 1，但它在本轮 witness 里自报 commit=2。
        c.process("apply", {"node": "n3", "entry": "e1", "index": 1}, 5)
        c.process("read", {"qid": "r", "key": "run", "target": "n3",
                           "min_index": 1, "deadline": 20}, 6)
        c.process("witness", {"qid": "r", "node": "n1", "term": 1, "commit": 1}, 7)
        out = c.process("witness", {"qid": "r", "node": "n3", "term": 1, "commit": 2}, 8)
        waiting = only(out, "read_waiting_apply")
        # 多数水位 G=1，但 target 自己承诺了 2，必须应用到 2。
        self.assertEqual(waiting["guarantee_index"], 1)
        self.assertEqual(waiting["required_index"], 2)
        self.assertEqual(c.inspect_read("r")["status"], "waiting_apply")
        out = c.process("apply", {"node": "n3", "entry": "e2", "index": 2}, 9)
        ans = only(out, "read_answered")
        self.assertEqual(ans["seen_index"], 2)

    def test_watermark_must_cover_min_index(self):
        c = cluster()
        for i in (1, 2):
            c.process("put", {"entry": f"e{i}", "term": 1, "index": i,
                              "key": "run", "value": i}, 1)
        for i in (1, 2):
            for n in ("n1", "n2"):
                c.process("replica", {"node": n, "term": 1, "entry": f"e{i}", "index": i}, 2 + i)
        c.process("apply", {"node": "n3", "entry": "e1", "index": 1}, 5)
        c.process("apply", {"node": "n3", "entry": "e2", "index": 2}, 5)
        c.process("read", {"qid": "r", "key": "run", "target": "n3",
                           "min_index": 2, "deadline": 20}, 6)
        c.process("witness", {"qid": "r", "node": "n1", "term": 1, "commit": 1}, 7)
        out = c.process("witness", {"qid": "r", "node": "n2", "term": 1, "commit": 1}, 8)
        self.assertIn("evidence_watermark_insufficient", kinds(out))
        self.assertEqual(c.inspect_read("r")["status"], "waiting_evidence")
        # 第二名证人把 commit 提到 2：[1,2] 在 3 节点里水位仍是 1，需要多数 >=2。
        c.process("witness", {"qid": "r", "node": "n2", "term": 1, "commit": 2}, 9)
        self.assertEqual(c.inspect_read("r")["status"], "waiting_evidence")
        c.process("witness", {"qid": "r", "node": "n3", "term": 1, "commit": 2}, 10)
        report = c.inspect_read("r")
        self.assertEqual(report["status"], "answered")
        self.assertEqual(report["answer"]["seen_index"], 2)
        self.assertEqual(report["answer"]["value"], 2)


class IsolationAndDeadlineTests(unittest.TestCase):
    def test_apply_delay_blocks_only_its_own_target_read(self):
        c = cluster()
        c.process("put", {"entry": "e1", "term": 1, "index": 1,
                          "key": "run", "value": "closed"}, 1)
        for n in ("n1", "n2"):
            c.process("replica", {"node": n, "term": 1, "entry": "e1", "index": 1}, 2)
        c.process("apply", {"node": "n3", "entry": "e1", "index": 1}, 3)

        c.process("read", {"qid": "slow", "key": "run", "target": "n2",
                           "min_index": 1, "deadline": 30}, 4)
        c.process("read", {"qid": "fast", "key": "run", "target": "n3",
                           "min_index": 1, "deadline": 30}, 4)
        for qid in ("slow", "fast"):
            c.process("witness", {"qid": qid, "node": "n1", "term": 1, "commit": 1}, 5)
            c.process("witness", {"qid": qid, "node": "n3", "term": 1, "commit": 1}, 5)

        self.assertEqual(c.inspect_read("fast")["status"], "answered")
        self.assertEqual(c.inspect_read("slow")["status"], "waiting_apply")

        out = c.process("apply", {"node": "n2", "entry": "e1", "index": 1}, 6)
        self.assertTrue(
            any(e.get("qid") == "slow" and e["type"] == "read_answered" for e in out)
        )
        self.assertEqual(c.inspect_read("fast")["status"], "answered")

    def test_tick_at_deadline_reports_unavailable_and_late_evidence_rejected(self):
        c = cluster()
        c.process("read", {"qid": "r", "key": "run", "target": "n3",
                           "min_index": 1, "deadline": 8}, 1)
        out = c.process("tick", {"now": 8}, 8)
        self.assertIn("read_unavailable", kinds(out))
        report = c.inspect_read("r")
        self.assertEqual(report["status"], "unavailable")
        self.assertEqual(report["failure"]["reason"], "cannot_guarantee_by_deadline")

        late = c.process("witness", {"qid": "r", "node": "n1", "term": 1, "commit": 1}, 9)
        self.assertEqual(only(late, "evidence_ignored")["reason"], "read_unavailable")

    def test_late_message_past_deadline_lazily_expires(self):
        c = cluster()
        c.process("read", {"qid": "r", "key": "run", "target": "n3",
                           "min_index": 1, "deadline": 8}, 1)
        out = c.process("inspect", {"qid": "r"}, 11)
        self.assertIn("read_unavailable", kinds(out))

    def test_witness_at_deadline_second_before_tick_still_counts(self):
        c = cluster()
        c.process("put", {"entry": "e1", "term": 1, "index": 1,
                          "key": "run", "value": "v"}, 1)
        for n in ("n1", "n2"):
            c.process("replica", {"node": n, "term": 1, "entry": "e1", "index": 1}, 2)
        c.process("apply", {"node": "n3", "entry": "e1", "index": 1}, 3)
        c.process("read", {"qid": "r", "key": "run", "target": "n3",
                           "min_index": 1, "deadline": 10}, 4)
        c.process("witness", {"qid": "r", "node": "n1", "term": 1, "commit": 1}, 10)
        out = c.process("witness", {"qid": "r", "node": "n2", "term": 1, "commit": 1}, 10)
        self.assertIn("read_answered", kinds(out))
        tick = c.process("tick", {"now": 10}, 10)
        self.assertNotIn("read_unavailable", kinds(tick))


class RestartTests(unittest.TestCase):
    def test_file_store_restart_preserves_everything(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "state.json")
            c = Coordinator(JsonFileStore(path))
            c.process("members", {"nodes": ["n1", "n2", "n3"]}, 0)
            c.process("leader", {"node": "n1", "term": 1}, 0)
            c.process("put", {"entry": "e1", "term": 1, "index": 1,
                              "key": "run", "value": "closed"}, 1)
            for n in ("n1", "n2"):
                c.process("replica", {"node": n, "term": 1, "entry": "e1", "index": 1}, 2)
            c.process("apply", {"node": "n3", "entry": "e1", "index": 1}, 3)
            c.process("read", {"qid": "r", "key": "run", "target": "n3",
                               "min_index": 1, "deadline": 99}, 4)
            c.process("witness", {"qid": "r", "node": "n1", "term": 1, "commit": 1}, 5)
            restart_events = c.process("restart", {}, 6)
            board = only(restart_events, "service_restarted")
            self.assertEqual(board["committed_index"], 1)
            self.assertEqual(board["reads_waiting_evidence"], ["r"])
            self.assertEqual(board["node_applied"]["n3"], 1)

            # 真正新建一个对象/进程，从磁盘状态文件恢复。
            c2 = Coordinator(JsonFileStore(path))
            self.assertEqual(c2.state["committed_index"], 1)
            self.assertEqual(len(c2.state["credentials"]), 1)
            self.assertEqual(c2.inspect_node("n3")["applied_index"], 1)
            self.assertEqual(set(c2.inspect_read("r")["active_witnesses"]), {"n1"})

            c2.process("witness", {"qid": "r", "node": "n2", "term": 1, "commit": 1}, 7)
            report = c2.inspect_read("r")
            self.assertEqual(report["status"], "answered")
            self.assertEqual(report["answer"]["value"], "closed")

    def test_memory_store_round_trips_json(self):
        c = Coordinator(JsonMemoryStore())
        c.process("members", {"nodes": ["a", "b", "c"]}, 0)
        c.process("leader", {"node": "a", "term": 3}, 0)
        c.process("put", {"entry": "old", "term": 3, "index": 1,
                          "key": "run", "value": "pending"}, 1)
        # 状态必须全部 JSON 可序列化。
        json.dumps(c.snapshot())


class ShippedScenarioTests(unittest.TestCase):
    def test_term_change_before_follower_apply(self):
        c = Coordinator(JsonMemoryStore())
        events = feed_file(c, "term-change-before-follower-apply.jsonl")

        committed = only(events, "write_committed")
        self.assertEqual((committed["entry"], committed["quorum"]), ("e1", ["n1", "n2"]))

        report = c.inspect_read("r")
        self.assertEqual(report["status"], "answered")
        self.assertEqual(report["generation"], 2)
        self.assertEqual(report["answer"]["served_term"], 2)
        self.assertEqual(report["answer"]["served_leader"], "n2")
        self.assertEqual(report["answer"]["entry"], "e1")
        self.assertEqual(report["answer"]["value"], "closed")
        self.assertEqual(report["answer"]["evidence_quorum"], ["n2", "n3"])
        self.assertEqual([g["term"] for g in report["generations"]], [1, 2])
        # n1/n2 没有 apply 回执，值班台必须能看出它们仍落后。
        board = c.board()
        self.assertEqual(board["nodes"]["n3"]["applied_index"], 1)
        self.assertEqual(board["nodes"]["n1"]["applied_index"], 0)
        self.assertIn("n1", board["lagging_nodes"])

    def test_same_index_is_not_same_entry(self):
        c = Coordinator()
        feed_file(c, "same-index-is-not-same-entry.jsonl")
        report = c.inspect_read("r")
        self.assertEqual(report["status"], "answered")
        self.assertEqual(report["answer"]["entry"], "new")
        self.assertEqual(report["answer"]["value"], "complete")
        self.assertEqual(report["answer"]["served_term"], 4)
        old = c.inspect_entry("old")
        new = c.inspect_entry("new")
        self.assertEqual(old["status"], "truncated")
        self.assertEqual(new["status"], "committed")
        self.assertEqual([x["entry"] for x in c.state["credentials"]], ["new"])


class HttpServiceTests(unittest.TestCase):
    def test_http_message_board_and_state(self):
        from duty_server import build_server

        server, _app = build_server("127.0.0.1", 0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_address[1]}"

            def post(path, payload):
                req = urllib.request.Request(
                    base + path,
                    data=json.dumps(payload).encode(),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    return json.loads(resp.read().decode())

            def get(path):
                with urllib.request.urlopen(base + path, timeout=5) as resp:
                    return json.loads(resp.read().decode())

            post("/v1/message", {"at": 0, "kind": "members",
                                 "record": {"nodes": ["n1", "n2", "n3"]}})
            post("/v1/message", {"at": 0, "kind": "leader",
                                 "record": {"node": "n1", "term": 1}})
            result = post("/v1/message", {
                "at": 1, "kind": "put",
                "record": {"entry": "e1", "term": 1, "index": 1, "key": "k", "value": "v"}})
            self.assertTrue(any(e["type"] == "write_accepted" for e in result["events"]))
            board = get("/v1/board")
            self.assertEqual(board["majority"], 2)
            state = get("/v1/state")
            self.assertIn("proposals", state)
            health = get("/healthz")
            self.assertTrue(health["ok"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


class CliReplayTests(unittest.TestCase):
    def test_main_replays_example_and_json_output(self):
        import io
        import contextlib
        import duty_console

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = duty_console.main([
                "replay", str(EXAMPLES / "term-change-before-follower-apply.jsonl"),
                "--json",
            ])
        self.assertEqual(rc, 0)
        lines = [json.loads(line) for line in buf.getvalue().splitlines() if line.strip()]
        types_ = [row["type"] for row in lines]
        self.assertIn("read_answered", types_)
        self.assertIn("final_board", types_)
        answered = next(row for row in lines if row["type"] == "read_answered")
        self.assertEqual(answered["served_term"], 2)

    def test_human_output_explains_generation(self):
        import io
        import contextlib
        import duty_console

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = duty_console.main([
                str(EXAMPLES / "same-index-is-not-same-entry.jsonl"),
                "--no-board",
            ])
        self.assertEqual(rc, 0)
        text = buf.getvalue()
        self.assertIn("第 1 代权威 b/term4", text)
        self.assertIn("new@index1", text)
        self.assertIn("complete", text)


if __name__ == "__main__":
    unittest.main()
