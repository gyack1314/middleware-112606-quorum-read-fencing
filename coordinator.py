"""可重演的主从协调读服务核心。

只使用 Python 标准库。输入契约见 README.md / examples/message_types.json：

    members  固定成员名单
    leader   新的权威角色通知 (node/term)
    put      在 term 权威下于 index 提出 entry 写
    replica  node/term 回报 index 上确切的 entry（身份证据）
    apply    node 证实已应用 entry/index
    read     qid/key/target/min_index/deadline
    witness  node/term 为本次 qid 出具 commit 上界证据
    tick     推进时间、宣判截止
    inspect  值班台检查
    restart  进程重启，从持久化状态恢复

设计要点（与任务语义逐条对应）：

* 多数派 = 固定当前成员的 floor(n/2)+1，按“不同节点”计票，重复回报不翻倍。
* 写成功必须在当前任期权威下拿到多数“确切 entry@index”的复制事实；
  低任期回报、非成员、身份不符、被截断分叉的回报一律不计。
* 已确认（committed prefix）写不可改写；未确认尾部可被同 index 的新 entry
  截断替换，旧变体的票不与新变体混用；未确认写从未产生成功凭据。
* 读的多数证据必须发生在读创建之后、绑定本次 qid 与当前任期；换权威
  （leader 通知或更高任期回报）会作废尚未回答的旧证据并要求重新取证。
* 多数证据给出“多数派可担保位置”G（第 majority 高的 witness commit），
  且 G >= min_index；目标节点必须真的把前缀应用到 G 才回答。
* 应用进度按位置核实，支持重复（幂等）与乱序（先记账、补齐前缀）。
* 每个读只等自己 target 的应用进度，单节点延迟不阻塞其他读。
* 凭据、已确认前缀、各节点应用进度与未决读全部可 JSON 持久化，
  restart 从 Store 重新加载，模拟真实进程重启。
"""

from __future__ import annotations

import json
import os
import tempfile
from copy import deepcopy
from typing import Any, Iterable

SCHEMA_VERSION = 1


# ---------------------------------------------------------------------------
# 持久化：默认内存 JSON 仓（强制可序列化），可选原子落盘文件仓
# ---------------------------------------------------------------------------


class JsonMemoryStore:
    """以 JSON 文本保存状态的内存仓；restart 时经过一次序列化往返。"""

    def __init__(self) -> None:
        self._blob: str | None = None

    def save_state(self, state: dict) -> None:
        self._blob = json.dumps(state, ensure_ascii=False, sort_keys=True)

    def load_state(self) -> dict | None:
        if self._blob is None:
            return None
        return json.loads(self._blob)


class JsonFileStore:
    """每次状态变更原子写盘；restart / 进程退出后值班员仍可核对该文件。"""

    def __init__(self, path: str) -> None:
        self.path = os.fspath(path)

    def save_state(self, state: dict) -> None:
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".duty-state-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(state, handle, ensure_ascii=False, sort_keys=True, indent=2)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def load_state(self) -> dict | None:
        if not os.path.exists(self.path):
            return None
        with open(self.path, encoding="utf-8") as handle:
            return json.load(handle)


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------


def _is_int(value: Any) -> bool:
    return type(value) is int


def _is_number(value: Any) -> bool:
    return (_is_int(value) or isinstance(value, float)) and not isinstance(value, bool)


def majority_size(member_count: int) -> int:
    return member_count // 2 + 1


def quorum_watermark(commits: Iterable[int], majority: int) -> int | None:
    """多数派可共同担保的最高位置：第 majority 大的 commit。

    例如 3 节点多数 2，commit 为 [1,2,2] -> 水位 2（至少 2 个节点 >=2）；
    [1,1,2] -> 水位 1。
    """
    ordered = sorted(commits, reverse=True)
    if len(ordered) < majority:
        return None
    return ordered[majority - 1]


def fresh_state() -> dict:
    return {
        "version": SCHEMA_VERSION,
        "clock": 0,
        "members": [],
        "majority": 0,
        # {"node": str|None, "term": int}；通过更高任期回报观察到新任期时
        # node 可能为 None，直到 leader 通知补全。
        "authority": None,
        "committed_index": 0,
        # str(index) -> 活跃日志格
        "log": {},
        # entry -> 写提案（含全部复制回报）
        "proposals": {},
        # 已发给客户的成功写凭据，只追加，重启不丢
        "credentials": [],
        # node -> {str(index): {"entry":..., "at":...}}，仅保存与已确认
        # 身份核实一致的应用记录
        "applied": {},
        # 尚未能按已确认身份核实的提前/乱序应用回报
        "pending_apply": [],
        # qid -> 读状态
        "reads": {},
        "read_order": [],
    }


def normalize_state(data: dict | None) -> dict:
    if not isinstance(data, dict) or data.get("version") != SCHEMA_VERSION:
        return fresh_state()
    base = fresh_state()
    base.update(data)
    # 重启恢复时不依赖内存派生字段；结构关键字段兜底。
    base.setdefault("log", {})
    base.setdefault("proposals", {})
    base.setdefault("credentials", [])
    base.setdefault("applied", {})
    base.setdefault("pending_apply", [])
    base.setdefault("reads", {})
    base.setdefault("read_order", [])
    return base


# ---------------------------------------------------------------------------
# 协调服务
# ---------------------------------------------------------------------------


class Coordinator:
    """接收连续消息、维护状态并输出事件的确定性引擎。

    所有公共入口：
        coord.handle(envelope)   # {"at":int,"kind":str,"record":dict}
        coord.process(kind, record=None, at=0) -> list[event dict]
        coord.board()            # 值班总览
        coord.inspect(selector)  # 指定 qid/entry/node/index 的报告
    """

    def __init__(self, store: JsonMemoryStore | JsonFileStore | None = None) -> None:
        self.store = store if store is not None else JsonMemoryStore()
        loaded = self.store.load_state()
        self.state = normalize_state(loaded)

    # -- 入口 --------------------------------------------------------------

    def handle(self, envelope: dict) -> list[dict]:
        if not isinstance(envelope, dict):
            return [self._invalid("信封必须是对象", envelope)]
        at = envelope.get("at", 0)
        kind = envelope.get("kind")
        record = envelope.get("record", {})
        if not _is_int(at) or at < 0:
            return [self._invalid("at 必须是非负整数", envelope)]
        if not isinstance(kind, str) or not kind:
            return [self._invalid("kind 必须是非空字符串", envelope)]
        if not isinstance(record, dict):
            return [self._invalid("record 必须是对象", envelope)]
        return self.process(kind, record, at)

    def process(self, kind: str, record: dict | None = None, at: int = 0) -> list[dict]:
        record = record or {}
        if not _is_int(at) or at < 0:
            at = 0
        # 时间只进不退（直接 API 乱序时取 max，文件重演由 reader 保证单调）。
        if at > self.state["clock"]:
            self.state["clock"] = at

        events: list[dict] = []
        events.extend(self._expire_overdue(strict=True, now=self.state["clock"]))

        if kind == "restart":
            events.extend(self._restart(at))
        else:
            handler = {
                "members": self._on_members,
                "leader": self._on_leader,
                "put": self._on_put,
                "replica": self._on_replica,
                "apply": self._on_apply,
                "read": self._on_read,
                "witness": self._on_witness,
                "tick": self._on_tick,
                "inspect": self._on_inspect,
            }.get(kind)
            if handler is None:
                events.append(
                    {
                        "type": "unsupported_message",
                        "at": at,
                        "kind": kind,
                        "record": record,
                        "note": "未登记消息类型，已忽略，不影响既有状态",
                    }
                )
            else:
                events.extend(handler(record, at))

        self.store.save_state(self.state)
        return events

    # -- 基础工具 ----------------------------------------------------------

    def _invalid(self, reason: str, raw: Any) -> dict:
        return {"type": "invalid_input", "reason": reason, "raw": raw}

    @property
    def members(self) -> list[str]:
        return self.state["members"]

    @property
    def majority(self) -> int:
        return self.state["majority"] or majority_size(max(len(self.members), 1))

    def _authority_term(self) -> int | None:
        auth = self.state["authority"]
        return auth["term"] if auth else None

    def _is_current_member(self, node: Any) -> bool:
        return isinstance(node, str) and node in set(self.members)

    def _credential_for_index(self, index: int) -> dict | None:
        for cred in self.state["credentials"]:
            if cred["index"] == index:
                return cred
        return None

    # -- members / leader --------------------------------------------------

    def _on_members(self, record: dict, at: int) -> list[dict]:
        nodes = record.get("nodes")
        if not isinstance(nodes, list) or not nodes or not all(isinstance(n, str) and n for n in nodes):
            return [{"type": "invalid_input", "at": at, "kind": "members", "reason": "nodes 应为非空字符串名单"}]
        unique = list(dict.fromkeys(nodes))
        if self.state["members"] and unique != self.state["members"]:
            return [
                {
                    "type": "membership_fixed_conflict",
                    "at": at,
                    "wanted": unique,
                    "kept": list(self.state["members"]),
                    "reason": "成员固定，拒绝变更名单",
                }
            ]
        self.state["members"] = unique
        self.state["majority"] = majority_size(len(unique))
        return [
            {
                "type": "members_established",
                "at": at,
                "nodes": unique,
                "member_count": len(unique),
                "majority": self.state["majority"],
            }
        ]

    def _install_authority(
        self, node: str | None, term: int, at: int, events: list[dict], *, via: str
    ) -> None:
        """安装新权威世代；任何 (node,term) 权威对变化都作废未回答读的证据。"""
        old = self.state["authority"]
        if old is not None:
            if term < old["term"]:
                events.append(
                    {
                        "type": "authority_stale_notice",
                        "at": at,
                        "via": via,
                        "noticed_node": node,
                        "noticed_term": term,
                        "current": dict(old),
                        "reason": "低任期角色通知/回报，不能代表当前权威",
                    }
                )
                return
            if term == old["term"]:
                if old.get("node") is None and node is not None:
                    old["node"] = node
                    events.append(
                        {
                            "type": "authority_leader_named",
                            "at": at,
                            "leader": node,
                            "term": term,
                            "via": via,
                        }
                    )
                    return
                if node is None or node == old.get("node"):
                    # 同世代重复通知：幂等，不能冲掉已经取得的本次证据。
                    events.append(
                        {
                            "type": "authority_unchanged",
                            "at": at,
                            "leader": old.get("node"),
                            "term": term,
                        }
                    )
                    return
                # 同任期换了主节点：权威对变化，旧证据同样不能继续担保。
        new_auth = {"node": node, "term": term}
        self.state["authority"] = new_auth
        reset_qids = self._reset_pending_evidence(
            at, reason="authority_changed", detail=f"权威更换为 {node!r} 任期 {term}"
        )
        events.append(
            {
                "type": "authority_changed",
                "at": at,
                "leader": node,
                "term": term,
                "via": via,
                "reset_reads": reset_qids,
                "committed_index": self.state["committed_index"],
                "note": "已确认前缀由新主承接，未回答读需在新任期重新取得本次多数证据",
            }
        )

    def _on_leader(self, record: dict, at: int) -> list[dict]:
        node, term = record.get("node"), record.get("term")
        if not isinstance(node, str) or not node:
            return [{"type": "invalid_input", "at": at, "kind": "leader", "reason": "缺少 node"}]
        if not _is_int(term) or term < 0:
            return [{"type": "invalid_input", "at": at, "kind": "leader", "reason": "term 应为非负整数"}]
        if self.state["members"] and node not in set(self.members):
            return [
                {
                    "type": "invalid_input",
                    "at": at,
                    "kind": "leader",
                    "reason": f"{node} 不是固定成员",
                }
            ]
        events: list[dict] = []
        self._install_authority(node, term, at, events, via="leader")
        return events

    def _observe_report_term(self, term: int, node: str, at: int, events: list[dict]) -> bool:
        """回报中出现更高任期时，先推进权威世代（主节点待 leader 通知补全）。

        返回该回报任期是否等于当前已安装任期。
        """
        current = self._authority_term()
        if current is None:
            events.append(
                {"type": "evidence_ignored", "at": at, "node": node, "term": term,
                 "reason": "no_authority", "note": "尚无任何权威任期，回报无法计票"}
            )
            return False
        if term > current:
            self._install_authority(None, term, at, events, via=f"report:{node}")
            return False
        return term == current

    # -- put / replica / 提交 ----------------------------------------------

    def _on_put(self, record: dict, at: int) -> list[dict]:
        entry = record.get("entry")
        term = record.get("term")
        index = record.get("index")
        key = record.get("key")
        value = record.get("value")
        if not isinstance(entry, str) or not entry:
            return [{"type": "invalid_input", "at": at, "kind": "put", "reason": "缺少 entry 身份"}]
        if not _is_int(term) or term < 0:
            return [{"type": "invalid_input", "at": at, "kind": "put", "entry": entry, "reason": "term 非法"}]
        if not _is_int(index) or index < 1:
            return [{"type": "invalid_input", "at": at, "kind": "put", "entry": entry, "reason": "index 应为 >=1 整数"}]
        if not isinstance(key, str):
            return [{"type": "invalid_input", "at": at, "kind": "put", "entry": entry, "reason": "key 应为字符串"}]

        auth = self.state["authority"]
        if auth is None:
            return [{"type": "write_rejected", "at": at, "entry": entry, "index": index,
                     "reason": "no_authority", "note": "还没有当前任期主权威，不接受写"}]
        if term != auth["term"]:
            return [
                {
                    "type": "write_rejected",
                    "at": at,
                    "entry": entry,
                    "index": index,
                    "put_term": term,
                    "current_term": auth["term"],
                    "reason": "not_current_term",
                    "note": "写必须在本任期权威下提出",
                }
            ]

        proposals = self.state["proposals"]
        if entry in proposals:
            old = proposals[entry]
            if (old["index"], old["term"], old["key"]) != (index, term, key) or old.get("value") != value:
                return [{"type": "write_rejected", "at": at, "entry": entry,
                         "reason": "entry_identity_not_reusable", "existing": old}]
            return [{"type": "write_duplicate", "at": at, "entry": entry, "index": index,
                     "status": old["status"]}]

        idx_key = str(index)
        incumbent = self.state["log"].get(idx_key)
        events: list[dict] = []
        if incumbent is not None and incumbent["entry"] != entry:
            if incumbent["status"] == "committed" or index <= self.state["committed_index"]:
                events.append(
                    {
                        "type": "write_rejected",
                        "at": at,
                        "entry": entry,
                        "index": index,
                        "conflict_entry": incumbent["entry"],
                        "reason": "index_already_committed",
                        "note": "已确认写不可改写，不能在该位置混入另一条内容",
                    }
                )
                return events
            # 未确认分叉：截断旧尾部变体。它从未确认，因此不存在成功凭据。
            old_entry = incumbent["entry"]
            old_prop = proposals.get(old_entry)
            if old_prop is not None:
                old_prop["status"] = "truncated"
                old_prop["resolved_at"] = at
            events.append(
                {
                    "type": "unconfirmed_tail_truncated",
                    "at": at,
                    "index": index,
                    "discarded_entry": old_entry,
                    "new_entry": entry,
                    "note": "同一位置的未确认内容被新主尾部替换；旧票不与新写混用",
                }
            )

        proposal = {
            "entry": entry,
            "index": index,
            "term": term,
            "key": key,
            "value": value,
            "status": "proposed",
            "created_at": at,
            "resolved_at": None,
            "receipts": [],
        }
        proposals[entry] = proposal
        self.state["log"][idx_key] = {
            "index": index,
            "entry": entry,
            "term": term,
            "key": key,
            "value": value,
            "status": "proposed",
            "committed_at": None,
            "commit_quorum": [],
            "commit_term": None,
        }
        events.append(
            {
                "type": "write_accepted",
                "at": at,
                "entry": entry,
                "index": index,
                "term": term,
                "key": key,
                "need_distinct_members": self.majority,
                "note": f"等待 {self.majority} 个不同当前成员对确切 {entry}@{index} 的本任期复制回报",
            }
        )
        return events

    def _on_replica(self, record: dict, at: int) -> list[dict]:
        node, term = record.get("node"), record.get("term")
        entry, index = record.get("entry"), record.get("index")
        if not isinstance(node, str) or not node:
            return [{"type": "invalid_input", "at": at, "kind": "replica", "reason": "缺少 node"}]
        if not _is_int(term) or term < 0:
            return [{"type": "invalid_input", "at": at, "kind": "replica", "node": node,
                     "reason": "term 非法"}]
        if not isinstance(entry, str) or not entry:
            return [
                {
                    "type": "replica_ignored",
                    "at": at,
                    "node": node,
                    "term": term,
                    "index": index,
                    "reason": "missing_entry_identity",
                    "note": "健康心跳或只报最大日志位置不能证明持有同一条写",
                }
            ]
        if not _is_int(index) or index < 1:
            return [{"type": "invalid_input", "at": at, "kind": "replica", "node": node,
                     "entry": entry, "reason": "index 非法"}]

        events: list[dict] = []
        if not self._is_current_member(node):
            events.append({"type": "replica_ignored", "at": at, "node": node, "term": term,
                           "entry": entry, "index": index, "reason": "not_current_member"})
            return events

        term_matches = self._observe_report_term(term, node, at, events)
        # _observe_report_term 可能因更高任期推进了权威；以最新权威为准。
        if not term_matches or term != self._authority_term():
            if term < (self._authority_term() or 0):
                reason = "stale_lower_term"
            elif self.state["authority"] and self.state["authority"].get("node") is None:
                reason = "await_leader_notice"
            else:
                reason = "term_not_authoritative"
            events.append(
                {"type": "replica_ignored", "at": at, "node": node, "term": term,
                 "entry": entry, "index": index, "reason": reason,
                 "note": "低任期/非本任期回报不足以作为本任期权威下的复制事实"}
            )
            return events

        proposal = self.state["proposals"].get(entry)
        if proposal is None:
            events.append({"type": "replica_ignored", "at": at, "node": node, "term": term,
                           "entry": entry, "index": index, "reason": "unknown_entry"})
            return events
        if proposal["index"] != index:
            events.append({"type": "replica_ignored", "at": at, "node": node, "term": term,
                           "entry": entry, "index": index, "reason": "position_identity_mismatch",
                           "reported_index": index, "known_index": proposal["index"]})
            return events

        cell = self.state["log"].get(str(index))
        if cell is None or cell["entry"] != entry:
            events.append({"type": "replica_ignored", "at": at, "node": node, "term": term,
                           "entry": entry, "index": index,
                           "reason": "unconfirmed_fork_truncated",
                           "active_entry": cell["entry"] if cell else None,
                           "note": "该位置的活跃身份是另一条（可能已截断）写，票不能混用"})
            return events

        # 记录原始回报（同节点同任期重复回报保留事实但计票按节点去重）。
        proposal["receipts"].append({"node": node, "term": term, "at": at})
        ack_nodes = self._current_ack_nodes(proposal)
        events.append(
            {
                "type": "replica_counted",
                "at": at,
                "node": node,
                "term": term,
                "entry": entry,
                "index": index,
                "distinct_acks": sorted(ack_nodes),
                "count": len(ack_nodes),
                "majority": self.majority,
            }
        )
        events.extend(self._recompute_commit(at))
        return events

    def _current_ack_nodes(self, proposal: dict) -> set[str]:
        term = self._authority_term()
        members = set(self.members)
        return {
            r["node"]
            for r in proposal["receipts"]
            if r["term"] == term and r["node"] in members
        }

    def _recompute_commit(self, at: int) -> list[dict]:
        """从 committed_index+1 起尽量延长已确认前缀。"""
        events: list[dict] = []
        advanced = False
        while True:
            index = self.state["committed_index"] + 1
            cell = self.state["log"].get(str(index))
            if cell is None or cell["status"] == "truncated":
                break
            proposal = self.state["proposals"].get(cell["entry"])
            if proposal is None or proposal["status"] == "truncated":
                break
            ack_nodes = self._current_ack_nodes(proposal)
            if len(ack_nodes) < self.majority:
                break

            quorum = sorted(ack_nodes)
            auth = self.state["authority"] or {}
            cell["status"] = "committed"
            cell["committed_at"] = at
            cell["commit_quorum"] = quorum
            cell["commit_term"] = auth.get("term")
            proposal["status"] = "committed"
            proposal["resolved_at"] = at
            self.state["committed_index"] = index
            credential = {
                "entry": cell["entry"],
                "index": index,
                "entry_term": cell["term"],
                "commit_term": auth.get("term"),
                "key": cell["key"],
                "quorum": quorum,
                "issued_at": at,
            }
            self.state["credentials"].append(credential)
            events.append(
                {
                    "type": "write_committed",
                    "at": at,
                    "entry": cell["entry"],
                    "index": index,
                    "term": cell["term"],
                    "quorum": quorum,
                    "committed_index": index,
                    "credential": credential,
                    "note": "已取得不同当前成员对确切写身份的本任期多数复制，写成功凭据已发出并持久化",
                }
            )
            advanced = True

        if advanced:
            events.extend(self._revalidate_pending_applies(at))
            events.extend(self._advance_all_reads(at))
        return events

    # -- apply -------------------------------------------------------------

    def _on_apply(self, record: dict, at: int) -> list[dict]:
        node, entry, index = record.get("node"), record.get("entry"), record.get("index")
        if not isinstance(node, str) or not self._is_current_member(node):
            return [{"type": "apply_ignored", "at": at, "node": node,
                     "reason": "not_current_member"}]
        if not isinstance(entry, str) or not entry or not _is_int(index) or index < 1:
            return [{"type": "invalid_input", "at": at, "kind": "apply", "node": node,
                     "reason": "apply 需要 entry 与 >=1 的 index"}]

        applied_map = self.state["applied"].setdefault(node, {})
        existing = applied_map.get(str(index))
        if existing is not None:
            if existing["entry"] == entry:
                return [{"type": "apply_duplicate", "at": at, "node": node,
                         "entry": entry, "index": index,
                         "applied_index": self._applied_prefix(node),
                         "note": "重复应用通知，幂等忽略"}]
            return [{"type": "apply_identity_mismatch", "at": at, "node": node,
                     "index": index, "claimed_entry": entry,
                     "verified_entry": existing["entry"],
                     "reason": "position_identity_mismatch",
                     "note": "该位置已按另一条已确认身份核实，拒绝本条应用通知"}]

        events: list[dict] = []
        expected = self._committed_entry_at(index)
        if expected is not None:
            if entry != expected:
                events.append(
                    {"type": "apply_identity_mismatch", "at": at, "node": node,
                     "index": index, "claimed_entry": entry, "committed_entry": expected,
                     "reason": "position_identity_mismatch",
                     "note": "应用回报身份与该位置已确认日志不符，不能推进应用前缀"}
                )
                return events
            self._promote_apply(node, index, entry, at)
            prefix = self._applied_prefix(node)
            events.append({"type": "apply_recorded", "at": at, "node": node,
                           "entry": entry, "index": index, "applied_index": prefix})
            events.extend(self._advance_reads_for(node, at))
            return events

        # 位置尚未确认：先留存声明，提交时按已确认身份再核实。
        duplicate_pending = any(
            p["node"] == node and p["index"] == index and p["entry"] == entry
            for p in self.state["pending_apply"]
        )
        if not duplicate_pending:
            self.state["pending_apply"].append(
                {"node": node, "index": index, "entry": entry, "at": at}
            )
        events.append(
            {"type": "apply_pending_verification", "at": at, "node": node,
             "entry": entry, "index": index,
             "committed_index": self.state["committed_index"],
             "note": "应用通知先于该位置确认/乱序到达，已按位置留存，待身份确认后核实"}
        )
        return events

    def _committed_entry_at(self, index: int) -> str | None:
        if index < 1 or index > self.state["committed_index"]:
            return None
        cell = self.state["log"].get(str(index))
        return cell["entry"] if cell and cell["status"] == "committed" else None

    def _promote_apply(self, node: str, index: int, entry: str, at: int) -> None:
        self.state["applied"].setdefault(node, {})[str(index)] = {"entry": entry, "at": at}

    def _applied_prefix(self, node: str) -> int:
        applied_map = self.state["applied"].get(node, {})
        prefix = 0
        while str(prefix + 1) in applied_map:
            prefix += 1
        return prefix

    def _revalidate_pending_applies(self, at: int) -> list[dict]:
        events: list[dict] = []
        remaining: list[dict] = []
        for claim in self.state["pending_apply"]:
            expected = self._committed_entry_at(claim["index"])
            if expected is None:
                remaining.append(claim)
                continue
            if claim["entry"] == expected:
                applied_map = self.state["applied"].setdefault(claim["node"], {})
                if str(claim["index"]) not in applied_map:
                    self._promote_apply(claim["node"], claim["index"], claim["entry"], claim["at"])
                    events.append(
                        {"type": "apply_verified", "at": at, "node": claim["node"],
                         "entry": claim["entry"], "index": claim["index"],
                         "applied_index": self._applied_prefix(claim["node"]),
                         "note": "早先到达的应用通知现已按已确认身份核实"}
                    )
            else:
                events.append(
                    {"type": "apply_identity_mismatch", "at": at, "node": claim["node"],
                     "index": claim["index"], "claimed_entry": claim["entry"],
                     "committed_entry": expected,
                     "reason": "early_claim_identity_mismatch",
                     "note": "早先留存的应用声明与最终确认身份不符，已丢弃"}
                )
        self.state["pending_apply"] = remaining
        return events

    # -- read 生命周期 -----------------------------------------------------

    def _on_read(self, record: dict, at: int) -> list[dict]:
        qid = record.get("qid")
        key = record.get("key")
        target = record.get("target")
        min_index = record.get("min_index", 0)
        deadline = record.get("deadline")

        if not isinstance(qid, str) or not qid:
            return [{"type": "invalid_input", "at": at, "kind": "read", "reason": "缺少 qid"}]
        if qid in self.state["reads"]:
            return [{"type": "read_duplicate", "at": at, "qid": qid,
                     "status": self.state["reads"][qid]["status"]}]
        if not isinstance(key, str):
            return [{"type": "invalid_input", "at": at, "kind": "read", "qid": qid,
                     "reason": "key 应为字符串"}]
        if not isinstance(target, str) or not self._is_current_member(target):
            return [{"type": "read_rejected", "at": at, "qid": qid, "target": target,
                     "reason": "target_not_current_member"}]
        if not _is_int(min_index) or min_index < 0:
            return [{"type": "invalid_input", "at": at, "kind": "read", "qid": qid,
                     "reason": "min_index 应为非负整数"}]
        if deadline is not None and (not _is_number(deadline) or deadline < 0):
            return [{"type": "invalid_input", "at": at, "kind": "read", "qid": qid,
                     "reason": "deadline 应为非负秒点或 null"}]

        auth = self.state["authority"]
        read = {
            "qid": qid,
            "key": key,
            "target": target,
            "min_index": min_index,
            "deadline": deadline,
            "created_at": at,
            "target_role_at_creation": self._role_of(target),
            "status": "waiting_evidence",
            "generation": 1,
            "authority_at_creation": deepcopy(auth),
            "witnesses": {},
            "rejections": [],
            "guarantee_index": None,
            "guarantee_quorum": [],
            "guarantee_secured_at": None,
            "required_applied_index": None,
            "rounds": [
                {
                    "generation": 1,
                    "leader": auth.get("node") if auth else None,
                    "term": auth.get("term") if auth else None,
                    "started_at": at,
                    "ended_at": None,
                    "end_reason": None,
                }
            ],
            "answer": None,
            "failure": None,
            "updated_at": at,
        }
        self.state["reads"][qid] = read
        self.state["read_order"].append(qid)
        events = [
            {
                "type": "read_opened",
                "at": at,
                "qid": qid,
                "key": key,
                "target": target,
                "target_role": read["target_role_at_creation"],
                "min_index": min_index,
                "deadline": deadline,
                "authority": deepcopy(auth),
                "majority": self.majority,
                "note": "查询之后需要一次绑定本 qid 与当前任期的新鲜多数证据，旧确认不能复用",
            }
        ]
        return events

    def _role_of(self, node: str) -> str:
        auth = self.state["authority"]
        if auth and auth.get("node") == node:
            return "leader"
        if node in set(self.members):
            return "follower"
        return "unknown"

    def _reset_pending_evidence(self, at: int, reason: str, detail: str) -> list[str]:
        reset: list[str] = []
        for qid in self.state["read_order"]:
            read = self.state["reads"][qid]
            if read["status"] in ("answered", "unavailable"):
                continue
            # 关闭当前代际轨迹。
            current = read["rounds"][-1]
            if current.get("ended_at") is None:
                current["ended_at"] = at
                current["end_reason"] = reason
                current["witness_nodes"] = sorted(read["witnesses"].keys())
                current["had_guarantee"] = read["guarantee_index"]
            read["witnesses"] = {}
            read["guarantee_index"] = None
            read["guarantee_quorum"] = []
            read["guarantee_secured_at"] = None
            read["required_applied_index"] = None
            read["generation"] += 1
            auth = self.state["authority"]
            read["rounds"].append(
                {
                    "generation": read["generation"],
                    "leader": auth.get("node") if auth else None,
                    "term": auth.get("term") if auth else None,
                    "started_at": at,
                    "ended_at": None,
                    "end_reason": None,
                }
            )
            read["status"] = "waiting_evidence"
            read["updated_at"] = at
            reset.append(qid)
            read.setdefault("rejections", []).append(
                {"at": at, "reason": reason, "detail": detail}
            )
        return reset

    def _on_witness(self, record: dict, at: int) -> list[dict]:
        qid, node, term = record.get("qid"), record.get("node"), record.get("term")
        commit = record.get("commit")
        if not isinstance(qid, str) or not qid:
            return [{"type": "invalid_input", "at": at, "kind": "witness", "reason": "缺少 qid"}]
        if not isinstance(node, str) or not self._is_current_member(node):
            return [{"type": "evidence_ignored", "at": at, "qid": qid, "node": node,
                     "reason": "not_current_member"}]
        if not _is_int(term) or term < 0:
            return [{"type": "invalid_input", "at": at, "kind": "witness", "qid": qid,
                     "node": node, "reason": "term 非法"}]
        if not _is_int(commit) or commit < 0:
            return [{"type": "invalid_input", "at": at, "kind": "witness", "qid": qid,
                     "node": node, "reason": "commit 应为非负整数位置"}]

        read = self.state["reads"].get(qid)
        if read is None:
            return [
                {"type": "evidence_ignored", "at": at, "qid": qid, "node": node, "term": term,
                 "reason": "unknown_qid",
                 "note": "查询尚未发生（或 qid 不存在），查询前的确认不能充当本次新证明"}
            ]
        if read["status"] in ("answered", "unavailable"):
            return [{"type": "evidence_ignored", "at": at, "qid": qid, "node": node, "term": term,
                     "reason": f"read_{read['status']}",
                     "note": "读已有终局，迟到证据不能改变已给出的答复/无法保证结论"}]

        events: list[dict] = []
        before_term = self._authority_term()
        term_matches = self._observe_report_term(term, node, at, events)
        if not term_matches or term != self._authority_term():
            reason = "stale_lower_term" if before_term is not None and term < before_term else "term_not_authoritative"
            read["rejections"].append({"at": at, "node": node, "term": term,
                                       "commit": commit, "reason": reason})
            events.append(
                {"type": "evidence_ignored", "at": at, "qid": qid, "node": node,
                 "term": term, "commit": commit, "reason": reason,
                 "note": "旧任期/非本任期确认不能证明当前主权威，需在现任期重新取证"}
            )
            return events

        auth = self.state["authority"]
        if auth is None or auth.get("node") is None:
            read["rejections"].append({"at": at, "node": node, "term": term,
                                       "commit": commit, "reason": "await_leader_notice"})
            events.append({"type": "evidence_ignored", "at": at, "qid": qid, "node": node,
                           "term": term, "reason": "await_leader_notice",
                           "note": "观察到新任期但尚不知谁是主，等待 leader 角色通知"})
            return events

        if commit > self.state["committed_index"]:
            read["rejections"].append({"at": at, "node": node, "term": term,
                                       "commit": commit, "reason": "commit_beyond_confirmed"})
            events.append(
                {"type": "evidence_ignored", "at": at, "qid": qid, "node": node,
                 "term": term, "commit": commit,
                 "committed_index": self.state["committed_index"],
                 "reason": "commit_beyond_confirmed",
                 "note": "该 commit 上界超出集群已确认前缀，身份无法匹配核实，不能用于担保"}
            )
            return events

        # 同一节点在同一新鲜轮次内以最后一次合规证据为准（节点集合仍去重）。
        read["witnesses"][node] = {"node": node, "term": term, "commit": commit, "at": at}
        witness_nodes = sorted(read["witnesses"].keys())
        events.append(
            {
                "type": "evidence_recorded",
                "at": at,
                "qid": qid,
                "node": node,
                "term": term,
                "commit": commit,
                "witness_nodes": witness_nodes,
                "count": len(witness_nodes),
                "majority": self.majority,
            }
        )
        events.extend(self._evaluate_evidence(read, at))
        return events

    def _evaluate_evidence(self, read: dict, at: int) -> list[dict]:
        events: list[dict] = []
        witnesses = list(read["witnesses"].values())
        if len({w["node"] for w in witnesses}) < self.majority:
            return events
        guarantee = quorum_watermark((w["commit"] for w in witnesses), self.majority)
        if guarantee is None or guarantee < read["min_index"]:
            read["status"] = "waiting_evidence"
            events.append(
                {
                    "type": "evidence_watermark_insufficient",
                    "at": at,
                    "qid": read["qid"],
                    "witness_nodes": sorted(read["witnesses"].keys()),
                    "watermark": guarantee,
                    "min_index": read["min_index"],
                    "majority": self.majority,
                    "note": "虽有多数节点回报，但多数派可担保位置仍低于 min_index，继续等待更高 commit 证据",
                }
            )
            return events

        first_securing = read["guarantee_index"] is None
        advanced = guarantee > (read["guarantee_index"] or 0)
        read["guarantee_index"] = guarantee
        read["guarantee_quorum"] = sorted(read["witnesses"].keys())
        if first_securing:
            read["guarantee_secured_at"] = at
            read["rounds"][-1]["secured_at"] = at
            read["rounds"][-1]["guarantee_index"] = guarantee
            read["rounds"][-1]["quorum_nodes"] = read["guarantee_quorum"]
            events.append(
                {
                    "type": "evidence_secured",
                    "at": at,
                    "qid": read["qid"],
                    "term": self._authority_term(),
                    "quorum": read["guarantee_quorum"],
                    "guarantee_index": guarantee,
                    "note": "获得绑定本次查询与当前任期的多数确认；该担保只适用于本 qid 本轮",
                }
            )
        elif advanced:
            events.append(
                {
                    "type": "guarantee_advanced",
                    "at": at,
                    "qid": read["qid"],
                    "guarantee_index": guarantee,
                    "quorum": read["guarantee_quorum"],
                }
            )
        events.extend(self._maybe_answer(read, at))
        return events

    def _maybe_answer(self, read: dict, at: int) -> list[dict]:
        if read["status"] in ("answered", "unavailable"):
            return []
        guarantee = read["guarantee_index"]
        if guarantee is None:
            read["status"] = "waiting_evidence"
            return []
        target = read["target"]
        # target 自己在本轮出具的 commit 是它“承诺的前缀”；若高于多数担保
        # 水位，回答它自己的读前必须也应用到该位置。
        own_witness = read["witnesses"].get(target)
        promised_by_target = own_witness["commit"] if own_witness else 0
        required = max(guarantee, promised_by_target)
        read["required_applied_index"] = required
        applied = self._applied_prefix(target)
        if applied < required:
            read["status"] = "waiting_apply"
            read["updated_at"] = at
            return [
                {
                    "type": "read_waiting_apply",
                    "at": at,
                    "qid": read["qid"],
                    "target": target,
                    "applied_index": applied,
                    "guarantee_index": guarantee,
                    "target_promised_index": promised_by_target or None,
                    "required_index": required,
                    "note": "担保已适用于本次请求；目标节点尚未应用完它承诺的前缀，应用延迟只阻塞该 target 上的读",
                }
            ]

        # 目标前缀已落实：在其“已连续应用”的前缀内找该 key 最新一条。
        # applied >= required >= guarantee，读到新于担保水位的已提交记录
        # 只会更新、不会破坏线性一致。
        seen_index = 0
        seen_cell = None
        for index in range(1, applied + 1):
            cell = self.state["log"].get(str(index))
            if cell and cell["status"] == "committed" and cell["key"] == read["key"]:
                if index > seen_index:
                    seen_index, seen_cell = index, cell
        auth = self.state["authority"] or {}
        answer = {
            "answered_at": at,
            "served_leader": auth.get("node"),
            "served_term": auth.get("term"),
            "generation": read["generation"],
            "evidence_quorum": list(read["guarantee_quorum"]),
            "guarantee_index": guarantee,
            "target_applied_index": applied,
            "seen_index": seen_index or None,
            "entry": seen_cell["entry"] if seen_cell else None,
            "entry_term": seen_cell["term"] if seen_cell else None,
            "key": read["key"],
            "value": seen_cell["value"] if seen_cell else None,
            "found": seen_cell is not None,
        }
        read["answer"] = answer
        read["status"] = "answered"
        read["updated_at"] = at
        return [
            {
                "type": "read_answered",
                "at": at,
                "qid": read["qid"],
                "target": target,
                **answer,
                "note": (
                    f"第 {answer['generation']} 代权威 {answer['served_leader']}/"
                    f"任期 {answer['served_term']} 的新鲜多数证据担保位置 {guarantee}，"
                    f"{target} 已应用到 {applied}，读到已提交 {answer['entry']}@{seen_index}"
                ),
            }
        ]

    def _advance_reads_for(self, node: str, at: int) -> list[dict]:
        events: list[dict] = []
        for qid in self.state["read_order"]:
            read = self.state["reads"][qid]
            if read["status"] == "waiting_apply" and read["target"] == node:
                events.extend(self._maybe_answer(read, at))
        return events

    def _advance_all_reads(self, at: int) -> list[dict]:
        events: list[dict] = []
        for qid in self.state["read_order"]:
            read = self.state["reads"][qid]
            if read["status"] in ("waiting_apply", "waiting_evidence"):
                events.extend(self._maybe_answer(read, at))
        return events

    # -- 截止 / tick -------------------------------------------------------

    def _expire_overdue(self, *, strict: bool, now: float) -> list[dict]:
        """非 tick 消息带来的时间推进：严格超过 deadline 才惰性宣判。"""
        events: list[dict] = []
        for qid in self.state["read_order"]:
            read = self.state["reads"][qid]
            deadline = read["deadline"]
            if deadline is None or read["status"] in ("answered", "unavailable"):
                continue
            if strict and now > deadline:
                events.append(self._expire_read(read, now, "deadline_exceeded_late_message"))
        return events

    def _expire_read(self, read: dict, at: float, reason: str) -> dict:
        phase = read["status"]
        failure = {
            "reason": "cannot_guarantee_by_deadline",
            "detail_reason": reason,
            "at": at,
            "last_phase": phase,
            "generation": read["generation"],
            "guarantee_index": read["guarantee_index"],
            "target_applied_index": self._applied_prefix(read["target"]),
            "required_index": read["guarantee_index"],
        }
        read["status"] = "unavailable"
        read["failure"] = failure
        read["updated_at"] = at
        current = read["rounds"][-1]
        if current.get("ended_at") is None:
            current["ended_at"] = at
            current["end_reason"] = "deadline"
            current["witness_nodes"] = sorted(read["witnesses"].keys())
        return {
            "type": "read_unavailable",
            "at": at,
            "qid": read["qid"],
            "deadline": read["deadline"],
            "last_phase": phase,
            "generation": read["generation"],
            "reason": "cannot_guarantee_by_deadline",
            "detail": failure,
            "note": "截止时仍未取得当前权威的新鲜多数证据/应用前缀，向客户报无法保证",
        }

    def _on_tick(self, record: dict, at: int) -> list[dict]:
        now = record.get("now", at)
        if not _is_number(now):
            now = at
        if now > self.state["clock"]:
            self.state["clock"] = now
        tick_event = {"type": "tick", "at": at, "now": now, "waiting_reads": []}
        events: list[dict] = [tick_event]
        for qid in self.state["read_order"]:
            read = self.state["reads"][qid]
            deadline = read["deadline"]
            if deadline is None or read["status"] in ("answered", "unavailable"):
                continue
            # tick 是值班台在截止点的正式宣判：now >= deadline 即截止。
            if now >= deadline:
                events.append(self._expire_read(read, now, "tick_at_deadline"))
        tick_event["waiting_reads"] = [
            {"qid": qid, "status": self.state["reads"][qid]["status"]}
            for qid in self.state["read_order"]
            if self.state["reads"][qid]["status"] not in ("answered", "unavailable")
        ]
        return events

    # -- restart ----------------------------------------------------------

    def _restart(self, at: int) -> list[dict]:
        # 先把当前状态落盘作为检查点，再从 Store 重新加载，模拟新进程。
        self.store.save_state(self.state)
        reloaded = normalize_state(self.store.load_state())
        self.state = reloaded
        board = self.board()
        return [
            {
                "type": "service_restarted",
                "at": at,
                "recovered_from": type(self.store).__name__,
                "committed_index": board["committed_index"],
                "credentials": len(board["credentials"]),
                "node_applied": {n: board["nodes"][n]["applied_index"] for n in board["nodes"]},
                "nodes_lagging": board["lagging_nodes"],
                "reads_waiting_evidence": board["reads_waiting_evidence"],
                "reads_waiting_apply": board["reads_waiting_apply"],
                "note": "重启恢复：已确认位置、写凭据、各节点应用进度与未决读均已核对",
            }
        ]

    # -- inspect / board --------------------------------------------------

    def snapshot(self) -> dict:
        """返回完整、JSON 可序列化的持久化状态深拷贝。"""
        return deepcopy(self.state)

    def board(self) -> dict:
        nodes_view: dict[str, dict] = {}
        ci = self.state["committed_index"]
        for node in self.members:
            applied_index = self._applied_prefix(node)
            applied_indices = sorted(int(k) for k in self.state["applied"].get(node, {}))
            nodes_view[node] = {
                "role": self._role_of(node),
                "applied_index": applied_index,
                "applied_indices": applied_indices,
                "lag": max(ci - applied_index, 0),
                "pending_claims": [p for p in self.state["pending_apply"] if p["node"] == node],
            }
        read_summaries = [self._read_summary(qid) for qid in self.state["read_order"]]
        return {
            "kind": "board",
            "clock": self.state["clock"],
            "members": list(self.members),
            "majority": self.majority,
            "authority": deepcopy(self.state["authority"]),
            "committed_index": ci,
            "credentials": deepcopy(self.state["credentials"]),
            "nodes": nodes_view,
            "lagging_nodes": [n for n, view in nodes_view.items() if view["lag"] > 0],
            "reads": read_summaries,
            "reads_waiting_evidence": [
                q for q in self.state["read_order"]
                if self.state["reads"][q]["status"] == "waiting_evidence"
            ],
            "reads_waiting_apply": [
                q for q in self.state["read_order"]
                if self.state["reads"][q]["status"] == "waiting_apply"
            ],
            "reads_answered": [
                q for q in self.state["read_order"]
                if self.state["reads"][q]["status"] == "answered"
            ],
            "reads_unavailable": [
                q for q in self.state["read_order"]
                if self.state["reads"][q]["status"] == "unavailable"
            ],
        }

    def _read_summary(self, qid: str) -> dict:
        read = self.state["reads"][qid]
        return {
            "qid": qid,
            "key": read["key"],
            "target": read["target"],
            "status": read["status"],
            "generation": read["generation"],
            "min_index": read["min_index"],
            "deadline": read["deadline"],
            "guarantee_index": read["guarantee_index"],
            "guarantee_quorum": list(read["guarantee_quorum"]),
            "required_applied_index": read.get("required_applied_index"),
            "target_applied_index": self._applied_prefix(read["target"]),
            "answer": deepcopy(read["answer"]),
            "failure": deepcopy(read["failure"]),
        }

    def inspect_read(self, qid: str) -> dict:
        read = self.state["reads"].get(qid)
        if read is None:
            return {"kind": "read", "qid": qid, "found": False,
                    "known_qids": list(self.state["read_order"])}
        report = self._read_summary(qid)
        report.update(
            {
                "kind": "read",
                "found": True,
                "target_role_now": self._role_of(read["target"]),
                "authority_now": deepcopy(self.state["authority"]),
                "active_witnesses": deepcopy(read["witnesses"]),
                "rejections": deepcopy(read["rejections"]),
                "generations": deepcopy(read["rounds"]),
                "created_at": read["created_at"],
            }
        )
        return report

    def inspect_entry(self, entry: str) -> dict:
        proposal = self.state["proposals"].get(entry)
        if proposal is None:
            return {"kind": "write", "entry": entry, "found": False}
        index = proposal["index"]
        cell = self.state["log"].get(str(index), {})
        ack_nodes = sorted(self._current_ack_nodes(proposal))
        return {
            "kind": "write",
            "found": True,
            "entry": entry,
            "index": index,
            "term": proposal["term"],
            "key": proposal["key"],
            "value": proposal["value"],
            "status": proposal["status"],
            "active_identity_at_index": cell.get("entry"),
            "is_active_identity": cell.get("entry") == entry,
            "committed_index": self.state["committed_index"],
            "current_term_acks": ack_nodes,
            "current_term_ack_count": len(ack_nodes),
            "majority": self.majority,
            "all_receipts": deepcopy(proposal["receipts"]),
            "credential": self._credential_for_index(index)
            if proposal["status"] == "committed"
            else None,
            "created_at": proposal["created_at"],
            "resolved_at": proposal["resolved_at"],
        }

    def inspect_node(self, node: str) -> dict:
        if not self._is_current_member(node):
            return {"kind": "node", "node": node, "found": False, "members": list(self.members)}
        return {
            "kind": "node",
            "found": True,
            "node": node,
            "role": self._role_of(node),
            "committed_index": self.state["committed_index"],
            "applied_index": self._applied_prefix(node),
            "applied_indices": sorted(int(k) for k in self.state["applied"].get(node, {})),
            "applied": deepcopy(self.state["applied"].get(node, {})),
            "pending_claims": [p for p in self.state["pending_apply"] if p["node"] == node],
        }

    def inspect_index(self, index: int) -> dict:
        cell = self.state["log"].get(str(index))
        if cell is None:
            return {"kind": "index", "index": index, "found": False,
                    "committed_index": self.state["committed_index"]}
        return {"kind": "index", "found": True, **deepcopy(cell)}

    def inspect(self, selector: dict | None = None) -> dict:
        selector = selector or {}
        if selector.get("qid") is not None:
            return self.inspect_read(selector["qid"])
        if selector.get("entry") is not None:
            return self.inspect_entry(selector["entry"])
        if selector.get("node") is not None:
            return self.inspect_node(selector["node"])
        if selector.get("index") is not None and _is_int(selector["index"]):
            return self.inspect_index(selector["index"])
        return self.board()

    def _on_inspect(self, record: dict, at: int) -> list[dict]:
        return [{"type": "inspection", "at": at, "report": self.inspect(record)}]


# ---------------------------------------------------------------------------
# 重演辅助
# ---------------------------------------------------------------------------


def replay(
    messages: Iterable[dict],
    store: JsonMemoryStore | JsonFileStore | None = None,
) -> tuple[Coordinator, list[dict]]:
    """按到达顺序处理全部信封，返回引擎与扁平事件列表。"""
    coord = Coordinator(store=store)
    events: list[dict] = []
    for envelope in messages:
        events.extend(coord.handle(envelope))
    return coord, events


if __name__ == "__main__":
    # 允许 `python3 coordinator.py ...`，参数与 duty_console.py 相同。
    from duty_console import main

    raise SystemExit(main())
