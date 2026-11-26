#!/usr/bin/env python3
"""值班台 / 重演入口。

用法：

    # 重演一个或多个 JSONL 场景（目录则重演其中全部 *.jsonl）
    python3 duty_console.py examples/term-change-before-follower-apply.jsonl
    python3 duty_console.py replay examples --json

    # 连续服务：从 stdin 逐行吃 {"at","kind","record"}，逐行输出事件
    tail -f feed.jsonl | python3 duty_console.py stream --json
    python3 duty_console.py stream --state .duty-state.json

    # 本地 HTTP 协调服务（仅标准库）
    python3 duty_console.py serve --host 127.0.0.1 --port 8765

不加参数且 stdin 是管道时，等价于 stream。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable

from coordinator import Coordinator, JsonFileStore, JsonMemoryStore

ROOT = Path(__file__).resolve().parent
DEFAULT_REGISTRY = ROOT / "examples" / "message_types.json"

REASON_ZH = {
    "not_current_member": "非当前固定成员",
    "stale_lower_term": "低任期回报",
    "term_not_authoritative": "任期不是当前权威任期",
    "no_authority": "尚无当前权威",
    "await_leader_notice": "已观察到更高任期，等待 leader 角色通知指名主节点",
    "missing_entry_identity": "缺少确切 entry 身份（心跳/最大位置不足以证明同一条写）",
    "unknown_entry": "系统没有这条写的提案",
    "position_identity_mismatch": "位置与 entry 身份不匹配",
    "unconfirmed_fork_truncated": "该位置是另一条活跃/已截断分叉，票不能混用",
    "index_already_committed": "该位置已确认，不能用另一条写覆盖",
    "not_current_term": "写任期不等于当前权威任期",
    "entry_identity_not_reusable": "entry 身份不可复用",
    "unknown_qid": "qid 不存在（查询尚未发生）",
    "read_answered": "读已回答，终局不可更改",
    "read_unavailable": "读已判无法保证，终局不可更改",
    "commit_beyond_confirmed": "commit 超出集群已确认前缀，身份无法匹配",
    "target_not_current_member": "target 不是当前成员",
    "authority_changed": "权威已更换世代",
}


def zh_reason(reason: str | None) -> str:
    if not reason:
        return ""
    return REASON_ZH.get(reason, reason)


# ---------------------------------------------------------------------------
# 人类可读渲染
# ---------------------------------------------------------------------------


def render_event(event: dict) -> str:
    etype = event.get("type", "?")
    at = event.get("at")
    prefix = f"[t={at:>2}] " if at is not None else ""
    renderer = RENDERERS.get(etype)
    if renderer is None:
        return prefix + json.dumps(event, ensure_ascii=False)
    return prefix + renderer(event)


def _r_members(e: dict) -> str:
    return (
        f"固定成员 {e['member_count']} 名：{', '.join(e['nodes'])}；"
        f"多数派需 {e['majority']} 个不同成员"
    )


def _r_authority_changed(e: dict) -> str:
    reset = f"；未回答读 {e['reset_reads']} 的本次证据全部作废，需重新取多数" if e.get("reset_reads") else ""
    leader = e.get("leader") or "（主节点待 leader 通知补全）"
    return f"权威换代 → 主={leader}，任期 term={e['term']}（依据 {e.get('via')}）；已确认前缀 {e['committed_index']} 由新主承接{reset}"


def _r_authority_unchanged(e: dict) -> str:
    return f"权威通知幂等重复：{e.get('leader')}/term{e.get('term')}，已有证据保留"


def _r_authority_stale(e: dict) -> str:
    return f"忽略低任期角色通知 {e.get('noticed_node')}/term{e.get('noticed_term')}（当前 {e['current'].get('node')}/term{e['current'].get('term')}）"


def _r_authority_named(e: dict) -> str:
    return f"新任期 term={e['term']} 的主节点由角色通知补全：{e['leader']}"


def _r_write_accepted(e: dict) -> str:
    return f"收到写 {e['entry']}@index{e['index']}（term{e['term']}，{e['key']}=…），{e['note']}"


def _r_write_duplicate(e: dict) -> str:
    return f"重复 put {e['entry']}@{e['index']}，幂等（状态 {e['status']}）"


def _r_write_rejected(e: dict) -> str:
    return f"✗ 拒绝写 {e.get('entry')}@{e.get('index')}：{zh_reason(e.get('reason'))}。{e.get('note','')}"


def _r_truncated(e: dict) -> str:
    return (
        f"⌇ 截断 index={e['index']} 的未确认分叉：废弃 {e['discarded_entry']}，"
        f"改由 {e['new_entry']} 承接；已确认写与成功凭据不受影响，旧票不混用"
    )


def _r_replica_counted(e: dict) -> str:
    return (
        f"复制回执 ✓ {e['node']}/term{e['term']} 确切持有 {e['entry']}@{e['index']}；"
        f"不同当前成员 {e['count']}/{e['majority']}：{e['distinct_acks']}"
    )


def _r_replica_ignored(e: dict) -> str:
    return (
        f"复制回执 ✗ 不计票 {e.get('node')}/term{e.get('term')} {e.get('entry')}@{e.get('index')}："
        f"{zh_reason(e.get('reason'))}。{e.get('note','')}"
    )


def _r_committed(e: dict) -> str:
    c = e["credential"]
    return (
        f"✔ 已确认并发出写凭据：{e['entry']}@index{e['index']}（写任期 term{e['term']}，"
        f"提交世代 term{c['commit_term']}），复制多数 {e['quorum']}；"
        f"committed_index={e['committed_index']}，凭据已持久化、重启不丢"
    )


def _r_apply_recorded(e: dict) -> str:
    return f"应用记录 ✓ {e['node']} 已落地 {e['entry']}@{e['index']}；连续应用前缀 applied_index={e['applied_index']}"


def _r_apply_duplicate(e: dict) -> str:
    return f"应用记录重复，幂等：{e['node']} {e['entry']}@{e['index']}；applied_index={e['applied_index']}"


def _r_apply_mismatch(e: dict) -> str:
    return f"应用记录 ✗ 身份不符 {e['node']} index={e['index']} 报 {e.get('claimed_entry')}，已确认/已核实为 {e.get('committed_entry', e.get('verified_entry'))}；不推进应用前缀"


def _r_apply_pending(e: dict) -> str:
    return f"应用记录暂存待核实：{e['node']} {e['entry']}@{e['index']}（当前 committed_index={e['committed_index']}）；乱序/提前到达，提交后按位置身份核实"


def _r_apply_verified(e: dict) -> str:
    return f"暂存应用记录现已核实 ✓ {e['node']} {e['entry']}@{e['index']}；applied_index={e['applied_index']}"


def _r_apply_ignored(e: dict) -> str:
    return f"应用记录忽略：{e.get('node')}：{zh_reason(e.get('reason'))}"


def _r_read_opened(e: dict) -> str:
    auth = e.get("authority") or {}
    leader = auth.get("node") or "主节点待通知"
    term = auth.get("term", "?")
    return (
        f"读 {e['qid']} 发起：key={e['key']} target={e['target']}"
        f"（{e.get('target_role')}）min_index={e['min_index']} deadline={e['deadline']}；"
        f"当前权威 {leader}/term{term}，等待本次 qid 的新鲜多数证据"
    )


def _r_read_duplicate(e: dict) -> str:
    return f"读 qid={e['qid']} 已存在，幂等返回状态 {e['status']}"


def _r_read_rejected(e: dict) -> str:
    return f"✗ 拒绝读 {e.get('qid')}：{zh_reason(e.get('reason'))}"


def _r_evidence_recorded(e: dict) -> str:
    return (
        f"证据 ✓ witness {e['node']}/term{e['term']} 为 qid={e['qid']} 本次背书，"
        f"commit={e['commit']}；有效节点 {e['count']}/{e['majority']} {e['witness_nodes']}"
    )


def _r_evidence_ignored(e: dict) -> str:
    return f"证据 ✗ 不计入 qid={e.get('qid')} {e.get('node')}/term{e.get('term')}：{zh_reason(e.get('reason'))}。{e.get('note','')}"


def _r_watermark_insufficient(e: dict) -> str:
    return (
        f"qid={e['qid']} 已有多数节点 {e['witness_nodes']}，但多数担保水位 "
        f"{e['watermark']} < min_index={e['min_index']}，继续等待能担保更高前缀的本次证据"
    )


def _r_evidence_secured(e: dict) -> str:
    return (
        f"● qid={e['qid']} 在 term{e['term']} 取得新鲜多数确认 {e['quorum']}："
        f"本次查询的请求专属担保位置 guarantee_index={e['guarantee_index']}；"
        f"旧确认不能再给后续读复用"
    )


def _r_guarantee_advanced(e: dict) -> str:
    return f"qid={e['qid']} 担保水位上移至 {e['guarantee_index']}（多数 {e['quorum']}）"


def _r_read_waiting_apply(e: dict) -> str:
    return (
        f"… qid={e['qid']} 担保已适用，但 target={e['target']} 仅应用到 "
        f"{e['applied_index']}，需应用完前缀 {e['required_index']} 才回答；"
        f"其他 target 的读不受此节点延迟影响"
    )


def _r_read_answered(e: dict) -> str:
    return (
        f"✅ 读 {e['qid']} 回答：第 {e['generation']} 代权威 "
        f"{e['served_leader']}/term{e['served_term']} 担保 G={e['guarantee_index']}"
        f"（多数 {e['evidence_quorum']}），{e['target']} 已应用到 {e['target_applied_index']}；"
        f"看到已提交记录 {e['entry']}@index{e['seen_index']}（entry_term={e['entry_term']}）"
        f" key={e['key']} value={e['value']!r}"
    )


def _r_read_unavailable(e: dict) -> str:
    d = e["detail"]
    return (
        f"⛔ 读 {e['qid']} 无法保证：deadline={e['deadline']} 已到，"
        f"终止于 {e['last_phase']}（第 {e['generation']} 代，"
        f"担保位置={d['guarantee_index']}，target 应用={d['target_applied_index']}）；"
        f"可在截止前重新取证，或向客户报 cannot_guarantee"
    )


def _r_tick(e: dict) -> str:
    waiting = [w["qid"] for w in e.get("waiting_reads", [])]
    return f"tick now={e['now']}；未终局读：{waiting or '无'}"


def _r_unsupported(e: dict) -> str:
    return f"未登记消息 kind={e['kind']}，忽略不影响状态：{e.get('note','')}"


def _r_invalid(e: dict) -> str:
    return f"⚠ 输入不合法：{e.get('reason')}（原始：{json.dumps(e.get('raw', e.get('record')), ensure_ascii=False)}）"


def _r_restarted(e: dict) -> str:
    return (
        f"↻ 服务重启并从 {e['recovered_from']} 恢复：committed_index={e['committed_index']}，"
        f"写凭据 {e['credentials']} 份；各节点应用位置 {e['node_applied']}；"
        f"落后节点 {e['nodes_lagging']}；等多数证据的读 {e['reads_waiting_evidence']}；"
        f"等应用的读 {e['reads_waiting_apply']}"
    )


def _r_inspection(e: dict) -> str:
    return render_report(e["report"])


RENDERERS = {
    "members_established": _r_members,
    "authority_changed": _r_authority_changed,
    "authority_unchanged": _r_authority_unchanged,
    "authority_stale_notice": _r_authority_stale,
    "authority_leader_named": _r_authority_named,
    "write_accepted": _r_write_accepted,
    "write_duplicate": _r_write_duplicate,
    "write_rejected": _r_write_rejected,
    "unconfirmed_tail_truncated": _r_truncated,
    "replica_counted": _r_replica_counted,
    "replica_ignored": _r_replica_ignored,
    "write_committed": _r_committed,
    "apply_recorded": _r_apply_recorded,
    "apply_duplicate": _r_apply_duplicate,
    "apply_identity_mismatch": _r_apply_mismatch,
    "apply_pending_verification": _r_apply_pending,
    "apply_verified": _r_apply_verified,
    "apply_ignored": _r_apply_ignored,
    "read_opened": _r_read_opened,
    "read_duplicate": _r_read_duplicate,
    "read_rejected": _r_read_rejected,
    "evidence_recorded": _r_evidence_recorded,
    "evidence_ignored": _r_evidence_ignored,
    "evidence_watermark_insufficient": _r_watermark_insufficient,
    "evidence_secured": _r_evidence_secured,
    "guarantee_advanced": _r_guarantee_advanced,
    "read_waiting_apply": _r_read_waiting_apply,
    "read_answered": _r_read_answered,
    "read_unavailable": _r_read_unavailable,
    "tick": _r_tick,
    "unsupported_message": _r_unsupported,
    "invalid_input": _r_invalid,
    "service_restarted": _r_restarted,
    "inspection": _r_inspection,
}


# ---------------------------------------------------------------------------
# inspect 报告渲染
# ---------------------------------------------------------------------------


def render_report(report: dict) -> str:
    kind = report.get("kind")
    if kind == "board":
        return _render_board(report)
    if kind == "read":
        return _render_read_report(report)
    if kind == "write":
        return _render_write_report(report)
    if kind == "node":
        return _render_node_report(report)
    if kind == "index":
        return _render_index_report(report)
    return json.dumps(report, ensure_ascii=False, indent=2)


def _render_board(r: dict) -> str:
    lines = ["┌─ 值班总览 ─────────────────────────────"]
    auth = r.get("authority") or {}
    lines.append(
        f"│ 时钟 t={r['clock']}　成员={', '.join(r['members'])}　多数派={r['majority']}"
    )
    lines.append(f"│ 当前权威={auth.get('node')}/term{auth.get('term')}　已确认前缀 committed_index={r['committed_index']}")
    if r["credentials"]:
        lines.append("│ 写凭据（重启不丢）：")
        for c in r["credentials"]:
            lines.append(
                f"│   • {c['entry']}@{c['index']} term{c['entry_term']} "
                f"{c['key']}　quorum={c['quorum']}　t={c['issued_at']}"
            )
    else:
        lines.append("│ 写凭据：无")
    lines.append("│ 节点应用进度：")
    for node, view in r["nodes"].items():
        lag = f" 落后 {view['lag']}" if view["lag"] else " 已追平"
        lines.append(
            f"│   • {node} [{view['role']}] applied_index={view['applied_index']}{lag}"
        )
    lines.append(
        f"│ 读：等证据 {r['reads_waiting_evidence']}　等应用 {r['reads_waiting_apply']}　"
        f"已回答 {r['reads_answered']}　无法保证 {r['reads_unavailable']}"
    )
    for s in r["reads"]:
        ans = s.get("answer")
        tail = (
            f"→ {ans['entry']}@{ans['seen_index']} value={ans['value']!r} "
            f"({ans['served_leader']}/t{ans['served_term']}, 第{ans['generation']}代)"
            if ans
            else ("→ 无法保证" if s.get("failure") else f"→ {s['status']}")
        )
        lines.append(f"│   · qid={s['qid']} target={s['target']} {tail}")
    lines.append("└────────────────────────────────────────")
    return "\n".join(lines)


def _render_read_report(r: dict) -> str:
    if not r.get("found"):
        return f"🔎 inspect qid={r['qid']}：不存在；已知 qid={r.get('known_qids')}"
    lines = [f"🔎 读 {r['qid']} 值班结论 ────────────────────"]
    lines.append(
        f"  key={r['key']} target={r['target']}（当前角色 {r.get('target_role_now')}）"
        f" min_index={r['min_index']} deadline={r['deadline']}"
    )
    for g in r.get("generations", []):
        term = g.get("term")
        head = f"  第{g['generation']}代 权威 {g.get('leader')}/term{term} t={g['started_at']}"
        if g.get("ended_at") is not None:
            head += f" → t={g['ended_at']} 终止原因={g.get('end_reason')}"
        lines.append(head)
        if g.get("quorum_nodes"):
            lines.append(
                f"      多数证据 {g['quorum_nodes']}，担保 G={g.get('guarantee_index')}，"
                f"取证于 t={g.get('secured_at')}"
            )
        if g.get("witness_nodes") is not None and g.get("end_reason"):
            lines.append(f"      作废时本轮有效证人={g.get('witness_nodes')}（不得继续复用）")
    active = r.get("active_witnesses") or {}
    if active:
        lines.append(f"  当前轮有效证人：{sorted(active)}")
    if r.get("rejections"):
        lines.append("  被排除的证据/事件：")
        for item in r["rejections"][-8:]:
            if item.get("node") is None:
                lines.append(f"    - t={item.get('at')} {item.get('reason')}：{item.get('detail', '')}")
            else:
                lines.append(
                    f"    - t={item.get('at')} {item.get('node')}/term{item.get('term')} "
                    f"commit={item.get('commit')} {item.get('reason')}"
                )
    lines.append(
        f"  target 应用位置={r.get('target_applied_index')}　担保位置={r.get('guarantee_index')}"
    )
    ans = r.get("answer")
    if ans:
        lines.append(
            f"  ✅ 已回答：第{ans['generation']}代 {ans['served_leader']}/term{ans['served_term']}"
            f" 多数={ans['evidence_quorum']}；担保 G={ans['guarantee_index']}，"
            f"target_applied={ans['target_applied_index']}；"
            f"读到 {ans['entry']}@{ans['seen_index']}（entry_term={ans['entry_term']}）"
            f" value={ans['value']!r}"
        )
    elif r.get("failure"):
        f = r["failure"]
        lines.append(f"  ⛔ 无法保证：{f['detail_reason']}，终止阶段 {f['last_phase']} @t={f['at']}")
    else:
        lines.append(f"  … 当前状态：{r['status']}")
    lines.append("────────────────────────────────────────")
    return "\n".join(lines)


def _render_write_report(r: dict) -> str:
    if not r.get("found"):
        return f"🔎 inspect entry={r['entry']}：系统中没有该写提案"
    lines = [f"🔎 写 {r['entry']} 值班结论 ────────────────────"]
    lines.append(
        f"  index={r['index']} term={r['term']} key={r['key']} value={r['value']!r}"
    )
    lines.append(
        f"  状态={r['status']}；该 index 当前活跃身份={r.get('active_identity_at_index')}"
        f"（本 entry 是否活跃身份：{r['is_active_identity']}）"
    )
    lines.append(
        f"  当前任期有效确切复制票 {r['current_term_ack_count']}/{r['majority']}："
        f"{r['current_term_acks']}；committed_index={r['committed_index']}"
    )
    if r.get("credential"):
        c = r["credential"]
        lines.append(f"  ✔ 成功凭据：quorum={c['quorum']} issued_at={c['issued_at']}（持久化，重启不丢）")
    else:
        lines.append("  尚无成功凭据（未确认写在截断时不会留下凭据）")
    lines.append(f"  全部回报：{r['all_receipts']}")
    return "\n".join(lines)


def _render_node_report(r: dict) -> str:
    if not r.get("found"):
        return f"🔎 inspect node={r['node']}：不是固定成员；members={r.get('members')}"
    return (
        f"🔎 节点 {r['node']}（{r['role']}）：committed_index={r['committed_index']}，"
        f"真正连续应用到 applied_index={r['applied_index']}；"
        f"已核实应用={sorted((r['applied'] or {}).keys(), key=int)}；"
        f"待确认申请 {len(r['pending_claims'])} 条"
    )


def _render_index_report(r: dict) -> str:
    if not r.get("found"):
        return f"🔎 index={r['index']} 尚不存在（committed_index={r.get('committed_index')}）"
    return (
        f"🔎 index={r['index']} 活跃身份={r['entry']} term={r['term']} "
        f"status={r['status']} key={r['key']} value={r['value']!r} "
        f"commit_quorum={r.get('commit_quorum')}"
    )


# ---------------------------------------------------------------------------
# 输入加载
# ---------------------------------------------------------------------------


def load_supported_kinds() -> set[str]:
    try:
        return set(json.loads(DEFAULT_REGISTRY.read_text(encoding="utf-8")))
    except OSError:
        return {
            "apply", "inspect", "leader", "members", "put", "read",
            "replica", "restart", "tick", "witness",
        }


def load_envelopes(path: Path) -> list[dict]:
    """优先复用 scenario_reader 的严格封装校验。"""
    try:
        from scenario_reader import iter_messages

        return list(iter_messages(path, load_supported_kinds()))
    except ImportError:
        rows = []
        for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if raw.strip():
                rows.append(json.loads(raw))
        return rows


def gather_jsonl(paths: list[str]) -> list[Path]:
    files: list[Path] = []
    for raw in paths:
        path = Path(raw)
        if path.is_dir():
            files.extend(sorted(path.glob("*.jsonl")))
        elif path.exists():
            files.append(path)
        else:
            raise FileNotFoundError(raw)
    return files


def make_store(state_path: str | None) -> JsonMemoryStore | JsonFileStore:
    return JsonFileStore(state_path) if state_path else JsonMemoryStore()


# ---------------------------------------------------------------------------
# 命令实现
# ---------------------------------------------------------------------------


def run_replay(paths: list[str], *, as_json: bool, state_path: str | None, final_board: bool) -> int:
    files = gather_jsonl(paths)
    if not files:
        print("没有可重演的 .jsonl 文件", file=sys.stderr)
        return 2
    multi = len(files) > 1
    rc = 0
    for index, path in enumerate(files):
        # 多场景时 --state 若给了路径，按文件名派生独立状态文件。
        if state_path and multi:
            p = Path(state_path)
            derived = str(p.with_name(f"{p.stem}.{path.stem}{p.suffix}"))
            store = JsonFileStore(derived)
        else:
            store = make_store(state_path)
        coord = Coordinator(store=store)
        if as_json:
            print(json.dumps({"type": "scenario_start", "path": str(path)}, ensure_ascii=False))
        else:
            print(f"\n══ 重演场景 {path} ══")
        try:
            envelopes = load_envelopes(path)
        except (ValueError, json.JSONDecodeError) as error:
            print(f"输入有误：{error}", file=sys.stderr)
            rc = 2
            continue
        for envelope in envelopes:
            for event in coord.handle(envelope):
                emit_event(event, as_json=as_json)
        if final_board:
            board_event = {"type": "final_board", "report": coord.board()}
            if as_json:
                print(json.dumps(board_event, ensure_ascii=False))
            else:
                print(render_event({"type": "inspection", "report": coord.board()}))
        if as_json:
            print(json.dumps({"type": "scenario_end", "path": str(path)}, ensure_ascii=False))
    return rc


def emit_event(event: dict, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(event, ensure_ascii=False), flush=True)
    else:
        print(render_event(event))


def run_stream(*, as_json: bool, state_path: str | None) -> int:
    coord = Coordinator(store=make_store(state_path))
    if sys.stdin is None:
        print("stream 需要 stdin 输入", file=sys.stderr)
        return 2
    for line_no, raw in enumerate(sys.stdin, 1):
        text = raw.strip()
        if not text:
            continue
        try:
            envelope = json.loads(text)
        except json.JSONDecodeError as error:
            err = {"type": "invalid_input", "line": line_no, "reason": f"JSON 解析失败：{error}"}
            emit_event(err, as_json=as_json)
            continue
        for event in coord.handle(envelope):
            event.setdefault("line", line_no)
            emit_event(event, as_json=as_json)
    return 0


def run_serve(host: str, port: int, state_path: str | None) -> int:
    from duty_server import serve_http

    return serve_http(host=host, port=port, state_path=state_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="可重演的主从协调读服务 / 值班台")
    sub = parser.add_subparsers(dest="command")

    p_replay = sub.add_parser("replay", help="重演 JSONL 场景文件或目录")
    p_replay.add_argument("paths", nargs="+")
    p_replay.add_argument("--json", action="store_true", help="输出 JSONL 事件而不是中文解读")
    p_replay.add_argument("--state", help="状态 JSON 落盘路径（默认内存 JSON 仓）")
    p_replay.add_argument("--no-board", action="store_true", help="结束时不打印值班总览")

    p_stream = sub.add_parser("stream", help="从 stdin 连续读取消息并输出事件")
    p_stream.add_argument("--json", action="store_true")
    p_stream.add_argument("--state")

    p_serve = sub.add_parser("serve", help="启动本地 HTTP 协调服务")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8765)
    p_serve.add_argument("--state", default=".duty-state.json")
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    # 便利形式：第一个参数是存在的路径时按 replay 处理。
    if argv and argv[0] not in {"replay", "stream", "serve", "-h", "--help"} and not argv[0].startswith("-"):
        argv = ["replay", *argv]
    elif not sys.stdin.isatty() and (not argv or argv == ["--json"]):
        # 管道直连且未给子命令：等价 stream（可带 --json）。
        return run_stream(as_json=bool(argv), state_path=None)

    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "replay":
        return run_replay(
            args.paths, as_json=args.json, state_path=args.state, final_board=not args.no_board
        )
    if args.command == "stream":
        return run_stream(as_json=args.json, state_path=args.state)
    if args.command == "serve":
        return run_serve(args.host, args.port, args.state)
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
