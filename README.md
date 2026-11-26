# 主从协调服务的线性一致读与候选领导交接

成员组成固定。term 是仿真选主任期。entry 代表日志写入不可复用的身份。index 是从 1 递增的位置。相同 index 可在未确认的尾部出现别的 entry。

本仓库在输入契约与演练资料之外，提供一套**只依赖 Python 3.11 标准库、可重演、可重启恢复**的本地协调读服务：连续送入写、复制回报、角色变化、读请求、witness 证据与应用记录，引擎输出确定性事件，值班台据此解释“读走了哪一代（term）、由哪次新鲜多数确认担保、target 真正应用到哪里、最终看到哪条已提交记录”。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **members**　nodes 是固定成员名单。
- **leader**　node/term 表示新的角色通知。
- **put**　entry/term/index/key/value 发起写。
- **replica**　node/term 回报 index 上确切的 entry。
- **apply**　node 证实已应用 entry/index。 对应用前缀中的单条落地通知，重复与乱序到达需按位置核实。
- **read**　qid/key/target/min_index/deadline。
- **witness**　node/term 对请求 qid 和知道的 commit 上界出具本次证据。 commit 是节点可用于担保、且与集群已确认日志身份匹配的位置。
- **tick**　处理查询截止与等待。
- **inspect**　查看写、节点或读状态（`{}` 为值班总览，也可给 `qid` / `entry` / `node` / `index`）。
- **restart**　服务重启。

消息名登记在 `examples/message_types.json`，`scenario_reader.py` 负责严格封装校验。

## 运行测试与清点资料

```bash
python3 scenario_reader.py examples
python3 -m unittest discover -v
```

当前 30 个测试全部通过，其中 `test_coordinator.py` 的 27 个用例把下列安全规则逐条锁成可执行规格，并直接重演两个原始示例。

## 判定规则（值班口径）

固定 N 成员时多数派为 `floor(N/2)+1`，始终按**不同当前成员节点**计票。

1. **写成功凭据**：`put` 的 term 必须等于当前权威任期；只有来自当前成员、当前任期、且确切证明持有 `entry@index` 的复制回报才计票。同节点重复回报不翻倍；非成员、低任期回报、缺少 entry 身份的“最大位置/心跳”、位置-身份不符、已被截断的分叉票一律不计。从 `committed_index+1` 起按连续前缀提交；达到多数即发出不可丢失的写凭据。
2. **同 index 不得混内容**：未确认位置允许新任期的新 entry 截断旧尾部，旧提案标记 `truncated`，其票不与新写混用；未确认写从未产生凭据，因此截断不会“遗留成功凭据”。已确认位置拒绝任何不同 entry 的覆盖，既有凭据原样保留，新主承接整个已确认前缀。
3. **读的新鲜多数证据**：witness 必须发生在 `read` 之后、带同一 `qid`、节点为当前成员且 term 等于当前权威任期。每个 qid 每一代重新集票，旧 qid/旧任期确认不能给后来的读复用。多数证人的 `commit` 取第 majority 高值作为**多数派担保水位 G**，要求 `G >= min_index`；`commit` 超出服务已知 `committed_index` 的证据因身份无法核实而排除。
4. **target 应用前缀**：担保成立后，target 必须真正连续应用完请求专属前缀才回答——至少到多数担保水位 G；若 target 自己在本轮 witness 中上报了更高 commit，该值视为它“承诺的前缀”，取 `max(G, target_commit)`。apply 按位置与已确认身份核实：重复幂等、身份不符拒绝、乱序先记账（如先 index2 后 index1，到 index1 时前缀跳到 2）、早于提交到达的声明在提交后再核实。回答在 target 已连续应用的前缀内取该 key 最新记录（可能新于 G，仍是合法的更晚状态），并报告 entry/index/value 与担保世代。
5. **应用延迟按节点隔离**：每个读只等自己的 target；一个节点应用慢，只阻塞 target 是它的读，其他能兑现的读照常回答。
6. **换任期/换权威**：收到更高 term 的 `leader` 通知，或从 replica/witness 中观察到更高 term 时，所有尚未回答的读立即作废本轮证据、世代号加一，需要在新任期重新取得本次多数证据；同任期完全相同的 leader 通知按幂等处理，不冲掉有效证据。若直到截止仍无法重新取证，tick（`now >= deadline`）或更晚的消息会把读判为 `unavailable / cannot_guarantee_by_deadline`，迟到证据不能翻案。同一秒内按行序决定：截止 tick 之前到达并完成的证据仍有效。
7. **重启核对**：已确认前缀、写凭据、各节点已核实应用位置、未决读及其代际/证据都可 JSON 持久化。`restart` 从 Store 重新加载（文件仓经过真实磁盘原子写），重启事件直接给出值班三核对：committed_index、谁落后未应用、哪些 qid 还在等多数证据/应用。

## 重演与值班台

```bash
# 中文解读 + 结尾值班总览；路径可以是文件或目录
python3 duty_console.py examples/term-change-before-follower-apply.jsonl
python3 duty_console.py examples

# 机器消费：每行一个事件 JSON（含 final_board）
python3 duty_console.py replay examples --json

# 连续送入：stdin 逐行吃信封，逐行吐事件；可接 tail -f
tail -f feed.jsonl | python3 duty_console.py stream --json --state .duty-state.json

# 也可以用 coordinator 入口
python3 coordinator.py examples/same-index-is-not-same-entry.jsonl
```

`--state state.json` 把每个消息后的状态原子落盘；不带时使用经过 JSON 序列化往返的内存仓（`restart` 仍真实经历“保存→重新加载”）。

## 本地 HTTP 协调服务

```bash
python3 duty_console.py serve --host 127.0.0.1 --port 8765 --state .duty-state.json
```

- `POST /v1/message`：单条信封 `{"at":1,"kind":"put","record":{...}}` → `{"events":[...]}`
- `POST /v1/batch`：`{"messages":[信封,...]}` → `{"results":[[事件,...],...]}`
- `GET  /v1/board`：值班总览（确认位置、凭据、节点 applied/lag、各 qid 状态）
- `GET  /v1/state`：完整 JSON 状态快照
- `GET  /healthz`

发一条 `kind:"restart"` 消息即模拟从 `--state` 文件恢复的进程重启。所有请求经同一把锁串行化。

## Python API

```python
from coordinator import Coordinator, JsonFileStore

coord = Coordinator(JsonFileStore("duty-state.json"))
events = coord.handle({"at": 2, "kind": "replica",
                       "record": {"node": "n2", "term": 1,
                                  "entry": "e1", "index": 1}})
board = coord.board()
read_report = coord.inspect_read("r")   # 含 generations 跨代轨迹
```

## 示例演练资料

- `term-change-before-follower-apply.jsonl`：e1 在 term1 获 n1/n2 多数确认；读 r 先拿到 term1 多数担保但 target n3 尚未应用；term2 上台使旧证据作废，n3 应用 e1，term1 witness 被拒，重启后状态仍在，最终 n2/n3 的 term2 新鲜证据让读在**第 2 代**读到 `closed`。
- `same-index-is-not-same-entry.jsonl`：`old@1` 未确认即换 term4 主，`new@1` 截断分叉；a 的低任期 old 回报不能混票；b/c 对 new 的确切多数使其确认，c 应用后读看到 `complete`。
- `apply-delay-is-per-node.jsonl`：乱序/重复 apply、前缀跳跃；c 已追平使 rc 立即回答，b 落后使 rb 只等 b，补齐 e1 后前缀跳到 2 才回答。
- `deadline-reports-cannot-guarantee.jsonl`：5 节点只有 2 个证人，tick 到截止判无法保证，截止后的第三个 witness 不能翻案。
- `restart-duty-board.jsonl`：重启后核对确认位置、节点应用进度与等待多数证据的 qid，重启前的部分证据持久保留，补一个证人即凑成多数。

## 文件结构

| 文件 | 职责 |
| --- | --- |
| `coordinator.py` | 纯标准库确定性引擎：状态、计票、提交、证据世代、应用前缀、截止、重启恢复、inspect/board |
| `duty_console.py` | 重演目录/文件、中文值班台、JSONL 事件流、stdin stream、serve 子命令 |
| `duty_server.py` | `http.server` 实现的本地 JSON HTTP 服务 |
| `scenario_reader.py` | 既有输入契约校验与清点（未改动其行为） |
| `test_coordinator.py` | 27 个引擎/HTTP/CLI 语义测试 |
| `test_scenario_reader.py` | 既有契约测试 |
| `examples/` | 消息类型登记与 5 个可重演场景 |
