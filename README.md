# 主从协调服务的线性一致读与候选领导交接

成员组成固定。term 是仿真选主任期。entry 代表日志写入不可复用的身份。index 是从 1 递增的位置。相同 index 可在未确认的尾部出现别的 entry。

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
- **inspect**　查看写、节点或读状态。
- **restart**　服务重启。

## 示例

- term-change-before-follower-apply.jsonl
- same-index-is-not-same-entry.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。
