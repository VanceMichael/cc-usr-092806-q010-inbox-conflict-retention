# 协同事务平台

协同事务平台为需要多机构参与的业务提供统一的机构、授权、案件、证据、审批、资源预约、资金分录、消息归并、通知、定时任务和历史回放能力。项目只使用 Python 标准库与 SQLite，适合在单个 Linux 应用容器内运行。

## 目录

- `src/civicflow/`：领域服务、SQLite 持久化、权限和命令行入口。
- `tests/`：核心流程、边界条件和异常路径测试。
- `examples/`：本地演示输入。

## 配置

通过 `CIVICFLOW_DB` 指定 SQLite 文件路径；不设置时命令行使用当前目录下的 `civicflow.sqlite3`。所有时间使用带时区的 ISO 8601 字符串。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 编译或构建

```bash
PYTHONPATH=src python3 -m compileall -q src
```

## 使用

初始化数据库并运行离线演示：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 demo
```

查看当前案件：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 list-cases
```

## 消息接入与异文冲突

`Inbox.receive` 按来源、编号和序号去重：相同内容重放返回 `duplicate`；同序号不同内容时，原始消息保持不变，异文摘要（仅字段名，不含取值）与双方指纹持久写入 `inbox_conflicts`，随后向调用方抛出带冲突编号的 `ConflictError`。接收、重复、异文和人工裁决都会写入统一审计链。

- `Inbox.timeline(source, source_key)` / `Inbox.chain(source, source_key, sequence)`：回放完整链路，服务重启与旧表迁移（自动补齐缺失列）后均可用。
- `InboxConflictService`：冲突记录默认最小化暴露，`read:inbox_conflicts` 只能看到脱敏记录；持有 `review:inbox_conflicts` 且开启 `reveal_sensitive` 的复核岗位才能通过 `reveal_payload` 查看异文原文，并通过 `resolve(decision="dismissed"|"adopted", reason=...)` 作出人工裁决。
