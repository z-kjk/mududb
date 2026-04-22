# mcli 管理接口（HTTP）

本文介绍如何通过 `mcli` 的 HTTP 管理命令完成应用管理与路由查询。

## 适用范围

- 适用于 `mcli` 通过 `--http-addr` 连接 `mudud` HTTP 管理接口的场景。
- 管理命令走 HTTP API，不需要手写 HTTP 请求。
- 如果服务端未启用对应管理能力，会返回错误信息（例如 `admin service is not configured`）。

## 前置条件

1. 启动 `mudud`。
2. 确认 HTTP 管理端口可访问（默认 `127.0.0.1:8300`）。

## 命令一览

### 1) 列出已安装应用

```bash
mcli --http-addr 127.0.0.1:8300 app-list
```

### 2) 安装应用包（`.mpk`）

```bash
mcli --http-addr 127.0.0.1:8300 app-install --mpk target/wasm32-wasip2/release/wallet.mpk
```

### 3) 查看应用过程列表

```bash
mcli --http-addr 127.0.0.1:8300 app-detail --app wallet
```

### 4) 查看单个过程详情

```bash
mcli --http-addr 127.0.0.1:8300 app-detail --app wallet --module wallet --proc create_user
```

### 5) 卸载应用

```bash
mcli --http-addr 127.0.0.1:8300 app-uninstall --app wallet
```

### 6) 查看服务拓扑

```bash
mcli --http-addr 127.0.0.1:8300 server-topology
```

### 7) 分区路由查询

按精确 key 路由：

```bash
mcli --http-addr 127.0.0.1:8300 partition-route --rule-name user_rule --key user-100
```

按范围路由（支持逗号分隔多列值）：

```bash
mcli --http-addr 127.0.0.1:8300 partition-route --rule-name user_rule --start 100 --end 200
```

多列示例：

```bash
mcli --http-addr 127.0.0.1:8300 partition-route --rule-name order_rule --key tenant-1,order-100
```

## 参数约束

- `app-detail` 中：
  - `--proc` 必须与 `--module` 一起使用。
- `partition-route` 中：
  - `--key` 与 `--start/--end` 二选一，不可同时使用。
  - 必须提供 `--key`，或至少提供 `--start` / `--end` 之一。

## 返回结果

- 默认输出为格式化 JSON。
- 使用 `--compact` 可输出紧凑 JSON。
- 出错时命令返回非 0，并在 stderr 输出错误原因。
