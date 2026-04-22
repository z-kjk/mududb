# mcli Management Interface (HTTP)

This document describes how to use `mcli` HTTP management commands for app management and partition routing queries.

## Scope

- Applies when `mcli` connects to `mudud` management endpoint via `--http-addr`.
- Management commands run over the HTTP API, without manual HTTP calls.
- If the server does not expose admin capabilities, commands return an error (for example, `admin service is not configured`).

## Prerequisites

1. Start `mudud`.
2. Ensure the HTTP endpoint is reachable (default `127.0.0.1:8300`).

## Command Reference

### 1) List installed applications

```bash
mcli --http-addr 127.0.0.1:8300 app-list
```

### 2) Install an application package (`.mpk`)

```bash
mcli --http-addr 127.0.0.1:8300 app-install --mpk target/wasm32-wasip2/release/wallet.mpk
```

### 3) Show procedures of an application

```bash
mcli --http-addr 127.0.0.1:8300 app-detail --app wallet
```

### 4) Show one procedure detail

```bash
mcli --http-addr 127.0.0.1:8300 app-detail --app wallet --module wallet --proc create_user
```

### 5) Uninstall an application

```bash
mcli --http-addr 127.0.0.1:8300 app-uninstall --app wallet
```

### 6) Show server topology

```bash
mcli --http-addr 127.0.0.1:8300 server-topology
```

### 7) Partition route query

Route by exact key:

```bash
mcli --http-addr 127.0.0.1:8300 partition-route --rule-name user_rule --key user-100
```

Route by range (supports comma-delimited composite values):

```bash
mcli --http-addr 127.0.0.1:8300 partition-route --rule-name user_rule --start 100 --end 200
```

Composite key example:

```bash
mcli --http-addr 127.0.0.1:8300 partition-route --rule-name order_rule --key tenant-1,order-100
```

## Argument Constraints

- For `app-detail`:
  - `--proc` must be used together with `--module`.
- For `partition-route`:
  - `--key` and `--start/--end` are mutually exclusive.
  - You must provide `--key`, or at least one of `--start` / `--end`.

## Output

- Default output is pretty JSON.
- Use `--compact` for compact JSON.
- On failure, the command exits non-zero and prints the error to stderr.
