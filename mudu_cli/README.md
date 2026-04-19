# mcli

`mcli` is the TCP protocol client CLI for MuduDB.

`put`, `get`, `range`, `invoke`, and `app-invoke` create and close a temporary session automatically for each command.

It talks directly to the server TCP protocol and exposes these operations:

- `command`
- `put`
- `get`
- `range`
- `invoke`
- `app-install`
- `app-invoke`

## Examples

Query:

```bash
mcli --addr 127.0.0.1:9527 command --json '{"app_name":"demo","sql":"select 1"}'
```

Put:

```bash
mcli --addr 127.0.0.1:9527 put --json '{
  "key": "user-1",
  "value": "value-1"
}'
```

Get:

```bash
mcli --addr 127.0.0.1:9527 get --json '{
  "key": "user-1"
}'
```

Range scan:

```bash
mcli --addr 127.0.0.1:9527 range --json '{
  "start_key": "a",
  "end_key": "z"
}'
```

Invoke:

```bash
mcli --addr 127.0.0.1:9527 invoke --json '{
  "procedure_name": "app/mod/proc",
  "procedure_parameters": {"base64": "cGF5bG9hZA=="}
}'
```

Install `.mpk` through the management HTTP API:

```bash
mcli --http-addr 127.0.0.1:8300 app-install --mpk target/wasm32-wasip2/release/key-value.mpk
```

Invoke an installed procedure through the TCP protocol:

```bash
mcli --addr 127.0.0.1:9527 --http-addr 127.0.0.1:8300 app-invoke --app kv --module key_value --proc kv_read --json '{
  "user_key": "user-1"
}'
```

## JSON input

JSON request bodies can be supplied in three ways:

- `--json '<json>'`
- `--json-file request.json`
- `--json-file -` to read from stdin

`put`, `get`, and `range` accept ordinary JSON values for keys and values. `mcli` still creates and injects a temporary session automatically for those commands.
