# Syscall Semantics

## Worker Identity

In the current runtime path, session routing is expressed in terms of `worker_id`.

- `worker_id` is the routing target for session-local execution
- once a session is bound to a worker, its requests must be handled by that worker

## Session Open

`open` accepts an optional JSON string parameter.

The JSON payload is used to describe session routing and session configuration changes. The payload contains at least:

- `session_id`
- `worker_id`

Example:

```json
{
  "session_id": 0,
  "worker_id": 3
}
```

## `session_id` Meaning

`session_id` controls whether the call creates a new session or updates an existing one.

- if `session_id == 0`, the kernel creates a new session
- if `session_id != 0`, the call refers to an existing session and changes that session's configuration

The configuration change described here is the target worker binding carried by the same JSON payload.

## `worker_id` Meaning

`worker_id` tells the kernel which worker should own the session.

- if the current connection is already attached to the target worker, the session is created or updated there
- if the current connection is not attached to that worker, the kernel transfers the connection to that worker

After this transfer, the target worker becomes the owner of that session.

## Connection Default Routing

When a session causes the connection to move to another worker, that worker also becomes the default worker for the current connection.

This means:

- later requests for that same session go to that worker
- later requests on the same connection also go to that worker by default

This default stays in effect until another session on the same connection explicitly changes the setting again through `open`.

## Routing Rules

The effective behavior is:

1. Parse the optional JSON argument passed to `open`.
2. Read `session_id` and `worker_id`.
3. If `session_id == 0`, create a new session.
4. If `session_id != 0`, update the existing session configuration.
5. Ensure the session is owned by the worker identified by `worker_id`.
6. If necessary, transfer the current connection to that worker.
7. Use that worker as the default connection target until another explicit session routing change happens.

## Notes

- session routing is explicit
- connection routing may change as a side effect of opening or reconfiguring a session
- the session owner worker and the connection default worker are expected to stay aligned after such a change
- a later `open` on another session may move the same connection again
