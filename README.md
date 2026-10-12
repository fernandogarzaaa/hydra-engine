## 🌀 Overview

`hydra-engine` is a durable, event-driven orchestration engine designed to execute complex, non-deterministic multi-turn AI agent trajectories with ironclad runtime resilience. In production AI systems, agent states are inherently volatile—downstream APIs time out, LLM connections drop, or long-running worker processes crash mid-flight. If agent state lives purely in ephemeral memory, a single network hiccup destroys the entire workflow execution history.

`hydra-engine` resolves this paradigm by treating agent trajectories as deterministic state transitions backed by an asynchronous relational database. By checkpointing every thought, tool call, and tool response, the engine provides native distributed recovery. If an execution container is terminated mid-trajectory, a secondary worker safely assumes the task once that trajectory's claim lease expires, replays the historical state without duplicating completed API footprints, and resumes processing from the exact point of failure. Every trajectory is claimed via an atomic, lease-based `UPDATE ... WHERE` before a worker touches it, so concurrent workers polling the same pending trajectory can never both execute it at once.

## Getting started

Requires Python 3.11-3.13, PostgreSQL, and Redis. (The test suite runs on SQLite via the `dev` group — no external services needed for tests.)

```bash
# install (uv is the pinned path; plain pip works too)
uv sync --group dev
# or: pip install -e . "sqlalchemy[asyncio]<2.1" aiosqlite pytest pytest-asyncio ruff mypy
# (the <2.1 pin keeps the sqlalchemy mypy plugin that `mypy app` expects)

# configure
cp .env.example .env   # then edit DATABASE_URL / REDIS_URL as needed
```

### Authentication

Set `HYDRA_API_KEY` to require a key on every workflow route (`/workflows/trigger`, `/workflows/{id}/resume`, `/workflows/{id}/audit`). Send it as `X-API-Key: <key>` or `Authorization: Bearer <key>`; wrong or missing keys get `401`. `/healthz` stays open. When the variable is unset the API is unauthenticated (fine for local use) and logs a warning at startup, so keep uvicorn on its default `127.0.0.1` bind or put it behind your own auth. `tenant_id` is still client-supplied: the key gates access, it does not isolate tenants from each other.

Run the API and the worker (two terminals):

```bash
uvicorn app.main:app --port 8081
python -m app.worker_main
```

Trigger a workflow and poll its audit trail:

```bash
curl -s -X POST localhost:8081/workflows/trigger \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo","steps":[{"prompt":"plan"},{"tool":"x"},{"result_consumer":"op"}]}'
# -> {"id":"<uuid>",...}
curl -s localhost:8081/workflows/<uuid>/audit | python3 -m json.tool
```

## Failure model

- **Claims and leases.** A worker claims a trajectory with one atomic `UPDATE ... WHERE` (unclaimed, or lease expired). While a step runs, a heartbeat renews the lease every `TRAJECTORY_LEASE_SECONDS / 3`, so a long step is not reclaimed by another worker. The lease is released when the trajectory completes, when a step fails and a retry is scheduled, and when it fails permanently.
- **Retries.** A failed step is retried after `BACKOFF_FACTOR ** retry_count` seconds, up to `MAX_STEP_RETRIES`. Retries wait in a Redis sorted set (`REDIS_DELAYED_QUEUE_NAME`); every worker loop moves due entries back to the work queue (at most every `DELAYED_POLL_SECONDS`), and only the worker whose `ZREM` removed an entry re-queues it, so each retry is delivered once.
- **Messages that arrive early.** If a retry or resume message arrives while another worker still holds the lease, it is parked on the delayed set until that lease ends instead of being dropped.
- **Bad messages.** A malformed queue message is pushed to `REDIS_DEAD_LETTER_QUEUE_NAME` (default `hydra:workflow:dead`) and logged; an exception while processing a trajectory is logged. Neither stops the worker loop.
- **Resume.** `/workflows/{id}/resume` resets failed steps to pending with a fresh retry budget.
- **Limits.** The schema is created with `create_all` at startup (no migrations yet). The tests use SQLite and fakeredis; there is no multi-worker integration test against real Postgres/Redis yet.

Run the checks:

```bash
pytest
ruff check . && ruff format --check .
mypy app
```
