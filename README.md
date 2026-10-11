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

Run the checks:

```bash
pytest
ruff check . && ruff format --check .
mypy app
```
