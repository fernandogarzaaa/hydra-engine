## 🌀 Overview

`hydra-engine` is a durable, event-driven orchestration engine designed to execute complex, non-deterministic multi-turn AI agent trajectories with ironclad runtime resilience. In production AI systems, agent states are inherently volatile—downstream APIs time out, LLM connections drop, or long-running worker processes crash mid-flight. If agent state lives purely in ephemeral memory, a single network hiccup destroys the entire workflow execution history.

`hydra-engine` resolves this paradigm by treating agent trajectories as deterministic state transitions backed by an asynchronous relational database. By checkpointing every thought, tool call, and tool response, the engine provides native distributed recovery. If an execution container is terminated mid-trajectory, a secondary worker instantly assumes the task, replays the historical state without duplicating completed API footprints, and resumes processing from the exact point of failure.
