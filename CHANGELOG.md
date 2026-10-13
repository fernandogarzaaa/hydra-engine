# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-10-13

### Fixed

- Lease fencing: a worker whose lease expired mid-step (for example a stalled
  heartbeat) and was reclaimed by another worker no longer commits that step's
  output, marks the trajectory completed, records a failure/retry, or clears the
  new holder's claim. Every write now re-asserts ownership with a conditional
  `UPDATE ... WHERE claimed_by = <worker>` in the same transaction and rolls back
  if the claim moved, so a step is not recorded twice and two workers do not race
  through the remaining steps.

## [0.1.0] - 2026-10-13

First tagged release.

### Fixed

- Failed steps are actually retried: the worker loop promotes due entries from the
  delayed sorted set back onto the work queue (previously retries were written to a
  set nothing read, so a failed step stayed PENDING forever).
- Claim leases are released on completion, retry and permanent failure, and renewed
  by a heartbeat while a step runs, so long steps are not reclaimed and run twice.
- A retry/resume that arrives while another worker holds the lease is re-parked until
  the lease ends instead of being dropped.
- Malformed queue messages go to a dead-letter list and processing errors are logged;
  neither stops the worker loop.

### Security

- Optional `HYDRA_API_KEY` authentication for the HTTP API.

### Added

- Tests for retry promotion, lease release/heartbeat, early-retry parking, dead letter,
  resume budget and API-key auth (fakeredis).

- Dependabot for uv and GitHub Actions (weekly)
- .env.example listing the environment settings read by app/config.py
- SECURITY.md and this CHANGELOG
- README "Getting started": install, configuration, running the API and
  worker, triggering a workflow, and the check commands (from wave-3
  dogfood; the README previously had no install/usage docs)
