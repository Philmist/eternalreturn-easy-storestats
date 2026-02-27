# Eternal Return API Behavior Notes

## Purpose

This document records API behaviors that are hard to infer from the official
documentation and that affect ingestion logic in this repository.

Official reference:
`https://developer.eternalreturn.io/static/media/OpenAPI_EN_20251118.html`

These notes are based on observed production behavior and local reproducible
samples. Treat this file as the source of truth for this project when behavior
conflicts with the official docs.

## Scope

- Endpoint-specific payload semantics
- Ingestion fallback and stop rules
- Operational assumptions (including limited match retention)

## Observed Behaviors

### 1) Match retention can be limited (for example, around 90 days)

For some existing users, the server can return no match history even though the
nickname/user itself exists. This appears to be a server-side data retention
constraint.

### 2) `/v1/user/games/uid/{uid}` payload code semantics (project rule)

For this endpoint, this project uses the following interpretation:

- `code: 200`: Normal success
- `code: 401`: Treat as missing/invalid UID and attempt seed recovery when context is available
- `code: 404`: Ambiguous (`no games` or stale UID after nickname change). Attempt seed recovery when context is available

Important:
- Do not use the payload `message` alone for classification.
- Always classify by `endpoint + payload code`.

### 3) `/v1/user/nickname?query=...` payload code semantics

- `code: 404`: Nickname does not resolve to a user (for current run)

### 4) Transport-layer HTTP 404 semantics

- HTTP status `404` (not payload code) indicates endpoint/path-level failure.
- This is treated as unrecoverable and should raise/abort.

## Repository Behavior (Current Implementation)

### A) UID recovery trigger

`uid -> user/games` recovery logic is triggered when:

- Endpoint is `/v1/user/games/uid/{uid}`
- Payload code is `401` or `404`
- Seed nickname context is available

### B) No-games handling

When `/v1/user/games/uid/{uid}` returns payload `404`:

- First try seed nickname re-resolution (same guardrails as `401` path)
- If recovery succeeds with a different UID, continue ingest using that UID
- If recovery is unavailable or fails, treat as "no games available" for this ingest attempt and stop gracefully

### C) Nickname not found caching

When `/v1/user/nickname` returns payload `404`:

- Cache nickname as unresolved for the current run
- Avoid repeated lookups for the same nickname in that run

### D) Loop guardrails for repeated UID recovery

When recovery keeps returning UID variants that fail with payload `401`/`404`, stop
the current seed using both guardrails:

- UID variant limit per seed: `3`
- Resolve-attempt limit per seed: `5`

## Local Reference Samples

Reference-only payload files under `api-result-examples/`:

- Missing/invalid UID sample:
  - `api-result-examples/unbounded-uid-games.json`
  - Expected payload: `{"code": 401, "message": "Unauthorized"}`
- Existing user but no games sample:
  - `api-result-examples/existing-user-no-games.json`
  - Expected payload: `{"code": 404, "message": "User Not Found"}`

These files are examples only. Production code must not depend on them.

## Maintenance Rule

If API behavior changes, update the following together in one change set:

1. This document
2. `src/er_stats/api_client.py` classification helpers
3. `src/er_stats/ingest.py` recovery/no-games handling
4. Tests in `tests/test_api_client.py` and `tests/test_ingest.py`
