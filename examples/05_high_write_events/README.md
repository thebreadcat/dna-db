# Example: High Write Events

## What this shows
Append-first event ingest throughput under large write volume.

## Run it
```bash
npm install
node index.js
```

## What to notice
- Throughput metrics print at the end.
- No schema migration or index setup before ingest.

## Why this matters
Event-heavy workloads can start writing immediately.
