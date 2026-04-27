# Example: Hybrid Queries

## What this shows
Indexed and non-indexed predicates run together in one query flow.

## Run it
```bash
npm install
node index.js
```

## What to notice
- `status` behaves like indexed narrowing.
- `notes like` behaves like scan fallback.

## Why this matters
You get practical queries without forcing full-text setup first.
