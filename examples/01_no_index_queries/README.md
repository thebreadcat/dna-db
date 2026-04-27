# Example: No Index Queries

## What this shows
Querying 100k events with no explicit index creation step.

## Run it
```bash
npm install
node index.js
```

## What to notice
- No index creation.
- Filtered query still returns quickly.

## Why this matters
Reduces index planning overhead during fast iteration.
