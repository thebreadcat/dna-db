# Example: Schema Evolution

## What this shows
Mixed record shapes are queryable without migrations.

## Run it
```bash
npm install
node index.js
```

## What to notice
- Earlier rows have fewer fields.
- New rows add fields without schema lock.

## Why this matters
You can evolve data shape safely while shipping fast.
