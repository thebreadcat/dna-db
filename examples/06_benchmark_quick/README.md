# Example: Benchmark Quick

## What this shows
Instant readout of the latest `bench-output/*.json` metrics.

## Run it
```bash
npm install
node index.js
```

## What to notice
- Pulls the latest benchmark file automatically.
- Prints write/decode/point-read throughput.

## Why this matters
Gives a fast sanity check before heavier performance loops.
