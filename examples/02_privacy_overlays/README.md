# Example: Privacy Overlays

## What this shows
Field-level masking enforced at the database layer.

## Run it
```bash
npm install
node index.js
```

## What to notice
- Admin sees full `ssn`.
- Support role sees masked `ssn`.

## Why this matters
Privacy policy is applied by the DB, not custom app logic.
