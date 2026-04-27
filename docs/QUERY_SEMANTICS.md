# Query semantics: `LIMIT` and `ORDER BY`

This document describes behavior in the **durable MVCC runtime** used by the experimental `dnadb_http` CMS lab and related wire paths. It matters whenever a query has a **`limit`** but no explicit **`order_by`**.

## Implicit ordering policy

If a query specifies **`limit`** and does **not** specify **`order_by`**, the engine applies a **default sort** before choosing an access path. That means:

- `WHERE status = 'published' LIMIT 100` is **not** “any 100 matching rows.”
- It is **“up to 100 matching rows after applying the collection’s default ordering.”**

This matches how most CMS-style apps think (latest first), and it keeps top‑`N` queries on a **deterministic, index-friendly** path instead of an unordered scan.

## How the default field is chosen

For a given collection, the default order direction is **descending**. The sort field is chosen in this order, using the collection’s **configured sort-index fields** (see `*.sort_indexes.json` and the HTTP `sort-indexes` API):

1. `created_at` — if indexed for sorting  
2. else `updated_at` — if indexed for sorting  
3. else the first configured sort-index field (lexicographic order of field names), if any  

If there is **no** configured sort index, the engine cannot apply this shortcut and falls back to existing scan/planner behavior.

**Exception:** a synthetic default order is **not** applied for single-clause string equality on any field listed in the collection’s **exact-string index config** (persisted in `*.sort_indexes.json` as `exact_string_fields`; legacy files omit it and default to `slug`, `title`, `email`). That keeps exact-index and point-style lookups fast. Use an explicit `order_by` if you need a particular ordering for those fields.

## What you should do in application code

- **Always pass an explicit `order_by`** when the sort order is part of your contract (pagination, APIs, exports).
- Rely on the implicit default only when **“newest first (or configured default)”** is what you intend.

## CMS lab HTTP API

`POST /api/collections/:name/query` accepts `sort` as a map (e.g. `{ "updated_at": -1 }`). If you omit `sort` but pass `limit`, the engine’s implicit ordering policy above applies.

## Future: true unordered reads

Some workloads genuinely want **arbitrary** matching rows with no order guarantee. That is **not** exposed today; a future escape hatch (e.g. `unordered: true` on the request or an explicit SQL/Mongo extension) would restore literal “any N” semantics without changing CMS defaults.

## Related files

- Engine: `engine/src/transaction_durable.rs` — `with_default_order_for_limit` and query planning  
- CMS lab: `cms/README.md`  
