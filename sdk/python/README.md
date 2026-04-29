# DNA-DB Python SDK

Python baseline SDK for DNA-DB query/mutation flows.

## Current Surface

- `DNAdbClientConfig`: host/port/database/api key configuration
- `DNAdb`: root client with `collection(name)` accessor
- `CollectionClient`:
  - `insert(record)`
  - `where(field, op, value)` fluent entrypoint
  - `query()` fluent entrypoint
- `QueryBuilder`:
  - `where(...)`, `include(...)`, `order_by(...)`, `limit(...)`
  - `fetch()`, `fetch_one()`
- `Transport` protocol for pluggable runtime transport
- `NotImplementedTransport` guard transport by default

This stage provides API shape parity with the current TypeScript SDK. Network transport/wire binding remains a subsequent integration step.

## Collection Configuration

The SDK surface now supports collection-level sort index configuration through
the pluggable transport:

```python
db.collection("products").configure(
    sort_indexes=["price", "created_at", "rating"],
    composite_sort_indexes=[("status", "updated_at")],
    exact_string_fields=["sku", "title"],
)
```
