# DNA-DB Python SDK

Install:

```bash
pip install dnadb-python-sdk
```

The PyPI distribution is `dnadb-python-sdk`; the importable package is still `dnadb`.

Quick start:

```python
from dnadb import DNAdb, DNAdbClientConfig

db = DNAdb(DNAdbClientConfig(host="127.0.0.1", port=8787, database="default"))
posts = db.collection("posts")

posts.insert({"id": 1, "title": "Hello", "status": "published"})
rows = posts.where("status", "=", "published").limit(10).fetch()
```

Configure indexes:

```python
db.collection("posts").configure(
    sort_indexes=["updated_at", "created_at"],
    exact_string_fields=["slug", "status"],
)
```

Integration test (requires running server):

```bash
DNADB_URL=http://127.0.0.1:8787 python -m unittest tests/test_integration.py
```
