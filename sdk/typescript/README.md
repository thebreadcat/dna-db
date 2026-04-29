# DNA-DB TypeScript SDK

Install:

```bash
npm install @dnadb/sdk
```

Quick start:

```ts
import { DNAdb } from "@dnadb/sdk";

const db = new DNAdb({ url: "http://127.0.0.1:8787", database: "default" });
const posts = db.collection("posts");

await posts.insert({ id: 1, title: "Hello", status: "published" });
const rows = await posts.where("status", "=", "published").limit(10).fetch();
```

Configure indexes:

```ts
await db.collection("posts").configure({
  sortIndexes: ["updated_at", "created_at"],
  exactStringFields: ["slug", "status"],
});
```

Integration test (requires running server):

```bash
DNADB_URL=http://127.0.0.1:8787 npm run integration
```
