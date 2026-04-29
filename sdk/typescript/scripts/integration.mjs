import assert from "node:assert/strict";
import { DNAdb } from "../dist/index.js";

const baseUrl = process.env.DNADB_URL ?? "http://127.0.0.1:8787";

function toConfig(url) {
  const u = new URL(url);
  return {
    url,
    host: u.hostname,
    port: Number(u.port || 80),
    database: "default",
  };
}

async function run() {
  const db = new DNAdb(toConfig(baseUrl));
  const coll = db.collection("sdk_test_ts");

  await coll.insert({ id: 1, title: "Hello", status: "published", updated_at: 1 });
  await coll.insert({ id: 2, title: "World", status: "draft", updated_at: 2 });

  const rows = await coll.where("status", "=", "published").limit(10).fetch();
  assert.equal(rows.length, 1, "expected one published row");
  assert.equal(rows[0].title, "Hello");

  await coll.configure({
    sortIndexes: ["updated_at"],
    exactStringFields: ["status"],
  });

  console.log("typescript integration ok");
}

run().catch((err) => {
  console.error("typescript integration failed");
  console.error(err);
  process.exit(1);
});
