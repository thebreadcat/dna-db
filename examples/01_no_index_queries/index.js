const { DemoDb, timed } = require("../shared/demo_db");

function generateEvents(n) {
  const countries = ["US", "CA", "DE", "IN"];
  const types = ["click", "view", "purchase"];
  const now = Date.now();
  return Array.from({ length: n }, (_, i) => ({
    id: `evt_${i}`,
    type: types[i % types.length],
    country: countries[i % countries.length],
    timestamp: now - (i % 172800000),
  }));
}

async function main() {
  const db = new DemoDb();
  const events = db.collection("events");

  await events.insertMany(generateEvents(100_000));
  const { ms, result } = timed("no-index-query", () =>
    events
      .where("type", "=", "click")
      .where("country", "=", "US")
      .where("timestamp", ">", Date.now() - 86400000)
      .fetch()
  );
  const rows = await result;

  console.log("No Index Query Demo");
  console.log(`rows: ${rows.length}`);
  console.log(`query_ms: ${ms.toFixed(2)}`);
  console.log("index_creation_step: skipped");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
