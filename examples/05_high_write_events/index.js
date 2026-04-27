const { DemoDb, timed } = require("../shared/demo_db");

async function main() {
  const db = new DemoDb();
  const events = db.collection("events");
  const target = 1_000_000;
  const now = Date.now();

  const batch = 25_000;
  let inserted = 0;
  const benchmark = timed("event-ingest", () => {
    while (inserted < target) {
      const rows = Array.from({ length: Math.min(batch, target - inserted) }, (_, i) => ({
        type: "click",
        user_id: (inserted + i) % 1000,
        ts: now + inserted + i,
      }));
      events.insertMany(rows);
      inserted += rows.length;
    }
  });

  const seconds = benchmark.ms / 1000;
  console.log("High Write Throughput Demo");
  console.log(`records_inserted: ${inserted}`);
  console.log(`elapsed_ms: ${benchmark.ms.toFixed(2)}`);
  console.log(`writes_per_sec: ${(inserted / seconds).toFixed(0)}`);
  console.log("schema_or_index_prep: none");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
