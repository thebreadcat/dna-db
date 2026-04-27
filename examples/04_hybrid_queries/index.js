const { DemoDb, timed } = require("../shared/demo_db");

async function main() {
  const db = new DemoDb();
  const orders = db.collection("orders");
  const statuses = ["completed", "pending", "failed"];

  await orders.insertMany(
    Array.from({ length: 60_000 }, (_, i) => ({
      id: `ord_${i}`,
      status: statuses[i % statuses.length],
      notes: i % 17 === 0 ? "customer asked for refund on damaged item" : "standard order flow",
      total: (i % 250) + 20,
    }))
  );

  const { ms, result } = timed("hybrid-query", () =>
    orders
      .where("status", "=", "completed")
      .where("notes", "like", "%refund%")
      .limit(20)
      .fetch()
  );
  const rows = await result;

  console.log("Hybrid Query Demo");
  console.log("planner_path: index(status) + scan(notes like)");
  console.log(`rows: ${rows.length}`);
  console.log(`query_ms: ${ms.toFixed(2)}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
