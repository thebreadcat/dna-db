const { DemoDb, timed } = require("../shared/demo_db");

function generateEvents(n) {
  const now = Date.now();
  return Array.from({ length: n }, (_, i) => ({
    id: `evt_${i}`,
    type: i % 2 === 0 ? "click" : "view",
    country: i % 3 === 0 ? "US" : "CA",
    timestamp: now - (i % 43200000),
  }));
}

async function main() {
  const db = new DemoDb();
  db.setOverlay("users", "support", "ssn", () => "***-**-****");

  const events = db.collection("events");
  const users = db.collection("users");

  const ingest = timed("ingest", () => events.insertMany(generateEvents(100_000)));
  await ingest.result;

  const query = timed("query", () =>
    events
      .where("type", "=", "click")
      .where("country", "=", "US")
      .where("timestamp", ">", Date.now() - 86400000)
      .fetch()
  );
  await query.result;

  await users.insert({ email: "user@test.com", ssn: "123-45-6789" });
  const supportRow = (await db.as("support").collection("users").fetch())[0];

  console.log("✔ inserted 100k records");
  console.log(`✔ ran query in ${query.ms.toFixed(2)}ms`);
  console.log("✔ no indexes created");
  console.log(`✔ privacy overlay applied (${supportRow.ssn})`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
