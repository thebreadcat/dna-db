const { DemoDb } = require("../shared/demo_db");

async function main() {
  const db = new DemoDb();
  const users = db.collection("users");

  await users.insert({ id: "u_1", name: "Alice" });
  await users.insert({ id: "u_2", name: "Bob", age: 30 });
  await users.insert({ id: "u_3", name: "Casey", age: 33, plan: "pro" });

  const all = await users.fetch();
  console.log("Schema Evolution Demo");
  console.log(JSON.stringify(all, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
