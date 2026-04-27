const { DemoDb } = require("../shared/demo_db");

async function main() {
  const db = new DemoDb();
  db.setOverlay("users", "support", "ssn", () => "***-**-****");

  await db.collection("users").insert({
    id: "u_1",
    email: "user@test.com",
    ssn: "123-45-6789",
  });

  const adminView = await db.as("admin").collection("users").fetch();
  const supportView = await db.as("support").collection("users").fetch();

  console.log("Privacy Overlay Demo");
  console.log("admin:", JSON.stringify(adminView[0], null, 2));
  console.log("support:", JSON.stringify(supportView[0], null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
