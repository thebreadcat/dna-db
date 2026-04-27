const fs = require("fs");
const path = require("path");

function main() {
  const root = path.resolve(__dirname, "..", "..");
  const benchDir = path.join(root, "bench-output");
  const files = fs.existsSync(benchDir)
    ? fs.readdirSync(benchDir).filter((name) => name.endsWith(".json")).sort()
    : [];

  if (files.length === 0) {
    console.log("No benchmark JSON files found in bench-output/.");
    return;
  }

  const latest = files[files.length - 1];
  const parsed = JSON.parse(fs.readFileSync(path.join(benchDir, latest), "utf-8"));
  console.log("Quick Benchmark Snapshot");
  console.log(`file: ${latest}`);
  console.log(`mode: ${parsed.mode}`);
  console.log(`phase: ${parsed.phase || "full"}`);
  console.log(`records: ${parsed.records}`);
  console.log(`write_records_per_sec: ${Number(parsed.write_records_per_sec || 0).toFixed(0)}`);
  console.log(`decode_records_per_sec: ${Number(parsed.decode_records_per_sec || 0).toFixed(0)}`);
  console.log(`point_reads_per_sec: ${Number(parsed.point_reads_per_sec || 0).toFixed(0)}`);
}

main();
