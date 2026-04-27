const fs = require("fs");
const path = require("path");
const http = require("http");
const { URL } = require("url");

const PORT = Number(process.env.PORT || 4173);
const MAX_PORT_PROBES = 25;
const ROOT = path.resolve(__dirname, "..", "..");
const PUBLIC_DIR = path.join(__dirname, "public");
const BENCH_DIR_CANDIDATES = [
  path.join(ROOT, "bench-output"),
  path.join(ROOT, "engine", "bench-output"),
];

function sendJson(res, status, payload) {
  const body = JSON.stringify(payload, null, 2);
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(body),
  });
  res.end(body);
}

function sendFile(res, filePath, contentType) {
  fs.readFile(filePath, (err, content) => {
    if (err) {
      sendJson(res, 404, { error: "File not found" });
      return;
    }
    res.writeHead(200, { "Content-Type": contentType });
    res.end(content);
  });
}

function discoverBenchmarkDir() {
  const existing = BENCH_DIR_CANDIDATES.filter((dir) => fs.existsSync(dir));
  const withJson = existing.find((dir) =>
    fs.readdirSync(dir).some((name) => name.endsWith(".json"))
  );
  return {
    selectedDir: withJson || existing[0] || BENCH_DIR_CANDIDATES[0],
    searchedDirs: BENCH_DIR_CANDIDATES,
  };
}

function parseBenchmarks() {
  const { selectedDir, searchedDirs } = discoverBenchmarkDir();
  if (!fs.existsSync(selectedDir)) {
    return { sourceDir: selectedDir, searchedDirs, benchmarks: [] };
  }
  const files = fs
    .readdirSync(selectedDir)
    .filter((name) => name.endsWith(".json"))
    .sort();

  const rows = [];
  for (const file of files) {
    const fullPath = path.join(selectedDir, file);
    try {
      const parsed = JSON.parse(fs.readFileSync(fullPath, "utf-8"));
      rows.push({
        file,
        mode: parsed.mode || "unknown",
        phase: parsed.phase || "full",
        records: parsed.records || 0,
        write_records_per_sec: parsed.write_records_per_sec || 0,
        decode_records_per_sec: parsed.decode_records_per_sec || 0,
        point_reads_per_sec: parsed.point_reads_per_sec || 0,
        query_seconds: parsed.query_seconds || 0,
        full_scan_seconds: parsed.full_scan_seconds || 0,
        query_found: Boolean(parsed.query_found),
      });
    } catch (error) {
      rows.push({
        file,
        parse_error: String(error),
      });
    }
  }
  return { sourceDir: selectedDir, searchedDirs, benchmarks: rows };
}

function buildAdapterExamples() {
  const logicalQuery = {
    collection: "users",
    where: [
      { field: "age", op: ">=", value: 25 },
      { field: "region", op: "=", value: "us-west" },
      { field: "email", op: "like", value: "%@gmail.com" },
    ],
    orderBy: { field: "age", direction: "desc" },
    limit: 10,
  };

  return {
    logicalQuery,
    typescript: `const rows = await db
.collection("users")
.query()
.where("age", ">=", 25)
.where("region", "=", "us-west")
.where("email", "like", "%@gmail.com")
.orderBy("age", "desc")
.limit(10)
.fetch();`,
    postgres: `SELECT * FROM users
WHERE age >= 25
  AND region = 'us-west'
  AND email LIKE '%@gmail.com'
ORDER BY age DESC
LIMIT 10;`,
    graphql: `query UsersInRegion {
  collection(
    name: "users"
    where: [
      { field: "age", op: ">=", value: 25 }
      { field: "region", op: "=", value: "us-west" }
      { field: "email", op: "like", value: "%@gmail.com" }
    ]
    orderBy: { field: "age", direction: "desc" }
    limit: 10
  )
}`,
    mongodb: `db.users.find({
  age: { $gte: 25 },
  region: "us-west",
  email: { $regex: /@gmail\\.com$/i }
}).sort({ age: -1 }).limit(10);`,
    response: {
      rows: [
        { id: "u_199", email: "anita199@gmail.com", age: 41, region: "us-west" },
        { id: "u_301", email: "jo301@gmail.com", age: 39, region: "us-west" },
      ],
      count: 2,
      planner_path: "index_lookup",
      timing_ms: {
        parse: 0.04,
        planner: 0.11,
        execution: 1.92,
      },
    },
  };
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url || "/", "http://localhost");

  if (url.pathname === "/api/benchmarks") {
    const benchmarkPayload = parseBenchmarks();
    sendJson(res, 200, {
      updated_at: new Date().toISOString(),
      source_dir: benchmarkPayload.sourceDir,
      searched_dirs: benchmarkPayload.searchedDirs,
      benchmarks: benchmarkPayload.benchmarks,
    });
    return;
  }

  if (url.pathname === "/api/examples") {
    sendJson(res, 200, buildAdapterExamples());
    return;
  }

  if (url.pathname === "/") {
    sendFile(res, path.join(PUBLIC_DIR, "index.html"), "text/html; charset=utf-8");
    return;
  }
  if (url.pathname === "/styles.css") {
    sendFile(res, path.join(PUBLIC_DIR, "styles.css"), "text/css; charset=utf-8");
    return;
  }
  if (url.pathname === "/app.js") {
    sendFile(res, path.join(PUBLIC_DIR, "app.js"), "application/javascript; charset=utf-8");
    return;
  }

  sendJson(res, 404, { error: "Not found", path: url.pathname });
});

function listenWithFallback(startPort, attempt = 0) {
  const port = startPort + attempt;
  server.removeAllListeners("error");
  server.removeAllListeners("listening");

  server.once("listening", () => {
    console.log(`DNA-DB basic dashboard listening on http://localhost:${port}`);
  });

  server.once("error", (error) => {
    if (error && error.code === "EADDRINUSE" && attempt < MAX_PORT_PROBES) {
      console.warn(`Port ${port} in use, trying ${port + 1}...`);
      listenWithFallback(startPort, attempt + 1);
      return;
    }
    throw error;
  });

  server.listen(port);
}

listenWithFallback(PORT);
