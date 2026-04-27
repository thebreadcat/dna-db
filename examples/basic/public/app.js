const $ = (id) => document.getElementById(id);

const FORMAT = new Intl.NumberFormat("en-US");

function fmt(value, digits = 2) {
  if (typeof value !== "number" || Number.isNaN(value)) return "-";
  return value.toFixed(digits);
}

function card(label, value) {
  return `<article class="stat"><div class="label">${label}</div><div class="value">${value}</div></article>`;
}

function highlightCode(id, code, language) {
  const el = $(id);
  el.className = language ? `language-${language}` : "";
  el.textContent = code;
  if (window.hljs) {
    window.hljs.highlightElement(el);
  }
}

function renderBenchmarks(payload) {
  const benches = payload.benchmarks || [];
  const valid = benches.filter((row) => !row.parse_error);
  const latest = valid[valid.length - 1];

  const statGrid = $("stat-grid");
  if (!latest) {
    const searched = (payload.searched_dirs || []).join(", ");
    $("bench-status").innerHTML = `<span class="bad">No readable benchmark JSON files found. Searched: ${searched}</span>`;
    statGrid.innerHTML = "";
    $("bench-table").innerHTML = `<tr><td colspan="8">No benchmark rows.</td></tr>`;
    return;
  }

  const parseErrors = benches.length - valid.length;
  $("bench-status").textContent =
    parseErrors > 0
      ? `Loaded ${valid.length}/${benches.length} readable benchmark file(s) from ${payload.source_dir} (${parseErrors} malformed).`
      : `Loaded ${valid.length} benchmark file(s) from ${payload.source_dir}`;

  statGrid.innerHTML = [
    card("Mode", latest.mode),
    card("Phase", latest.phase),
    card("Records", FORMAT.format(latest.records)),
    card("Write/s", FORMAT.format(Math.round(latest.write_records_per_sec))),
    card("Decode/s", FORMAT.format(Math.round(latest.decode_records_per_sec))),
    card("Point read/s", FORMAT.format(Math.round(latest.point_reads_per_sec))),
    card("Query sec", fmt(latest.query_seconds, 4)),
    card("Full scan sec", fmt(latest.full_scan_seconds, 4)),
  ].join("");

  $("bench-table").innerHTML = benches
    .map((row) => {
      if (row.parse_error) {
        return `<tr><td>${row.file}</td><td colspan="7" class="bad">${row.parse_error}</td></tr>`;
      }
      return `<tr>
        <td>${row.file}</td>
        <td>${row.mode}</td>
        <td>${row.phase}</td>
        <td>${FORMAT.format(row.records)}</td>
        <td>${FORMAT.format(Math.round(row.write_records_per_sec))}</td>
        <td>${FORMAT.format(Math.round(row.decode_records_per_sec))}</td>
        <td>${FORMAT.format(Math.round(row.point_reads_per_sec))}</td>
        <td>${fmt(row.query_seconds, 4)}</td>
      </tr>`;
    })
    .join("");
}

function renderExamples(payload) {
  highlightCode("logical-view", JSON.stringify(payload.logicalQuery, null, 2), "json");
  highlightCode("response-view", JSON.stringify(payload.response, null, 2), "json");

  const variants = [
    ["TypeScript", payload.typescript, "typescript"],
    ["Postgres", payload.postgres, "sql"],
    ["GraphQL", payload.graphql, "graphql"],
    ["Mongo", payload.mongodb, "javascript"],
  ];

  let active = 0;
  const tabs = $("tabs");
  const codeView = $("code-view");

  function repaint() {
    tabs.innerHTML = variants
      .map(
        ([name], idx) =>
          `<button class="tab ${idx === active ? "active" : ""}" data-idx="${idx}">${name}</button>`
      )
      .join("");
    highlightCode("code-view", variants[active][1], variants[active][2]);
    tabs.querySelectorAll("button").forEach((btn) => {
      btn.addEventListener("click", () => {
        active = Number(btn.dataset.idx || 0);
        repaint();
      });
    });
  }

  repaint();
}

async function boot() {
  const [benchmarks, examples] = await Promise.all([
    fetch("/api/benchmarks").then((r) => r.json()),
    fetch("/api/examples").then((r) => r.json()),
  ]);
  renderBenchmarks(benchmarks);
  renderExamples(examples);
}

boot().catch((err) => {
  $("bench-status").innerHTML = `<span class="bad">Dashboard failed to load: ${String(err)}</span>`;
});
