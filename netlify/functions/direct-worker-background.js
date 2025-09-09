// netlify/functions/direct-worker-background.js (CommonJS + Lambda-style + detailed logs)

const { parse: csvParse } = require("csv-parse");
const { stringify: csvStringify } = require("csv-stringify");

exports.config = { /* path: "/api/direct-worker-background" */ };

const MAX_DIRECT_CONCURRENCY = Number(process.env.MAX_DIRECT_CONCURRENCY || 8);
const supportsTemperature = (name) => !/^gpt-5(\b|[-_])/.test(name);

function res(statusCode, bodyObj) {
  return { statusCode, headers: { "Content-Type": "application/json" }, body: JSON.stringify(bodyObj ?? {}) };
}
async function readJson(event) {
  if (event && typeof event.json === "function") { try { return await event.json(); } catch {} }
  const b64 = !!event?.isBase64Encoded;
  const raw = event?.body || "";
  if (!raw) return {};
  try { return JSON.parse(b64 ? Buffer.from(raw, "base64").toString("utf8") : raw); } catch { return {}; }
}

exports.handler = async function (event) {
  // ESM deps
  const { getStore } = await import("@netlify/blobs");
  const { default: OpenAI } = await import("openai");
  const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  // Blobs (with optional manual site creds)
  const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
  const token  = process.env.NETLIFY_BLOBS_TOKEN || process.env.NETLIFY_AUTH_TOKEN;
  const store  = (siteID && token)
    ? getStore({ name: "openai-batch-csv", siteID, token })
    : getStore("openai-batch-csv");

  let jobId = "";

  // ---- status+log writer (keeps last 50 events) ----
  async function writeStatus(status, extra = {}, message) {
    if (!jobId) return;
    const now = new Date().toISOString();
    let prev = null;
    try { prev = await store.get(`jobs/${jobId}.status.json`, { type: "json" }); } catch {}
    const events = Array.isArray(prev?.events) ? prev.events.slice(-49) : [];
    if (message) events.push({ ts: now, msg: message });
    const payload = {
      jobId,
      status,                           // "running" | "ready" | "failed"
      updatedAt: now,
      // carry forward anything useful, then overwrite with latest fields
      ...prev,
      ...extra,
      events,
    };
    try {
      await store.set(`jobs/${jobId}.status.json`, JSON.stringify(payload), {
        contentType: "application/json",
      });
    } catch {}
  }

  // ---- retry helper (logs retries) ----
  async function callWithRetry(fn, { retries = 5, base = 300, max = 5000, label = "" } = {}) {
    let attempt = 0;
    for (;;) {
      try {
        if (attempt > 0) await writeStatus("running", {}, `${label}: retry ${attempt}`);
        return await fn();
      } catch (err) {
        const status = err?.status || err?.statusCode || err?.response?.status;
        const retriable = status === 429 || (status >= 500 && status < 600) || !status;
        if (attempt >= retries || !retriable) {
          await writeStatus("failed", { lastErrorStatus: status }, `${label}: giving up after ${attempt} retries → ${err?.message || err}`);
          throw err;
        }
        const delay = Math.min(max, base * Math.pow(2, attempt)) * (0.5 + Math.random());
        await writeStatus("running", { lastErrorStatus: status }, `${label}: ${status || "err"} → backoff ${Math.round(delay)}ms`);
        await new Promise(r => setTimeout(r, delay));
        attempt++;
      }
    }
  }

  try {
    const body = await readJson(event);
    jobId = body?.jobId || "";
    if (!jobId) return res(400, { error: "Missing jobId" });

    await writeStatus("running", {}, "worker: start");

    // Load job meta saved by batch-create
    const meta = await store.get(`jobs/${jobId}.json`, { type: "json" }).catch(() => null);
    if (!meta) {
      await writeStatus("failed", {}, "meta not found");
      return res(404, { error: "Job metadata not found" });
    }

    const {
      model,
      prompt,
      inputCol = "text",
      chunkSize = 200,
      reasoningEffort = "",
      concurrency: desiredConcurrency = 4,
    } = meta;

    const supportsReasoning = /^o\d/i.test(model) || model.startsWith("o") || model.startsWith("gpt-5");
    await writeStatus("running", { model, chunkSize, desiredConcurrency }, `meta loaded: model=${model}, K=${chunkSize}, conc=${desiredConcurrency}`);

    // Read original CSV
    const csvTxt = await store.get(`csv/${jobId}.csv`, { type: "text" }).catch(() => null);
    if (!csvTxt) {
      await writeStatus("failed", {}, "csv missing");
      return res(404, { error: "Original CSV not found" });
    }

    const rows = await new Promise((resolve, reject) => {
      const out = [];
      csvParse(csvTxt, { columns: true, relax_quotes: true, bom: true, skip_empty_lines: true })
        .on("data", (r) => out.push(r))
        .on("end", () => resolve(out))
        .on("error", reject);
    });
    await writeStatus("running", { rowCount: rows.length }, `csv parsed: ${rows.length} rows`);

    const items = rows.map((r, idx) => ({ id: idx, text: String(r?.[inputCol] ?? "") }));

    const suffix =
      ' You will receive a json object {"rows":[{"id":number,"text":string},...]}.'
      + ' For each item, produce {"id": same id, "result": <string>} following the user instructions above.'
      + ' The output must be valid json. Return ONLY a json object exactly like: {"results":[{"id":number,"result":string},...]}'
      + ' in the SAME ORDER as input. Do not include any commentary.';

    function buildBody(rowsChunk) {
      const b = {
        model,
        input: [
          { role: "system", content: `${prompt}${suffix}` },
          { role: "user", content: "Return only a json object as specified. The output must be valid json." },
          { role: "user", content: JSON.stringify({ rows: rowsChunk }) }
        ],
        text: { format: { type: "json_object" } },
      };
      if (supportsTemperature(model)) b.temperature = 0;
      if (supportsReasoning && reasoningEffort) b.reasoning = { effort: reasoningEffort };
      return b;
    }

    // Chunk + plan
    const chunks = [];
    for (let i = 0; i < items.length; i += chunkSize) chunks.push(items.slice(i, i + chunkSize));
    const totalChunks = chunks.length;
    const concurrency = Math.max(1, Math.min(MAX_DIRECT_CONCURRENCY, Number(desiredConcurrency || 4)));
    await writeStatus("running", { totalChunks, concurrency }, `plan: ${totalChunks} chunks @ conc=${concurrency}`);

    // Run
    let completedChunks = 0;
    let i = 0;
    const parts = new Array(totalChunks);

    async function worker(wid) {
      while (i < totalChunks) {
        const my = i++;
        const label = `chunk#${my + 1}`;
        const resp = await callWithRetry(() => client.responses.create(buildBody(chunks[my])), { label });
        let parsed = null; try { parsed = JSON.parse(resp.output_text || ""); } catch {}
        parts[my] = parsed;
        completedChunks++;
        const processedRows = Math.min(completedChunks * chunkSize, items.length);
        // update every chunk (small jobs) or every 3 (bigger)
        if (totalChunks <= 50 || completedChunks % 3 === 0 || completedChunks === totalChunks) {
          await writeStatus("running",
            { completedChunks, totalChunks, processedRows },
            `${label} done (${chunks[my].length} rows) → ${completedChunks}/${totalChunks}`
          );
        }
      }
    }
    await Promise.all(Array.from({ length: concurrency }, (_, k) => worker(k)));

    // Merge → CSV
    const merged = rows.map((r) => ({ ...r, result: "" }));
    for (const part of parts) {
      if (!part?.results) continue;
      for (const item of part.results) {
        const idx = Number(item?.id);
        if (Number.isFinite(idx) && idx >= 0 && idx < merged.length) {
          merged[idx].result =
            typeof item?.result === "string" ? item.result :
            (item?.result != null ? String(item.result) : "");
        }
      }
    }

    const headers = Object.keys(merged[0] || {});
    const csvStr = await new Promise((resolve, reject) => {
      csvStringify(merged, { header: true, columns: headers }, (err, out) => err ? reject(err) : resolve(out));
    });

    await store.set(`results/${jobId}.csv`, csvStr, { contentType: "text/csv; charset=utf-8" });
    await writeStatus("ready", { finishedAt: new Date().toISOString() }, "csv written: ready");

    return res(202, { ok: true, jobId, rows: rows.length });
  } catch (err) {
    console.error("direct-worker-background error:", err);
    try {
      await writeStatus("failed", {}, `fatal: ${err?.message || String(err)}`);
    } catch {}
    return res(500, { error: err?.message || String(err) });
  }
};
