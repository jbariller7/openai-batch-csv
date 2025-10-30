// netlify/functions/direct-worker-background.js
// CommonJS + Lambda-style + resumable processing + monotonic status + lock

const { parse: csvParse } = require("csv-parse");
const { stringify: csvStringify } = require("csv-stringify");

exports.config = { /* path: "/api/direct-worker-background" */ };

const MAX_DIRECT_CONCURRENCY = Number(process.env.MAX_DIRECT_CONCURRENCY || 8);
const LOCK_TTL_MS = 15 * 60 * 1000; // 15 minutes
const LOCK_HEARTBEAT_MS = 60 * 1000; // refresh lock every 60s

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
function ensureUtf8Bom(str) {
  return str && !str.startsWith("\uFEFF") ? "\uFEFF" + str : str;
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
  let lockTimer = null;

  // ---- status writer with monotonic counters ----
  async function writeStatus(status, extra = {}, message) {
    if (!jobId) return;
    const now = new Date().toISOString();
    let prev = null;
    try { prev = await store.get(`jobs/${jobId}.status.json`, { type: "json" }); } catch {}
    const prevEvents = Array.isArray(prev?.events) ? prev.events.slice(-49) : [];
    if (message) prevEvents.push({ ts: now, msg: message });

    // enforce monotonic counters
    const prevCompleted = Number(prev?.completedChunks || 0);
    const prevProcessed = Number(prev?.processedRows || 0);
    const nextCompleted = Math.max(prevCompleted, Number(extra?.completedChunks ?? prevCompleted));
    const nextProcessed = Math.max(prevProcessed, Number(extra?.processedRows ?? prevProcessed));

    const payload = {
      jobId,
      status: status || prev?.status || "running",
      updatedAt: now,
      totalChunks: Number(extra?.totalChunks ?? prev?.totalChunks ?? 0),
      concurrency: Number(extra?.concurrency ?? prev?.concurrency ?? 0),
      completedChunks: nextCompleted,
      processedRows: nextProcessed,
      partial: Boolean(extra?.partial || prev?.partial || false),
      lastErrorStatus: extra?.lastErrorStatus ?? prev?.lastErrorStatus,
      events: prevEvents,
    };

    try {
      await store.set(`jobs/${jobId}.status.json`, JSON.stringify(payload), {
        contentType: "application/json",
      });
    } catch {}
  }

  // ---- simple lock helpers to prevent double workers ----
  async function acquireLock() {
    const now = Date.now();
    let lock = null;
    try { lock = await store.get(`jobs/${jobId}.lock.json`, { type: "json" }); } catch {}
    const lockTs = lock?.ts ? new Date(lock.ts).getTime() : 0;
    const live = lockTs && (now - lockTs) < LOCK_TTL_MS;
    if (live) {
      await writeStatus("running", {}, "worker: another active worker holds lock, exiting");
      return false;
    }
    await store.set(`jobs/${jobId}.lock.json`, JSON.stringify({ ts: new Date().toISOString() }), { contentType: "application/json" });
    // start heartbeat
    lockTimer = setInterval(async () => {
      try {
        await store.set(`jobs/${jobId}.lock.json`, JSON.stringify({ ts: new Date().toISOString() }), { contentType: "application/json" });
      } catch {}
    }, LOCK_HEARTBEAT_MS);
    return true;
  }
  async function releaseLock() {
    if (lockTimer) clearInterval(lockTimer);
    lockTimer = null;
    try { await store.delete?.(`jobs/${jobId}.lock.json`); } catch {}
  }

  // ---- retry helper ----
  async function callWithRetry(fn, { retries = 5, base = 300, max = 5000, label = "" } = {}) {
    let attempt = 0;
    for (;;) {
      try {
        if (attempt > 0) await writeStatus("running", {}, `${label}: retry ${attempt}`);
        return await fn();
      } catch (err) {
        let errMsg = err?.message || String(err);
        try {
          const data = await err?.response?.json?.();
          if (data?.error?.message) errMsg = data.error.message;
        } catch {}
        const status = err?.status || err?.statusCode || err?.response?.status;
        const retriable = status === 429 || (status >= 500 && status < 600) || !status;
        if (attempt >= retries || !retriable) {
          await writeStatus("failed", { lastErrorStatus: status }, `${label}: giving up after ${attempt} retries -> ${errMsg}`);
          throw err;
        }
        const delay = Math.min(max, base * Math.pow(2, attempt)) * (0.5 + Math.random());
        await writeStatus("running", { lastErrorStatus: status }, `${label}: ${status || "err"} -> backoff ${Math.round(delay)}ms`);
        await new Promise(r => setTimeout(r, delay));
        attempt++;
      }
    }
  }

  try {
    const body = await readJson(event);
    jobId = body?.jobId || "";
    if (!jobId) return res(400, { error: "Missing jobId" });

    // Acquire lock or exit if another worker is active
    const locked = await acquireLock();
    if (!locked) return res(202, { ok: true, ignored: true, jobId });

    await writeStatus("running", {}, "worker: start");

    // Load job meta saved by batch-create
    const meta = await store.get(`jobs/${jobId}.json`, { type: "json" }).catch(() => null);
    if (!meta) {
      await writeStatus("failed", {}, "meta not found");
      await releaseLock();
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
      await releaseLock();
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

    // Instruction suffix
    const suffix =
      ' You will receive a json object {"rows":[{"id":number,"text":string},...]}.' +
      ' For each item, produce {"id": same id, "cols": { /* one or more named columns */ }} following the user instructions above.' +
      ' The "cols" object must contain only string values (no nested objects/arrays).' +
      ' The output must be valid json. Return ONLY a json object exactly like: {"results":[{"id":number,"cols":{...}},...]} in the SAME ORDER as input.' +
      ' Do not include any commentary.';

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

    // Build chunks
    const chunks = [];
    for (let i = 0; i < items.length; i += chunkSize) chunks.push(items.slice(i, i + chunkSize));
    const totalChunks = chunks.length;

    // Resume detection - list already finished partials
    const done = new Array(totalChunks).fill(false);
    let completedChunks = 0;
    try {
      for await (const entry of store.list({ prefix: `partials/${jobId}/` })) {
        const m = entry?.key && entry.key.match(/\/(\d+)\.json$/);
        if (m) {
          const idx = Number(m[1]);
          if (Number.isFinite(idx) && idx >= 0 && idx < totalChunks && !done[idx]) {
            done[idx] = true;
            completedChunks++;
          }
        }
      }
    } catch {}
    const processedRowsInit = Math.min(completedChunks * chunkSize, items.length);

    // Initial status with resume info
    const concurrency = Math.max(1, Math.min(MAX_DIRECT_CONCURRENCY, Number(desiredConcurrency || 4)));
    await writeStatus("running",
      { totalChunks, concurrency, completedChunks, processedRows: processedRowsInit, partial: completedChunks > 0 },
      `plan: ${totalChunks} chunks @ conc=${concurrency} - resume ${completedChunks}/${totalChunks}`
    );

    // parts array to merge at the end, fill with nulls and keep already done as sentinel
    const parts = new Array(totalChunks);
    // pickNext returns the next unfinished index
    let nextIdx = 0;
    function pickNext() {
      while (nextIdx < totalChunks && done[nextIdx]) nextIdx++;
      if (nextIdx >= totalChunks) return -1;
      return nextIdx++;
    }

    async function doChunk(idx) {
      const label = `chunk#${idx + 1}`;
      const resp = await callWithRetry(
        () => client.responses.create(buildBody(chunks[idx])),
        { label }
      );
      let parsed = null;
      try { parsed = JSON.parse(resp.output_text || ""); } catch {}
      parts[idx] = parsed;

      // persist partial
      try {
        await store.set(
          `partials/${jobId}/${idx}.json`,
          JSON.stringify(parsed || {}),
          { contentType: "application/json" }
        );
      } catch {}

      done[idx] = true;
      completedChunks++;
      const processedRows = Math.min(completedChunks * chunkSize, items.length);
      await writeStatus(
        "running",
        { completedChunks, totalChunks, processedRows, partial: true },
        `${label} done (${chunks[idx].length} rows) -> ${completedChunks}/${totalChunks}`
      );
    }

    async function worker() {
      // refresh lock regularly while this worker runs
      const beat = setInterval(async () => {
        try {
          await store.set(`jobs/${jobId}.lock.json`, JSON.stringify({ ts: new Date().toISOString() }), { contentType: "application/json" });
        } catch {}
      }, LOCK_HEARTBEAT_MS);
      try {
        for (;;) {
          const idx = pickNext();
          if (idx === -1) break;
          await doChunk(idx);
        }
      } finally {
        clearInterval(beat);
      }
    }

    // Spawn workers
    await Promise.all(Array.from({ length: concurrency }, () => worker()));

    // Merge results to CSV
    const merged = rows.map((r) => ({ ...r }));
    const colSet = new Set();

    for (let idx = 0; idx < totalChunks; idx++) {
      const part = parts[idx] || await store.get(`partials/${jobId}/${idx}.json`, { type: "json" }).catch(() => null);
      if (!part) continue;
      const arr = Array.isArray(part.results) ? part.results
                : (Array.isArray(part) ? part : null);
      if (!arr) continue;

      for (const item of arr) {
        const outIdx = Number(item?.id);
        if (!Number.isFinite(outIdx) || outIdx < 0 || outIdx >= merged.length) continue;

        if (item && typeof item.cols === "object" && item.cols !== null) {
          for (const [k, v] of Object.entries(item.cols)) {
            const vv = (v == null) ? "" : String(v);
            merged[outIdx][k] = vv;
            colSet.add(k);
          }
        } else if (typeof item?.result === "string") {
          merged[outIdx].result = item.result;
          colSet.add("result");
        }
      }
    }

    const originalHeaders = Object.keys(rows[0] || {});
    const dynamicHeaders = Array.from(colSet);
    const headers = [...originalHeaders, ...dynamicHeaders];

const csvStr = await new Promise((resolve, reject) => {
  csvStringify(
    merged,
    {
      header: true,
      columns: headers,
      // use backticks for quoting
      quote: '`',
      escape: '`',
    },
    (err, out) => (err ? reject(err) : resolve(out))
  );
});

    const csvWithBom = ensureUtf8Bom(csvStr);

    await store.set(`results/${jobId}.csv`, csvWithBom, { contentType: "text/csv; charset=utf-8" });
    await writeStatus("ready", { finishedAt: new Date().toISOString(), partial: true, completedChunks: totalChunks, processedRows: items.length }, "csv written: ready");

    await releaseLock();
    return res(202, { ok: true, jobId, rows: rows.length });
  } catch (err) {
    console.error("direct-worker-background error:", err);
    try { await writeStatus("failed", {}, `fatal: ${err?.message || String(err)}`); } catch {}
    try { await releaseLock(); } catch {}
    return res(500, { error: err?.message || String(err) });
  }
};
