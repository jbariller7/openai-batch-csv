// netlify/functions/direct-worker-background.js (CommonJS + Lambda-style)

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
  const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
const token  = process.env.NETLIFY_BLOBS_TOKEN || process.env.NETLIFY_AUTH_TOKEN;
const store  = (siteID && token)
  ? getStore({ name: "openai-batch-csv", siteID, token })
  : getStore("openai-batch-csv");

  let jobId = "";

  async function writeStatus(status, extra = {}) {
    if (!jobId) return;
    const payload = { jobId, status, updatedAt: new Date().toISOString(), ...extra };
    try {
      await store.set(`jobs/${jobId}.status.json`, JSON.stringify(payload), { contentType: "application/json" });
    } catch {}
  }

  try {
    const body = await readJson(event);
    jobId = body?.jobId || "";
    if (!jobId) return res(400, { error: "Missing jobId" });

    await writeStatus("running");

    const meta = await store.get(`jobs/${jobId}.json`, { type: "json" }).catch(() => null);
    if (!meta) {
      await writeStatus("failed", { error: "Job metadata not found" });
      return res(404, { error: "Job metadata not found" });
    }

    const { model, prompt, inputCol = "text", chunkSize = 200, reasoningEffort = "", concurrency: desiredConcurrency = 4 } = meta;
    const supportsReasoning = /^o\d/i.test(model) || model.startsWith("o") || model.startsWith("gpt-5");

    const csvTxt = await store.get(`csv/${jobId}.csv`, { type: "text" }).catch(() => null);
    if (!csvTxt) {
      await writeStatus("failed", { error: "Original CSV not found" });
      return res(404, { error: "Original CSV not found" });
    }

    const rows = await new Promise((resolve, reject) => {
      const out = [];
      csvParse(csvTxt, { columns: true, relax_quotes: true, bom: true, skip_empty_lines: true })
        .on("data", (r) => out.push(r))
        .on("end", () => resolve(out))
        .on("error", reject);
    });

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

    // Chunk & run
    const chunks = [];
    for (let i = 0; i < items.length; i += chunkSize) chunks.push(items.slice(i, i + chunkSize));

    async function callWithRetry(fn, { retries = 5, base = 300, max = 5000 } = {}) {
      let attempt = 0;
      for (;;) {
        try { return await fn(); }
        catch (err) {
          const status = err?.status || err?.statusCode || err?.response?.status;
          const retriable = status === 429 || (status >= 500 && status < 600) || !status;
          if (attempt >= retries || !retriable) throw err;
          const delay = Math.min(max, base * Math.pow(2, attempt)) * (0.5 + Math.random());
          await new Promise(r => setTimeout(r, delay));
          attempt++;
        }
      }
    }

    const concurrency = Math.max(1, Math.min(MAX_DIRECT_CONCURRENCY, Number(desiredConcurrency || 4)));
    let i = 0;
    const parts = new Array(chunks.length);

    async function worker() {
      while (i < chunks.length) {
        const my = i++;
        const resp = await callWithRetry(() => client.responses.create(buildBody(chunks[my])));
        let parsed = null; try { parsed = JSON.parse(resp.output_text || ""); } catch {}
        parts[my] = parsed;
      }
    }
    await Promise.all(Array.from({ length: concurrency }, worker));

    // Merge → CSV
    const merged = rows.map((r) => ({ ...r, result: "" }));
    for (const part of parts) {
      if (!part?.results) continue;
      for (const item of part.results) {
        const idx = Number(item?.id);
        if (Number.isFinite(idx) && idx >= 0 && idx < merged.length) {
          merged[idx].result = typeof item?.result === "string" ? item.result :
            (item?.result != null ? String(item.result) : "");
        }
      }
    }

    const headers = Object.keys(merged[0] || {});
    const csvStr = await new Promise((resolve, reject) => {
      csvStringify(merged, { header: true, columns: headers }, (err, out) => err ? reject(err) : resolve(out));
    });

    await store.set(`results/${jobId}.csv`, csvStr, { contentType: "text/csv; charset=utf-8" });
    await writeStatus("ready", { finishedAt: new Date().toISOString() });

    // For background functions, Netlify replies 202 regardless; returning 202 is fine.
    return res(202, { ok: true, jobId, rows: rows.length });
  } catch (err) {
    console.error("direct-worker-background error:", err);
    try {
      await (await import("@netlify/blobs")).getStore("openai-batch-csv").set(
        `jobs/${jobId}.status.json`,
        JSON.stringify({ jobId, status: "failed", error: err?.message || String(err), updatedAt: new Date().toISOString() }),
        { contentType: "application/json" }
      );
    } catch {}
    return res(500, { error: err?.message || String(err) });
  }
};
