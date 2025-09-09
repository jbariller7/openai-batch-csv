// netlify/functions/direct-worker-background.js
// Background Function: processes a Direct job and writes results/{jobId}.csv

import OpenAI from "openai";
import { getStore } from "@netlify/blobs";
import { parse as csvParse } from "csv-parse";
import { stringify as csvStringify } from "csv-stringify";

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// Optional: cap concurrency via env
const MAX_DIRECT_CONCURRENCY = Number(process.env.MAX_DIRECT_CONCURRENCY || 8);
// Some models (gpt-5 family) don't support temperature
const supportsTemperature = (name) => !/^gpt-5(\b|[-_])/.test(name);

export const config = { /* path: "/api/direct-worker-background" */ };

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export default async function handler(req) {
  if (req.method !== "POST") return json({ error: "POST only" }, 405);

  try {
    const { jobId } = await req.json();
    if (!jobId) return json({ error: "Missing jobId" }, 400);

    const store = getStore("openai-batch-csv");
    const meta = await store.getJSON(`jobs/${jobId}.json`);
    if (!meta) return json({ error: "Job metadata not found" }, 404);

    const {
      model,
      prompt,
      inputCol = "text",
      chunkSize = 200,
      reasoningEffort = "",
    } = meta;

    const supportsReasoning =
      /^o\d/i.test(model) || model.startsWith("o") || model.startsWith("gpt-5");

    // Read original CSV
    const csvBuf = await store.get(`csv/${jobId}.csv`, { type: "buffer" });
    if (!csvBuf) return json({ error: "Original CSV not found" }, 404);

    const rows = await new Promise((resolve, reject) => {
      const out = [];
      csvParse(csvBuf, { columns: true, relax_quotes: true, bom: true, skip_empty_lines: true })
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
      return {
        model,
        input: [
          { role: "system", content: `${prompt}${suffix}` },
          { role: "user", content: "Return only a json object as specified. The output must be valid json." },
          { role: "user", content: JSON.stringify({ rows: rowsChunk }) }
        ],
        text: { format: { type: "json_object" } },
        ...(supportsTemperature(model) ? { temperature: 0 } : {}),
        ...(supportsReasoning && reasoningEffort ? { reasoning: { effort: reasoningEffort } } : {}),
      };
    }

    // Chunk the work
    const chunks = [];
    for (let i = 0; i < items.length; i += chunkSize) chunks.push(items.slice(i, i + chunkSize));

    // Simple retry with backoff (helps if you raise concurrency)
    async function callWithRetry(fn, { retries = 5, base = 300, max = 5000 } = {}) {
      let attempt = 0;
      for (;;) {
        try {
          return await fn();
        } catch (err) {
          const status = err?.status || err?.statusCode || err?.response?.status;
          const retriable = status === 429 || (status >= 500 && status < 600) || !status;
          if (attempt >= retries || !retriable) throw err;
          const delay = Math.min(max, base * Math.pow(2, attempt)) * (0.5 + Math.random());
          await new Promise(r => setTimeout(r, delay));
          attempt++;
        }
      }
    }

    const concurrency = Math.max(1, Math.min(MAX_DIRECT_CONCURRENCY, Number(meta.concurrency || 4)));
    let i = 0;
    const parts = new Array(chunks.length);

    async function worker() {
      while (i < chunks.length) {
        const my = i++;
        const resp = await callWithRetry(() => client.responses.create(buildBody(chunks[my])));
        let parsed = null;
        try { parsed = JSON.parse(resp.output_text || ""); } catch {}
        parts[my] = parsed;
      }
    }
    await Promise.all(Array.from({ length: concurrency }, worker));

    // Merge results
    const mergedRows = rows.map((r) => ({ ...r, result: "" }));
    for (const part of parts) {
      if (!part?.results) continue;
      for (const item of part.results) {
        const idx = Number(item?.id);
        if (Number.isFinite(idx) && idx >= 0 && idx < mergedRows.length) {
          mergedRows[idx].result = typeof item?.result === "string" ? item.result : (item?.result != null ? String(item.result) : "");
        }
      }
    }

    // Write CSV
    const headers = Object.keys(mergedRows[0] || {});
    const csvStr = await new Promise((resolve, reject) => {
      csvStringify(mergedRows, { header: true, columns: headers }, (err, out) => {
        if (err) reject(err);
        else resolve(out);
      });
    });

    await store.set(`results/${jobId}.csv`, csvStr, { contentType: "text/csv; charset=utf-8" });
    await store.setJSON(`jobs/${jobId}.status.json`, { jobId, status: "ready", finishedAt: new Date().toISOString() });

    // Background functions return immediately anyway
    return json({ ok: true, jobId, rows: rows.length }, 202);
  } catch (err) {
    console.error("direct-worker-background error:", err);
    return json({ error: err?.message || String(err) }, 500);
  }
}
