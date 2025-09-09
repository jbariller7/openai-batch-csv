// netlify/functions/batch-download.js (CommonJS + Lambda-style + partial support)

const { parse: csvParse } = require("csv-parse");
const { stringify: csvStringify } = require("csv-stringify");

exports.config = { /* path: "/api/batch-download" */ };

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,OPTIONS,HEAD",
  "Access-Control-Allow-Headers": "Content-Type",
};

function res(statusCode, body, headers) {
  return {
    statusCode,
    headers: { ...(headers || {}), ...CORS },
    body: typeof body === "string" ? body : JSON.stringify(body ?? {}),
  };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS" || event.httpMethod === "HEAD") {
    return { statusCode: 204, headers: CORS, body: "" };
  }

  // Parse query once (avoid duplicate declarations)
  const rawUrl = typeof event?.rawUrl === "string" ? event.rawUrl : "";
  const url = rawUrl ? new URL(rawUrl) : null;
  const id = url?.searchParams.get("id") || event?.queryStringParameters?.id || "";
  const wantPartial = (url?.searchParams.get("partial") === "1");

  if (!id) return res(400, { error: "Missing id" });

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

  try {
    // ---------- DIRECT MODE: full CSV already written ----------
    let directCsvText = null;
    try { directCsvText = await store.get(`results/${id}.csv`, { type: "text" }); } catch {}
    if (typeof directCsvText === "string" && !wantPartial) {
      return res(200, directCsvText, {
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Disposition": `attachment; filename="${id}.csv"`,
      });
    }

    // ---------- DIRECT MODE: partial download requested ----------
    if (wantPartial) {
      // Identify if `id` is a Direct job (no batchId in meta)
      const meta = await store.get(`jobs/${id}.json`, { type: "json" }).catch(() => null);
      if (meta && !meta.batchId) {
        // Read original CSV rows
        const csvTxt = await store.get(`csv/${id}.csv`, { type: "text" }).catch(() => null);
        if (!csvTxt) return res(404, { error: "Original CSV not found" });

        const rows = await new Promise((resolve, reject) => {
          const out = [];
          csvParse(csvTxt, { columns: true, relax_quotes: true })
            .on("data", (r) => out.push(r))
            .on("end", () => resolve(out))
            .on("error", reject);
        });

        // Read status for bounds
        let statusJson = null;
        try { statusJson = await store.get(`jobs/${id}.status.json`, { type: "json" }); } catch {}
        const K = Number(meta?.chunkSize || 200);
        const totalChunks = Number(statusJson?.totalChunks || Math.ceil(rows.length / K));
        const completedChunks = Number(statusJson?.completedChunks || 0);

        // Merge available partials
        const merged = rows.map((r) => ({ ...r, result: "" }));
        const upper = completedChunks || totalChunks || 0;
        for (let i = 0; i < upper; i++) {
          let part = null;
          try { part = await store.get(`partials/${id}/${i}.json`, { type: "json" }); } catch {}
          if (!part) continue;

          const arr = Array.isArray(part?.results) ? part.results
                   : (Array.isArray(part) ? part : null);
          if (!arr) continue;

          for (const item of arr) {
            const idx = Number(item?.id);
            if (Number.isFinite(idx) && idx >= 0 && idx < merged.length) {
              merged[idx].result =
                typeof item?.result === "string"
                  ? item.result
                  : (item?.result != null ? String(item.result) : "");
            }
          }
        }

        // Serialize partial CSV
        const headers = Object.keys(merged[0] || {});
        const csvStr = await new Promise((resolve, reject) => {
          csvStringify(merged, { header: true, columns: headers }, (err, out) => err ? reject(err) : resolve(out));
        });

        return res(200, csvStr, {
          "Content-Type": "text/csv; charset=utf-8",
          "Content-Disposition": `attachment; filename="${id}.partial.csv"`,
          "X-Partial": "1",
        });
      }
      // If it's not a Direct job, fall through to Batch path below
    }

    // ---------- BATCH MODE: wait for completion, then merge ----------
    const b = await client.batches.retrieve(id);
    if (b.status !== "completed") {
      return res(400, { error: `Batch not completed. Status: ${b.status}` });
    }
    if (!b.output_file_id) {
      return res(400, { error: "No output file id" });
    }

    // Load job meta (to locate original CSV)
    const meta = await store.get(`jobs/${id}.json`, { type: "json" }).catch(() => null);
    if (!meta) return res(404, { error: "Job metadata not found" });

    // Read original CSV rows (Batch path stores original under jobId in meta)
    const origCsvTxt = await store.get(`csv/${meta.jobId}.csv`, { type: "text" }).catch(() => null);
    if (!origCsvTxt) return res(404, { error: "Original CSV not found" });

    const rows = await new Promise((resolve, reject) => {
      const out = [];
      csvParse(origCsvTxt, { columns: true, relax_quotes: true })
        .on("data", (r) => out.push(r))
        .on("end", () => resolve(out))
        .on("error", reject);
    });

    // Download batch output JSONL
    const fileResp = await client.files.content(b.output_file_id);
    const outputBuf = Buffer.from(await fileResp.arrayBuffer());
    const jsonlLines = outputBuf.toString("utf8").trim().split("\n");

    // Prepare merged rows (copy original + result column)
    const merged = rows.map((r) => ({ ...r, result: "" }));

    const assignByArray = (arr, baseIndex = 0) => {
      arr.forEach((item, j) => {
        const idx = Number.isFinite(Number(item?.id)) ? Number(item.id) : baseIndex + j;
        if (idx >= 0 && idx < merged.length) {
          merged[idx].result =
            typeof item?.result === "string"
              ? item.result
              : (item?.toString?.() ?? "");
        }
      });
    };

    // Each JSONL line = one micro-batch response
    for (const line of jsonlLines) {
      if (!line.trim()) continue;

      let obj; try { obj = JSON.parse(line); } catch { continue; }

      const base = Number(obj?.custom_id) || 0;
      const body = obj?.response?.body || {};
      const text =
        typeof body?.output_text === "string"
          ? body.output_text
          : (typeof body?.content === "string" ? body.content : "");

      let parsed = null;
      if (text) { try { parsed = JSON.parse(text); } catch {} }

      if (parsed && Array.isArray(parsed.results)) {
        assignByArray(parsed.results, base);
      } else if (Array.isArray(parsed)) {
        assignByArray(parsed, base);
      } else if (parsed && typeof parsed.result === "string") {
        if (base >= 0 && base < merged.length) merged[base].result = parsed.result;
      } else if (typeof text === "string" && text) {
        if (base >= 0 && base < merged.length) merged[base].result = text;
      }
    }

    // Serialize merged CSV
    const headers = Object.keys(merged[0] || {});
    const csvStr = await new Promise((resolve, reject) => {
      csvStringify(merged, { header: true, columns: headers }, (err, out) => err ? reject(err) : resolve(out));
    });

    return res(200, csvStr, {
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": `attachment; filename="${id}.csv"`,
    });

  } catch (e) {
    console.error("batch-download error:", e);
    return res(500, { error: e.message || String(e) });
  }
};
