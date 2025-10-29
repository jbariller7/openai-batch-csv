// netlify/functions/batch-reconstruct.js
// Rebuild a CSV *only* from an OpenAI batch id (batch_...), no prior blobs needed.
// CommonJS + Lambda style. It also caches the rebuilt CSV to Blobs for future use.

const { stringify: csvStringify } = require("csv-stringify");

exports.config = { /* path: "/api/batch-reconstruct" */ };

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

  // Parse query
  const rawUrl = typeof event?.rawUrl === "string" ? event.rawUrl : "";
  const url = rawUrl ? new URL(rawUrl) : null;
  const id = url?.searchParams.get("id") || event?.queryStringParameters?.id || "";
  if (!id || !id.startsWith("batch_")) {
    return res(400, { error: "Pass a valid ?id=batch_XXXXXX" });
  }

  // Lazy ESM imports to remain CommonJS-compatible in Netlify
  const { getStore } = await import("@netlify/blobs");
  const { default: OpenAI } = await import("openai");
  const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  // Blobs (site creds optional; helpful in local/dev)
  const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
  const token  = process.env.NETLIFY_BLOBS_TOKEN || process.env.NETLIFY_AUTH_TOKEN;
  const store  = (siteID && token)
    ? getStore({ name: "openai-batch-csv", siteID, token })
    : getStore("openai-batch-csv");

  try {
    // If we already rebuilt this in the past, serve it from cache
    try {
      const cached = await store.get(`results/${id}.reconstructed.csv`, { type: "text" });
      if (typeof cached === "string" && cached.length) {
        return res(200, cached, {
          "Content-Type": "text/csv; charset=utf-8",
          "Content-Disposition": `attachment; filename="${id}.csv"`,
          "X-Reconstructed": "1",
          "X-Cache": "hit",
        });
      }
    } catch {}

    // 1) Retrieve the batch
    const batch = await client.batches.retrieve(id);
    if (batch.status !== "completed") {
      return res(400, { error: `Batch not completed. Status: ${batch.status}` });
    }
    if (!batch.output_file_id) {
      return res(400, { error: "Batch has no output_file_id" });
    }

    // 2) Download OUTPUT JSONL
    const outResp = await client.files.content(batch.output_file_id);
    const outBuf  = Buffer.from(await outResp.arrayBuffer());
    const outLines = outBuf.toString("utf8").trim().split("\n");

    // 3) Optionally download INPUT JSONL (to recover the original text)
    //    Not all SDKs expose input id back, but when present it helps.
    //    Fall back to empty map if not available or fails.
    let inputMap = new Map(); // id -> input_text
    const inputFileId = batch.input_file_id || batch.request_file_id || batch.inputFileId; // try a few likely names
    if (inputFileId) {
      try {
        const inResp = await client.files.content(inputFileId);
        const inBuf  = Buffer.from(await inResp.arrayBuffer());
        const inLines = inBuf.toString("utf8").trim().split("\n");

        for (const line of inLines) {
          if (!line.trim()) continue;
          let obj; try { obj = JSON.parse(line); } catch { continue; }
          // Our JSONL lines look like: { method, url, custom_id, body }
          // body.input[2] is { role: 'user', content: JSON.stringify({ rows:[{id,text},...]}) }
          const base = Number(obj?.custom_id) || 0;
          const body = obj?.body || obj?.request_body || {};
          const msgs = Array.isArray(body?.input) ? body.input : [];
          const rowPayloadMsg = msgs.find(m => m && m.role === "user" && typeof m.content === "string" && m.content.includes('"rows"'));
          if (!rowPayloadMsg) continue;
          let payload; try { payload = JSON.parse(rowPayloadMsg.content); } catch { continue; }
          const arr = Array.isArray(payload?.rows) ? payload.rows : [];
          for (let j = 0; j < arr.length; j++) {
            const item = arr[j];
            const idNum = Number.isFinite(Number(item?.id)) ? Number(item.id) : (base + j);
            if (typeof item?.text === "string") inputMap.set(idNum, item.text);
          }
        }
      } catch {
        // ignore if input file retrieval fails
      }
    }

    // 4) Parse OUTPUT JSONL and build a normalized table keyed by id
    //    We support:
    //    - {"results":[{"id":n,"cols":{...}},...]}
    //    - {"results":[{"id":n,"result":"..."}...]}
    //    - free-text fallback to "result"
    const table = new Map(); // id -> { cols: {k:v,...} } or { result: "..." }
    const dynamicCols = new Set();

    function assignResult(idNum, obj) {
      if (!Number.isFinite(idNum)) return;
      const row = table.get(idNum) || { cols: {} };
      if (obj && typeof obj.cols === "object" && obj.cols !== null) {
        for (const [k, v] of Object.entries(obj.cols)) {
          row.cols[k] = v == null ? "" : String(v);
          dynamicCols.add(k);
        }
      } else if (typeof obj?.result === "string") {
        row.cols.result = obj.result;
        dynamicCols.add("result");
      }
      table.set(idNum, row);
    }

    for (const line of outLines) {
      if (!line.trim()) continue;
      let obj; try { obj = JSON.parse(line); } catch { continue; }

      const base = Number(obj?.custom_id) || 0;
      const body = obj?.response?.body || obj?.body || {};
      const text =
        typeof body?.output_text === "string" ? body.output_text
        : typeof body?.content === "string" ? body.content
        : "";

      let parsed = null; if (text) { try { parsed = JSON.parse(text); } catch {} }

      const arr =
        (parsed && Array.isArray(parsed.results)) ? parsed.results
      : (Array.isArray(parsed) ? parsed : null);

      if (arr) {
        arr.forEach((item, j) => {
          const idNum = Number.isFinite(Number(item?.id)) ? Number(item.id) : (base + j);
          assignResult(idNum, item);
        });
      } else if (parsed && typeof parsed.cols === "object") {
        assignResult(base, parsed);
      } else if (parsed && typeof parsed.result === "string") {
        assignResult(base, parsed);
      } else if (typeof text === "string" && text) {
        assignResult(base, { result: text });
      }
    }

    // 5) Build rows for CSV
    // Sort by numeric id (stable, predictable)
    const ids = Array.from(table.keys()).sort((a, b) => a - b);

    // Columns: id, input_text (if available), then dynamic result cols
    const dynamicHeaders = Array.from(dynamicCols);
    const headers = ["id", ...(inputMap.size ? ["input_text"] : []), ...dynamicHeaders];

    const records = ids.map(idNum => {
      const rec = { id: idNum };
      if (inputMap.size) rec.input_text = inputMap.get(idNum) ?? "";
      const data = table.get(idNum) || { cols: {} };
      for (const k of dynamicHeaders) {
        rec[k] = data.cols?.[k] ?? "";
      }
      return rec;
    });

    // 6) Serialize CSV
    const csvStr = await new Promise((resolve, reject) => {
      csvStringify(records, { header: true, columns: headers }, (err, out) => {
        if (err) reject(err);
        else resolve(out);
      });
    });

    // Cache for future requests
    try {
      await store.set(`results/${id}.reconstructed.csv`, csvStr, {
        contentType: "text/csv; charset=utf-8",
      });
    } catch {}

    return res(200, csvStr, {
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": `attachment; filename="${id}.csv"`,
      "X-Reconstructed": "1",
      "X-Cache": "miss",
    });
  } catch (e) {
    console.error("batch-reconstruct error:", e);
    return res(500, { error: e.message || String(e) });
  }
};
