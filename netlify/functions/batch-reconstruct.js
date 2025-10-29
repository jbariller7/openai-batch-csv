// netlify/functions/batch-reconstruct.js
// Rebuild CSV from an OpenAI batch_<...> id by merging OUTPUT with your original CSV
// CommonJS + Lambda-style + UTF-8 BOM + multi-column support + gpt-5 output shape + repair for U+FFFD corruption

const { parse: csvParse } = require("csv-parse");
const { stringify: csvStringify } = require("csv-stringify");

exports.config = { /* path: "/api/batch-reconstruct" */ };

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,OPTIONS,HEAD",
  "Access-Control-Allow-Headers": "Content-Type",
};

// --- Unicode helpers ---
function normalizeUtf(s) {
  if (s == null) return "";
  let t = String(s);

  // Normalize to NFC (composed accents)
  if (typeof t.normalize === "function") t = t.normalize("NFC");

  // Fix the most common replacement-char cases we saw:
  //  - "\uFFFDdiga" → "¡diga"
  t = t.replace(/(?:^|\s)\uFFFD(diga)/gi, (m, g1) => ` ¡${g1}`);
  //  - "\uFFFD?" → "¿"
  t = t.replace(/\uFFFD\?/g, "¿");
  //  - "s\uFFFDndwich" → "sándwich" (common Spanish word)
  t = t.replace(/s\uFFFDndwich/gi, "sándwich");

  // As a last resort, drop any leftover U+FFFD
  t = t.replace(/\uFFFD/g, "");

  return t;
}

// If the model sometimes returns a stringified JSON inside `result`,
// parse it and return { cols } if that shape is present.
function parseResultPossiblyJson(resultStr) {
  if (typeof resultStr !== "string") return null;
  const r = resultStr.trim();
  if (!r.startsWith("{") || r.length < 2) return null;
  try {
    const obj = JSON.parse(r);
    if (obj && typeof obj.cols === "object" && obj.cols !== null) {
      return obj; // { cols: {...} }
    }
  } catch {}
  return null;
}

// --- Generic helpers ---
function res(statusCode, body, headers) {
  return {
    statusCode,
    headers: { ...(headers || {}), ...CORS },
    body: typeof body === "string" ? body : JSON.stringify(body ?? {}),
  };
}

function ensureUtf8Bom(str) {
  return str && !str.startsWith("\uFEFF") ? "\uFEFF" + str : str;
}

// Pull the JSON string from the many response.body shapes
function extractOutputJsonText(body) {
  if (typeof body?.output_text === "string" && body.output_text.trim()) return body.output_text;
  if (typeof body?.content === "string" && body.content.trim()) return body.content;
  if (Array.isArray(body?.output)) {
    for (const out of body.output) {
      if (Array.isArray(out?.content)) {
        const part = out.content.find(p => p?.type === "output_text" && typeof p?.text === "string" && p.text.trim());
        if (part) return part.text;
        const anyPart = out.content.find(p => typeof p?.text === "string" && p.text.trim());
        if (anyPart) return anyPart.text;
      }
    }
  }
  return "";
}

async function parseCsvText(csvTxt) {
  return new Promise((resolve, reject) => {
    const out = [];
    csvParse(csvTxt, { columns: true, relax_quotes: true, bom: true, skip_empty_lines: true })
      .on("data", r => out.push(r))
      .on("end", () => resolve(out))
      .on("error", reject);
  });
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS" || event.httpMethod === "HEAD") {
    return { statusCode: 204, headers: CORS, body: "" };
  }

  try {
    const rawUrl = typeof event?.rawUrl === "string" ? event.rawUrl : "";
    const url = rawUrl ? new URL(rawUrl) : null;
    const batchId = url?.searchParams.get("id") || event?.queryStringParameters?.id || "";
    if (!batchId || !batchId.startsWith("batch_")) {
      return res(400, { error: "Provide a valid OpenAI batch id starting with 'batch_' via ?id=batch_xxx" });
    }

    // Lazy ESM deps
    const { default: OpenAI } = await import("openai");
    const { getStore } = await import("@netlify/blobs");

    const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

    // Optional manual creds for local/dev
    const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
    const token  = process.env.NETLIFY_BLOBS_TOKEN || process.env.NETLIFY_AUTH_TOKEN;
    const store  = (siteID && token)
      ? getStore({ name: "openai-batch-csv", siteID, token })
      : getStore("openai-batch-csv");

    // 1) Retrieve batch metadata from OpenAI
    const b = await client.batches.retrieve(batchId);
    if (!b) return res(404, { error: "Batch not found" });
    if (!b.output_file_id) return res(400, { error: "Batch has no output_file_id. It may not be completed yet." });

    // 2) Try to load your job metadata from Netlify to locate original CSV
    let meta = null;
    try { meta = await store.get(`jobs/${batchId}.json`, { type: "json" }); } catch {}
    const hasOriginalCsv = !!meta?.jobId;

    // 3) If we have original CSV, load it
    let originalRows = null;
    let originalHeaders = [];
    if (hasOriginalCsv) {
      const origCsvTxt = await store.get(`csv/${meta.jobId}.csv`, { type: "text" }).catch(() => null);
      if (origCsvTxt) {
        originalRows = await parseCsvText(origCsvTxt);
        originalHeaders = Object.keys(originalRows[0] || {});
      }
    }

    // 4) Build id -> input (fallback from OpenAI INPUT file if original is missing)
    const idToInput = new Map();
    if (!originalRows && b.input_file_id) {
      const inputResp = await client.files.content(b.input_file_id);
      const inputBuf = Buffer.from(await inputResp.arrayBuffer());
      const inputLines = inputBuf.toString("utf8").split(/\r?\n/).filter(Boolean);

      for (const line of inputLines) {
        let obj;
        try { obj = JSON.parse(line); } catch { continue; }
        const base = Number(obj?.custom_id) || 0;
        const body = obj?.body || {};
        let rowsJson = null;
        try {
          const msg = Array.isArray(body?.input) ? body.input[2] : null;
          const content = typeof msg?.content === "string" ? msg.content : null;
          if (content) rowsJson = JSON.parse(content);
        } catch {}
        const rowsChunk = rowsJson && Array.isArray(rowsJson.rows) ? rowsJson.rows : null;
        if (!rowsChunk) continue;
        rowsChunk.forEach((r, j) => {
          const globalId = Number.isFinite(Number(r?.id)) ? Number(r.id) : (base + j);
          const text = normalizeUtf(String(r?.text ?? ""));
          idToInput.set(globalId, text);
        });
      }
    }

    // 5) Read OpenAI OUTPUT JSONL to collect results
    const idToCols = new Map();   // id -> { [col]: value }
    const idToError = new Map();  // id -> error text
    const outResp = await client.files.content(b.output_file_id);
    const outBuf = Buffer.from(await outResp.arrayBuffer());
    const outLines = outBuf.toString("utf8").split(/\r?\n/).filter(Boolean);

    const pushCols = (id, colsObj) => {
      if (!idToCols.has(id)) idToCols.set(id, {});
      const acc = idToCols.get(id);
      for (const [k, v] of Object.entries(colsObj)) {
        acc[k] = normalizeUtf(v == null ? "" : String(v));
      }
    };

    for (const line of outLines) {
      let obj; try { obj = JSON.parse(line); } catch { continue; }
      const base = Number(obj?.custom_id) || 0;
      const body = obj?.response?.body || {};
      const outputText = extractOutputJsonText(body);

      let parsed = null;
      if (outputText) { try { parsed = JSON.parse(outputText); } catch { parsed = null; } }

      if (parsed && Array.isArray(parsed.results)) {
        parsed.results.forEach((item, j) => {
          const id = Number.isFinite(Number(item?.id)) ? Number(item.id) : (base + j);
          if (item && typeof item.cols === "object" && item.cols !== null) {
            pushCols(id, item.cols);
          } else if (typeof item?.result === "string") {
            const maybe = parseResultPossiblyJson(item.result);
            if (maybe?.cols) pushCols(id, maybe.cols);
            else pushCols(id, { result: item.result });
          }
        });
      } else if (Array.isArray(parsed)) {
        parsed.forEach((item, j) => {
          const id = Number.isFinite(Number(item?.id)) ? Number(item.id) : (base + j);
          if (item && typeof item.cols === "object" && item.cols !== null) {
            pushCols(id, item.cols);
          } else if (typeof item?.result === "string") {
            const maybe = parseResultPossiblyJson(item.result);
            if (maybe?.cols) pushCols(id, maybe.cols);
            else pushCols(id, { result: item.result });
          }
        });
      } else if (parsed && typeof parsed === "object") {
        const id = base;
        if (parsed.cols && typeof parsed.cols === "object") {
          pushCols(id, parsed.cols);
        } else if (typeof parsed.result === "string") {
          const maybe = parseResultPossiblyJson(parsed.result);
          if (maybe?.cols) pushCols(id, maybe.cols);
          else pushCols(id, { result: parsed.result });
        }
      } else if (typeof outputText === "string" && outputText) {
        pushCols(base, { result: outputText });
      }

      if (obj?.error) {
        const emsg = obj.error?.message || JSON.stringify(obj.error);
        idToError.set(base, emsg);
      }
    }

    // 6) Decide headers and rows
    const resultColSet = new Set();
    for (const [, cols] of idToCols) for (const k of Object.keys(cols)) resultColSet.add(k);

    let headers = [];
    let outRows = [];

    if (originalRows && originalRows.length) {
      const dynamicHeaders = Array.from(resultColSet).filter(h => !originalHeaders.includes(h));
      headers = [...originalHeaders, ...dynamicHeaders];

      outRows = originalRows.map((orig, idx) => {
        const row = { ...orig };
        const cols = idToCols.get(idx) || {};
        for (const h of dynamicHeaders) row[h] = cols[h] ?? "";
        return row;
      });
    } else {
      const allIds = new Set([
        ...Array.from(idToInput.keys()),
        ...Array.from(idToCols.keys()),
        ...Array.from(idToError.keys()),
      ]);
      const sortedIds = Array.from(allIds).sort((a, b) => a - b);

      headers = ["id", "input_text", ...Array.from(resultColSet)];
      outRows = sortedIds.map(id => {
        const base = { id, input_text: idToInput.get(id) ?? "" };
        const cols = idToCols.get(id) || {};
        for (const h of headers) {
          if (h === "id" || h === "input_text") continue;
          base[h] = cols[h] ?? "";
        }
        return base;
      });
    }

    if (!outRows.length) {
      return res(404, { error: "No rows reconstructed. Check that blobs metadata exists or the batch output contains JSON." });
    }

    const csvStr = await new Promise((resolve, reject) => {
      csvStringify(outRows, { header: true, columns: headers }, (err, out) => {
        if (err) reject(err); else resolve(out);
      });
    });
    const csvWithBom = ensureUtf8Bom(csvStr);

    // Save a copy for convenience
    try {
      await store.set(`results/${batchId}.reconstructed.csv`, csvWithBom, {
        contentType: "text/csv; charset=utf-8",
      });
    } catch {}

    return res(200, csvWithBom, {
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": `attachment; filename="${batchId}.csv"`,
    });
  } catch (e) {
    console.error("batch-reconstruct error:", e);
    return res(500, { error: e.message || String(e) });
  }
};
