// netlify/functions/batch-reconstruct.js
// Rebuild CSV from an OpenAI batch_<…> id by merging input and output JSONL
// CommonJS + Lambda-style + UTF-8 BOM + multi-column support

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

function ensureUtf8Bom(str) {
  return str && !str.startsWith("\uFEFF") ? "\uFEFF" + str : str;
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS" || event.httpMethod === "HEAD") {
    return { statusCode: 204, headers: CORS, body: "" };
  }

  try {
    // Read query
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

    // 1) Retrieve batch
    const b = await client.batches.retrieve(batchId);
    if (!b) return res(404, { error: "Batch not found" });
    if (!b.output_file_id) return res(400, { error: "Batch has no output_file_id (not completed?)" });
    if (!b.input_file_id) {
      // Older or unusual runs: we can still reconstruct results-only CSV
      // but we won’t have input_text. We try anyway below.
    }

    // 2) Download INPUT JSONL to rebuild the original inputs per global id
    const idToInput = new Map(); // id -> input_text
    if (b.input_file_id) {
      const inputResp = await client.files.content(b.input_file_id);
      const inputBuf = Buffer.from(await inputResp.arrayBuffer());
      const inputLines = inputBuf.toString("utf8").split(/\r?\n/).filter(Boolean);

      for (const line of inputLines) {
        let obj;
        try { obj = JSON.parse(line); } catch { continue; }
        const base = Number(obj?.custom_id) || 0;
        const body = obj?.body || {};

        // Our request shape: body.input is an array of messages, and the rows chunk
        // was put into the 3rd message (index 2), as a JSON string.
        //   body.input[2].content === JSON.stringify({ rows: [...] })
        let rowsJson = null;
        try {
          const msg = Array.isArray(body?.input) ? body.input[2] : null;
          const content = typeof msg?.content === "string" ? msg.content : null;
          if (content) rowsJson = JSON.parse(content);
        } catch {}

        const rowsChunk = rowsJson && Array.isArray(rowsJson.rows) ? rowsJson.rows : null;
        if (!rowsChunk) continue;

        // Map each local row to global id: globalId = base + indexWithinChunk
        rowsChunk.forEach((r, j) => {
          const globalId = Number.isFinite(Number(r?.id)) ? Number(r.id) : (base + j);
          const text = String(r?.text ?? "");
          idToInput.set(globalId, text);
        });
      }
    }

    // 3) Download OUTPUT JSONL and gather results per id
    const idToCols = new Map();   // id -> { [colName]: value }
    const idToError = new Map();  // id -> string (if any)

    const outResp = await client.files.content(b.output_file_id);
    const outBuf = Buffer.from(await outResp.arrayBuffer());
    const outLines = outBuf.toString("utf8").split(/\r?\n/).filter(Boolean);

    for (const line of outLines) {
      let obj; try { obj = JSON.parse(line); } catch { continue; }

      const base = Number(obj?.custom_id) || 0;
      const body = obj?.response?.body || {};
      const outputText =
        typeof body?.output_text === "string"
          ? body.output_text
          : (typeof body?.content === "string" ? body.content : "");

      // Try parsing the JSON results object we asked the model for
      let parsed = null;
      if (outputText) {
        try { parsed = JSON.parse(outputText); } catch { /* leave null */ }
      }

      const pushCols = (id, colsObj) => {
        if (!idToCols.has(id)) idToCols.set(id, {});
        const acc = idToCols.get(id);
        for (const [k, v] of Object.entries(colsObj)) {
          acc[k] = v == null ? "" : String(v);
        }
      };

      // Cases:
      //  - {"results":[{"id":..., "cols": {...}}, ...]}
      //  - {"results":[{"id":..., "result": "..."}, ...]}
      //  - {"id":..., "cols": {...}} or {"id":..., "result": "..."} (unlikely but safe)
      //  - array fallback
      if (parsed && Array.isArray(parsed.results)) {
        parsed.results.forEach((item, j) => {
          const id = Number.isFinite(Number(item?.id)) ? Number(item.id) : (base + j);
          if (item && typeof item.cols === "object" && item.cols !== null) {
            pushCols(id, item.cols);
          } else if (typeof item?.result === "string") {
            pushCols(id, { result: item.result });
          }
        });
      } else if (Array.isArray(parsed)) {
        parsed.forEach((item, j) => {
          const id = Number.isFinite(Number(item?.id)) ? Number(item.id) : (base + j);
          if (item && typeof item.cols === "object" && item.cols !== null) {
            pushCols(id, item.cols);
          } else if (typeof item?.result === "string") {
            pushCols(id, { result: item.result });
          }
        });
      } else if (parsed && typeof parsed === "object") {
        const id = base;
        if (parsed.cols && typeof parsed.cols === "object") {
          pushCols(id, parsed.cols);
        } else if (typeof parsed.result === "string") {
          pushCols(id, { result: parsed.result });
        }
      } else if (typeof outputText === "string" && outputText) {
        // Fallback: free text into result column
        pushCols(base, { result: outputText });
      }

      // Capture error text if present
      if (obj?.error) {
        const id = base;
        const emsg = obj.error?.message || JSON.stringify(obj.error);
        idToError.set(id, emsg);
      }
    }

    // 4) Build rows: union of ids we saw anywhere
    const allIds = new Set([
      ...Array.from(idToInput.keys()),
      ...Array.from(idToCols.keys()),
      ...Array.from(idToError.keys()),
    ]);
    const sortedIds = Array.from(allIds).sort((a, b) => a - b);

    // Collect all dynamic column names encountered in results
    const colSet = new Set();
    for (const [, cols] of idToCols) {
      for (const k of Object.keys(cols)) colSet.add(k);
    }
    // Common first columns
    const headers = ["id"];
    // Include input_text if we have at least one
    const hasAnyInput = Array.from(idToInput.values()).some(v => v !== undefined);
    if (hasAnyInput) headers.push("input_text");
    // Then dynamic result columns in stable order
    headers.push(...Array.from(colSet));

    // 5) Serialize CSV with BOM
    const outRows = [];
    for (const id of sortedIds) {
      const row = { id };
      if (hasAnyInput) row.input_text = idToInput.get(id) ?? "";
      const cols = idToCols.get(id) || {};
      for (const h of headers) {
        if (h === "id" || h === "input_text") continue;
        row[h] = cols[h] ?? "";
      }
      // If there was an error for this id and no cols, you could optionally expose it.
      // Example: row.error = idToError.get(id) || "";
      outRows.push(row);
    }

    // If nothing parsed, produce a helpful error
    if (!outRows.length) {
      return res(404, {
        error: "No rows reconstructed. Double-check the batch id, and ensure your jobs returned JSON in output_text.",
      });
    }

    const csvStr = await new Promise((resolve, reject) => {
      csvStringify(outRows, { header: true, columns: headers }, (err, out) => {
        if (err) reject(err); else resolve(out);
      });
    });
    const csvWithBom = ensureUtf8Bom(csvStr);

    // Persist a copy in blobs for future downloads
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
