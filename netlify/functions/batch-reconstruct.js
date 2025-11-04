// netlify/functions/batch-reconstruct.js
// Rebuild CSV from an OpenAI batch_<...> id by merging OUTPUT with your original CSV
// Stream large CSV content using chunks to avoid ResponseSizeTooLarge error.

const { parse: csvParse } = require("csv-parse");
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
    isBase64Encoded: false,
  };
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

    // 3) Load original CSV if we have it
    let originalRows = null;
    let originalHeaders = [];
    if (hasOriginalCsv) {
      const origCsvTxt = await store.get(`csv/${meta.jobId}.csv`, { type: "text" }).catch(() => null);
      if (origCsvTxt) {
        originalRows = await parseCsvText(origCsvTxt);
        originalHeaders = Object.keys(originalRows[0] || {});
      }
    }

    // 4) Read OpenAI OUTPUT JSONL to collect results
    const idToCols = new Map();
    const outResp = await client.files.content(b.output_file_id);
    const outBuf = Buffer.from(await outResp.arrayBuffer());
    const outLines = outBuf.toString("utf8").split(/\r?\n/).filter(Boolean);

    const pushCols = (id, colsObj) => {
      if (!idToCols.has(id)) idToCols.set(id, {});
      const acc = idToCols.get(id);
      for (const [k, v] of Object.entries(colsObj)) {
        acc[k] = v == null ? "" : String(v);
      }
    };

    for (const line of outLines) {
      let obj; try { obj = JSON.parse(line); } catch { continue; }
      const base = Number(obj?.custom_id) || 0;
      const body = obj?.response?.body || {};
      const outputText = body?.output_text || "";

      let parsed = null;
      if (outputText) { try { parsed = JSON.parse(outputText); } catch { parsed = null; } }

      if (parsed && Array.isArray(parsed.results)) {
        parsed.results.forEach((item, j) => {
          const id = Number.isFinite(Number(item?.id)) ? Number(item.id) : (base + j);
          if (item && typeof item.cols === "object" && item.cols !== null) {
            pushCols(id, item.cols);
          } else if (typeof item?.result === "string") {
            pushCols(id, { result: item.result });
          }
        });
      }
    }

    // 5) Use original CSV rows and headers, then add dynamic result columns not already present
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
        ...Array.from(idToCols.keys()),
      ]);
      const sortedIds = Array.from(allIds).sort((a, b) => a - b);

      headers = ["id", "input_text", ...Array.from(resultColSet)];
      outRows = sortedIds.map(id => {
        const base = { id, input_text: idToCols.get(id)?.input_text ?? "" };
        const cols = idToCols.get(id) || {};
        for (const h of headers) {
          if (h === "id" || h === "input_text") continue;
          base[h] = cols[h] ?? "";
        }
        return base;
      });
    }

    if (!outRows.length) {
      return res(404, { error: "No rows reconstructed." });
    }

    // 6) Handle large file sizes: Write to blobs if too big
    const csvStr = await new Promise((resolve, reject) => {
      csvStringify(outRows, { header: true, columns: headers }, (err, out) => {
        if (err) reject(err); else resolve(out);
      });
    });

    let buf = Buffer.from(ensureUtf8Bom(csvStr), "utf8");
    const HARD_LIMIT = 6291556; // 6MB approx

    if (buf.length > HARD_LIMIT) {
      // Save the file to blobs
      const key = `results/${batchId}.csv`;
      await store.set(key, buf, { contentType: "text/csv; charset=utf-8" });

      // Return a 303 redirect to a streaming proxy
      const hdrs = event.headers || {};
      const host = hdrs["x-forwarded-host"] || hdrs["host"] || hdrs["Host"] || "";
      const proto = hdrs["x-forwarded-proto"] || (host.startsWith("localhost") ? "http" : "https");
      const origin = host ? `${proto}://${host}` : (process.env.URL || "http://localhost:8888");

      return {
        statusCode: 303,
        headers: {
          ...CORS,
          Location: `${origin}/.netlify/functions/blob-proxy?key=${encodeURIComponent(key)}&filename=${encodeURIComponent(batchId + ".csv")}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          redirected: true,
          download: `/\.netlify/functions/blob-proxy?key=${encodeURIComponent(key)}&filename=${encodeURIComponent(batchId + ".csv")}`,
          note: "Large file stored in blobs and streamed via proxy.",
        }),
      };
    }

    return {
      statusCode: 200,
      headers: {
        ...CORS,
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Disposition": `attachment; filename="${batchId}.csv"`,
      },
      body: buf.toString(),
    };

  } catch (e) {
    console.error("batch-reconstruct error:", e);
    return res(500, { error: e.message || String(e) });
  }
};
