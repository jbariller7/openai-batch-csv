const { parse: csvParse } = require("csv-parse");
const { stringify: csvStringify } = require("csv-stringify");

exports.config = { /* path: "/api/batch-reconstruct" */ };

const CORS = { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET,OPTIONS,HEAD", "Access-Control-Allow-Headers": "Content-Type" };
function res(statusCode, body, headers) { return { statusCode, headers: { ...(headers || {}), ...CORS }, body: typeof body === "string" ? body : JSON.stringify(body ?? {}) }; }
function ensureUtf8Bom(str) { return str && !str.startsWith("\uFEFF") ? "\uFEFF" + str : str; }
function flattenNewlines(rows) { return rows.map((r) => { const out = {}; for (const [k, v] of Object.entries(r)) { let s = v == null ? "" : String(v); s = s.replace(/\r\n/g, " ").replace(/\n/g, " ").replace(/\r/g, " ").replace(/[ \t]+/g, " ").trim(); out[k] = s; } return out; }); }
function normalizeUtf(s) { if (s == null) return ""; let t = String(s); if (typeof t.normalize === "function") t = t.normalize("NFC"); return t.replace(/\uFFFD/g, ""); }

// JSON repair helpers
function tryParseJsonWithRepairs(raw) {
  if (raw == null) return null; let s = String(raw).trim(); if (!s) return null;
  const attempt = (t) => { try { return JSON.parse(t); } catch { return null; } };
  let obj = attempt(s); if (obj) return obj;
  s = s.replace(/(?<!\\)\\(?!["\\/bfnrtu])/g, "\\\\"); obj = attempt(s); if (obj) return obj;
  s = s.replace(/,\s*([}\]])/g, "$1"); obj = attempt(s); if (obj) return obj;
  const balance = (t, o, c) => t + c.repeat(Math.max(0, (t.match(new RegExp("\\"+o,"g"))||[]).length - (t.match(new RegExp("\\"+c,"g"))||[]).length));
  s = balance(balance(s, "{", "}"), "[", "]"); obj = attempt(s); if (obj) return obj;
  const lp = Math.max(s.lastIndexOf("}"), s.lastIndexOf("]")); if (lp > 0) return attempt(s.slice(0, lp + 1));
  return null;
}

function parseResultPossiblyJson(raw) {
  if (typeof raw !== "string") return null; let r = raw.trim(); if (!r) return null;
  if (!r.includes('"cols"') && !r.includes("'cols'") && !r.startsWith("{")) return null;
  let obj = tryParseJsonWithRepairs(r);
  if (obj?.cols && typeof obj.cols === "object") return { cols: obj.cols };
  if (obj?.result && typeof obj.result === "string") {
    let nested = tryParseJsonWithRepairs(obj.result);
    if (nested?.cols) return { cols: nested.cols };
  }
  return null;
}

function extractOutputJsonText(body) {
  if (typeof body?.output_text === "string" && body.output_text.trim()) return body.output_text;
  if (typeof body?.content === "string" && body.content.trim()) return body.content;
  if (Array.isArray(body?.output)) {
    for (const out of body.output) {
      if (Array.isArray(out?.content)) {
        const p = out.content.find(p => p?.type === "output_text" && p.text?.trim()) || out.content.find(p => p.text?.trim());
        if (p) return p.text;
      }
    }
  }
  return "";
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS" || event.httpMethod === "HEAD") return { statusCode: 204, headers: CORS, body: "" };

  try {
    const url = event?.rawUrl ? new URL(event.rawUrl) : null;
    const batchIdParam = url?.searchParams.get("id") || event?.queryStringParameters?.id || "";
    const isAnalyze = url?.searchParams.get("analyze") === "1";
    
    if (!batchIdParam) return res(400, { error: "Provide batch id(s) via ?id=batch_xxx" });
    const batchIds = batchIdParam.split(",").map(s => s.trim()).filter(Boolean);

    const { default: OpenAI } = await import("openai");
    const { getStore } = await import("@netlify/blobs");
    const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
    const store = getStore({ name: "openai-batch-csv", siteID: process.env.SITE_ID || process.env.NETLIFY_SITE_ID, token: process.env.NETLIFY_AUTH_TOKEN || process.env.NETLIFY_BLOBS_TOKEN });

    // Load original meta & CSV using the FIRST batch ID
    const firstBatchId = batchIds[0];
    const meta = await store.get(`jobs/${firstBatchId}.json`, { type: "json" }).catch(()=>null);
    if (!meta) return res(404, { error: "Original Job metadata not found for the primary batch ID." });
    
    const origCsvTxt = await store.get(`csv/${meta.jobId}.csv`, { type: "text" }).catch(()=>null);
    let originalRows = [];
    if (origCsvTxt) {
      originalRows = await new Promise((resolve, reject) => {
        const out = []; csvParse(origCsvTxt, { columns: true, relax_quotes: true, bom: true, skip_empty_lines: true }).on("data", r => out.push(r)).on("end", () => resolve(out)).on("error", reject);
      });
    } else {
        return res(404, { error: "Original CSV file has expired or is missing from storage." });
    }

    const idToCols = new Map();
    
    // Loop through ALL provided batch IDs and merge their outputs
    for (const bId of batchIds) {
      const b = await client.batches.retrieve(bId).catch(() => null);
      if (!b || !b.output_file_id) continue; 
      
      const outResp = await client.files.content(b.output_file_id);
      const outLines = Buffer.from(await outResp.arrayBuffer()).toString("utf8").split(/\r?\n/).filter(Boolean);

      const pushCols = (id, colsObj) => {
        if (!idToCols.has(id)) idToCols.set(id, {});
        const acc = idToCols.get(id);
        for (const [k, v] of Object.entries(colsObj)) acc[k] = normalizeUtf(v == null ? "" : String(v));
      };

      for (const line of outLines) {
        let obj; try { obj = JSON.parse(line); } catch { continue; }
        const base = parseInt(obj?.custom_id, 10) || 0;
        const text = extractOutputJsonText(obj?.response?.body);
        let parsed = text ? tryParseJsonWithRepairs(text) : null;

        const processItem = (item, j) => {
            const id = Number.isFinite(Number(item?.id)) ? Number(item.id) : base + j;
            if (item?.cols && typeof item.cols === "object") pushCols(id, item.cols);
            else if (typeof item?.result === "string") {
                const maybe = parseResultPossiblyJson(item.result);
                if (maybe?.cols) pushCols(id, maybe.cols);
                else pushCols(id, { result: item.result });
            }
        };

        if (parsed?.results && Array.isArray(parsed.results)) parsed.results.forEach((item, j) => processItem(item, j));
        else if (parsed?.results && typeof parsed.results === "object") processItem(parsed.results, 0);
        else if (Array.isArray(parsed)) parsed.forEach((item, j) => processItem(item, j));
        else if (parsed && typeof parsed === "object") {
            if (parsed.cols) pushCols(base, parsed.cols);
            else if (parsed.result) {
                const maybe = parseResultPossiblyJson(parsed.result);
                if (maybe?.cols) pushCols(base, maybe.cols);
                else pushCols(base, { result: parsed.result });
            }
        }
      }
    }

    for (const [id, cols] of idToCols.entries()) {
      if (cols?.result && typeof cols.result === "string") {
        const maybe = parseResultPossiblyJson(cols.result);
        if (maybe?.cols) { delete cols.result; for (const [k, v] of Object.entries(maybe.cols)) cols[k] = normalizeUtf(v == null ? "" : String(v)); }
      }
    }

    const resultColSet = new Set();
    for (const cols of idToCols.values()) for (const k of Object.keys(cols)) resultColSet.add(k);
    
    // CRITICAL FIX: Only add dynamic headers that aren't already in the CSV
    const originalHeaders = Object.keys(originalRows[0] || {});
    const dynamicHeaders = Array.from(resultColSet).filter(h => !originalHeaders.includes(h));
    const headers = [...originalHeaders, ...dynamicHeaders];

    const missingIds = [];
    const outRows = originalRows.map((orig, idx) => {
      const row = { ...orig };
      const cols = idToCols.get(idx) || {};
      
      // CRITICAL FIX: Explicitly merge the AI's output into the row first!
      for (const [k, v] of Object.entries(cols)) {
         if (v !== undefined && v !== null && String(v).trim() !== "") {
             row[k] = String(v); 
         }
      }
      
      const targetInputText = String(orig[meta.inputCol] || "").trim();
      const skipText = meta.skipCol ? String(orig[meta.skipCol] || "").trim() : "";
      
      if (targetInputText && !skipText) {
          let isMissing = false;
          if (meta.targetCols && meta.targetCols.length > 0) {
              // Check the MERGED row to see if every required column is populated
              isMissing = meta.targetCols.some(c => !row[c] || String(row[c]).trim() === "");
          } else {
              isMissing = (!idToCols.has(idx) || Object.keys(cols).length === 0);
          }
          if (isMissing) missingIds.push(idx);
      }

      return row;
    });

    const flattenedRows = flattenNewlines(outRows);

    if (isAnalyze) {
        return res(200, {
            headers,
            totalRows: flattenedRows.length,
            missingCount: missingIds.length,
            missingIds: missingIds,
            previewData: flattenedRows.slice(0, 10), 
            primaryBatchId: firstBatchId
        });
    }

    const csvStr = await new Promise((resolve, reject) => {
      csvStringify(flattenedRows, { header: true, columns: headers }, (err, out) => err ? reject(err) : resolve(out));
    });

    return res(200, ensureUtf8Bom(csvStr), {
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": `attachment; filename="merged_${firstBatchId}.csv"`,
    });
  } catch (e) {
    return res(500, { error: e.message || String(e) });
  }
};
