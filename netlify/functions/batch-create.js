const Busboy = require("busboy");
const { parse: csvParse } = require("csv-parse");

exports.config = { /* path: "/api/batch-create" */ };
const CORS = { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "POST,OPTIONS,HEAD", "Access-Control-Allow-Headers": "Content-Type" };

function res(statusCode, bodyObj) { return { statusCode, headers: { "Content-Type": "application/json", ...CORS }, body: JSON.stringify(bodyObj ?? {}) }; }
function getQuery(event) { try { return event.queryStringParameters || Object.fromEntries(new URL(event.rawUrl).searchParams.entries()); } catch { return {}; } }

function parseMultipartEvent(event) {
  return new Promise((resolve, reject) => {
    const bb = Busboy({ headers: { "content-type": event.headers["content-type"] || event.headers["Content-Type"] || "" } });
    const fields = {}; let fileBuffers = [];
    bb.on("file", (_name, file) => file.on("data", (d) => fileBuffers.push(d)));
    bb.on("field", (name, val) => (fields[name] = val));
    bb.on("error", reject);
    bb.on("finish", () => resolve({ fields, fileBuffer: fileBuffers.length ? Buffer.concat(fileBuffers) : null }));
    bb.end(event.isBase64Encoded ? Buffer.from(event.body || "", "base64") : Buffer.from(event.body || ""));
  });
}

exports.handler = async function (event) {
  if (event.httpMethod === "OPTIONS" || event.httpMethod === "HEAD") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "POST") return res(405, { error: "POST only" });

  const { getStore } = await import("@netlify/blobs");
  const openaiMod = await import("openai");
  const OpenAI = openaiMod.default; const { toFile } = openaiMod;
  const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  try {
    const { fields, fileBuffer } = await parseMultipartEvent(event);
    if (!fileBuffer) return res(400, { error: "CSV file is required" });

    const inputCol = fields.inputCol || "text";
    const skipCol = fields.skipCol || ""; 
    const targetColsRaw = fields.targetCols || "";
    const targetCols = targetColsRaw.split(",").map(s => s.trim()).filter(Boolean);

    const model = fields.model || "gpt-5.4-nano";
    const prompt = fields.prompt || "Translate to English.";
    const contextDoc = fields.contextDoc || "";
    const chunkSize = Math.max(1, Math.min(1000, Number(fields.chunkSize || 500)));

    const query = getQuery(event);
    const maxRows = Number(fields.maxRows || query.maxRows || 0) || 0;
    const dryRun = String(fields.dryRun || query.dryRun || "") === "1";
    const direct = String(fields.direct || query.direct || "") === "1";
    const concurrency = Math.max(1, Math.min(Number(process.env.MAX_DIRECT_CONCURRENCY || 8), Number(fields.concurrency || query.concurrency || 4)));

    const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
    const token  = process.env.NETLIFY_BLOBS_TOKEN || process.env.NETLIFY_AUTH_TOKEN;
    const store  = (siteID && token) ? getStore({ name: "openai-batch-csv", siteID, token }) : getStore("openai-batch-csv");

    const jobId = crypto.randomUUID();
    await store.set(`csv/${jobId}.csv`, fileBuffer, { contentType: "text/csv; charset=utf-8" });

    const rows = await new Promise((resolve, reject) => {
      const out = [];
      csvParse(fileBuffer, { columns: true, relax_quotes: true, skip_empty_lines: true, bom: true }).on("data", (r) => out.push(r)).on("end", () => resolve(out)).on("error", reject);
    });
    if (!rows.length) return res(400, { error: "CSV has no rows" });

    const effectiveRows = maxRows > 0 ? rows.slice(0, maxRows) : rows;
    
    const validItems = [];
    effectiveRows.forEach((r, idx) => {
      const text = String(r?.[inputCol] ?? "").trim();
      const skipText = skipCol ? String(r?.[skipCol] ?? "").trim() : "";
      if (text && !skipText) validItems.push({ id: idx, text }); 
    });

    if (validItems.length === 0) return res(400, { error: "No valid rows found (all empty or already skipped)." });

    function buildBody(rowsChunk, targetModel, targetColName) {
      let currentPrompt = prompt;
      let suffix = ' You will receive a json object {"rows":[{"id":number,"text":string},...]}. For each item, produce {"id": same id, "result": <string>} following the user instructions above. The output must be valid json. Return ONLY a json object exactly like: {"results":[{"id":number,"result":string},...]} in the SAME ORDER as input.';
      
      if (targetColName) {
        currentPrompt = prompt.replace(/\$\{columnName(s)?\}/gi, targetColName);
        suffix = ` You will receive a json object {"rows":[{"id":number,"text":string},...]}. For each item, produce {"id": same id, "cols": {"${targetColName}": <string>}} following the user instructions above. The output must be valid json. Return ONLY a json object exactly like: {"results":[{"id":number,"cols":{"${targetColName}": "..."}},...]} in the SAME ORDER as input.`;
      }
      
      const systemPromptContent = contextDoc ? `[REFERENCE CONTEXT]\n${contextDoc}\n\n[INSTRUCTIONS]\n${currentPrompt}` : currentPrompt;
      
      return {
        model: targetModel,
        input: [
          { role: "system", content: `${systemPromptContent}${suffix}` },
          { role: "user", content: "Return only a json object as specified. The output must be valid json." },
          { role: "user", content: JSON.stringify({ rows: rowsChunk }) }
        ],
        text: { format: { type: "json_object" } },
      };
    }

    if (dryRun) {
      const dryK = Math.min(chunkSize, 5);
      const firstChunk = validItems.slice(0, dryK);
      const firstCol = targetCols.length > 0 ? targetCols[0] : null;
      const resp = await client.responses.create(buildBody(firstChunk, model.startsWith("gpt-5") ? "gpt-4.1-mini" : model, firstCol));
      let parsed = null; try { parsed = JSON.parse(resp.output_text || ""); } catch {}
      return res(200, { mode: "dryRun", jobId, usedRows: firstChunk.length, model, response: resp, parsed });
    }

    if (direct) {
      await store.set(`jobs/${jobId}.json`, JSON.stringify({ jobId, model, prompt, contextDoc, inputCol, skipCol, targetCols, chunkSize, concurrency, createdAt: new Date().toISOString() }), { contentType: "application/json" });
      await store.set(`jobs/${jobId}.status.json`, JSON.stringify({ jobId, status: "queued", updatedAt: new Date().toISOString(), events: [{ ts: new Date().toISOString(), msg: "queued" }] }), { contentType: "application/json" });
      
      const hdrs = event.headers || {};
      const host = hdrs["x-forwarded-host"] || hdrs["host"] || "localhost:8888";
      const origin = host ? `${hdrs["x-forwarded-proto"] || "http"}://${host}` : (process.env.URL || "http://localhost:8888");

      try {
        const wr = await fetch(`${origin}/.netlify/functions/direct-worker-background`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ jobId }) });
        if (!wr.ok) throw new Error(`worker HTTP ${wr.status}`);
      } catch (e) { return res(500, { error: `Worker invoke failed: ${e.message}` }); }
      return res(202, { mode: "direct", jobId, model, rowCount: validItems.length, download: `/.netlify/functions/batch-download?id=${jobId}` });
    }

    // BATCH MODE (Multi-Column Splitting)
    const lines = [];
    if (targetCols.length > 0) {
      targetCols.forEach((colName, colIdx) => {
        for (let start = 0; start < validItems.length; start += chunkSize) {
          const chunk = validItems.slice(start, start + chunkSize);
          lines.push(JSON.stringify({ custom_id: `${start}_${colIdx}`, method: "POST", url: "/v1/responses", body: buildBody(chunk, model, colName) }));
        }
      });
    } else {
      for (let start = 0; start < validItems.length; start += chunkSize) {
        const chunk = validItems.slice(start, start + chunkSize);
        lines.push(JSON.stringify({ custom_id: String(start), method: "POST", url: "/v1/responses", body: buildBody(chunk, model, null) }));
      }
    }

    const jsonlBuffer = Buffer.from(lines.join("\n"), "utf8");
    const jsonlFile = await client.files.create({ file: await toFile(jsonlBuffer, `${jobId}.jsonl`, { type: "application/jsonl" }), purpose: "batch" });
    const batch = await client.batches.create({ input_file_id: jsonlFile.id, endpoint: "/v1/responses", completion_window: "24h" });

    await store.set(`jobs/${batch.id}.json`, JSON.stringify({ jobId, batchId: batch.id, inputCol, skipCol, targetCols, model, prompt, chunkSize, createdAt: new Date().toISOString() }), { contentType: "application/json" });
    return res(200, { mode: "batch", batchId: batch.id, jobId });
  } catch (err) { return res(500, { error: err?.message || String(err) }); }
};
