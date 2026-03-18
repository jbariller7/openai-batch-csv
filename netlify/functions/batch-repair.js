const { parse: csvParse } = require("csv-parse");
exports.config = { /* path: "/api/batch-repair" */ };
const CORS = { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "POST,OPTIONS,HEAD", "Access-Control-Allow-Headers": "Content-Type" };

function res(statusCode, bodyObj) { return { statusCode, headers: { "Content-Type": "application/json", ...CORS }, body: JSON.stringify(bodyObj ?? {}) }; }

exports.handler = async function (event) {
  if (event.httpMethod === "OPTIONS" || event.httpMethod === "HEAD") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "POST") return res(405, { error: "POST only" });

  try {
    const { batchIds, missingIds } = JSON.parse(event.body || "{}");
    if (!batchIds || !missingIds || !missingIds.length) return res(400, { error: "Missing required parameters." });

    const primaryBatchId = batchIds.split(",")[0].trim();
    
    const { getStore } = await import("@netlify/blobs");
    const openaiMod = await import("openai");
    const OpenAI = openaiMod.default; const { toFile } = openaiMod;
    const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
    const store = getStore({ name: "openai-batch-csv", siteID: process.env.SITE_ID || process.env.NETLIFY_SITE_ID, token: process.env.NETLIFY_AUTH_TOKEN || process.env.NETLIFY_BLOBS_TOKEN });

    // 1. Fetch original job meta
    const meta = await store.get(`jobs/${primaryBatchId}.json`, { type: "json" });
    if (!meta) return res(404, { error: "Original metadata not found." });

    // 2. Fetch original CSV
    const csvTxt = await store.get(`csv/${meta.jobId}.csv`, { type: "text" });
    if (!csvTxt) return res(404, { error: "Original CSV data missing." });

    const rows = await new Promise((resolve, reject) => {
      const out = []; csvParse(csvTxt, { columns: true, relax_quotes: true, bom: true, skip_empty_lines: true }).on("data", r => out.push(r)).on("end", () => resolve(out)).on("error", reject);
    });

    // 3. Filter rows strictly by the missing IDs
    const repairItems = [];
    missingIds.forEach(id => {
        const r = rows[id];
        if (r) repairItems.push({ id: id, text: String(r[meta.inputCol] || "").trim() });
    });

    if (!repairItems.length) return res(400, { error: "Could not extract valid text for the missing rows." });

    // 4. Construct JSONL for the repair batch
    const systemPromptContent = meta.contextDoc ? `[REFERENCE CONTEXT]\n${meta.contextDoc}\n\n[INSTRUCTIONS]\n${meta.prompt}` : meta.prompt;
    const suffix = ' You will receive a json object {"rows":[{"id":number,"text":string},...]}. For each item, produce {"id": same id, "result": <string>} following the user instructions above. The output must be valid json. Return ONLY a json object exactly like: {"results":[{"id":number,"result":string},...]} in the SAME ORDER as input.';

    const chunkSize = meta.chunkSize || 500;
    const lines = [];
    
    for (let start = 0; start < repairItems.length; start += chunkSize) {
        const chunk = repairItems.slice(start, start + chunkSize);
        const body = {
            model: meta.model,
            input: [
              { role: "system", content: `${systemPromptContent}${suffix}` },
              { role: "user", content: "Return only a json object as specified. The output must be valid json." },
              { role: "user", content: JSON.stringify({ rows: chunk }) }
            ],
            text: { format: { type: "json_object" } },
        };
        lines.push(JSON.stringify({ custom_id: String(chunk[0].id), method: "POST", url: "/v1/responses", body }));
    }

    const jsonlBuffer = Buffer.from(lines.join("\n"), "utf8");
    const jsonlFile = await client.files.create({ file: await toFile(jsonlBuffer, `repair_${primaryBatchId}.jsonl`, { type: "application/jsonl" }), purpose: "batch" });
    const batch = await client.batches.create({ input_file_id: jsonlFile.id, endpoint: "/v1/responses", completion_window: "24h" });

    // 5. Save the new batch meta, preserving the original jobId so it still references the correct CSV!
    await store.set(`jobs/${batch.id}.json`, JSON.stringify({ ...meta, batchId: batch.id, isRepair: true, createdAt: new Date().toISOString() }), { contentType: "application/json" });

    return res(200, { newBatchId: batch.id });
  } catch (err) {
    return res(500, { error: err.message || String(err) });
  }
};
