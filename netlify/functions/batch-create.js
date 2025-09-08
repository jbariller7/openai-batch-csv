import OpenAI from "openai";
import { getStore } from "@netlify/blobs";
import Busboy from "busboy";
import { parse as csvParse } from "csv-parse";
import fs from "node:fs/promises";
import { createReadStream } from "node:fs";
import path from "node:path";

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// optional: you can remove config.path, since we now call /.netlify/functions/batch-create
export const config = { /* path: "/api/batch-create" */ };

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST,OPTIONS,HEAD",
  "Access-Control-Allow-Headers": "Content-Type"
};

// Parse multipart (CSV + fields)
function parseMultipart(event) {
  return new Promise((resolve, reject) => {
    const bb = Busboy({
      headers: { "content-type": event.headers["content-type"] || event.headers["Content-Type"] }
    });

    const fields = {};
    let fileBuffers = [];
    let fileInfo = null;

    bb.on("file", (_name, file, info) => {
      fileInfo = info; // { filename, mimeType, encoding }
      file.on("data", (d) => fileBuffers.push(d));
    });

    bb.on("field", (name, val) => { fields[name] = val; });
    bb.on("error", reject);
    bb.on("finish", () => {
      const fileBuffer = fileBuffers.length ? Buffer.concat(fileBuffers) : null;
      resolve({ fields, fileBuffer, fileInfo });
    });

    const body = event.isBase64Encoded ? Buffer.from(event.body, "base64") : Buffer.from(event.body || "");
    bb.end(body);
  });
}

export default async (event) => {
  if (event.httpMethod === "OPTIONS" || event.httpMethod === "HEAD") {
    return new Response("", { status: 204, headers: CORS });
  }
  if (event.httpMethod !== "POST") {
    return new Response(JSON.stringify({ error: "POST only" }), { status: 405, headers: CORS });
  }

  try {
    const { fields, fileBuffer } = await parseMultipart(event);
    const inputCol = fields.inputCol || "text";
    const model = fields.model || "gpt-4.1-mini";
    const prompt = fields.prompt || "Translate to English.";
    const completionWindow = fields.completionWindow || "24h";
    const chunkSize = Math.max(1, Math.min(1000, Number(fields.chunkSize || 200)));

    const reasoningEffort = (fields.reasoning_effort || "").trim(); // minimal|low|medium|high
    const verbosity = (fields.verbosity || "").trim(); // "", low|medium|high (GPT-5)

    if (!fileBuffer) return new Response(JSON.stringify({ error: "CSV file is required" }), { status: 400, headers: CORS });

    // Persist original CSV by jobId
    const jobId = crypto.randomUUID();
    const store = getStore("openai-batch-csv");
    await store.set(`csv/${jobId}.csv`, fileBuffer);

    // Read CSV rows
    const rows = await new Promise((resolve, reject) => {
      const out = [];
      csvParse(fileBuffer, { columns: true, relax_quotes: true })
        .on("data", (r) => out.push(r))
        .on("end", () => resolve(out))
        .on("error", reject);
    });
    if (!rows.length) return new Response(JSON.stringify({ error: "CSV has no rows" }), { status: 400, headers: CORS });

    // Build JSONL with micro-batches of size K
    const isGpt5 = model.startsWith("gpt-5");
    const isOseries = /^o\d/i.test(model) || model.startsWith("o");

    const suffix =
      ' You will receive a JSON object {"rows":[{"id":number,"text":string},...]}.' +
      ' For each item, produce {"id": same id, "result": <string>} following the user instructions above.' +
      ' Return ONLY a JSON object: {"results":[{"id":number,"result":string},...]} in the SAME ORDER as input. Do not include any commentary.';

    const lines = [];
    for (let start = 0; start < rows.length; start += chunkSize) {
      const chunk = rows.slice(start, start + chunkSize).map((r, j) => ({
        id: start + j,
        text: String(r?.[inputCol] ?? "")
      }));

      const body = {
        model,
        input: [
          { role: "system", content: `${prompt}${suffix}` },
          { role: "user", content: JSON.stringify({ rows: chunk }) }
        ],
        response_format: { type: "json_object" },
        temperature: 0,
        ...(((isGpt5 || isOseries) && reasoningEffort) ? { reasoning_effort: reasoningEffort } : {}),
        ...(isGpt5 && verbosity ? { text: { verbosity } } : {})
      };

      lines.push(JSON.stringify({
        custom_id: String(start), // helps mapping if ids are missing
        method: "POST",
        url: "/v1/responses",
        body
      }));
    }

    const jsonlBuffer = Buffer.from(lines.join("\n"), "utf8");

    // Upload JSONL to OpenAI via tmp file
    const tmpPath = path.join("/tmp", `${jobId}.jsonl`);
    await fs.writeFile(tmpPath, jsonlBuffer);
    const jsonlFile = await client.files.create({
      file: createReadStream(tmpPath),
      purpose: "batch"
    });

    const batch = await client.batches.create({
      input_file_id: jsonlFile.id,
      endpoint: "/v1/responses",
      completion_window: completionWindow
    });

    // Save minimal job metadata
    await store.setJSON(`jobs/${batch.id}.json`, {
      jobId,
      batchId: batch.id,
      inputCol,
      model,
      prompt,
      chunkSize,
      reasoningEffort,
      verbosity,
      createdAt: new Date().toISOString()
    });

    return new Response(JSON.stringify({ batchId: batch.id, jobId }), { status: 200, headers: CORS });
  } catch (err) {
    console.error("batch-create error:", err);
    return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: CORS });
  }
};
