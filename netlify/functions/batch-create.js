import OpenAI from "openai";
import { getStore } from "@netlify/blobs";
import Busboy from "busboy";
import { parse as csvParse } from "csv-parse";

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

export const config = { path: "/api/batch-create" };

// Parse multipart form (file + fields)
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
  if (event.httpMethod !== "POST") {
    return new Response(JSON.stringify({ error: "POST only" }), { status: 405 });
  }

  try {
    const { fields, fileBuffer } = await parseMultipart(event);
    const inputCol = fields.inputCol || "text";
    const model = fields.model || "gpt-4.1-mini";
    const prompt = fields.prompt || "Translate to English.";
    const completionWindow = fields.completionWindow || "24h";
    const chunkSize = Math.max(1, Math.min(1000, Number(fields.chunkSize || 200))); // K
    const reasoningEffort = (fields.reasoning_effort || '').trim(); // minimal|low|medium|high
const verbosity = (fields.verbosity || '').trim();              // "", low|medium|high

    if (!fileBuffer) return new Response(JSON.stringify({ error: "CSV file is required" }), { status: 400 });

    // 1) Persist original CSV (by jobId) so we can merge later
    const jobId = crypto.randomUUID();
    const store = getStore("openai-batch-csv");
    await store.set(`csv/${jobId}.csv`, fileBuffer);

    // 2) Read CSV rows
    const rows = await new Promise((resolve, reject) => {
      const out = [];
      csvParse(fileBuffer, { columns: true, relax_quotes: true })
        .on("data", (r) => out.push(r))
        .on("end", () => resolve(out))
        .on("error", reject);
    });
    if (!rows.length) return new Response(JSON.stringify({ error: "CSV has no rows" }), { status: 400 });

    // 3) Build JSONL with micro-batches of size K
    //    The model receives: {"rows":[{"id":<rowIndex>,"text":"..."}...]}
    //    It must return only: {"results":[{"id":<rowIndex>,"result":"..."}...]}
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

const isGpt5 = model.startsWith('gpt-5');          // gpt-5, gpt-5-mini, gpt-5-nano
const isOseries = model.startsWith('o');           // o3, o4-mini, etc.

const body = {
  model,
  input: [
    { role: "system", content: `${prompt}${suffix}` },
    { role: "user", content: JSON.stringify({ rows: chunk }) }
  ],
  response_format: { type: "json_object" },
  temperature: 0,
  ...( (isGpt5 || isOseries) && reasoningEffort ? { reasoning_effort: reasoningEffort } : {} ),
  ...( isGpt5 && verbosity ? { text: { verbosity } } : {} )
};


      lines.push(JSON.stringify({
        custom_id: String(start), // helps us map results if ids are missing
        method: "POST",
        url: "/v1/responses",
        body
      }));
    }

    const jsonlBuffer = Buffer.from(lines.join("\n"), "utf8");

    // 4) Upload JSONL to OpenAI and create a Batch
    const jsonlFile = await client.files.create({
      file: new File([jsonlBuffer], `${jobId}.jsonl`, { type: "application/jsonl" }),
      purpose: "batch"
    });

    const batch = await client.batches.create({
      input_file_id: jsonlFile.id,
      endpoint: "/v1/responses",
      completion_window: completionWindow
    });

    // 5) Save minimal job metadata
    await store.setJSON(`jobs/${batch.id}.json`, {
      jobId,
      batchId: batch.id,
      inputCol,
      model,
      prompt,
      chunkSize,
      createdAt: new Date().toISOString()
    });

    return new Response(JSON.stringify({ batchId: batch.id, jobId }), { status: 200 });
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), { status: 500 });
  }
};
