import OpenAI from "openai";
import { getStore } from "@netlify/blobs";
import Busboy from "busboy";
import { parse as csvParse } from "csv-parse";

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

export const config = { path: "/api/batch-create" };

function parseMultipart(event) {
  return new Promise((resolve, reject) => {
    const bb = Busboy({
      headers: {
        "content-type": event.headers["content-type"] || event.headers["Content-Type"],
      },
    });

    const fields = {};
    let fileBuffers = [];
    let fileInfo = null;

    bb.on("file", (name, file, info) => {
      fileInfo = info;
      file.on("data", (data) => fileBuffers.push(data));
    });

    bb.on("field", (name, val) => {
      fields[name] = val;
    });

    bb.on("error", reject);

    bb.on("finish", () => {
      const fileBuffer = fileBuffers.length ? Buffer.concat(fileBuffers) : null;
      resolve({ fields, fileBuffer, fileInfo });
    });

    const body = event.isBase64Encoded
      ? Buffer.from(event.body, "base64")
      : Buffer.from(event.body || "");
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

    if (!fileBuffer) {
      return new Response(JSON.stringify({ error: "CSV file is required" }), { status: 400 });
    }

    // 1) Save original CSV in Blobs under a job id
    const jobId = crypto.randomUUID();
    const store = getStore("openai-batch-csv");
    await store.set(`csv/${jobId}.csv`, fileBuffer);

    // 2) Read CSV rows
    const rows = await new Promise((resolve, reject) => {
      const out = [];
      csvParse(fileBuffer, { columns: true, relax_quotes: true })
        .on("data", (r) => out.push(r))
        .on("end", () => resolve(out))
        .on("error", (err) => reject(err));
    });

    if (!rows.length) {
      return new Response(JSON.stringify({ error: "CSV has no rows" }), { status: 400 });
    }

    // 3) Build JSONL in memory
    const suffix = ' Return only a JSON object: {"result":"..."}';
    const lines = rows.map((r, i) => {
      const body = {
        model,
        input: [
          { role: "system", content: `${prompt}${suffix}` },
          { role: "user", content: String(r[inputCol] ?? "") },
        ],
        response_format: { type: "json_object" },
      };
      return JSON.stringify({
        custom_id: String(i),
        method: "POST",
        url: "/v1/responses",
        body,
      });
    });
    const jsonlBuffer = Buffer.from(lines.join("\n"), "utf8");

    // 4) Upload JSONL to OpenAI Files
    const jsonlFile = await client.files.create({
      file: new File([jsonlBuffer], `${jobId}.jsonl`, { type: "application/jsonl" }),
      purpose: "batch",
    });

    // 5) Create Batch
    const batch = await client.batches.create({
      input_file_id: jsonlFile.id,
      endpoint: "/v1/responses",
      completion_window: completionWindow,
    });

    // 6) Save job metadata in Blobs
    await store.setJSON(`jobs/${batch.id}.json`, {
      jobId,
      batchId: batch.id,
      inputCol,
      model,
      prompt,
      createdAt: new Date().toISOString(),
    });

    return new Response(JSON.stringify({ batchId: batch.id, jobId }), { status: 200 });
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), { status: 500 });
  }
};
