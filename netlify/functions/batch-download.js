import OpenAI from "openai";
import { getStore } from "@netlify/blobs";
import { parse as csvParse } from "csv-parse";
import { stringify as csvStringify } from "csv-stringify";

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

export const config = { path: "/api/batch-download" };

export default async (event) => {
  const url = new URL(event.rawUrl);
  const id = url.searchParams.get("id");
  if (!id) {
    return new Response(JSON.stringify({ error: "Missing id" }), { status: 400 });
  }
  try {
    const b = await client.batches.retrieve(id);
    if (b.status !== "completed") {
      return new Response(JSON.stringify({ error: `Batch not completed. Status: ${b.status}` }), { status: 400 });
    }
    if (!b.output_file_id) {
      return new Response(JSON.stringify({ error: "No output file id" }), { status: 400 });
    }
    const store = getStore("openai-batch-csv");
    const meta = await store.getJSON(`jobs/${id}.json`);
    if (!meta) {
      return new Response(JSON.stringify({ error: "Job metadata not found" }), { status: 404 });
    }
    const csvBuf = await store.get(`csv/${meta.jobId}.csv`, { type: "buffer" });
    if (!csvBuf) {
      return new Response(JSON.stringify({ error: "Original CSV not found" }), { status: 404 });
    }
    const out = await client.files.content(b.output_file_id);
    const outputBuf = Buffer.from(await out.arrayBuffer());
    const lines = outputBuf.toString("utf8").trim().split("\n");
    const rows = await new Promise((resolve, reject) => {
      const arr = [];
      csvParse(csvBuf, { columns: true, relax_quotes: true })
        .on("data", (r) => arr.push(r))
        .on("end", () => resolve(arr))
        .on("error", reject);
    });
    const results = rows.map((r) => ({ ...r }));
    let i = 0;
    for (const line of lines) {
      const obj = JSON.parse(line);
      const text = obj?.response?.body?.output_text ?? "";
      let parsed;
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = { result: text };
      }
      results[i].result = parsed.result ?? "";
      i++;
    }
    const headers = Object.keys(results[0] || {});
    const csvStr = await new Promise((resolve, reject) => {
      csvStringify(results, { header: true, columns: headers }, (err, out) => {
        if (err) reject(err);
        else resolve(out);
      });
    });
    return new Response(csvStr, {
      status: 200,
      headers: {
        "Content-Type": "text/csv",
        "Content-Disposition": `attachment; filename=\"${id}.csv\"`,
      },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), { status: 500 });
  }
};
