import OpenAI from "openai";
import { getStore } from "@netlify/blobs";
import { parse as parseCsvSync } from "csv-parse/sync";
import { stringify as stringifyCsvSync } from "csv-stringify/sync";

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

export const config = { /* path: "/api/batch-download" */ };

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,OPTIONS,HEAD",
  "Access-Control-Allow-Headers": "Content-Type",
};

function getMethod(req) {
  if (req && typeof req.method === "string") return req.method;
  if (req && typeof req.httpMethod === "string") return req.httpMethod;
  return "GET";
}
function getUrl(req) {
  return typeof req.url === "string" ? req.url : (req.rawUrl || "");
}

export default async (reqOrEvent) => {
  const method = getMethod(reqOrEvent);
  if (method === "OPTIONS" || method === "HEAD")
    return new Response("", { status: 204, headers: CORS });

  const url = new URL(getUrl(reqOrEvent));
  const id = url.searchParams.get("id");
  if (!id)
    return new Response(JSON.stringify({ error: "Missing id" }), {
      status: 400,
      headers: CORS,
    });

  try {
    // 1) Confirm batch is completed or terminal
    const b = await client.batches.retrieve(id);
    if (!["completed", "failed", "cancelled", "expired"].includes(b.status)) {
      return new Response(
        JSON.stringify({ error: `Batch not completed. Status: ${b.status}` }),
        { status: 400, headers: CORS }
      );
    }

    // 2) Read stored job metadata and original CSV
    const store = getStore("openai-batch-csv");
    const meta = await store.get(`jobs/${id}.json`, { type: "json" });
    if (!meta)
      return new Response(JSON.stringify({ error: "Job metadata not found" }), {
        status: 404,
        headers: CORS,
      });

    const csvBuf = await store.get(`csv/${meta.jobId}.csv`, { type: "buffer" });
    if (!csvBuf)
      return new Response(JSON.stringify({ error: "Original CSV not found" }), {
        status: 404,
        headers: CORS,
      });

    const rows = parseCsvSync(csvBuf.toString("utf8"), {
      columns: true,
      relax_quotes: true,
      skip_empty_lines: true,
    });

    // Prepare merged rows
    const merged = rows.map((r) => ({ ...r, result: "" }));

    const assignByArray = (arr, baseIndex = 0) => {
      arr.forEach((item, j) => {
        const idx = Number.isFinite(Number(item?.id)) ? Number(item.id) : baseIndex + j;
        if (idx >= 0 && idx < merged.length) {
          merged[idx].result =
            typeof item?.result === "string"
              ? item.result
              : item?.toString?.() ?? "";
        }
      });
    };

    // 3) Retrieve output (or error) JSONL file from OpenAI
    const fileId = b.output_file_id || b.error_file_id;
    if (!fileId) {
      return new Response(JSON.stringify({ error: "No output or error file id" }), {
        status: 400,
        headers: CORS,
      });
    }

    const fileResp = await client.files.content(fileId);
    const text = await fileResp.text();
    const lines = text.trim().split("\n");
    const isErrorOnly = !b.output_file_id && !!b.error_file_id;
    const K = Number(meta.chunkSize || 1) || 1;

    for (const line of lines) {
      if (!line.trim()) continue;
      let obj;
      try {
        obj = JSON.parse(line);
      } catch {
        continue;
      }

      const base = Number(obj?.custom_id) || 0;

      // Success path
      const body = obj?.response?.body || {};
      const successText =
        typeof body?.output_text === "string"
          ? body.output_text
          : typeof body?.content === "string"
          ? body.content
          : "";

      // Error path
      const errObj = obj?.error || null;
      const errMsg = errObj
        ? errObj?.message || errObj?.code || JSON.stringify(errObj)
        : "";

      if (!isErrorOnly && successText) {
        let parsed = null;
        try {
          parsed = JSON.parse(successText);
        } catch {
          parsed = null;
        }

        if (parsed && Array.isArray(parsed.results)) {
          assignByArray(parsed.results, base);
        } else if (Array.isArray(parsed)) {
          assignByArray(parsed, base);
        } else if (parsed && typeof parsed.result === "string") {
          if (base >= 0 && base < merged.length) merged[base].result = parsed.result;
        } else if (typeof successText === "string" && successText) {
          if (base >= 0 && base < merged.length) merged[base].result = successText;
        }
      } else if (errMsg) {
        // Entire chunk failed -> mark each row in that chunk with the same error
        for (let j = 0; j < K; j++) {
          const idx = base + j;
          if (idx < merged.length) merged[idx].result = `ERROR: ${errMsg}`;
        }
      }
    }

    const csvOut = stringifyCsvSync(merged, { header: true });
    const fname = `${meta.jobId || "results"}.csv`;

    return new Response(csvOut, {
      status: 200,
      headers: {
        ...CORS,
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Disposition": `attachment; filename="${fname}"`,
        "Cache-Control": "no-store",
      },
    });
  } catch (e) {
    console.error("batch-download error:", e);
    return new Response(JSON.stringify({ error: e.message }), {
      status: 500,
      headers: CORS,
    });
  }
};
