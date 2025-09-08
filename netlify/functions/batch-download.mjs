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
      return new Response(JSON.stringify({ error: "
