import OpenAI from "openai";
const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

export const config = { /* path: "/api/batch-status" */ };

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
  if (method === "OPTIONS" || method === "HEAD") {
    return new Response("", { status: 204, headers: CORS });
  }

  const url = new URL(getUrl(reqOrEvent));
  const id = url.searchParams.get("id");
  if (!id) {
    return new Response(JSON.stringify({ error: "Missing id" }), {
      status: 400,
      headers: CORS,
    });
  }

  try {
    const b = await client.batches.retrieve(id);
    const body = {
      id: b.id,
      status: b.status,
      created_at: b.created_at,
      in_progress_at: b.in_progress_at || null,
      completed_at: b.completed_at || null,
      output_file_id: b.output_file_id || null,
      error_file_id: b.error_file_id || null,
      request_counts: b.request_counts || null,
      errors: b.errors || null,
    };
    return new Response(JSON.stringify(body), {
      status: 200,
      headers: { ...CORS, "Content-Type": "application/json" },
    });
  } catch (e) {
    console.error("batch-status error:", e);
    return new Response(JSON.stringify({ error: e.message }), {
      status: 500,
      headers: COR
