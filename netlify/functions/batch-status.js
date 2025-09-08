import OpenAI from "openai";
const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

export const config = { path: "/api/batch-status" };

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type"
};

export default async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return new Response("", { status: 204, headers: CORS });
  }

  const url = new URL(event.rawUrl);
  const id = url.searchParams.get("id");
  if (!id) {
    return new Response(JSON.stringify({ error: "Missing id" }), { status: 400, headers: CORS });
  }
  try {
    const b = await client.batches.retrieve(id);
    return new Response(
      JSON.stringify({ id: b.id, status: b.status, created_at: b.created_at, output_file_id: b.output_file_id || null }),
      { status: 200, headers: { ...CORS, "Content-Type": "application/json" } }
    );
  } catch (e) {
    console.error("batch-status error:", e);
    return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: CORS });
  }
};
