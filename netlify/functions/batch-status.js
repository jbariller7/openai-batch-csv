import OpenAI from "openai";
const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

export const config = { path: "/api/batch-status" };

export default async (event) => {
  const url = new URL(event.rawUrl);
  const id = url.searchParams.get("id");
  if (!id) {
    return new Response(JSON.stringify({ error: "Missing id" }), { status: 400 });
  }
  try {
    const b = await client.batches.retrieve(id);
    return new Response(
      JSON.stringify({ id: b.id, status: b.status, created_at: b.created_at, output_file_id: b.output_file_id || null }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  } catch (e) {
    console.error("batch-status error:", e);
    return new Response(JSON.stringify({ error: e.message }), { status: 500 });
  }
};
