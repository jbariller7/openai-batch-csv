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
      JSON.stringify({
        id: b.id,
        status: b.status,
        request_counts: b.request_counts,
        output_file_id: b.output_file_id || null,
      }),
      { status: 200 }
    );
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), { status: 500 });
  }
};
