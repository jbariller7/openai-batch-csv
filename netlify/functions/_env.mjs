// Minimal ESM function to confirm the runtime that Netlify uses for your functions.
import fs from "node:fs/promises";
import path from "node:path";

export default async () => {
  // Try to read the functions-level package.json if it exists
  let functionsPkgType = "not-found";
  try {
    const pkgPath = path.join(process.cwd(), "netlify", "functions", "package.json");
    const text = await fs.readFile(pkgPath, "utf8");
    functionsPkgType = JSON.parse(text)?.type || "none";
  } catch (_e) {
    // ignore
  }

  const body = {
    message: "Netlify functions environment",
    node: process.versions.node,
    runtime: process.env.AWS_LAMBDA_JS_RUNTIME || "(unset)",
    netlify: !!process.env.NETLIFY,
    functionsPkgType,
    cwd: process.cwd(),
    envHints: {
      NODE_VERSION: process.env.NODE_VERSION || "(unset)",
    }
  };

  return new Response(JSON.stringify(body, null, 2), {
    headers: { "content-type": "application/json" },
  });
};

