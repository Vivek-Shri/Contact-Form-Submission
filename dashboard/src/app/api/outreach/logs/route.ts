import { NextResponse } from "next/server";
import { backendJson, extractError } from "@/lib/backend-proxy";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  const url = new URL(request.url);
  const run_id = url.searchParams.get("run_id")?.trim();
  const tail = url.searchParams.get("tail")?.trim() || "200";

  let backendUrl = `/api/outreach/logs?tail=${tail}`;
  if (run_id) backendUrl += `&run_id=${encodeURIComponent(run_id)}`;

  try {
    const result = await backendJson(backendUrl, { method: "GET" });

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to load outreach logs.") },
        { status: result.status || 500 }
      );
    }

    return NextResponse.json(result.payload ?? { lines: [] }, { status: 200 });
  } catch (error) {
    return NextResponse.json({ error: error instanceof Error ? error.message : "Error loading logs" }, { status: 500 });
  }
}
