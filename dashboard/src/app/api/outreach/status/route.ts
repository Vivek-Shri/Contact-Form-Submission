import { NextResponse } from "next/server";

import { backendJson, extractError } from "@/lib/backend-proxy";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const result = await backendJson("/outreach/status", { method: "GET" });

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to fetch run status.") },
        { status: result.status || 500 },
      );
    }

    return NextResponse.json(result.payload ?? {}, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to fetch run status.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
