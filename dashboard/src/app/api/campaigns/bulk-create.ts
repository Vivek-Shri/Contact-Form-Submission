import { NextResponse } from "next/server";
import { backendJson, extractError } from "@/lib/backend-proxy";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(request: Request) {

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON payload." }, { status: 400 });
  }

  try {
    // Forward to backend with 1500 submissions
    const payload = (body && typeof body === "object" && !Array.isArray(body))
      ? { ...(body as Record<string, unknown>), count: 1500 }
      : { count: 1500 };
    const result = await backendJson(`/api/campaigns/bulk-create`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });
    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to create submissions.") },
        { status: result.status || 500 },
      );
    }
    return NextResponse.json(result.payload ?? {}, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to create submissions.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
