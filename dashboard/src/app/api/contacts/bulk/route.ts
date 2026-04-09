import { NextResponse } from "next/server";
import { backendJson, extractError } from "@/lib/backend-proxy";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(request: Request) {
  try {
    const payload = await request.json();
    const result = await backendJson("/api/contacts/bulk", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Failed to bulk import contacts") },
        { status: result.status || 500 }
      );
    }
    return NextResponse.json(result.payload, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to bulk import contacts";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
