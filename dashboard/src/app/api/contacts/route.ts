import { NextResponse } from "next/server";

import { backendJson, extractError } from "@/lib/backend-proxy";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  const url = new URL(request.url);
  const campaignId = url.searchParams.get("campaignId")?.trim();
  const q = url.searchParams.get("q")?.trim();
  const page = url.searchParams.get("page")?.trim();
  const limit = url.searchParams.get("limit")?.trim();

  const params = new URLSearchParams();
  if (campaignId) {
    params.set("campaign_id", campaignId);
  }
  if (q) {
    params.set("q", q);
  }
  if (page) {
    params.set("page", page);
  }
  if (limit) {
    params.set("limit", limit);
  }
  const query = params.toString();

  try {
    const result = await backendJson(`/api/contacts${query ? `?${query}` : ""}`, {
      method: "GET",
    });
    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to load contacts.") },
        { status: result.status || 500 },
      );
    }

    return NextResponse.json(result.payload ?? { contacts: [] }, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to load contacts.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function DELETE() {
  try {
    const result = await backendJson(`/api/contacts`, {
      method: "DELETE",
    });

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Failed to delete contacts") },
        { status: result.status || 500 }
      );
    }
    return NextResponse.json({ success: true, payload: result.payload }, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to delete contacts";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
