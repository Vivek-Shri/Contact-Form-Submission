import { NextResponse } from "next/server";

import { backendJson, extractError } from "@/lib/backend-proxy";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  const url = new URL(request.url);
  const q = url.searchParams.get("q")?.trim() || "";
  const page = url.searchParams.get("page")?.trim() || "";
  const limit = url.searchParams.get("limit")?.trim() || "";

  const params = new URLSearchParams();
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
    const result = await backendJson(`/api/campaigns${query ? `?${query}` : ""}`, {
      method: "GET",
    });
    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to load campaigns.") },
        { status: result.status || 500 },
      );
    }

    return NextResponse.json(result.payload ?? { campaigns: [] }, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to load campaigns.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function POST(request: Request) {
  let body: unknown;

  try {
    body = (await request.json()) as unknown;
  } catch {
    return NextResponse.json({ error: "Invalid JSON payload." }, { status: 400 });
  }

  try {
    const result = await backendJson("/api/campaigns", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });

    if (!result.ok) {
      console.log("[POST /api/campaigns] Backend error:", result.status, result.payload);
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to create campaign.") },
        { status: result.status || 500 },
      );
    }

    return NextResponse.json(result.payload ?? {}, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to create campaign.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
