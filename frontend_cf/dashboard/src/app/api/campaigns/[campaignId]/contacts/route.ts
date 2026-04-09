import { NextResponse } from "next/server";

import { backendJson, extractError } from "@/lib/backend-proxy";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

interface Params {
  campaignId: string;
}

export async function GET(request: Request, { params }: { params: Params }) {
  const campaignId = encodeURIComponent(params.campaignId);
  const url = new URL(request.url);
  const q = url.searchParams.get("q")?.trim();
  const page = url.searchParams.get("page")?.trim();
  const limit = url.searchParams.get("limit")?.trim();

  const queryParams = new URLSearchParams();
  if (q) {
    queryParams.set("q", q);
  }
  if (page) {
    queryParams.set("page", page);
  }
  if (limit) {
    queryParams.set("limit", limit);
  }
  const query = queryParams.toString();

  try {
    const result = await backendJson(`/api/campaigns/${campaignId}/contacts${query ? `?${query}` : ""}`, {
      method: "GET",
    });

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to load campaign contacts.") },
        { status: result.status || 500 },
      );
    }

    return NextResponse.json(result.payload ?? { contacts: [] }, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to load campaign contacts.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function POST(request: Request, { params }: { params: Params }) {
  const campaignId = encodeURIComponent(params.campaignId);
  let body: unknown;

  try {
    body = (await request.json()) as unknown;
  } catch {
    return NextResponse.json({ error: "Invalid JSON payload." }, { status: 400 });
  }

  try {
    const result = await backendJson(`/api/campaigns/${campaignId}/contacts`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to create campaign contact.") },
        { status: result.status || 500 },
      );
    }

    return NextResponse.json(result.payload ?? {}, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to create campaign contact.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
