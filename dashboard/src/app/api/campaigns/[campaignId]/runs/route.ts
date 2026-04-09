import { NextResponse } from "next/server";

import { backendJson, extractError } from "@/lib/backend-proxy";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

interface Params {
  campaignId: string;
}

export async function GET(request: Request, { params }: { params: Promise<Params> }) {
  const resolvedParams = await params;
  const campaignId = encodeURIComponent(resolvedParams.campaignId);
  const url = new URL(request.url);
  const limit = url.searchParams.get("limit")?.trim();
  const query = limit ? `?limit=${encodeURIComponent(limit)}` : "";

  try {
    const result = await backendJson(`/api/campaigns/${campaignId}/runs${query}`, {
      method: "GET",
    });

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to load campaign runs.") },
        { status: result.status || 500 },
      );
    }

    return NextResponse.json(result.payload ?? { runs: [] }, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to load campaign runs.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
