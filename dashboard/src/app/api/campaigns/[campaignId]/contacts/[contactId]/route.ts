import { NextResponse } from "next/server";
import { backendJson, extractError } from "@/lib/backend-proxy";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

interface Params {
  campaignId: string;
  contactId: string;
}

export async function DELETE(
  _request: Request,
  { params }: { params: Promise<Params> }
) {
  const { campaignId, contactId } = await params;
  try {
    const result = await backendJson(
      `/api/campaigns/${encodeURIComponent(campaignId)}/contacts/${encodeURIComponent(contactId)}`,
      { method: "DELETE" }
    );
    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to delete contact.") },
        { status: result.status || 500 }
      );
    }
    return NextResponse.json(result.payload ?? {}, { status: 200 });
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unable to delete contact.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function PATCH(
  request: Request,
  { params }: { params: Promise<Params> }
) {
  const { campaignId, contactId } = await params;
  let body: unknown;
  try {
    body = (await request.json()) as unknown;
  } catch {
    return NextResponse.json({ error: "Invalid JSON payload." }, { status: 400 });
  }

  try {
    const result = await backendJson(
      `/api/campaigns/${encodeURIComponent(campaignId)}/contacts/${encodeURIComponent(contactId)}`,
      {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }
    );
    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to update contact.") },
        { status: result.status || 500 }
      );
    }
    return NextResponse.json(result.payload ?? {}, { status: 200 });
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unable to update contact.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
