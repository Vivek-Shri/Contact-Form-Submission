import { NextResponse } from "next/server";

import { backendJson, extractError } from "@/lib/backend-proxy";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

interface Params {
  campaignId: string;
  contactId: string;
}

export async function DELETE(_: Request, { params }: { params: Params }) {
  const campaignId = encodeURIComponent(params.campaignId);
  const contactId = encodeURIComponent(params.contactId);

  try {
    const result = await backendJson(`/api/campaigns/${campaignId}/contacts/${contactId}`, {
      method: "DELETE",
    });

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to delete campaign contact.") },
        { status: result.status || 500 },
      );
    }

    return NextResponse.json(result.payload ?? {}, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to delete campaign contact.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
