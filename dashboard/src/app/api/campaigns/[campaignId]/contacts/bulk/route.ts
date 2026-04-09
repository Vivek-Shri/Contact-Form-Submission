import { NextResponse } from "next/server";
import { backendJson } from "@/lib/backend-proxy";

export async function POST(
  request: Request,
  { params }: { params: Promise<{ campaignId: string }> }
) {
  try {
    const { campaignId } = await params;
    const body = await request.json();

    const result = await backendJson(`/api/campaigns/${campaignId}/contacts/bulk`, {
      method: "POST",
      body,
    });

    if (result.status && result.status >= 400) {
      return NextResponse.json(
        { error: "Failed to bulk import contacts." },
        { status: result.status }
      );
    }

    return NextResponse.json(result.payload ?? { message: "Success" }, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to bulk import contacts.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
