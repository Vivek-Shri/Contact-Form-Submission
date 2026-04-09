import { NextResponse } from "next/server";
import { backendJson, extractError } from "@/lib/backend-proxy";

export const runtime = "nodejs";

export async function DELETE(
  request: Request,
  { params }: { params: Promise<{ contactId: string }> }
) {
  try {
    const { contactId } = await params;
    const result = await backendJson(`/api/contacts/${contactId}`, {
      method: "DELETE",
    });

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Failed to delete contact") },
        { status: result.status || 500 }
      );
    }
    return NextResponse.json({ success: true }, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to delete contact";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
