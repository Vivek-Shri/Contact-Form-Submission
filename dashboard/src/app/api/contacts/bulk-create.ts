import { NextResponse } from "next/server";
import { backendJson, extractError } from "@/lib/backend-proxy";
import { getServerSession } from "next-auth";
import { authOptions } from "@/app/api/auth/[...nextauth]/route";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(
  request: Request,
  { params }: { params: Promise<{ campaignId: string }> }
) {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    const { campaignId } = await params;
    const body = (await request.json()) as unknown;

    const result = await backendJson(
      `/api/campaigns/${encodeURIComponent(campaignId)}/contacts/bulk`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      },
      { userId: (session.user as any).id, isAdmin: (session.user as any).isAdmin }
    );

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Failed to bulk import contacts.") },
        { status: result.status || 500 }
      );
    }

    return NextResponse.json(result.payload ?? { message: "Success" }, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to bulk import contacts.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
