import { backendJson, extractError } from "@/lib/backend-proxy";
import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/app/api/auth/[...nextauth]/route";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(request: Request) {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    const payload = await request.json();
    const result = await backendJson(
      "/api/contacts/check-exists",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      },
      { userId: (session.user as any).id, isAdmin: (session.user as any).isAdmin }
    );

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Failed to check existing contacts") },
        { status: result.status || 500 }
      );
    }
    return NextResponse.json(result.payload, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to check existing contacts";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
