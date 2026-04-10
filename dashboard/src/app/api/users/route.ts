import { backendJson, extractError } from "@/lib/backend-proxy";
import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/app/api/auth/[...nextauth]/route";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  // if (!(session.user as any).isAdmin) {
  //   return NextResponse.json({ error: "Admin access required" }, { status: 403 });
  // }

  try {
    const result = await backendJson(
      "/api/users",
      { method: "GET" },
      { userId: (session.user as any).id, isAdmin: true }
    );

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to load users.") },
        { status: result.status || 500 }
      );
    }

    return NextResponse.json(result.payload ?? { users: [] }, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to load users.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
