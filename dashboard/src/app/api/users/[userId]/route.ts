import { backendJson, extractError } from "@/lib/backend-proxy";
import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/app/api/auth/[...nextauth]/route";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

interface Params {
  userId: string;
}

export async function PUT(request: Request, { params }: { params: Promise<Params> }) {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }
  // if (!(session.user as any).isAdmin) {
  //   return NextResponse.json({ error: "Admin access required" }, { status: 403 });
  // }

  const { userId } = await params;
  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON" }, { status: 400 });
  }

  try {
    const result = await backendJson(
      `/api/users/${userId}/role`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: body,
      },
      { userId: (session.user as any).id, isAdmin: true }
    );

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to update user.") },
        { status: result.status || 500 }
      );
    }
    return NextResponse.json(result.payload ?? {}, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to update user.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function DELETE(_: Request, { params }: { params: Promise<Params> }) {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }
  // if (!(session.user as any).isAdmin) {
  //   return NextResponse.json({ error: "Admin access required" }, { status: 403 });
  // }

  const { userId } = await params;

  try {
    const result = await backendJson(
      `/api/users/${encodeURIComponent(userId)}`,
      { method: "DELETE" },
      { userId: (session.user as any).id, isAdmin: true }
    );

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to delete user.") },
        { status: result.status || 500 }
      );
    }
    return NextResponse.json(result.payload ?? {}, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to delete user.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
