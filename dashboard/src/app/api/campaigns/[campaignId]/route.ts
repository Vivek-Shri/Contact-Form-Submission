import { backendJson, extractError } from "@/lib/backend-proxy";
import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/app/api/auth/[...nextauth]/route";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

interface Params {
  campaignId: string;
}

export async function GET(_: Request, { params }: { params: Promise<Params> }) {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const resolvedParams = await params;
  const campaignId = encodeURIComponent(resolvedParams.campaignId);

  try {
    const result = await backendJson(
      `/api/campaigns/${campaignId}`,
      { method: "GET" },
      { userId: (session.user as any).id, isAdmin: (session.user as any).isAdmin }
    );
    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to load campaign details.") },
        { status: result.status || 500 },
      );
    }

    return NextResponse.json(result.payload ?? {}, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to load campaign details.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function PUT(request: Request, { params }: { params: Promise<Params> }) {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const resolvedParams = await params;
  const campaignId = encodeURIComponent(resolvedParams.campaignId);
  let body: unknown;

  try {
    body = (await request.json()) as unknown;
  } catch {
    return NextResponse.json({ error: "Invalid JSON payload." }, { status: 400 });
  }

  try {
    const result = await backendJson(
      `/api/campaigns/${campaignId}`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: body,
      },
      { userId: (session.user as any).id, isAdmin: (session.user as any).isAdmin }
    );

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to update campaign.") },
        { status: result.status || 500 },
      );
    }

    return NextResponse.json(result.payload ?? {}, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to update campaign.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function DELETE(_: Request, { params }: { params: Promise<Params> }) {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const resolvedParams = await params;
  const campaignId = encodeURIComponent(resolvedParams.campaignId);

  try {
    const result = await backendJson(
      `/api/campaigns/${campaignId}`,
      { method: "DELETE" },
      { userId: (session.user as any).id, isAdmin: (session.user as any).isAdmin }
    );

    if (!result.ok) {
      return NextResponse.json(
        { error: extractError(result.payload, "Unable to delete campaign.") },
        { status: result.status || 500 },
      );
    }

    return NextResponse.json(result.payload ?? {}, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to delete campaign.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}