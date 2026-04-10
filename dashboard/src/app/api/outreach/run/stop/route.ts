import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/app/api/auth/[...nextauth]/route";

import {
  extractBackendErrorMessage,
  fetchBackendSnapshot,
  parseJsonObject,
  resolveBackendBaseUrl,
  toDashboardSnapshot,
} from "../_backend";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

interface StopRunBody {
  runId?: string;
}

function payloadRunId(payload: Record<string, unknown> | null): string | null {
  if (!payload) {
    return null;
  }

  const runId = payload.run_id ?? payload.runId;
  if (typeof runId !== "string" || !runId.trim()) {
    return null;
  }

  return runId.trim();
}

export async function POST(request: Request) {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  let body: StopRunBody = {};

  try {
    body = (await request.json()) as StopRunBody;
  } catch {
    body = {};
  }

  const requestedRunId = body.runId?.trim();

  try {
    const backendBaseUrl = resolveBackendBaseUrl();
    const backendResponse = await fetch(`${backendBaseUrl}/outreach/stop`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-User-Id": (session.user as any).id,
        "X-Is-Admin": (session.user as any).isAdmin ? "true" : "false",
      },
      cache: "no-store",
      body: JSON.stringify({ run_id: requestedRunId || undefined }),
    });

    const payload = await parseJsonObject(backendResponse);

    if (!backendResponse.ok) {
      if (backendResponse.status === 409) {
        if (requestedRunId) {
          const options = { userId: (session.user as any).id, isAdmin: (session.user as any).isAdmin };
          const existingSnapshot = await fetchBackendSnapshot(requestedRunId, options);
          if (existingSnapshot) {
            return NextResponse.json(existingSnapshot, { status: 200 });
          }
        }

        return NextResponse.json({ error: "Run not found." }, { status: 404 });
      }

      const message = extractBackendErrorMessage(payload, "Unable to stop run.");
      return NextResponse.json({ error: message }, { status: 500 });
    }

    const resolvedRunId = requestedRunId || payloadRunId(payload);
    if (resolvedRunId) {
      const options = { userId: (session.user as any).id, isAdmin: (session.user as any).isAdmin };
      const snapshot = await fetchBackendSnapshot(resolvedRunId, options);
      if (snapshot) {
        return NextResponse.json(snapshot, { status: 200 });
      }
    }

    if (payload) {
      return NextResponse.json(toDashboardSnapshot(payload, []), { status: 200 });
    }

    return NextResponse.json({ error: "Unable to resolve stopped run." }, { status: 500 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to stop run.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
