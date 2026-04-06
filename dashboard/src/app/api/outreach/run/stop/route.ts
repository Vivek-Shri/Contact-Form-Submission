import { NextResponse } from "next/server";

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
      },
      cache: "no-store",
      body: JSON.stringify({ run_id: requestedRunId || undefined }),
    });

    const payload = await parseJsonObject(backendResponse);

    if (!backendResponse.ok) {
      if (backendResponse.status === 409) {
        if (requestedRunId) {
          const existingSnapshot = await fetchBackendSnapshot(requestedRunId);
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
      const snapshot = await fetchBackendSnapshot(resolvedRunId);
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
