import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/app/api/auth/[...nextauth]/route";

import {
  buildSnapshotFromStartPayload,
  extractBackendErrorMessage,
  fetchBackendSnapshot,
  parseJsonObject,
  resolveBackendBaseUrl,
} from "./_backend";
import { type RunLeadInput, type RunPersonaInput } from "./_store";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

interface StartRunRequestBody {
  persona?: RunPersonaInput;
  leads?: RunLeadInput[];
  resume?: boolean;
  resumeFromRunId?: string;
}

function isValidPersona(persona: unknown): persona is RunPersonaInput {
  if (!persona || typeof persona !== "object") {
    return false;
  }

  const candidate = persona as Record<string, unknown>;
  return (
    typeof candidate.id === "string" &&
    typeof candidate.title === "string" &&
    typeof candidate.aiInstruction === "string" &&
    (candidate.maxDailySubmissions === undefined ||
      (typeof candidate.maxDailySubmissions === "number" &&
        Number.isFinite(candidate.maxDailySubmissions)))
  );
}

function isValidLead(lead: unknown): lead is RunLeadInput {
  if (!lead || typeof lead !== "object") {
    return false;
  }

  const candidate = lead as Record<string, unknown>;
  return typeof candidate.companyName === "string" && typeof candidate.contactUrl === "string";
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

  let body: StartRunRequestBody;

  try {
    body = (await request.json()) as StartRunRequestBody;
  } catch {
    return NextResponse.json({ error: "Invalid JSON payload." }, { status: 400 });
  }

  const { persona, leads } = body;
  const resumeFromRunIdRaw = body.resumeFromRunId;
  const resume = body.resume === undefined ? true : body.resume;
  const resumeFromRunId =
    typeof resumeFromRunIdRaw === "string" ? resumeFromRunIdRaw.trim() : undefined;

  if (!isValidPersona(persona)) {
    return NextResponse.json({ error: "Invalid persona payload." }, { status: 400 });
  }

  if (!Array.isArray(leads) || !leads.every((lead) => isValidLead(lead))) {
    return NextResponse.json({ error: "Invalid leads payload." }, { status: 400 });
  }

  if (typeof resume !== "boolean") {
    return NextResponse.json({ error: "Invalid resume flag." }, { status: 400 });
  }

  if (resumeFromRunIdRaw !== undefined && typeof resumeFromRunIdRaw !== "string") {
    return NextResponse.json({ error: "Invalid resumeFromRunId payload." }, { status: 400 });
  }

  try {
    const backendBaseUrl = resolveBackendBaseUrl();
    const backendResponse = await fetch(`${backendBaseUrl}/outreach/start`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-User-Id": (session.user as any).id,
        "X-Is-Admin": (session.user as any).isAdmin ? "true" : "false",
      },
      cache: "no-store",
      body: JSON.stringify({
        persona,
        leads,
        resume,
        resume_from_run_id: resumeFromRunId || undefined,
      }),
    });

    const payload = await parseJsonObject(backendResponse);

    if (!backendResponse.ok) {
      const message = extractBackendErrorMessage(payload, "Unable to start backend outreach run.");
      const runId = payloadRunId(payload);
      const status =
        backendResponse.status === 409
          ? 409
          : backendResponse.status === 422
            ? 422
            : 500;

      return NextResponse.json(
        {
          error: message,
          runId,
        },
        { status },
      );
    }

    if (!payload) {
      return NextResponse.json(
        { error: "Backend returned an empty start response." },
        { status: 500 },
      );
    }

    const runId = payloadRunId(payload);
    const options = { userId: (session.user as any).id, isAdmin: (session.user as any).isAdmin };
    const snapshot = runId ? await fetchBackendSnapshot(runId, options) : null;
    return NextResponse.json(snapshot ?? buildSnapshotFromStartPayload(payload), { status: 200 });
  } catch (error) {
    const err = error as Error;
    return NextResponse.json(
      {
        error: err.message || "Unable to start backend outreach run.",
      },
      { status: 500 },
    );
  }
}

export async function GET(request: Request) {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const url = new URL(request.url);
  const runId = url.searchParams.get("runId")?.trim();

  if (!runId) {
    return NextResponse.json({ error: "runId query parameter is required." }, { status: 400 });
  }

  try {
    const options = { userId: (session.user as any).id, isAdmin: (session.user as any).isAdmin };
    const snapshot = await fetchBackendSnapshot(runId, options);
    if (!snapshot) {
      return NextResponse.json({ error: "Run not found." }, { status: 404 });
    }

    return NextResponse.json(snapshot, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to fetch run status.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
