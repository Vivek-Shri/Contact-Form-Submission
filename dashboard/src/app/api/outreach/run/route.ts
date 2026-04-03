import { NextResponse } from "next/server";

import {
  getOutreachRunSnapshot,
  startOutreachRun,
  type RunLeadInput,
  type RunPersonaInput,
} from "./_store";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

interface StartRunRequestBody {
  persona?: RunPersonaInput;
  leads?: RunLeadInput[];
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

export async function POST(request: Request) {
  let body: StartRunRequestBody;

  try {
    body = (await request.json()) as StartRunRequestBody;
  } catch {
    return NextResponse.json({ error: "Invalid JSON payload." }, { status: 400 });
  }

  const { persona, leads } = body;

  if (!isValidPersona(persona)) {
    return NextResponse.json({ error: "Invalid persona payload." }, { status: 400 });
  }

  if (!Array.isArray(leads) || !leads.every((lead) => isValidLead(lead))) {
    return NextResponse.json({ error: "Invalid leads payload." }, { status: 400 });
  }

  try {
    const snapshot = await startOutreachRun(persona, leads);
    return NextResponse.json(snapshot, { status: 200 });
  } catch (error) {
    const err = error as Error & { code?: string; runId?: string };

    if (err.code === "RUN_IN_PROGRESS") {
      return NextResponse.json(
        {
          error: err.message,
          runId: err.runId,
        },
        { status: 409 },
      );
    }

    if (
      err.code === "DAILY_LIMIT_REACHED" ||
      err.code === "CAPTCHA_CREDITS_EXHAUSTED" ||
      err.code === "NO_ELIGIBLE_LEADS"
    ) {
      return NextResponse.json(
        {
          error: err.message,
          code: err.code,
        },
        { status: 422 },
      );
    }

    return NextResponse.json(
      {
        error: err.message || "Unable to start backend outreach run.",
      },
      { status: 500 },
    );
  }
}

export async function GET(request: Request) {
  const url = new URL(request.url);
  const runId = url.searchParams.get("runId")?.trim();

  if (!runId) {
    return NextResponse.json({ error: "runId query parameter is required." }, { status: 400 });
  }

  const snapshot = getOutreachRunSnapshot(runId);
  if (!snapshot) {
    return NextResponse.json({ error: "Run not found." }, { status: 404 });
  }

  return NextResponse.json(snapshot, { status: 200 });
}
