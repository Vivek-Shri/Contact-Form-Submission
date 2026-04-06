import { NextResponse } from "next/server";

import { stopOutreachRun } from "../_store";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

interface StopRunBody {
  runId?: string;
}

export async function POST(request: Request) {
  let body: StopRunBody;

  try {
    body = (await request.json()) as StopRunBody;
  } catch {
    return NextResponse.json({ error: "Invalid JSON payload." }, { status: 400 });
  }

  const runId = body.runId?.trim();
  if (!runId) {
    return NextResponse.json({ error: "runId is required." }, { status: 400 });
  }

  const snapshot = stopOutreachRun(runId);
  if (!snapshot) {
    return NextResponse.json({ error: "Run not found." }, { status: 404 });
  }

  return NextResponse.json(snapshot, { status: 200 });
}
