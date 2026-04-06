import { NextResponse } from "next/server";

import { stopActiveOutreachRun, stopOutreachRun } from "../_store";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

interface StopRunBody {
  runId?: string;
}

export async function POST(request: Request) {
  let body: StopRunBody = {};

  try {
    body = (await request.json()) as StopRunBody;
  } catch {
    body = {};
  }

  const runId = body.runId?.trim();
  let snapshot = runId ? stopOutreachRun(runId) : null;

  if (!snapshot) {
    snapshot = stopActiveOutreachRun();
  }

  if (!snapshot) {
    return NextResponse.json({ error: "Run not found." }, { status: 404 });
  }

  return NextResponse.json(snapshot, { status: 200 });
}
