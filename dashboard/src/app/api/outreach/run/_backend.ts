import type { OutreachRunSnapshot, RunResultRow, RunStatus } from "./_store";

const LOCAL_BACKEND_URL = "http://64.227.188.12:8001";
const LOG_TAIL = 800;

type RuntimeEnv = Record<string, string | undefined>;

function readRuntimeEnv(): RuntimeEnv {
  const candidate = globalThis as { process?: { env?: RuntimeEnv } };
  return candidate.process?.env ?? {};
}

function normalizeBaseUrl(raw: string): string {
  const value = raw.trim();
  if (!value) {
    return "";
  }
  return value.replace(/\/+$/, "");
}

export function resolveBackendBaseUrl(): string {
  const env = readRuntimeEnv();
  const configured =
    env.OUTREACH_BACKEND_URL ||
    env.BACKEND_API_URL ||
    env.BACKEND_URL ||
    env.NEXT_PUBLIC_BACKEND_URL ||
    "";

  const normalized = normalizeBaseUrl(configured);
  if (normalized) {
    return normalized;
  }

  if (env.NODE_ENV !== "production") {
    return LOCAL_BACKEND_URL;
  }

  throw new Error("Missing OUTREACH_BACKEND_URL (or BACKEND_API_URL) environment variable.");
}

function toNumber(value: unknown, fallback: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return parsed;
}

function asString(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return "";
}

function mapRunStatus(rawStatus: string, runningFlag: boolean): RunStatus {
  const status = rawStatus.trim().toLowerCase();

  if (status === "completed" || status === "success") {
    return "completed";
  }

  if (status === "failed" || status === "error") {
    return "failed";
  }

  if (status === "stopped" || status === "stopping" || status === "cancelled") {
    return "stopped";
  }

  if (status === "running") {
    return "running";
  }

  if (status === "queued") {
    return "queued";
  }

  return runningFlag ? "running" : "queued";
}

function normalizeResultRow(raw: unknown): RunResultRow | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const candidate = raw as Record<string, unknown>;
  const submitted = asString(candidate.submitted).toLowerCase() === "yes" ? "Yes" : "No";
  const statusRaw = asString(candidate.status).toLowerCase();
  const status: "success" | "fail" | "warning" =
    statusRaw === "success" || statusRaw === "warning" ? statusRaw : "fail";

  return {
    campaignId: asString(candidate.campaignId) || asString(candidate.campaign_id) || "backend-run",
    campaignTitle:
      asString(candidate.campaignTitle) || asString(candidate.campaign_title) || "Backend Run",
    companyName: asString(candidate.companyName) || asString(candidate.company_name) || "Unknown",
    contactUrl: asString(candidate.contactUrl) || asString(candidate.contact_url),
    submitted,
    status,
    captchaStatus: asString(candidate.captchaStatus) || asString(candidate.captcha_status) || "n/a",
    confirmationMsg:
      asString(candidate.confirmationMsg) ||
      asString(candidate.confirmation_msg) ||
      asString(candidate.submission_assurance) ||
      "-",
    estCostUsd: toNumber(candidate.estCostUsd ?? candidate.est_cost, 0),
  };
}

function collectResults(raw: unknown): RunResultRow[] {
  if (!Array.isArray(raw)) {
    return [];
  }

  const rows: RunResultRow[] = [];
  for (const item of raw) {
    const normalized = normalizeResultRow(item);
    if (normalized) {
      rows.push(normalized);
    }
  }

  return rows;
}

function collectLogs(raw: unknown): string[] {
  if (!Array.isArray(raw)) {
    return [];
  }

  return raw.map((line) => asString(line)).filter((line) => line.length > 0);
}

export async function parseJsonObject(response: Response): Promise<Record<string, unknown> | null> {
  try {
    const parsed = (await response.json()) as unknown;
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
    return null;
  } catch {
    return null;
  }
}

export function extractBackendErrorMessage(
  payload: Record<string, unknown> | null,
  fallback: string,
): string {
  if (!payload) {
    return fallback;
  }

  const detail = payload.detail;
  if (typeof detail === "string" && detail.trim()) {
    return detail.trim();
  }

  const error = payload.error;
  if (typeof error === "string" && error.trim()) {
    return error.trim();
  }

  return fallback;
}

function payloadRunId(payload: Record<string, unknown>): string {
  return asString(payload.run_id) || asString(payload.runId);
}

export function toDashboardSnapshot(
  statusPayload: Record<string, unknown>,
  logs: string[],
): OutreachRunSnapshot {
  const runId = payloadRunId(statusPayload) || "unknown";
  const runningFlag = Boolean(statusPayload.running);
  
  let rawStatus = asString(statusPayload.status);
  // WORKAROUND: If backend says it's running but the `running` flag is false, assume completed.
  if (rawStatus === "running" && !runningFlag) {
    if (statusPayload.exit_code === 0 || statusPayload.exitCode === 0) {
      rawStatus = "completed";
    } else if (statusPayload.exit_code !== undefined && statusPayload.exit_code !== null) {
      rawStatus = "failed";
    } else {
      rawStatus = "stopped";
    }
  }

  const status = mapRunStatus(rawStatus, runningFlag);
  const inlineLogs = collectLogs(statusPayload.logs);
  const finalLogs = logs.length > 0 ? logs : inlineLogs;

  const initialResults = collectResults(statusPayload.results);
  
  let parsedFromLogs: NonNullable<ReturnType<typeof normalizeResultRow>>[] = [];
  if (initialResults.length === 0 && finalLogs.length > 0) {
    parsedFromLogs = finalLogs
      .filter(line => line.startsWith("[RESULT]"))
      .map(line => {
        try {
          return normalizeResultRow(JSON.parse(line.substring(8).trim()));
        } catch {
          return null;
        }
      })
      .filter((r): r is NonNullable<ReturnType<typeof normalizeResultRow>> => r !== null);
  }
  
  const results = initialResults.length > 0 ? initialResults : parsedFromLogs;

  const totalLeads = Math.max(0, toNumber(statusPayload.total_leads ?? statusPayload.totalLeads, results.length));
  const processedLeads = Math.max(
    0,
    toNumber(statusPayload.processed_leads ?? statusPayload.processedLeads, results.length),
  );

  const computedProgress =
    totalLeads > 0 ? Math.round((processedLeads / totalLeads) * 100) : status === "completed" ? 100 : 0;

  const progress = Math.max(
    0,
    Math.min(100, Math.round(toNumber(statusPayload.progress, computedProgress))),
  );

  const endedAt = asString(statusPayload.finished_at) || asString(statusPayload.ended_at) || asString(statusPayload.endedAt);
  return {
    runId,
    status,
    progress,
    totalLeads,
    processedLeads,
    currentLead:
      asString(statusPayload.current_lead) || asString(statusPayload.currentLead) || "-",
    logs: finalLogs,
    results,
    duplicatesSkipped: Math.max(
      0,
      Math.round(
        toNumber(statusPayload.duplicates_skipped ?? statusPayload.duplicatesSkipped, 0),
      ),
    ),
    resumeSkippedLeads: Math.max(
      0,
      Math.round(
        toNumber(statusPayload.resume_skipped_leads ?? statusPayload.resumeSkippedLeads, 0),
      ),
    ),
    socialSkippedLeads: Math.max(
      0,
      Math.round(
        toNumber(statusPayload.social_skipped_leads ?? statusPayload.socialSkippedLeads, 0),
      ),
    ),
    resumedFromRunId:
      asString(statusPayload.resumed_from_run_id) ||
      asString(statusPayload.resumedFromRunId) ||
      undefined,
    captchaCreditsUsedToday: Math.max(
      0,
      Math.round(
        toNumber(
          statusPayload.captcha_credits_used_today ?? statusPayload.captchaCreditsUsedToday,
          0,
        ),
      ),
    ),
    captchaCreditsLimit: Math.max(
      0,
      Math.round(
        toNumber(statusPayload.captcha_credits_limit ?? statusPayload.captchaCreditsLimit, 0),
      ),
    ),
    captchaCreditsRemaining: Math.max(
      0,
      Math.round(
        toNumber(
          statusPayload.captcha_credits_remaining ?? statusPayload.captchaCreditsRemaining,
          0,
        ),
      ),
    ),
    startedAt: asString(statusPayload.started_at) || asString(statusPayload.startedAt) || new Date().toISOString(),
    endedAt: endedAt || undefined,
    error: asString(statusPayload.error) || undefined,
  };
}

export function buildSnapshotFromStartPayload(payload: Record<string, unknown>): OutreachRunSnapshot {
  const fallback = {
    ...payload,
    status: "running",
    running: true,
    progress: 0,
    processed_leads: 0,
    total_leads: toNumber(payload.total_leads ?? payload.totalLeads, 0),
    current_lead: "-",
    results: [],
    duplicates_skipped: toNumber(payload.duplicates_skipped, 0),
    resume_skipped_leads: toNumber(payload.resume_skipped_leads, 0),
    social_skipped_leads: toNumber(payload.social_skipped_leads, 0),
    resumed_from_run_id: asString(payload.resumed_from_run_id),
  };

  return toDashboardSnapshot(fallback, []);
}

export async function fetchBackendSnapshot(
  requestedRunId?: string,
  options?: { userId?: string; isAdmin?: boolean }
): Promise<OutreachRunSnapshot | null> {
  const backendBaseUrl = resolveBackendBaseUrl();
  const headers = {} as Record<string, string>;
  if (options?.userId) headers["X-User-Id"] = options.userId;
  if (options?.isAdmin) headers["X-Is-Admin"] = "true";

  const statusResponse = await fetch(`${backendBaseUrl}/outreach/status`, {
    method: "GET",
    cache: "no-store",
    headers
  });

  if (statusResponse.status === 404) {
    return null;
  }

  if (!statusResponse.ok) {
    throw new Error(`Backend status request failed (${statusResponse.status}).`);
  }

  const statusPayload = await parseJsonObject(statusResponse);
  if (!statusPayload) {
    throw new Error("Backend returned an invalid status payload.");
  }

  const backendRunId = payloadRunId(statusPayload);
  const backendStatus = asString(statusPayload.status).toLowerCase();
  if (requestedRunId) {
    if (!backendRunId) {
      return null;
    }

    if (requestedRunId === "current") {
      if (backendStatus === "idle" && !statusPayload.running) {
        return null; // Not active
      }
    } else if (backendRunId !== requestedRunId) {
      return null;
    }

    if (backendStatus === "idle" && requestedRunId !== "current") {
      return null;
    }
  }

  let logs: string[] = [];
  if (backendRunId) {
    const logsUrl = `${backendBaseUrl}/outreach/logs?run_id=${encodeURIComponent(backendRunId)}&tail=${LOG_TAIL}`;
    const logsResponse = await fetch(logsUrl, {
      method: "GET",
      cache: "no-store",
      headers
    });

    if (logsResponse.ok) {
      const logsPayload = await parseJsonObject(logsResponse);
      logs = collectLogs(logsPayload?.logs);
    }
  }

  return toDashboardSnapshot(statusPayload, logs);
}
