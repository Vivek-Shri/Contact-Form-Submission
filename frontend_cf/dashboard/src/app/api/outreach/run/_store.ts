import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

export type RunStatus = "queued" | "running" | "completed" | "failed" | "stopped";

export interface RunLeadInput {
  companyName: string;
  contactUrl: string;
}

export interface RunPersonaInput {
  id: string;
  title: string;
  aiInstruction: string;
  maxDailySubmissions?: number;
  firstName?: string;
  lastName?: string;
  jobTitle?: string;
  professionalEmail?: string;
  verifiedPhone?: string;
  internationalPhone?: string;
  phoneDisplay?: string;
  company?: string;
  website?: string;
  zipCode?: string;
  address?: string;
  titleTemplate?: string;
  pitchMessage?: string;
}

export interface RunResultRow {
  campaignId: string;
  campaignTitle: string;
  companyName: string;
  contactUrl: string;
  submitted: "Yes" | "No";
  status: "success" | "fail" | "warning";
  captchaStatus: string;
  confirmationMsg: string;
  estCostUsd: number;
}

export interface OutreachRunSnapshot {
  runId: string;
  status: RunStatus;
  progress: number;
  totalLeads: number;
  processedLeads: number;
  currentLead: string;
  logs: string[];
  results: RunResultRow[];
  duplicatesSkipped: number;
  captchaCreditsUsedToday: number;
  captchaCreditsLimit: number;
  captchaCreditsRemaining: number;
  startedAt: string;
  endedAt?: string;
  error?: string;
}

interface OutreachRunJob extends OutreachRunSnapshot {
  child?: ChildProcess;
  csvPath: string;
  runsDir: string;
  campaignId: string;
  campaignTitle: string;
  dailySubmissionLimit: number;
  dayKey: string;
  stateWriteQueue: Promise<void>;
  stdoutBuffer: string;
  stderrBuffer: string;
}

interface SenderContext {
  firstName: string;
  lastName: string;
  fullName: string;
  professionalEmail: string;
  verifiedPhone: string;
  internationalPhone: string;
  phoneDisplay: string;
  company: string;
  website: string;
  zipCode: string;
  address: string;
  jobTitle: string;
  titleTemplate: string;
  pitchMessage: string;
}

interface SubmissionState {
  version: number;
  submittedUrlKeys: Record<
    string,
    {
      timestamp: string;
      campaignId: string;
      campaignTitle: string;
    }
  >;
  captchaUsageByDate: Record<string, number>;
  campaignSubmissionCountByDate: Record<string, Record<string, number>>;
}

const RESULT_PREFIX = "[RESULT]";
const MAX_LOG_LINES = 450;
const MAX_JOBS = 12;
const FINISHED_JOB_TTL_MS = 1000 * 60 * 60 * 4;
const SUBMISSION_STATE_FILE = "submission-state.json";
const DEFAULT_DAILY_SUBMISSION_LIMIT = Math.max(
  1,
  Number(process.env.OUTREACH_MAX_DAILY_SUBMISSIONS ?? "100") || 100,
);

function resolveNopechaKeyCountFromEnv(): number {
  const explicitCount = Math.floor(Number(process.env.OUTREACH_NOPECHA_KEY_COUNT ?? NaN));
  if (Number.isFinite(explicitCount) && explicitCount > 0) {
    return explicitCount;
  }

  const rawList =
    process.env.OUTREACH_NOPECHA_API_KEYS ?? process.env.NOPECHA_API_KEYS ?? "";
  if (rawList.trim()) {
    const tokens = rawList
      .split(/[\n,;|]/g)
      .map((token) => token.replace(/["'\[\]]/g, "").trim())
      .filter(Boolean);
    if (tokens.length > 0) {
      return tokens.length;
    }
  }

  if ((process.env.NOPECHA_API_KEY ?? "").trim()) {
    return 1;
  }

  // Outreach(1).py currently ships with two NopeCHA keys by default.
  return 2;
}

const DEFAULT_CAPTCHA_CREDITS_PER_KEY = Math.max(
  1,
  Number(process.env.OUTREACH_DAILY_CAPTCHA_CREDITS_PER_KEY ?? "900") || 900,
);

function resolveCaptchaCreditLimit(): number {
  // Backward-compatible explicit total limit override.
  const explicitTotal = Math.floor(Number(process.env.OUTREACH_DAILY_CAPTCHA_CREDITS ?? NaN));
  if (Number.isFinite(explicitTotal) && explicitTotal > 0) {
    return explicitTotal;
  }

  const keyCount = resolveNopechaKeyCountFromEnv();
  return Math.max(1, DEFAULT_CAPTCHA_CREDITS_PER_KEY * keyCount);
}

const DEFAULT_CAPTCHA_CREDIT_LIMIT = resolveCaptchaCreditLimit();
const runJobs = new Map<string, OutreachRunJob>();
let submissionStateCache: SubmissionState | null = null;
let submissionStatePath = "";

function toSnapshot(job: OutreachRunJob): OutreachRunSnapshot {
  return {
    runId: job.runId,
    status: job.status,
    progress: job.progress,
    totalLeads: job.totalLeads,
    processedLeads: job.processedLeads,
    currentLead: job.currentLead,
    logs: job.logs,
    results: job.results,
    duplicatesSkipped: job.duplicatesSkipped,
    captchaCreditsUsedToday: job.captchaCreditsUsedToday,
    captchaCreditsLimit: job.captchaCreditsLimit,
    captchaCreditsRemaining: job.captchaCreditsRemaining,
    startedAt: job.startedAt,
    endedAt: job.endedAt,
    error: job.error,
  };
}

function pushLog(job: OutreachRunJob, line: string): void {
  const cleanLine = line.replace(/\u0000/g, "").trimEnd();
  if (!cleanLine) {
    return;
  }
  job.logs = [...job.logs.slice(-(MAX_LOG_LINES - 1)), cleanLine];
}

function parseCost(value: unknown): number {
  const numeric = Number(String(value ?? "").replace(/[^0-9.-]/g, ""));
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  return numeric;
}

function defaultSubmissionState(): SubmissionState {
  return {
    version: 1,
    submittedUrlKeys: {},
    captchaUsageByDate: {},
    campaignSubmissionCountByDate: {},
  };
}

function currentDateKey(): string {
  return new Date().toISOString().slice(0, 10);
}

function normalizeUrlKey(rawUrl: string): string {
  const normalized = rawUrl.trim();
  if (!normalized) {
    return "";
  }

  try {
    const parsed = new URL(/^https?:\/\//i.test(normalized) ? normalized : `https://${normalized}`);
    const host = parsed.hostname.replace(/^www\./i, "").toLowerCase();
    const pathName = parsed.pathname.replace(/\/+$/g, "") || "/";
    return `${host}${pathName}`;
  } catch {
    return normalized.toLowerCase();
  }
}

async function loadSubmissionState(runsDir: string): Promise<SubmissionState> {
  const statePath = path.resolve(runsDir, SUBMISSION_STATE_FILE);

  if (submissionStateCache && submissionStatePath === statePath) {
    return submissionStateCache;
  }

  submissionStatePath = statePath;

  try {
    const raw = await fs.readFile(statePath, "utf8");
    const parsed = JSON.parse(raw) as SubmissionState;
    submissionStateCache = {
      ...defaultSubmissionState(),
      ...parsed,
      submittedUrlKeys: parsed.submittedUrlKeys ?? {},
      captchaUsageByDate: parsed.captchaUsageByDate ?? {},
      campaignSubmissionCountByDate: parsed.campaignSubmissionCountByDate ?? {},
    };
  } catch {
    submissionStateCache = defaultSubmissionState();
  }

  return submissionStateCache;
}

async function saveSubmissionState(runsDir: string): Promise<void> {
  if (!submissionStateCache) {
    return;
  }

  const statePath = path.resolve(runsDir, SUBMISSION_STATE_FILE);
  submissionStatePath = statePath;
  await fs.writeFile(statePath, JSON.stringify(submissionStateCache, null, 2), "utf8");
}

function isCaptchaConsumed(captchaStatus: string): boolean {
  const normalized = captchaStatus.toLowerCase();
  if (!normalized || normalized === "none" || normalized === "n/a") {
    return false;
  }

  return (
    normalized.includes("recaptcha") ||
    normalized.includes("hcaptcha") ||
    normalized.includes("turnstile") ||
    normalized.includes("solved") ||
    normalized.includes("captcha")
  );
}

function getCampaignSubmissionsForDate(
  state: SubmissionState,
  dateKey: string,
  campaignId: string,
): number {
  return state.campaignSubmissionCountByDate[dateKey]?.[campaignId] ?? 0;
}

function getCaptchaUsageForDate(state: SubmissionState, dateKey: string): number {
  return state.captchaUsageByDate[dateKey] ?? 0;
}

function refreshJobCreditSnapshot(job: OutreachRunJob, state: SubmissionState): void {
  const today = currentDateKey();
  job.captchaCreditsUsedToday = getCaptchaUsageForDate(state, today);
  job.captchaCreditsRemaining = Math.max(0, job.captchaCreditsLimit - job.captchaCreditsUsedToday);
}

function extractInstructionField(aiInstruction: string, label: string): string {
  const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const regex = new RegExp(`(?:^|\\n)\\s*${escaped}\\s*:\\s*(.+)`, "i");
  const match = aiInstruction.match(regex);
  return match?.[1]?.trim() ?? "";
}

function isLikelySectionHeading(line: string): boolean {
  const normalized = line
    .replace(/^[\s#>*`~\-]+/, "")
    .replace(/[\s`~]+$/, "")
    .trim();

  if (!normalized) {
    return false;
  }

  return /^[A-Z][A-Z0-9 /&()_+\-]{3,}:$/.test(normalized);
}

function extractInstructionSection(aiInstruction: string, label: string): string {
  const normalized = (aiInstruction ?? "").replace(/\r\n/g, "\n");
  const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const headingRegex = new RegExp(
    `(?:^|\\n)\\s*(?:[#>*\`~\\-]+\\s*)?${escaped}\\s*:?\\s*(?:\\n|$)`,
    "i",
  );
  const match = headingRegex.exec(normalized);

  if (!match || match.index === undefined) {
    return "";
  }

  const start = match.index + match[0].length;
  const tail = normalized.slice(start);
  const lines = tail.split("\n");
  const collected: string[] = [];
  let started = false;

  for (const line of lines) {
    const trimmed = line.trim();

    if (!started) {
      if (!trimmed || /^[-_*]{3,}$/.test(trimmed)) {
        continue;
      }
      started = true;
    }

    if (!trimmed) {
      collected.push("");
      continue;
    }

    if (/^[-_*]{3,}$/.test(trimmed)) {
      continue;
    }

    if (isLikelySectionHeading(trimmed)) {
      break;
    }

    collected.push(line);
    if (collected.length >= 60) {
      break;
    }
  }

  return collected.join("\n").trim();
}

function resolveDailySubmissionLimit(limit?: number): number {
  const parsed = Math.floor(Number(limit ?? NaN));
  if (Number.isFinite(parsed) && parsed > 0) {
    return parsed;
  }
  return DEFAULT_DAILY_SUBMISSION_LIMIT;
}

function splitFullName(fullName: string): { firstName: string; lastName: string } {
  const normalized = fullName.trim();
  if (!normalized) {
    return { firstName: "", lastName: "" };
  }

  const parts = normalized.split(/\s+/);
  return {
    firstName: parts[0] ?? "",
    lastName: parts.slice(1).join(" "),
  };
}

function buildSenderContext(persona: RunPersonaInput): SenderContext {
  const aiInstruction = persona.aiInstruction ?? "";

  const fullNameFromInstruction =
    extractInstructionField(aiInstruction, "Full Name") ||
    extractInstructionField(aiInstruction, "Sender Name");
  const fromFullName = splitFullName(fullNameFromInstruction);

  const firstName =
    persona.firstName?.trim() ||
    extractInstructionField(aiInstruction, "First Name") ||
    extractInstructionField(aiInstruction, "Sender First Name") ||
    fromFullName.firstName ||
    "Outreach";

  const lastName =
    persona.lastName?.trim() ||
    extractInstructionField(aiInstruction, "Last Name") ||
    extractInstructionField(aiInstruction, "Sender Last Name") ||
    fromFullName.lastName;

  const fullName = `${firstName} ${lastName}`.trim();
  const extractedTemplate =
    extractInstructionSection(aiInstruction, "Core Message Template") ||
    extractInstructionSection(aiInstruction, "Message Template") ||
    extractInstructionSection(aiInstruction, "Core Message") ||
    "";

  const resolvedPitch =
    persona.pitchMessage?.trim() ||
    extractInstructionField(aiInstruction, "Pitch Message") ||
    extractedTemplate;

  const professionalEmail =
    persona.professionalEmail?.trim() ||
    extractInstructionField(aiInstruction, "Professional Email") ||
    extractInstructionField(aiInstruction, "Email");

  const verifiedPhone =
    persona.verifiedPhone?.trim() ||
    extractInstructionField(aiInstruction, "Verified Phone") ||
    extractInstructionField(aiInstruction, "Phone");

  const internationalPhone =
    persona.internationalPhone?.trim() ||
    extractInstructionField(aiInstruction, "International Phone") ||
    extractInstructionField(aiInstruction, "Phone Intl") ||
    extractInstructionField(aiInstruction, "Phone International") ||
    verifiedPhone;

  const phoneDisplay =
    persona.phoneDisplay?.trim() ||
    extractInstructionField(aiInstruction, "Phone Display") ||
    extractInstructionField(aiInstruction, "Display Phone") ||
    internationalPhone ||
    verifiedPhone;

  const company =
    persona.company?.trim() ||
    extractInstructionField(aiInstruction, "Company") ||
    extractInstructionField(aiInstruction, "Sender Company") ||
    "Outreach Team";

  const website =
    persona.website?.trim() ||
    extractInstructionField(aiInstruction, "Website") ||
    extractInstructionField(aiInstruction, "Company Website");

  const zipCode =
    persona.zipCode?.trim() ||
    extractInstructionField(aiInstruction, "Zip Code") ||
    extractInstructionField(aiInstruction, "PIN Code");

  const address =
    persona.address?.trim() ||
    extractInstructionField(aiInstruction, "Address") ||
    extractInstructionField(aiInstruction, "Location");

  const jobTitle =
    persona.jobTitle?.trim() ||
    extractInstructionField(aiInstruction, "Job Title") ||
    extractInstructionField(aiInstruction, "Sender Role") ||
    "Outreach Specialist";

  const titleTemplate =
    persona.titleTemplate?.trim() ||
    extractInstructionField(aiInstruction, "Email Subject") ||
    extractInstructionField(aiInstruction, "Subject Line") ||
    extractInstructionField(aiInstruction, "Title Template") ||
    "Virtual Assistant Support for {company_name} - {MY_COMPANY}";

  return {
    firstName,
    lastName,
    fullName,
    professionalEmail,
    verifiedPhone,
    internationalPhone,
    phoneDisplay,
    company,
    website,
    zipCode,
    address,
    jobTitle,
    titleTemplate,
    // Never pass full AI instructions as message body.
    pitchMessage: resolvedPitch,
  };
}

function queueSubmissionStateSave(job: OutreachRunJob): void {
  job.stateWriteQueue = job.stateWriteQueue
    .then(() => saveSubmissionState(job.runsDir))
    .catch((error) => {
      pushLog(job, `[Runner] Failed to persist submission state: ${String(error)}`);
    });
}

function applyResultToSubmissionState(job: OutreachRunJob, result: RunResultRow): void {
  if (!submissionStateCache) {
    return;
  }

  const state = submissionStateCache;
  let shouldSave = false;

  if (isCaptchaConsumed(result.captchaStatus)) {
    state.captchaUsageByDate[job.dayKey] = (state.captchaUsageByDate[job.dayKey] ?? 0) + 1;
    shouldSave = true;
  }

  if (result.submitted === "Yes") {
    const daySubmissions = state.campaignSubmissionCountByDate[job.dayKey] ?? {};
    daySubmissions[job.campaignId] = (daySubmissions[job.campaignId] ?? 0) + 1;
    state.campaignSubmissionCountByDate[job.dayKey] = daySubmissions;
    shouldSave = true;

    const urlKey = normalizeUrlKey(result.contactUrl);
    if (urlKey) {
      state.submittedUrlKeys[urlKey] = {
        timestamp: new Date().toISOString(),
        campaignId: job.campaignId,
        campaignTitle: job.campaignTitle,
      };
      shouldSave = true;
    }
  }

  refreshJobCreditSnapshot(job, state);

  if (shouldSave) {
    queueSubmissionStateSave(job);
  }

  const campaignSubmissions = getCampaignSubmissionsForDate(state, job.dayKey, job.campaignId);
  if (campaignSubmissions >= job.dailySubmissionLimit && job.status === "running") {
    pushLog(
      job,
      `[Runner] Campaign daily submission limit (${job.dailySubmissionLimit}) reached. Stopping run.`,
    );
    stopOutreachRun(job.runId);
    return;
  }

  if (job.captchaCreditsRemaining <= 0 && job.status === "running") {
    pushLog(job, "[Runner] CAPTCHA credits exhausted for today. Stopping run.");
    stopOutreachRun(job.runId);
  }
}

function statusFromResult(
  submitted: string,
  captchaStatus: string,
  submissionStatus: string,
  assurance: string,
): "success" | "fail" | "warning" {
  const submittedLower = submitted.toLowerCase();
  if (submittedLower === "yes") {
    return "success";
  }

  const combined = `${captchaStatus} ${submissionStatus} ${assurance}`.toLowerCase();
  if (
    combined.includes("timeout") ||
    combined.includes("captcha") ||
    combined.includes("warning") ||
    combined.includes("not found")
  ) {
    return "warning";
  }

  return "fail";
}

function mapResultPayload(job: OutreachRunJob, payload: Record<string, unknown>): RunResultRow {
  const submittedRaw = String(payload.submitted ?? "No");
  const captchaStatus = String(payload.captcha_status ?? "n/a");
  const submissionStatus = String(payload.submission_status ?? "");
  const assurance = String(payload.submission_assurance ?? "");

  return {
    campaignId: job.campaignId,
    campaignTitle: job.campaignTitle,
    companyName: String(payload.company_name ?? "Unknown"),
    contactUrl: String(payload.contact_url ?? ""),
    submitted: submittedRaw.toLowerCase() === "yes" ? "Yes" : "No",
    status: statusFromResult(submittedRaw, captchaStatus, submissionStatus, assurance),
    captchaStatus,
    confirmationMsg: String(payload.confirmation_msg ?? assurance ?? "-") || "-",
    estCostUsd: parseCost(payload.est_cost),
  };
}

function handleProcessLine(job: OutreachRunJob, line: string, fromStdErr: boolean): void {
  const prefixed = fromStdErr ? `[stderr] ${line}` : line;
  pushLog(job, prefixed);

  if (fromStdErr) {
    return;
  }

  const workerMatch = line.match(/\]\s\[\d+\/\d+\]\s(.+?)\s\|/);
  if (workerMatch?.[1]) {
    job.currentLead = workerMatch[1].trim();
  }

  if (!line.startsWith(RESULT_PREFIX)) {
    return;
  }

  try {
    const parsed = JSON.parse(line.slice(RESULT_PREFIX.length).trim()) as Record<string, unknown>;
    const result = mapResultPayload(job, parsed);
    job.results = [...job.results, result];
    applyResultToSubmissionState(job, result);
    job.processedLeads = Math.min(job.totalLeads, job.results.length);
    job.progress =
      job.totalLeads > 0 ? Math.round((job.processedLeads / job.totalLeads) * 100) : 100;

    if (result.contactUrl) {
      job.currentLead = result.contactUrl;
    } else {
      job.currentLead = result.companyName;
    }
  } catch (error) {
    pushLog(job, `[Runner] Failed to parse result payload: ${String(error)}`);
  }
}

function flushStreamBuffer(job: OutreachRunJob, isStdErr: boolean): void {
  const buffer = isStdErr ? job.stderrBuffer : job.stdoutBuffer;
  if (!buffer.trim()) {
    if (isStdErr) {
      job.stderrBuffer = "";
    } else {
      job.stdoutBuffer = "";
    }
    return;
  }

  handleProcessLine(job, buffer.trim(), isStdErr);

  if (isStdErr) {
    job.stderrBuffer = "";
  } else {
    job.stdoutBuffer = "";
  }
}

function onStreamData(job: OutreachRunJob, chunk: string, isStdErr: boolean): void {
  const nextBuffer = (isStdErr ? job.stderrBuffer : job.stdoutBuffer) + chunk;
  const lines = nextBuffer.split(/\r?\n/);
  const remainder = lines.pop() ?? "";

  if (isStdErr) {
    job.stderrBuffer = remainder;
  } else {
    job.stdoutBuffer = remainder;
  }

  for (const line of lines) {
    if (line.trim().length === 0) {
      continue;
    }
    handleProcessLine(job, line, isStdErr);
  }
}

function escapeCsv(value: string): string {
  const compact = value.replace(/\r?\n/g, " ").trim();
  if (compact.includes(",") || compact.includes('"')) {
    return `"${compact.replace(/"/g, '""')}"`;
  }
  return compact;
}

function normalizeLead(input: RunLeadInput, index: number): RunLeadInput | null {
  const url = String(input.contactUrl ?? "").trim();
  if (!url) {
    return null;
  }

  const company = String(input.companyName ?? "").trim() || `Lead ${index + 1}`;
  return {
    companyName: company,
    contactUrl: url,
  };
}

function cleanupFinishedJobs(): void {
  const now = Date.now();

  for (const [runId, job] of runJobs.entries()) {
    if (job.status === "running" || job.status === "queued") {
      continue;
    }

    const endedAtTs = job.endedAt ? Date.parse(job.endedAt) : now;
    if (Number.isNaN(endedAtTs)) {
      continue;
    }

    if (now - endedAtTs > FINISHED_JOB_TTL_MS) {
      runJobs.delete(runId);
    }
  }

  if (runJobs.size <= MAX_JOBS) {
    return;
  }

  const ordered = Array.from(runJobs.values()).sort((a, b) => {
    const aTs = Date.parse(a.startedAt);
    const bTs = Date.parse(b.startedAt);
    return aTs - bTs;
  });

  for (const job of ordered) {
    if (runJobs.size <= MAX_JOBS) {
      break;
    }
    if (job.status === "running" || job.status === "queued") {
      continue;
    }
    runJobs.delete(job.runId);
  }
}

function toPersonaEnv(persona: RunPersonaInput, sender: SenderContext): Record<string, string> {
  const titleTemplate =
    sender.titleTemplate || "Virtual Assistant Support for {company_name} - {MY_COMPANY}";

  return {
    MY_FIRST_NAME: sender.firstName,
    MY_LAST_NAME: sender.lastName,
    MY_FULL_NAME: sender.fullName,
    MY_EMAIL: sender.professionalEmail,
    MY_PHONE: sender.verifiedPhone,
    MY_PHONE_INTL: sender.internationalPhone,
    MY_PHONE_DISPLAY: sender.phoneDisplay,
    MY_COMPANY: sender.company,
    MY_WEBSITE: sender.website,
    MY_PIN_CODE: sender.zipCode,
    MY_ADDRESS: sender.address,
    MY_JOB_TITLE: sender.jobTitle,
    MY_TITLE: titleTemplate,
    PITCH_MESSAGE: sender.pitchMessage,
    CAMPAIGN_ID: persona.id,
  };
}

function compactEnv(env: Record<string, string>): Record<string, string> {
  const compacted: Record<string, string> = {};
  for (const [key, value] of Object.entries(env)) {
    const trimmed = String(value ?? "").trim();
    if (trimmed) {
      compacted[key] = trimmed;
    }
  }
  return compacted;
}

async function removeCsvFile(csvPath: string): Promise<void> {
  try {
    await fs.unlink(csvPath);
  } catch {
    // best-effort cleanup only
  }
}

export function getActiveOutreachRun(): OutreachRunSnapshot | null {
  cleanupFinishedJobs();
  const active = Array.from(runJobs.values()).find(
    (job) => job.status === "running" || job.status === "queued",
  );

  return active ? toSnapshot(active) : null;
}

export function getOutreachRunSnapshot(runId: string): OutreachRunSnapshot | null {
  cleanupFinishedJobs();
  const job = runJobs.get(runId);
  if (!job) {
    return null;
  }
  return toSnapshot(job);
}

export function stopOutreachRun(runId: string): OutreachRunSnapshot | null {
  const job = runJobs.get(runId);
  if (!job) {
    return null;
  }

  if (job.status === "running" || job.status === "queued") {
    job.status = "stopped";
    job.endedAt = new Date().toISOString();
    pushLog(job, "[Runner] Stop requested by operator.");

    try {
      job.child?.kill("SIGINT");
    } catch {
      // ignored
    }

    try {
      job.child?.kill();
    } catch {
      // ignored
    }
  }

  return toSnapshot(job);
}

export async function startOutreachRun(
  persona: RunPersonaInput,
  rawLeads: RunLeadInput[],
): Promise<OutreachRunSnapshot> {
  cleanupFinishedJobs();

  const activeRun = getActiveOutreachRun();
  if (activeRun) {
    const conflictError = new Error("Another run is already active.");
    (conflictError as Error & { code?: string; runId?: string }).code = "RUN_IN_PROGRESS";
    (conflictError as Error & { code?: string; runId?: string }).runId = activeRun.runId;
    throw conflictError;
  }

  const normalizedLeads = rawLeads
    .map((lead, index) => normalizeLead(lead, index))
    .filter((lead): lead is RunLeadInput => lead !== null);

  if (normalizedLeads.length === 0) {
    throw new Error("No valid leads were provided.");
  }

  const workspaceRoot = path.resolve(process.cwd(), "..");
  const scriptPath = path.resolve(workspaceRoot, "Outreach(1).py");
  const runsDir = path.resolve(workspaceRoot, ".outreach-runs");

  await fs.access(scriptPath);
  await fs.mkdir(runsDir, { recursive: true });

  const submissionState = await loadSubmissionState(runsDir);
  const dayKey = currentDateKey();
  const dailySubmissionLimit = resolveDailySubmissionLimit(persona.maxDailySubmissions);
  const campaignSubmissionsToday = getCampaignSubmissionsForDate(submissionState, dayKey, persona.id);

  if (campaignSubmissionsToday >= dailySubmissionLimit) {
    const limitError = new Error(
      `Campaign has already reached today's submission limit (${dailySubmissionLimit}).`,
    );
    (limitError as Error & { code?: string }).code = "DAILY_LIMIT_REACHED";
    throw limitError;
  }

  const captchaCreditsUsedToday = getCaptchaUsageForDate(submissionState, dayKey);
  const captchaCreditsRemaining = Math.max(
    0,
    DEFAULT_CAPTCHA_CREDIT_LIMIT - captchaCreditsUsedToday,
  );
  if (captchaCreditsRemaining <= 0) {
    const captchaError = new Error("CAPTCHA credits are exhausted for today.");
    (captchaError as Error & { code?: string }).code = "CAPTCHA_CREDITS_EXHAUSTED";
    throw captchaError;
  }

  const seenInBatch = new Set<string>();
  let duplicatesSkipped = 0;
  const leads = normalizedLeads.filter((lead) => {
    const urlKey = normalizeUrlKey(lead.contactUrl);
    if (!urlKey) {
      return false;
    }

    if (seenInBatch.has(urlKey)) {
      duplicatesSkipped += 1;
      return false;
    }

    seenInBatch.add(urlKey);

    if (submissionState.submittedUrlKeys[urlKey]) {
      duplicatesSkipped += 1;
      return false;
    }

    return true;
  });

  if (leads.length === 0) {
    const noEligibleError = new Error("All leads were filtered out as previously submitted duplicates.");
    (noEligibleError as Error & { code?: string }).code = "NO_ELIGIBLE_LEADS";
    throw noEligibleError;
  }

  const senderContext = buildSenderContext(persona);

  const runId = randomUUID();
  const csvPath = path.resolve(runsDir, `run-${runId}.csv`);

  const csvLines = ["Company Name,Contact URL Found"];
  for (const lead of leads) {
    csvLines.push(`${escapeCsv(lead.companyName)},${escapeCsv(lead.contactUrl)}`);
  }
  await fs.writeFile(csvPath, `${csvLines.join("\n")}\n`, "utf8");

  const command = process.platform === "win32" ? "py" : "python3";
  const args = [scriptPath, csvPath];

  const startedAt = new Date().toISOString();
  const child = spawn(command, args, {
    cwd: workspaceRoot,
    env: {
      ...process.env,
      ...compactEnv(toPersonaEnv(persona, senderContext)),
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  const job: OutreachRunJob = {
    runId,
    status: "running",
    progress: 0,
    totalLeads: leads.length,
    processedLeads: 0,
    currentLead: "-",
    logs: [],
    results: [],
    duplicatesSkipped,
    captchaCreditsUsedToday,
    captchaCreditsLimit: DEFAULT_CAPTCHA_CREDIT_LIMIT,
    captchaCreditsRemaining,
    startedAt,
    endedAt: undefined,
    error: undefined,
    child,
    csvPath,
    runsDir,
    campaignId: persona.id,
    campaignTitle: persona.title,
    dailySubmissionLimit,
    dayKey,
    stateWriteQueue: Promise.resolve(),
    stdoutBuffer: "",
    stderrBuffer: "",
  };

  runJobs.set(runId, job);
  pushLog(job, `[Runner] Starting backend run ${runId}.`);
  pushLog(job, `[Runner] Skipped ${duplicatesSkipped} duplicate lead(s) before execution.`);
  pushLog(
    job,
    `[Runner] Today's submissions for this campaign: ${campaignSubmissionsToday}/${dailySubmissionLimit}.`,
  );
  pushLog(
    job,
    `[Runner] CAPTCHA credits remaining today: ${captchaCreditsRemaining}/${DEFAULT_CAPTCHA_CREDIT_LIMIT}.`,
  );
  pushLog(
    job,
    `[Runner] CAPTCHA limit is combined across configured NopeCHA APIs.`,
  );
  pushLog(job, `[Runner] Executing Outreach(1).py for ${leads.length} lead(s).`);

  if (child.stdout) {
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk: string | Buffer) => {
      onStreamData(job, String(chunk), false);
    });
  }

  if (child.stderr) {
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk: string | Buffer) => {
      onStreamData(job, String(chunk), true);
    });
  }

  child.on("error", (error) => {
    job.status = "failed";
    job.error = `Failed to start process: ${error.message}`;
    job.endedAt = new Date().toISOString();
    pushLog(job, `[Runner] ${job.error}`);
    void removeCsvFile(job.csvPath);
  });

  child.on("close", (code, signal) => {
    flushStreamBuffer(job, false);
    flushStreamBuffer(job, true);

    if (job.status !== "stopped" && job.status !== "failed") {
      if (code === 0) {
        job.status = "completed";
      } else {
        job.status = "failed";
        job.error = `Process exited with code ${code}${signal ? ` (${signal})` : ""}`;
      }
    }

    if (job.status === "completed") {
      job.progress = 100;
    } else {
      job.progress =
        job.totalLeads > 0 ? Math.round((job.processedLeads / job.totalLeads) * 100) : 0;
    }

    job.endedAt = new Date().toISOString();
    job.child = undefined;

    if (job.status === "completed") {
      pushLog(job, "[Runner] Backend outreach completed successfully.");
    } else if (job.status === "stopped") {
      pushLog(job, "[Runner] Backend outreach stopped.");
    } else {
      pushLog(job, `[Runner] Backend outreach failed.${job.error ? ` ${job.error}` : ""}`);
    }

    void removeCsvFile(job.csvPath);
  });

  return toSnapshot(job);
}
