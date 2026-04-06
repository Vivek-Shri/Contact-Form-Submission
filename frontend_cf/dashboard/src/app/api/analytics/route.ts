import fs from "node:fs/promises";
import path from "node:path";
import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

type UiStatus = "success" | "fail" | "warning";

interface TokenUsageRow {
  timestamp: string;
  company: string;
  totalTokens: number;
  estCostUsd: number;
  cumulativeCostUsd: number;
}

interface SubmissionSeries {
  run: string;
  success: number;
  fail: number;
}

interface RecentActivityRow {
  company: string;
  status: UiStatus;
  captcha: string;
  cost: string;
  time: string;
}

interface AnalyticsPayload {
  metrics: {
    totalRuns: number;
    leadsProcessed: number;
    successRate: number | null;
    totalApiCost: number;
    tokensUsed: number;
    captchasSolved: number;
  };
  submissionsPerRun: SubmissionSeries[];
  recentActivity: RecentActivityRow[];
  meta: {
    updatedAt: string;
    tokenRows: number;
    sheetRows: number;
  };
}

const SHEET_ID = "1jSfdjqQXgueTfatP10R3mIxr_Kee0zNN0tVEXq59rYE";

function parseCsvLine(line: string): string[] {
  const values: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const next = line[index + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      values.push(current.trim());
      current = "";
      continue;
    }

    current += char;
  }

  values.push(current.trim());
  return values;
}

function parseCsvRecords(content: string): Array<Record<string, string>> {
  const normalized = content.replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
  if (!normalized) {
    return [];
  }

  const lines = normalized.split("\n").filter((line) => line.trim().length > 0);
  if (lines.length < 2) {
    return [];
  }

  const headers = parseCsvLine(lines[0]).map((header) => header.trim());
  const records: Array<Record<string, string>> = [];

  for (let index = 1; index < lines.length; index += 1) {
    const parts = parseCsvLine(lines[index]);
    const record: Record<string, string> = {};

    headers.forEach((header, headerIndex) => {
      record[header] = parts[headerIndex] ?? "";
    });

    records.push(record);
  }

  return records;
}

function toNumber(value: string): number {
  const parsed = Number(value);
  if (Number.isNaN(parsed)) {
    return 0;
  }
  return parsed;
}

function toStatus(value: string): UiStatus {
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    return "warning";
  }
  if (normalized.includes("success") || normalized === "yes") {
    return "success";
  }
  if (normalized.includes("fail") || normalized === "no") {
    return "fail";
  }
  return "warning";
}

function pickValue(row: Record<string, string>, candidates: string[]): string {
  const lowerMap = Object.fromEntries(
    Object.entries(row).map(([key, value]) => [key.trim().toLowerCase(), value]),
  );

  for (const key of candidates) {
    const found = lowerMap[key];
    if (found) {
      return found;
    }
  }

  return "";
}

function formatCost(value: number): string {
  return `$${value.toFixed(6)}`;
}

async function loadTokenUsageRows(): Promise<TokenUsageRow[]> {
  const csvPath = path.resolve(process.cwd(), "..", "token_usage.csv");

  try {
    const fileContent = await fs.readFile(csvPath, "utf8");
    const rows = parseCsvRecords(fileContent);

    return rows.map((row) => ({
      timestamp: row.timestamp ?? "",
      company: row.company ?? "Unknown",
      totalTokens: toNumber(row.total_tokens ?? "0"),
      estCostUsd: toNumber(row.est_cost_usd ?? "0"),
      cumulativeCostUsd: toNumber(row.cumulative_cost_usd ?? "0"),
    }));
  } catch {
    return [];
  }
}

async function loadSheetRows(): Promise<Array<Record<string, string>>> {
  const exportUrl = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:csv`;

  try {
    const response = await fetch(exportUrl, { cache: "no-store" });
    if (!response.ok) {
      return [];
    }

    const csv = await response.text();
    return parseCsvRecords(csv);
  } catch {
    return [];
  }
}

function buildSeries(sheetRows: Array<Record<string, string>>): SubmissionSeries[] {
  const grouped = new Map<string, { success: number; fail: number }>();

  for (const row of sheetRows) {
    const runKey =
      pickValue(row, ["run id", "run_id", "batch id", "batch_id", "date", "timestamp", "created_at"]) ||
      "run";

    const statusValue =
      pickValue(row, ["submission status", "status", "submitted"]) || pickValue(row, ["submitted"]);
    const status = toStatus(statusValue);

    const existing = grouped.get(runKey) ?? { success: 0, fail: 0 };
    if (status === "success") {
      existing.success += 1;
    }
    if (status === "fail") {
      existing.fail += 1;
    }
    grouped.set(runKey, existing);
  }

  return Array.from(grouped.entries())
    .slice(-12)
    .map(([key, value], index) => ({
      run: key.length > 18 ? `#${index + 1}` : key,
      success: value.success,
      fail: value.fail,
    }));
}

function buildRecentActivity(
  sheetRows: Array<Record<string, string>>,
  tokenRows: TokenUsageRow[],
): RecentActivityRow[] {
  if (sheetRows.length > 0) {
    return sheetRows
      .slice(-8)
      .reverse()
      .map((row) => {
        const company = pickValue(row, ["company name", "company", "name"]) || "Unknown";
        const statusSource = pickValue(row, ["submission status", "status", "submitted"]);
        const status = toStatus(statusSource);
        const captcha = pickValue(row, ["captcha status", "captcha", "captcha_status"]) || "none";
        const costRaw = pickValue(row, ["est. cost", "est cost", "est_cost", "cost"]) || "0";
        const time = pickValue(row, ["timestamp", "time", "created at", "created_at"]) || "-";

        return {
          company,
          status,
          captcha,
          cost: formatCost(toNumber(costRaw.replace("$", ""))),
          time,
        };
      });
  }

  return tokenRows
    .slice(-8)
    .reverse()
    .map((row) => ({
      company: row.company,
      status: "warning",
      captcha: "n/a",
      cost: formatCost(row.estCostUsd),
      time: row.timestamp,
    }));
}

export async function GET() {
  const [tokenRows, sheetRows] = await Promise.all([loadTokenUsageRows(), loadSheetRows()]);

  const tokensUsed = tokenRows.reduce((sum, row) => sum + row.totalTokens, 0);
  const totalApiCost =
    tokenRows.length > 0
      ? Math.max(...tokenRows.map((row) => row.cumulativeCostUsd || row.estCostUsd))
      : 0;

  const leadsProcessed = sheetRows.length > 0 ? sheetRows.length : tokenRows.length;

  const successCount = sheetRows.filter((row) => {
    const statusRaw = pickValue(row, ["submission status", "status", "submitted"]);
    return toStatus(statusRaw) === "success";
  }).length;

  const failCount = sheetRows.filter((row) => {
    const statusRaw = pickValue(row, ["submission status", "status", "submitted"]);
    return toStatus(statusRaw) === "fail";
  }).length;

  const successRate =
    successCount + failCount > 0 ? Number(((successCount / (successCount + failCount)) * 100).toFixed(1)) : null;

  const captchaSolved = sheetRows.filter((row) => {
    const captcha = pickValue(row, ["captcha status", "captcha", "captcha_status"]).toLowerCase();
    if (!captcha || captcha === "none" || captcha === "n/a") {
      return false;
    }
    return captcha.includes("solved") || captcha.includes("recaptcha") || captcha.includes("hcaptcha");
  }).length;

  const runKeyCandidates = ["run id", "run_id", "batch id", "batch_id"];
  const hasRunId = sheetRows.some((row) => pickValue(row, runKeyCandidates).length > 0);
  const totalRuns = hasRunId
    ? new Set(sheetRows.map((row) => pickValue(row, runKeyCandidates)).filter(Boolean)).size
    : new Set(tokenRows.map((row) => row.timestamp.split(" ")[0]).filter(Boolean)).size;

  const payload: AnalyticsPayload = {
    metrics: {
      totalRuns,
      leadsProcessed,
      successRate,
      totalApiCost,
      tokensUsed,
      captchasSolved: captchaSolved,
    },
    submissionsPerRun: buildSeries(sheetRows),
    recentActivity: buildRecentActivity(sheetRows, tokenRows),
    meta: {
      updatedAt: new Date().toISOString(),
      tokenRows: tokenRows.length,
      sheetRows: sheetRows.length,
    },
  };

  return NextResponse.json(payload, { status: 200 });
}
