export function formatDateTime(value?: string): string {
  if (!value) {
    return "-";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "-";
  }

  return parsed.toLocaleString();
}

export function statusTone(status: string): "ok" | "warn" | "muted" {
  const normalized = status.trim().toLowerCase();
  if (normalized === "active" || normalized === "running" || normalized === "completed" || normalized === "success") {
    return "ok";
  }
  if (normalized === "failed" || normalized === "paused" || normalized === "stopped" || normalized === "fail" || normalized === "warning" || normalized === "warn") {
    return "warn";
  }
  return "muted";
}
