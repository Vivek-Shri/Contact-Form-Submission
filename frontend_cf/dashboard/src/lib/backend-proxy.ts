import { resolveBackendBaseUrl } from "@/app/api/outreach/run/_backend";

export interface BackendProxyResult {
  ok: boolean;
  status: number;
  payload: unknown;
}

function withNoStore(init: RequestInit): RequestInit {
  return {
    ...init,
    cache: "no-store",
  };
}

export async function parseJsonSafe(response: Response): Promise<unknown | null> {
  try {
    return (await response.json()) as unknown;
  } catch {
    return null;
  }
}

export async function backendJson(path: string, init: RequestInit = {}): Promise<BackendProxyResult> {
  const backendBaseUrl = resolveBackendBaseUrl();

  // If body is a plain object, automatically stringify and add the Content-Type header
  if (init.body && typeof init.body === "object" && !(init.body instanceof FormData) && !(init.body instanceof URLSearchParams) && !(init.body instanceof Blob)) {
    init.headers = {
      ...init.headers,
      "Content-Type": "application/json",
    };
    init.body = JSON.stringify(init.body) as any;
  }

  const response = await fetch(`${backendBaseUrl}${path}`, withNoStore(init));
  const payload = await parseJsonSafe(response);

  return {
    ok: response.ok,
    status: response.status,
    payload,
  };
}

export function extractError(payload: unknown, fallback: string): string {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return fallback;
  }

  const candidate = payload as Record<string, unknown>;
  const detail = candidate.detail;
  if (typeof detail === "string" && detail.trim()) {
    return detail.trim();
  }

  const error = candidate.error;
  if (typeof error === "string" && error.trim()) {
    return error.trim();
  }

  return fallback;
}
