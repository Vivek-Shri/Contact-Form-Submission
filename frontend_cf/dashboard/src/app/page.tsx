"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  FolderKanban,
  ListChecks,
  Loader2,
  Play,
  Plus,
  Settings2,
  StopCircle,
  Target,
  UploadCloud,
  Users,
  X,
} from "lucide-react";
import Papa from "papaparse";

type MainSection = "overview" | "contacts" | "campaigns";
type ContactsTab = "companies" | "lists";
type CampaignTab = "contacts" | "retarget" | "settings" | "logs";
type ContactStatus = "ready" | "submitted" | "failed" | "warning";
type RunStatus = "queued" | "running" | "completed" | "failed" | "stopped";
type StatusTone = "info" | "success" | "error";

interface RunResultRow {
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

interface OutreachRunSnapshot {
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

interface ContactResult {
  status: ContactStatus;
  confirmation: string;
  captchaStatus: string;
  costUsd: number;
}

interface ContactRecord {
  id: string;
  companyName: string;
  domain: string;
  location: string;
  industry: string;
  employeeSize: string;
  contactUrl: string;
  listIds: string[];
  status: ContactStatus;
  lastUpdatedAt: string;
  lastResult?: ContactResult;
}

interface ContactListRecord {
  id: string;
  name: string;
  createdAt: string;
  contactIds: string[];
}

interface RetargetStep {
  id: string;
  title: string;
  daysAfter: number;
  enabled: boolean;
}

interface CampaignRunSummary {
  runId: string;
  status: RunStatus;
  startedAt: string;
  endedAt?: string;
  processedLeads: number;
  totalLeads: number;
  successCount: number;
  failCount: number;
  warningCount: number;
  duplicatesSkipped: number;
  captchaCreditsUsedToday: number;
  captchaCreditsLimit: number;
  captchaCreditsRemaining: number;
  error?: string;
  logsTail: string[];
}

interface CampaignRecord {
  id: string;
  name: string;
  aiInstruction: string;
  maxDailySubmissions: number;
  listIds: string[];
  createdAt: string;
  steps: RetargetStep[];
  runSummaries: CampaignRunSummary[];
  lastRunId?: string;
}

interface BannerState {
  tone: StatusTone;
  message: string;
}

const LIST_SAAS_US = "list-saas-us";
const LIST_AGENCIES = "list-agencies";

const INITIAL_LISTS: ContactListRecord[] = [
  {
    id: LIST_SAAS_US,
    name: "US SaaS Contacts",
    createdAt: "2026-04-01T10:20:00.000Z",
    contactIds: ["contact-rapidflow", "contact-maplesoft"],
  },
  {
    id: LIST_AGENCIES,
    name: "Growth Agencies",
    createdAt: "2026-04-02T12:15:00.000Z",
    contactIds: ["contact-northstar", "contact-pixelmint"],
  },
];

const INITIAL_CONTACTS: ContactRecord[] = [
  {
    id: "contact-rapidflow",
    companyName: "RapidFlow Labs",
    domain: "rapidflow.ai",
    location: "Austin, US",
    industry: "SaaS",
    employeeSize: "51-200",
    contactUrl: "https://rapidflow.ai/contact",
    listIds: [LIST_SAAS_US],
    status: "ready",
    lastUpdatedAt: "2026-04-01T10:20:00.000Z",
  },
  {
    id: "contact-maplesoft",
    companyName: "MapleSoft Cloud",
    domain: "maplesoftcloud.com",
    location: "Toronto, CA",
    industry: "Cloud Software",
    employeeSize: "11-50",
    contactUrl: "https://maplesoftcloud.com/contact-us",
    listIds: [LIST_SAAS_US],
    status: "ready",
    lastUpdatedAt: "2026-04-01T10:20:00.000Z",
  },
  {
    id: "contact-northstar",
    companyName: "Northstar Growth",
    domain: "northstargrowth.co",
    location: "London, UK",
    industry: "Marketing Agency",
    employeeSize: "11-50",
    contactUrl: "https://northstargrowth.co/contact",
    listIds: [LIST_AGENCIES],
    status: "ready",
    lastUpdatedAt: "2026-04-02T12:15:00.000Z",
  },
  {
    id: "contact-pixelmint",
    companyName: "PixelMint Digital",
    domain: "pixelmint.io",
    location: "Berlin, DE",
    industry: "Performance Agency",
    employeeSize: "11-50",
    contactUrl: "https://pixelmint.io/contact",
    listIds: [LIST_AGENCIES],
    status: "ready",
    lastUpdatedAt: "2026-04-02T12:15:00.000Z",
  },
];

const DEFAULT_RETARGET_STEPS: RetargetStep[] = [
  {
    id: "step-1",
    title: "Step 1 - Follow-up email",
    daysAfter: 2,
    enabled: true,
  },
  {
    id: "step-2",
    title: "Step 2 - Reminder with value proof",
    daysAfter: 5,
    enabled: true,
  },
  {
    id: "step-3",
    title: "Step 3 - Final touchpoint",
    daysAfter: 9,
    enabled: false,
  },
];

const INITIAL_CAMPAIGNS: CampaignRecord[] = [
  {
    id: "campaign-spring-intake",
    name: "Spring Intake Push",
    aiInstruction:
      "Write a concise outreach message with one clear CTA. Mention efficiency and include a trustworthy tone.",
    maxDailySubmissions: 70,
    listIds: [LIST_SAAS_US],
    createdAt: "2026-04-03T08:40:00.000Z",
    steps: DEFAULT_RETARGET_STEPS,
    runSummaries: [],
  },
];

const CSV_TEMPLATE_FILE_NAME = "contacts-template.csv";
const POLL_INTERVAL_MS = 2500;

function createId(prefix: string): string {
  const raw =
    typeof globalThis.crypto !== "undefined" && typeof globalThis.crypto.randomUUID === "function"
      ? globalThis.crypto.randomUUID().replace(/-/g, "")
      : `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 10)}`;

  return `${prefix}-${raw.slice(0, 10)}`;
}

function ensureProtocol(rawUrl: string): string {
  const value = rawUrl.trim();
  if (!value) {
    return "";
  }

  if (/^https?:\/\//i.test(value)) {
    return value;
  }

  return `https://${value}`;
}

function normalizeUrlKey(rawUrl: string): string {
  const value = rawUrl.trim();
  if (!value) {
    return "";
  }

  try {
    const parsed = new URL(ensureProtocol(value));
    const host = parsed.hostname.replace(/^www\./i, "").toLowerCase();
    const pathname = parsed.pathname.replace(/\/+$/g, "") || "/";
    return `${host}${pathname}`;
  } catch {
    return value.toLowerCase();
  }
}

function getDomainFromUrl(rawUrl: string): string {
  const withProtocol = ensureProtocol(rawUrl);

  try {
    return new URL(withProtocol).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return withProtocol.replace(/^https?:\/\//i, "").replace(/^www\./i, "").split("/")[0];
  }
}

function pickCsvValue(row: Record<string, string>, candidates: string[]): string {
  const lowerMap = new Map<string, string>();

  for (const [key, value] of Object.entries(row)) {
    lowerMap.set(key.trim().toLowerCase(), String(value ?? "").trim());
  }

  for (const candidate of candidates) {
    const value = lowerMap.get(candidate.toLowerCase());
    if (value) {
      return value;
    }
  }

  return "";
}

function clampPositiveInt(raw: string, fallback: number): number {
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function formatPercent(numerator: number, denominator: number): string {
  if (denominator <= 0) {
    return "0%";
  }
  return `${Math.round((numerator / denominator) * 100)}%`;
}

function formatTimestamp(iso: string | undefined): string {
  if (!iso) {
    return "-";
  }

  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }

  return date.toLocaleString();
}

function formatCurrency(value: number): string {
  return `$${value.toFixed(4)}`;
}

function resultToContactStatus(result: RunResultRow): ContactStatus {
  if (result.status === "success" && result.submitted === "Yes") {
    return "submitted";
  }

  if (result.status === "warning") {
    return "warning";
  }

  return "failed";
}

function toRunSummary(snapshot: OutreachRunSnapshot): CampaignRunSummary {
  const successCount = snapshot.results.filter((result) => result.status === "success").length;
  const failCount = snapshot.results.filter((result) => result.status === "fail").length;
  const warningCount = snapshot.results.filter((result) => result.status === "warning").length;

  return {
    runId: snapshot.runId,
    status: snapshot.status,
    startedAt: snapshot.startedAt,
    endedAt: snapshot.endedAt,
    processedLeads: snapshot.processedLeads,
    totalLeads: snapshot.totalLeads,
    successCount,
    failCount,
    warningCount,
    duplicatesSkipped: snapshot.duplicatesSkipped,
    captchaCreditsUsedToday: snapshot.captchaCreditsUsedToday,
    captchaCreditsLimit: snapshot.captchaCreditsLimit,
    captchaCreditsRemaining: snapshot.captchaCreditsRemaining,
    error: snapshot.error,
    logsTail: snapshot.logs.slice(-120),
  };
}

function contactStatusClass(status: ContactStatus): string {
  if (status === "submitted") {
    return "bg-emerald-500/20 text-emerald-200 border border-emerald-300/40";
  }

  if (status === "failed") {
    return "bg-rose-500/20 text-rose-200 border border-rose-300/40";
  }

  if (status === "warning") {
    return "bg-amber-500/20 text-amber-200 border border-amber-300/40";
  }

  return "bg-slate-500/20 text-slate-200 border border-slate-300/35";
}

function runStatusClass(status: RunStatus): string {
  if (status === "completed") {
    return "bg-emerald-500/20 text-emerald-200 border border-emerald-300/35";
  }

  if (status === "failed" || status === "stopped") {
    return "bg-rose-500/20 text-rose-200 border border-rose-300/35";
  }

  return "bg-sky-500/20 text-sky-200 border border-sky-300/35";
}

function bannerClass(tone: StatusTone): string {
  if (tone === "success") {
    return "border-emerald-400/40 bg-emerald-500/10 text-emerald-100";
  }

  if (tone === "error") {
    return "border-rose-400/40 bg-rose-500/10 text-rose-100";
  }

  return "border-sky-400/40 bg-sky-500/10 text-sky-100";
}

interface StatCardProps {
  title: string;
  value: string;
  detail: string;
}

function StatCard({ title, value, detail }: StatCardProps) {
  return (
    <div className="glass-panel rounded-2xl p-5">
      <p className="text-xs uppercase tracking-[0.2em] text-slate-300/70">{title}</p>
      <p className="mt-2 text-3xl font-semibold text-slate-50">{value}</p>
      <p className="mt-1 text-sm text-slate-300/80">{detail}</p>
    </div>
  );
}

export default function Page() {
  const [section, setSection] = useState<MainSection>("overview");
  const [contactsTab, setContactsTab] = useState<ContactsTab>("companies");
  const [campaignTab, setCampaignTab] = useState<CampaignTab>("contacts");

  const [contacts, setContacts] = useState<ContactRecord[]>(INITIAL_CONTACTS);
  const [lists, setLists] = useState<ContactListRecord[]>(INITIAL_LISTS);
  const [campaigns, setCampaigns] = useState<CampaignRecord[]>(INITIAL_CAMPAIGNS);
  const [selectedCampaignId, setSelectedCampaignId] = useState<string>(INITIAL_CAMPAIGNS[0]?.id ?? "");

  const [banner, setBanner] = useState<BannerState | null>(null);

  const [showAddContactsModal, setShowAddContactsModal] = useState(false);
  const [addContactsStep, setAddContactsStep] = useState<1 | 2>(1);
  const [newListName, setNewListName] = useState("");
  const [csvFeedback, setCsvFeedback] = useState<string>("");
  const [isParsingCsv, setIsParsingCsv] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const [draftCampaignName, setDraftCampaignName] = useState("");
  const [draftAiInstruction, setDraftAiInstruction] = useState("");
  const [queuedListIds, setQueuedListIds] = useState<string[]>([]);

  const [activeRun, setActiveRun] = useState<OutreachRunSnapshot | null>(null);
  const [activeRunCampaignId, setActiveRunCampaignId] = useState<string | null>(null);
  const [isStartingRun, setIsStartingRun] = useState(false);
  const [isStoppingRun, setIsStoppingRun] = useState(false);
  const snapshotSyncKeyRef = useRef<string>("");

  const showBanner = useCallback((message: string, tone: StatusTone = "info") => {
    setBanner({ message, tone });
  }, []);

  const selectedCampaign = useMemo(() => {
    return campaigns.find((campaign) => campaign.id === selectedCampaignId) ?? null;
  }, [campaigns, selectedCampaignId]);

  useEffect(() => {
    if (campaigns.length === 0) {
      setSelectedCampaignId("");
      return;
    }

    if (selectedCampaignId === "") {
      return;
    }

    if (!campaigns.some((campaign) => campaign.id === selectedCampaignId)) {
      setSelectedCampaignId(campaigns[0].id);
    }
  }, [campaigns, selectedCampaignId]);

  const getContactsForCampaign = useCallback(
    (campaign: CampaignRecord): ContactRecord[] => {
      const listIdSet = new Set(campaign.listIds);
      return contacts.filter((contact) => contact.listIds.some((listId) => listIdSet.has(listId)));
    },
    [contacts],
  );

  const selectedCampaignContacts = useMemo(() => {
    if (!selectedCampaign) {
      return [] as ContactRecord[];
    }

    return getContactsForCampaign(selectedCampaign);
  }, [getContactsForCampaign, selectedCampaign]);

  const listCards = useMemo(() => {
    return lists.map((list) => {
      const listContacts = contacts.filter((contact) => list.contactIds.includes(contact.id));
      const submitted = listContacts.filter((contact) => contact.status === "submitted").length;
      const failed = listContacts.filter((contact) => contact.status === "failed").length;

      return {
        ...list,
        total: listContacts.length,
        submitted,
        failed,
        completion: formatPercent(submitted + failed, listContacts.length),
      };
    });
  }, [contacts, lists]);

  const totalSubmitted = useMemo(
    () => contacts.filter((contact) => contact.status === "submitted").length,
    [contacts],
  );

  const totalFailed = useMemo(
    () => contacts.filter((contact) => contact.status === "failed").length,
    [contacts],
  );

  const totalRuns = useMemo(
    () => campaigns.reduce((sum, campaign) => sum + campaign.runSummaries.length, 0),
    [campaigns],
  );

  const submissionRate = formatPercent(totalSubmitted, contacts.length);

  const isRunActive = activeRun?.status === "running" || activeRun?.status === "queued";
  const canStopRun = Boolean(isRunActive && activeRun);

  const syncSnapshotToUi = useCallback(
    (snapshot: OutreachRunSnapshot, campaignId: string) => {
      const summary = toRunSummary(snapshot);

      setCampaigns((previous) => {
        return previous.map((campaign) => {
          if (campaign.id !== campaignId) {
            return campaign;
          }

          const otherRuns = campaign.runSummaries.filter((run) => run.runId !== summary.runId);
          return {
            ...campaign,
            lastRunId: summary.runId,
            runSummaries: [summary, ...otherRuns],
          };
        });
      });

      if (snapshot.results.length === 0) {
        return;
      }

      const resultsByUrl = new Map<string, RunResultRow>();

      for (const result of snapshot.results) {
        const key = normalizeUrlKey(result.contactUrl);
        if (!key) {
          continue;
        }

        resultsByUrl.set(key, result);
      }

      setContacts((previous) => {
        return previous.map((contact) => {
          const result = resultsByUrl.get(normalizeUrlKey(contact.contactUrl));
          if (!result) {
            return contact;
          }

          return {
            ...contact,
            status: resultToContactStatus(result),
            lastUpdatedAt: new Date().toISOString(),
            lastResult: {
              status: resultToContactStatus(result),
              confirmation: result.confirmationMsg,
              captchaStatus: result.captchaStatus,
              costUsd: result.estCostUsd,
            },
          };
        });
      });
    },
    [setCampaigns, setContacts],
  );

  useEffect(() => {
    if (!activeRun || !activeRunCampaignId) {
      return;
    }

    const snapshotKey = `${activeRun.runId}:${activeRun.status}:${activeRun.results.length}:${activeRun.logs.length}:${activeRun.progress}`;

    if (snapshotSyncKeyRef.current === snapshotKey) {
      return;
    }

    snapshotSyncKeyRef.current = snapshotKey;
    syncSnapshotToUi(activeRun, activeRunCampaignId);

    if (activeRun.status === "completed") {
      showBanner(
        `Run completed. Processed ${activeRun.processedLeads}/${activeRun.totalLeads} contacts with ${activeRun.duplicatesSkipped} duplicate(s) skipped.`,
        "success",
      );
    }

    if (activeRun.status === "failed") {
      showBanner(activeRun.error ?? "Run failed. Please check campaign logs.", "error");
    }

    if (activeRun.status === "stopped") {
      showBanner("Run stopped by operator.", "info");
    }
  }, [activeRun, activeRunCampaignId, showBanner, syncSnapshotToUi]);

  useEffect(() => {
    if (!activeRun || (activeRun.status !== "running" && activeRun.status !== "queued")) {
      return;
    }

    const currentRunId = activeRun.runId;
    const timer = window.setInterval(async () => {
      try {
        const response = await fetch(`/api/outreach/run?runId=${encodeURIComponent(currentRunId)}`, {
          method: "GET",
          cache: "no-store",
        });

        if (!response.ok) {
          return;
        }

        const snapshot = (await response.json()) as OutreachRunSnapshot;
        setActiveRun(snapshot);
      } catch {
        // Silent retry on next poll.
      }
    }, POLL_INTERVAL_MS);

    return () => {
      window.clearInterval(timer);
    };
  }, [activeRun]);

  const startCampaignRun = useCallback(
    async (campaign: CampaignRecord) => {
      if (isRunActive) {
        showBanner("Another run is already active. Stop it before starting a new run.", "error");
        return;
      }

      const campaignContacts = getContactsForCampaign(campaign);
      if (campaignContacts.length === 0) {
        showBanner("This campaign has no contacts. Attach at least one list first.", "error");
        return;
      }

      setIsStartingRun(true);
      snapshotSyncKeyRef.current = "";

      try {
        const leads = campaignContacts.map((contact) => ({
          companyName: contact.companyName,
          contactUrl: contact.contactUrl,
        }));

        const response = await fetch("/api/outreach/run", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            persona: {
              id: campaign.id,
              title: campaign.name,
              aiInstruction: campaign.aiInstruction,
              maxDailySubmissions: campaign.maxDailySubmissions,
            },
            leads,
          }),
        });

        const payload = (await response.json().catch(() => null)) as
          | (Partial<OutreachRunSnapshot> & { error?: string; runId?: string; code?: string })
          | null;

        if (!response.ok) {
          if (response.status === 409 && payload?.runId) {
            const running = await fetch(`/api/outreach/run?runId=${encodeURIComponent(payload.runId)}`, {
              method: "GET",
              cache: "no-store",
            });

            if (running.ok) {
              const snapshot = (await running.json()) as OutreachRunSnapshot;
              setActiveRun(snapshot);
            }
          }

          showBanner(payload?.error ?? "Unable to start campaign run.", "error");
          return;
        }

        const snapshot = payload as OutreachRunSnapshot;
        setActiveRun(snapshot);
        setActiveRunCampaignId(campaign.id);
        setSelectedCampaignId(campaign.id);
        setCampaignTab("logs");
        setSection("campaigns");

        showBanner(`Campaign "${campaign.name}" started for ${leads.length} contacts.`, "success");
      } catch (error) {
        const message = error instanceof Error ? error.message : "Failed to start campaign.";
        showBanner(message, "error");
      } finally {
        setIsStartingRun(false);
      }
    },
    [getContactsForCampaign, isRunActive, showBanner],
  );

  const stopCampaignRun = useCallback(async () => {
    if (!activeRun) {
      return;
    }

    setIsStoppingRun(true);

    try {
      const response = await fetch("/api/outreach/run/stop", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ runId: activeRun.runId }),
      });

      const payload = (await response.json().catch(() => null)) as
        | (OutreachRunSnapshot & { error?: string })
        | null;

      if (!response.ok || !payload) {
        showBanner(payload?.error ?? "Unable to stop run.", "error");
        return;
      }

      setActiveRun(payload);
      showBanner("Run stop request submitted.", "info");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to stop run.";
      showBanner(message, "error");
    } finally {
      setIsStoppingRun(false);
    }
  }, [activeRun, showBanner]);

  const handleCreateCampaign = useCallback(
    (event: React.FormEvent<HTMLFormElement>) => {
      event.preventDefault();

      const name = draftCampaignName.trim();
      const instruction = draftAiInstruction.trim();

      if (!name) {
        showBanner("Campaign name is required.", "error");
        return;
      }

      if (!instruction) {
        showBanner("AI instruction is required.", "error");
        return;
      }

      const campaign: CampaignRecord = {
        id: createId("campaign"),
        name,
        aiInstruction: instruction,
        maxDailySubmissions: 100,
        listIds: queuedListIds,
        createdAt: new Date().toISOString(),
        steps: DEFAULT_RETARGET_STEPS.map((step) => ({ ...step })),
        runSummaries: [],
      };

      setCampaigns((previous) => [campaign, ...previous]);
      setSelectedCampaignId(campaign.id);
      setSection("campaigns");
      setCampaignTab("contacts");
      setDraftCampaignName("");
      setDraftAiInstruction("");
      setQueuedListIds([]);

      showBanner(`Campaign "${campaign.name}" created successfully.`, "success");
    },
    [draftAiInstruction, draftCampaignName, queuedListIds, showBanner],
  );

  const toggleCampaignList = useCallback((campaignId: string, listId: string) => {
    setCampaigns((previous) => {
      return previous.map((campaign) => {
        if (campaign.id !== campaignId) {
          return campaign;
        }

        const hasList = campaign.listIds.includes(listId);
        return {
          ...campaign,
          listIds: hasList
            ? campaign.listIds.filter((id) => id !== listId)
            : [...campaign.listIds, listId],
        };
      });
    });
  }, []);

  const toggleRetargetStep = useCallback((campaignId: string, stepId: string) => {
    setCampaigns((previous) => {
      return previous.map((campaign) => {
        if (campaign.id !== campaignId) {
          return campaign;
        }

        return {
          ...campaign,
          steps: campaign.steps.map((step) => {
            if (step.id !== stepId) {
              return step;
            }

            return {
              ...step,
              enabled: !step.enabled,
            };
          }),
        };
      });
    });
  }, []);

  const updateRetargetStepDelay = useCallback((campaignId: string, stepId: string, rawValue: string) => {
    const nextDelay = clampPositiveInt(rawValue, 1);

    setCampaigns((previous) => {
      return previous.map((campaign) => {
        if (campaign.id !== campaignId) {
          return campaign;
        }

        return {
          ...campaign,
          steps: campaign.steps.map((step) => {
            if (step.id !== stepId) {
              return step;
            }

            return {
              ...step,
              daysAfter: nextDelay,
            };
          }),
        };
      });
    });
  }, []);

  const updateCampaignDailyLimit = useCallback((campaignId: string, rawValue: string) => {
    const next = clampPositiveInt(rawValue, 1);

    setCampaigns((previous) => {
      return previous.map((campaign) => {
        if (campaign.id !== campaignId) {
          return campaign;
        }

        return {
          ...campaign,
          maxDailySubmissions: next,
        };
      });
    });
  }, []);

  const enqueueListForNextCampaign = useCallback(
    (listId: string) => {
      setQueuedListIds((previous) => {
        if (previous.includes(listId)) {
          return previous;
        }
        return [...previous, listId];
      });

      setSection("campaigns");
      showBanner("List added to campaign creation queue.", "info");
    },
    [showBanner],
  );

  const removeQueuedList = useCallback((listId: string) => {
    setQueuedListIds((previous) => previous.filter((id) => id !== listId));
  }, []);

  const clearSelectedCampaign = useCallback(() => {
    setSelectedCampaignId("");
    setCampaignTab("contacts");
  }, []);

  const downloadCsvTemplate = useCallback(() => {
    const csv = [
      "Company Name,Website URL,Location,Industry,Employee Size,Contact Form URL",
      "Acme Labs,https://acmelabs.com,San Francisco US,SaaS,51-200,https://acmelabs.com/contact",
    ].join("\n");

    const blob = new Blob([`${csv}\n`], { type: "text/csv;charset=utf-8" });
    const downloadUrl = URL.createObjectURL(blob);

    const anchor = document.createElement("a");
    anchor.href = downloadUrl;
    anchor.download = CSV_TEMPLATE_FILE_NAME;
    anchor.click();

    URL.revokeObjectURL(downloadUrl);
  }, []);

  const openAddContactsModal = useCallback(() => {
    setShowAddContactsModal(true);
    setAddContactsStep(1);
    setNewListName("");
    setCsvFeedback("");
    setIsParsingCsv(false);
    if (fileInputRef.current) {
      fileInputRef.current.value = "";
    }
  }, []);

  const closeAddContactsModal = useCallback(() => {
    setShowAddContactsModal(false);
    setAddContactsStep(1);
    setNewListName("");
    setCsvFeedback("");
    setIsParsingCsv(false);
    if (fileInputRef.current) {
      fileInputRef.current.value = "";
    }
  }, []);

  const handleNextAddContactsStep = useCallback(() => {
    if (!newListName.trim()) {
      setCsvFeedback("Please enter a list name before uploading CSV.");
      return;
    }

    setCsvFeedback("");
    setAddContactsStep(2);
  }, [newListName]);

  const handleCsvUpload = useCallback(
    (event: React.ChangeEvent<HTMLInputElement>) => {
      const file = event.target.files?.[0];
      if (!file) {
        return;
      }

      const trimmedListName = newListName.trim();
      if (!trimmedListName) {
        setCsvFeedback("List name is missing. Go back to step 1 and enter list name.");
        setAddContactsStep(1);
        return;
      }

      setIsParsingCsv(true);
      setCsvFeedback("Reading CSV and validating duplicates...");

      Papa.parse<Record<string, string>>(file, {
        header: true,
        skipEmptyLines: "greedy",
        complete: (results) => {
          const rows = results.data ?? [];
          const existingKeys = new Set(contacts.map((contact) => normalizeUrlKey(contact.contactUrl)));
          const inBatch = new Set<string>();

          const incomingContacts: ContactRecord[] = [];
          let duplicateCount = 0;
          let invalidCount = 0;

          for (const row of rows) {
            const companyName = pickCsvValue(row, [
              "company name",
              "company",
              "company_name",
              "name",
            ]);

            const contactUrl = pickCsvValue(row, [
              "contact form url",
              "contact url found",
              "contact url",
              "url",
              "contact_url",
            ]);

            if (!companyName || !contactUrl) {
              invalidCount += 1;
              continue;
            }

            const normalizedUrl = normalizeUrlKey(contactUrl);
            if (!normalizedUrl) {
              invalidCount += 1;
              continue;
            }

            if (existingKeys.has(normalizedUrl) || inBatch.has(normalizedUrl)) {
              duplicateCount += 1;
              continue;
            }

            inBatch.add(normalizedUrl);

            const website = pickCsvValue(row, ["website url", "website", "domain"]);
            const location = pickCsvValue(row, ["location", "city", "country"]) || "Unknown";
            const industry = pickCsvValue(row, ["industry", "category", "vertical"]) || "Unknown";
            const employeeSize = pickCsvValue(row, ["employee size", "employees", "team size"]) || "n/a";

            incomingContacts.push({
              id: createId("contact"),
              companyName,
              domain: website ? getDomainFromUrl(website) : getDomainFromUrl(contactUrl),
              location,
              industry,
              employeeSize,
              contactUrl: ensureProtocol(contactUrl),
              listIds: [],
              status: "ready",
              lastUpdatedAt: new Date().toISOString(),
            });
          }

          if (incomingContacts.length === 0) {
            setCsvFeedback(
              `No contacts imported. Duplicates skipped: ${duplicateCount}. Invalid rows: ${invalidCount}.`,
            );
            setIsParsingCsv(false);
            return;
          }

          const listId = createId("list");
          const createdAt = new Date().toISOString();
          const listContactIds = incomingContacts.map((contact) => contact.id);
          const contactsWithList = incomingContacts.map((contact) => ({
            ...contact,
            listIds: [listId],
          }));

          setContacts((previous) => [...contactsWithList, ...previous]);
          setLists((previous) => [
            {
              id: listId,
              name: trimmedListName,
              createdAt,
              contactIds: listContactIds,
            },
            ...previous,
          ]);

          setQueuedListIds((previous) => {
            if (previous.includes(listId)) {
              return previous;
            }
            return [...previous, listId];
          });

          setIsParsingCsv(false);
          setCsvFeedback(
            `Imported ${incomingContacts.length} contacts. Duplicates skipped: ${duplicateCount}. Invalid rows: ${invalidCount}.`,
          );

          showBanner(`List "${trimmedListName}" added with ${incomingContacts.length} contacts.`, "success");

          setContactsTab("lists");
          setSection("contacts");
          closeAddContactsModal();
        },
        error: (error) => {
          setCsvFeedback(`CSV parse failed: ${error.message}`);
          setIsParsingCsv(false);
        },
      });
    },
    [closeAddContactsModal, contacts, newListName, showBanner],
  );

  const selectedCampaignRun =
    selectedCampaign && activeRunCampaignId === selectedCampaign.id ? activeRun : null;
  const selectedCampaignLatestRun = selectedCampaign?.runSummaries[0] ?? null;

  return (
    <div className="nebula-shell min-h-screen">
      <div className="relative z-10 mx-auto w-full max-w-[1440px] px-4 py-6 sm:px-6 lg:px-8">
        <div className="grid gap-6 lg:grid-cols-[260px_minmax(0,1fr)]">
          <aside className="sidebar-float p-4 lg:sticky lg:top-6 lg:h-[calc(100vh-3rem)]">
            <div className="rounded-2xl border border-slate-400/20 bg-slate-900/45 p-4">
              <p className="text-xs uppercase tracking-[0.22em] text-slate-300/70">Automation Hub</p>
              <p className="mt-2 text-lg font-semibold text-slate-50">Outreach Workspace</p>
              <p className="mt-1 text-sm text-slate-300/80">Contacts and campaigns in one control room.</p>
            </div>

            <nav className="mt-4 space-y-2">
              {[
                {
                  id: "overview" as const,
                  label: "Overview",
                  icon: Activity,
                },
                {
                  id: "contacts" as const,
                  label: "Contacts",
                  icon: Users,
                },
                {
                  id: "campaigns" as const,
                  label: "Campaigns",
                  icon: FolderKanban,
                },
              ].map((item) => {
                const active = section === item.id;

                return (
                  <button
                    key={item.id}
                    type="button"
                    onClick={() => setSection(item.id)}
                    className={`flex w-full items-center gap-3 rounded-xl border px-3 py-2 text-left transition ${
                      active
                        ? "border-sky-300/55 bg-sky-500/15 text-sky-100"
                        : "border-slate-400/20 bg-slate-900/35 text-slate-300 hover:border-slate-300/35 hover:bg-slate-900/55"
                    }`}
                  >
                    <item.icon className="h-4 w-4" />
                    <span className="text-sm font-medium">{item.label}</span>
                  </button>
                );
              })}
            </nav>

            <div className="mt-6 rounded-2xl border border-slate-400/20 bg-slate-900/45 p-4">
              <p className="text-xs uppercase tracking-[0.18em] text-slate-300/70">Run Status</p>
              <div className="mt-3 flex items-center gap-2">
                <span className={isRunActive ? "breathing-dot" : "h-2 w-2 rounded-full bg-slate-500/70"} />
                <span className="text-sm text-slate-200">
                  {isRunActive ? "Active backend run in progress" : "No active run"}
                </span>
              </div>
              {activeRun ? (
                <p className="mt-3 text-xs text-slate-300/80">
                  {activeRun.processedLeads}/{activeRun.totalLeads} processed, duplicates skipped: {" "}
                  {activeRun.duplicatesSkipped}
                </p>
              ) : null}
            </div>
          </aside>

          <main className="space-y-6">
            <header className="glass-panel rounded-2xl px-5 py-4 sm:px-6 sm:py-5">
              <div className="flex flex-wrap items-center justify-between gap-4">
                <div>
                  <p className="text-xs uppercase tracking-[0.22em] text-slate-300/75">Lead Engine</p>
                  <h1 className="mt-1 text-2xl font-semibold text-slate-50 sm:text-3xl">
                    {section === "overview" && "Performance Overview"}
                    {section === "contacts" && "Contacts and Lists"}
                    {section === "campaigns" && "Campaign Control"}
                  </h1>
                  <p className="mt-1 text-sm text-slate-300/85">
                    {section === "overview" &&
                      "Monitor submissions, duplicate prevention, and CAPTCHA-credit usage across campaigns."}
                    {section === "contacts" &&
                      "Manage companies and list uploads. Add contacts using a list name + CSV flow."}
                    {section === "campaigns" &&
                      "Create simple campaigns and manage contacts, retarget steps, settings, and logs."}
                  </p>
                </div>

                <div className="flex flex-wrap items-center gap-2">
                  {section === "contacts" ? (
                    <button
                      type="button"
                      onClick={openAddContactsModal}
                      className="inline-flex items-center gap-2 rounded-xl border border-sky-300/55 bg-sky-500/20 px-3 py-2 text-sm font-medium text-sky-100 transition hover:bg-sky-500/30"
                    >
                      <Plus className="h-4 w-4" />
                      Add Contacts
                    </button>
                  ) : null}

                  {section === "campaigns" && selectedCampaign ? (
                    <>
                      <button
                        type="button"
                        onClick={() => void startCampaignRun(selectedCampaign)}
                        disabled={isStartingRun || isRunActive}
                        className="inline-flex items-center gap-2 rounded-xl border border-emerald-300/55 bg-emerald-500/20 px-3 py-2 text-sm font-medium text-emerald-100 transition hover:bg-emerald-500/30 disabled:cursor-not-allowed disabled:opacity-60"
                      >
                        {isStartingRun ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                          <Play className="h-4 w-4" />
                        )}
                        Start Campaign
                      </button>

                      <button
                        type="button"
                        onClick={() => void stopCampaignRun()}
                        disabled={!canStopRun || isStoppingRun}
                        className="inline-flex items-center gap-2 rounded-xl border border-rose-300/55 bg-rose-500/20 px-3 py-2 text-sm font-medium text-rose-100 transition hover:bg-rose-500/30 disabled:cursor-not-allowed disabled:opacity-60"
                      >
                        {isStoppingRun ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                          <StopCircle className="h-4 w-4" />
                        )}
                        Stop
                      </button>
                    </>
                  ) : null}
                </div>
              </div>
            </header>

            {banner ? (
              <div className={`glass-panel rounded-2xl border px-4 py-3 text-sm ${bannerClass(banner.tone)}`}>
                <div className="flex items-start justify-between gap-3">
                  <p>{banner.message}</p>
                  <button
                    type="button"
                    onClick={() => setBanner(null)}
                    aria-label="Dismiss message"
                    title="Dismiss message"
                    className="rounded-md p-1 text-slate-200/90 transition hover:bg-slate-800/50"
                  >
                    <X className="h-4 w-4" />
                  </button>
                </div>
              </div>
            ) : null}

            {section === "overview" ? (
              <section className="space-y-5">
                <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                  <StatCard title="Companies" value={String(contacts.length)} detail="Total contact records" />
                  <StatCard title="Lists" value={String(lists.length)} detail="Organized contact segments" />
                  <StatCard title="Campaigns" value={String(campaigns.length)} detail="Active campaign entities" />
                  <StatCard
                    title="Submission Rate"
                    value={submissionRate}
                    detail={`${totalSubmitted} submitted / ${totalFailed} failed`}
                  />
                </div>

                <div className="grid gap-4 xl:grid-cols-[2fr_1fr]">
                  <div className="glass-panel rounded-2xl p-5">
                    <div className="flex items-center justify-between gap-3">
                      <p className="text-sm font-semibold text-slate-100">Recent Campaign Runs</p>
                      <span className="rounded-full border border-slate-300/25 bg-slate-900/45 px-2.5 py-1 text-xs text-slate-300">
                        {totalRuns} total runs
                      </span>
                    </div>

                    <div className="mt-4 overflow-x-auto">
                      <table className="min-w-full text-left text-sm">
                        <thead>
                          <tr className="border-b border-slate-300/15 text-xs uppercase tracking-[0.16em] text-slate-300/70">
                            <th className="px-2 py-2">Campaign</th>
                            <th className="px-2 py-2">Status</th>
                            <th className="px-2 py-2">Processed</th>
                            <th className="px-2 py-2">Duplicates</th>
                            <th className="px-2 py-2">Captcha Credits</th>
                          </tr>
                        </thead>
                        <tbody>
                          {campaigns.flatMap((campaign) => campaign.runSummaries.slice(0, 1)).length === 0 ? (
                            <tr>
                              <td className="px-2 py-4 text-slate-300/70" colSpan={5}>
                                No run history yet. Start a campaign to populate analytics.
                              </td>
                            </tr>
                          ) : (
                            campaigns.flatMap((campaign) => {
                              const run = campaign.runSummaries[0];
                              if (!run) {
                                return [];
                              }

                              return [
                                <tr key={`${campaign.id}-${run.runId}`} className="border-b border-slate-300/10">
                                  <td className="px-2 py-3 text-slate-100">{campaign.name}</td>
                                  <td className="px-2 py-3">
                                    <span className={`rounded-full px-2 py-1 text-xs ${runStatusClass(run.status)}`}>
                                      {run.status}
                                    </span>
                                  </td>
                                  <td className="px-2 py-3 text-slate-200">
                                    {run.processedLeads}/{run.totalLeads}
                                  </td>
                                  <td className="px-2 py-3 text-slate-200">{run.duplicatesSkipped}</td>
                                  <td className="px-2 py-3 text-slate-200">
                                    {run.captchaCreditsUsedToday}/{run.captchaCreditsLimit}
                                  </td>
                                </tr>,
                              ];
                            })
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  <div className="glass-panel rounded-2xl p-5">
                    <p className="text-sm font-semibold text-slate-100">Live Run Telemetry</p>

                    {activeRun ? (
                      <div className="mt-4 space-y-3">
                        <div className="flex items-center justify-between">
                          <span className="text-sm text-slate-300">Status</span>
                          <span className={`rounded-full px-2 py-1 text-xs ${runStatusClass(activeRun.status)}`}>
                            {activeRun.status}
                          </span>
                        </div>

                        <div>
                          <div className="mb-1 flex items-center justify-between text-xs text-slate-300/85">
                            <span>Progress</span>
                            <span>{activeRun.progress}%</span>
                          </div>
                          <progress
                            value={activeRun.progress}
                            max={100}
                            className="progress-track progress-runner h-2 w-full"
                          />
                          <p className="mt-1 text-xs text-slate-300/75">
                            {activeRun.processedLeads}/{activeRun.totalLeads} processed
                          </p>
                        </div>

                        <div className="rounded-xl border border-slate-300/15 bg-slate-950/45 p-3 text-xs text-slate-300/90">
                          <p>Current lead: {activeRun.currentLead}</p>
                          <p className="mt-1">
                            CAPTCHA credits left today: {activeRun.captchaCreditsRemaining}/{activeRun.captchaCreditsLimit}
                          </p>
                          <p className="mt-1">Duplicates skipped: {activeRun.duplicatesSkipped}</p>
                        </div>
                      </div>
                    ) : (
                      <p className="mt-3 text-sm text-slate-300/75">No active run right now.</p>
                    )}
                  </div>
                </div>
              </section>
            ) : null}

            {section === "contacts" ? (
              <section className="space-y-4">
                <div className="flex flex-wrap items-center gap-2">
                  {[
                    { id: "companies" as const, label: "Companies", icon: Users },
                    { id: "lists" as const, label: "Lists", icon: ListChecks },
                  ].map((tab) => {
                    const active = contactsTab === tab.id;
                    return (
                      <button
                        key={tab.id}
                        type="button"
                        onClick={() => setContactsTab(tab.id)}
                        className={`inline-flex items-center gap-2 rounded-xl border px-3 py-1.5 text-sm transition ${
                          active
                            ? "border-sky-300/55 bg-sky-500/20 text-sky-100"
                            : "border-slate-300/20 bg-slate-900/40 text-slate-300 hover:bg-slate-900/60"
                        }`}
                      >
                        <tab.icon className="h-4 w-4" />
                        {tab.label}
                      </button>
                    );
                  })}
                </div>

                {contactsTab === "companies" ? (
                  <div className="glass-panel overflow-x-auto rounded-2xl p-4">
                    <table className="min-w-full text-left text-sm">
                      <thead>
                        <tr className="border-b border-slate-300/15 text-xs uppercase tracking-[0.16em] text-slate-300/70">
                          <th className="px-3 py-2">Company Name</th>
                          <th className="px-3 py-2">Website URL</th>
                          <th className="px-3 py-2">Location</th>
                          <th className="px-3 py-2">Industry</th>
                          <th className="px-3 py-2">Employee Size</th>
                          <th className="px-3 py-2">Contact Form URL</th>
                          <th className="px-3 py-2">Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {contacts.map((contact) => (
                          <tr key={contact.id} className="border-b border-slate-300/10 last:border-b-0">
                            <td className="px-3 py-3 text-slate-100">{contact.companyName}</td>
                            <td className="px-3 py-3 text-sky-200">{`https://${contact.domain}`}</td>
                            <td className="px-3 py-3 text-slate-200">{contact.location}</td>
                            <td className="px-3 py-3 text-slate-200">{contact.industry}</td>
                            <td className="px-3 py-3 text-slate-200">{contact.employeeSize}</td>
                            <td className="px-3 py-3 text-slate-200">{contact.contactUrl}</td>
                            <td className="px-3 py-3">
                              <span className={`rounded-full px-2 py-1 text-xs ${contactStatusClass(contact.status)}`}>
                                {contact.status}
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : null}

                {contactsTab === "lists" ? (
                  <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                    {listCards.map((list) => (
                      <div key={list.id} className="glass-panel rounded-2xl p-4">
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <p className="text-sm font-semibold text-slate-100">{list.name}</p>
                            <p className="text-xs text-slate-300/75">Created {formatTimestamp(list.createdAt)}</p>
                          </div>
                          <span className="rounded-full border border-slate-300/25 bg-slate-900/45 px-2 py-1 text-xs text-slate-200">
                            {list.total} contacts
                          </span>
                        </div>

                        <div className="mt-3 text-xs text-slate-300/85">
                          <p>Submitted: {list.submitted}</p>
                          <p>Failed: {list.failed}</p>
                          <p>Processed: {list.completion}</p>
                        </div>

                        <div className="mt-3">
                          <div className="mb-1 flex items-center justify-between text-xs text-slate-300/80">
                            <span>Progress</span>
                            <span>{list.completion}</span>
                          </div>
                          <progress
                            className="progress-track progress-runner h-2 w-full"
                            value={list.total === 0 ? 0 : list.submitted + list.failed}
                            max={list.total === 0 ? 1 : list.total}
                          />
                        </div>

                        <button
                          type="button"
                          onClick={() => enqueueListForNextCampaign(list.id)}
                          className="mt-4 inline-flex items-center gap-2 rounded-xl border border-sky-300/45 bg-sky-500/15 px-3 py-1.5 text-xs font-medium text-sky-100 transition hover:bg-sky-500/30"
                        >
                          <Plus className="h-3.5 w-3.5" />
                          Add to Campaign
                        </button>
                      </div>
                    ))}
                  </div>
                ) : null}
              </section>
            ) : null}

            {section === "campaigns" ? (
              <section className="grid gap-5 xl:grid-cols-[360px_minmax(0,1fr)]">
                <div className="space-y-4">
                  <form onSubmit={handleCreateCampaign} className="glass-panel rounded-2xl p-4">
                    <p className="text-sm font-semibold text-slate-100">Create Campaign</p>
                    <p className="mt-1 text-xs text-slate-300/75">
                      Only 2 fields required: campaign name + AI instruction.
                    </p>

                    <label className="mt-3 block text-xs uppercase tracking-[0.14em] text-slate-300/75">
                      Campaign Name
                    </label>
                    <input
                      value={draftCampaignName}
                      onChange={(event) => setDraftCampaignName(event.target.value)}
                      className="form-input mt-1"
                      placeholder="Q2 SaaS Founder Outreach"
                    />

                    <label className="mt-3 block text-xs uppercase tracking-[0.14em] text-slate-300/75">
                      AI Instruction
                    </label>
                    <textarea
                      value={draftAiInstruction}
                      onChange={(event) => setDraftAiInstruction(event.target.value)}
                      rows={5}
                      className="form-input mt-1 resize-y"
                      placeholder="Use consultative tone, mention one pain point, one proof point, and one CTA."
                    />

                    <div className="mt-3 rounded-xl border border-slate-300/15 bg-slate-950/45 p-3">
                      <p className="text-xs uppercase tracking-[0.14em] text-slate-300/70">Queued Lists</p>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {queuedListIds.length === 0 ? (
                          <span className="text-xs text-slate-300/75">No lists queued yet.</span>
                        ) : (
                          queuedListIds.map((listId) => {
                            const list = lists.find((item) => item.id === listId);
                            if (!list) {
                              return null;
                            }

                            return (
                              <button
                                key={list.id}
                                type="button"
                                onClick={() => removeQueuedList(list.id)}
                                className="inline-flex items-center gap-1 rounded-full border border-sky-300/40 bg-sky-500/15 px-2 py-0.5 text-xs text-sky-100 transition hover:bg-sky-500/30"
                                aria-label={`Remove ${list.name} from queued lists`}
                                title={`Remove ${list.name}`}
                              >
                                {list.name}
                                <X className="h-3 w-3" />
                              </button>
                            );
                          })
                        )}
                      </div>
                    </div>

                    <button
                      type="submit"
                      className="mt-4 inline-flex items-center gap-2 rounded-xl border border-emerald-300/55 bg-emerald-500/20 px-3 py-2 text-sm font-medium text-emerald-100 transition hover:bg-emerald-500/35"
                    >
                      <CheckCircle2 className="h-4 w-4" />
                      Create Campaign
                    </button>
                  </form>

                  <div className="glass-panel rounded-2xl p-4">
                    <p className="text-sm font-semibold text-slate-100">Campaigns</p>
                    <div className="mt-3 space-y-2">
                      {campaigns.map((campaign) => {
                        const selected = campaign.id === selectedCampaignId;
                        return (
                          <button
                            key={campaign.id}
                            type="button"
                            onClick={() => setSelectedCampaignId(campaign.id)}
                            className={`w-full rounded-xl border px-3 py-2 text-left transition ${
                              selected
                                ? "border-sky-300/55 bg-sky-500/20 text-sky-100"
                                : "border-slate-300/20 bg-slate-900/40 text-slate-300 hover:bg-slate-900/60"
                            }`}
                          >
                            <p className="text-sm font-medium">{campaign.name}</p>
                            <p className="mt-0.5 text-xs text-inherit/80">
                              {campaign.listIds.length} list(s) • {campaign.runSummaries.length} run(s)
                            </p>
                          </button>
                        );
                      })}
                    </div>
                  </div>
                </div>

                <div className="glass-panel rounded-2xl p-5">
                  {selectedCampaign ? (
                    <>
                      <div className="flex flex-wrap items-start justify-between gap-3">
                        <div>
                          <p className="text-xs uppercase tracking-[0.18em] text-slate-300/70">Selected Campaign</p>
                          <h2 className="mt-1 text-xl font-semibold text-slate-50">{selectedCampaign.name}</h2>
                          <p className="mt-1 text-sm text-slate-300/85">
                            Created {formatTimestamp(selectedCampaign.createdAt)}
                          </p>
                        </div>

                        <div className="flex flex-wrap items-center gap-2">
                          <button
                            type="button"
                            onClick={clearSelectedCampaign}
                            className="inline-flex items-center gap-2 rounded-xl border border-slate-300/35 bg-slate-900/45 px-3 py-2 text-xs font-medium text-slate-200 transition hover:bg-slate-900/65"
                          >
                            <X className="h-4 w-4" />
                            Close
                          </button>

                          <button
                            type="button"
                            onClick={() => void startCampaignRun(selectedCampaign)}
                            disabled={isStartingRun || isRunActive}
                            className="inline-flex items-center gap-2 rounded-xl border border-emerald-300/55 bg-emerald-500/20 px-3 py-2 text-xs font-medium text-emerald-100 transition hover:bg-emerald-500/35 disabled:cursor-not-allowed disabled:opacity-60"
                          >
                            {isStartingRun ? (
                              <Loader2 className="h-4 w-4 animate-spin" />
                            ) : (
                              <Play className="h-4 w-4" />
                            )}
                            Run Now
                          </button>

                          <button
                            type="button"
                            onClick={() => void stopCampaignRun()}
                            disabled={!canStopRun || isStoppingRun}
                            className="inline-flex items-center gap-2 rounded-xl border border-rose-300/55 bg-rose-500/20 px-3 py-2 text-xs font-medium text-rose-100 transition hover:bg-rose-500/35 disabled:cursor-not-allowed disabled:opacity-60"
                          >
                            {isStoppingRun ? (
                              <Loader2 className="h-4 w-4 animate-spin" />
                            ) : (
                              <StopCircle className="h-4 w-4" />
                            )}
                            Stop
                          </button>
                        </div>
                      </div>

                      <div className="mt-4 flex flex-wrap items-center gap-2">
                        {[
                          { id: "contacts" as const, label: "Contacts", icon: Users },
                          { id: "retarget" as const, label: "Retarget", icon: Target },
                          { id: "settings" as const, label: "Settings", icon: Settings2 },
                          { id: "logs" as const, label: "Logs", icon: Activity },
                        ].map((tab) => {
                          const active = campaignTab === tab.id;
                          return (
                            <button
                              key={tab.id}
                              type="button"
                              onClick={() => setCampaignTab(tab.id)}
                              className={`inline-flex items-center gap-2 rounded-xl border px-3 py-1.5 text-sm transition ${
                                active
                                  ? "border-sky-300/55 bg-sky-500/20 text-sky-100"
                                  : "border-slate-300/20 bg-slate-900/40 text-slate-300 hover:bg-slate-900/60"
                              }`}
                            >
                              <tab.icon className="h-4 w-4" />
                              {tab.label}
                            </button>
                          );
                        })}
                      </div>

                      {campaignTab === "contacts" ? (
                        <div className="mt-4 space-y-4">
                          <div className="rounded-2xl border border-slate-300/15 bg-slate-950/45 p-4">
                            <p className="text-sm font-semibold text-slate-100">Attached Lists</p>
                            <p className="mt-1 text-xs text-slate-300/75">
                              Select the lists this campaign should process.
                            </p>

                            <div className="mt-3 grid gap-2 sm:grid-cols-2">
                              {lists.map((list) => {
                                const attached = selectedCampaign.listIds.includes(list.id);
                                return (
                                  <label
                                    key={list.id}
                                    className={`flex cursor-pointer items-center justify-between rounded-xl border px-3 py-2 text-sm transition ${
                                      attached
                                        ? "border-sky-300/50 bg-sky-500/15 text-sky-100"
                                        : "border-slate-300/20 bg-slate-900/45 text-slate-300"
                                    }`}
                                  >
                                    <span>{list.name}</span>
                                    <input
                                      type="checkbox"
                                      checked={attached}
                                      onChange={() => toggleCampaignList(selectedCampaign.id, list.id)}
                                      className="h-4 w-4 accent-sky-500"
                                    />
                                  </label>
                                );
                              })}
                            </div>
                          </div>

                          <div className="overflow-x-auto rounded-2xl border border-slate-300/15 bg-slate-950/45 p-4">
                            <p className="mb-3 text-sm font-semibold text-slate-100">Campaign Contacts</p>
                            <table className="min-w-full text-left text-sm">
                              <thead>
                                <tr className="border-b border-slate-300/15 text-xs uppercase tracking-[0.16em] text-slate-300/70">
                                  <th className="px-2 py-2">Company</th>
                                  <th className="px-2 py-2">Contact URL</th>
                                  <th className="px-2 py-2">Status</th>
                                  <th className="px-2 py-2">Last Result</th>
                                </tr>
                              </thead>
                              <tbody>
                                {selectedCampaignContacts.length === 0 ? (
                                  <tr>
                                    <td className="px-2 py-4 text-slate-300/70" colSpan={4}>
                                      No contacts linked. Attach one or more lists above.
                                    </td>
                                  </tr>
                                ) : (
                                  selectedCampaignContacts.map((contact) => (
                                    <tr key={contact.id} className="border-b border-slate-300/10 last:border-b-0">
                                      <td className="px-2 py-3 text-slate-100">{contact.companyName}</td>
                                      <td className="px-2 py-3 text-slate-200">{contact.contactUrl}</td>
                                      <td className="px-2 py-3">
                                        <span
                                          className={`rounded-full px-2 py-1 text-xs ${contactStatusClass(contact.status)}`}
                                        >
                                          {contact.status}
                                        </span>
                                      </td>
                                      <td className="px-2 py-3 text-xs text-slate-300/90">
                                        {contact.lastResult
                                          ? `${contact.lastResult.confirmation || "-"} (${formatCurrency(contact.lastResult.costUsd)})`
                                          : "-"}
                                      </td>
                                    </tr>
                                  ))
                                )}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      ) : null}

                      {campaignTab === "retarget" ? (
                        <div className="mt-4 rounded-2xl border border-slate-300/15 bg-slate-950/45 p-4">
                          <p className="text-sm font-semibold text-slate-100">Retarget Steps</p>
                          <p className="mt-1 text-xs text-slate-300/75">
                            Follow-ups automatically trigger for non-submitted contacts.
                          </p>

                          <div className="mt-3 space-y-3">
                            {selectedCampaign.steps.map((step) => (
                              <div
                                key={step.id}
                                className="rounded-xl border border-slate-300/20 bg-slate-900/45 p-3"
                              >
                                <div className="flex flex-wrap items-center justify-between gap-3">
                                  <div>
                                    <p className="text-sm font-medium text-slate-100">{step.title}</p>
                                    <p className="text-xs text-slate-300/80">
                                      Runs after {step.daysAfter} day(s) for pending leads.
                                    </p>
                                  </div>

                                  <div className="flex items-center gap-3">
                                    <label className="text-xs text-slate-300/90">Delay (days)</label>
                                    <input
                                      type="number"
                                      min={1}
                                      value={step.daysAfter}
                                      aria-label={`Delay in days for ${step.title}`}
                                      title={`Delay in days for ${step.title}`}
                                      onChange={(event) =>
                                        updateRetargetStepDelay(
                                          selectedCampaign.id,
                                          step.id,
                                          event.target.value,
                                        )
                                      }
                                      className="form-input w-20"
                                    />
                                    <button
                                      type="button"
                                      onClick={() => toggleRetargetStep(selectedCampaign.id, step.id)}
                                      className={`rounded-lg border px-3 py-1.5 text-xs font-medium transition ${
                                        step.enabled
                                          ? "border-emerald-300/55 bg-emerald-500/20 text-emerald-100"
                                          : "border-slate-300/25 bg-slate-900/45 text-slate-300"
                                      }`}
                                    >
                                      {step.enabled ? "Enabled" : "Disabled"}
                                    </button>
                                  </div>
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      ) : null}

                      {campaignTab === "settings" ? (
                        <div className="mt-4 space-y-4">
                          <div className="rounded-2xl border border-slate-300/15 bg-slate-950/45 p-4">
                            <p className="text-sm font-semibold text-slate-100">Campaign Settings</p>

                            <label className="mt-3 block text-xs uppercase tracking-[0.14em] text-slate-300/75">
                              Max Daily Submissions
                            </label>
                            <input
                              type="number"
                              min={1}
                              value={selectedCampaign.maxDailySubmissions}
                              aria-label="Max daily submissions"
                              title="Max daily submissions"
                              onChange={(event) =>
                                updateCampaignDailyLimit(selectedCampaign.id, event.target.value)
                              }
                              className="form-input mt-1 w-52"
                            />
                            <p className="mt-2 text-xs text-slate-300/80">
                              This is enforced by backend safeguards before and during a run.
                            </p>
                          </div>

                          <div className="rounded-2xl border border-slate-300/15 bg-slate-950/45 p-4">
                            <p className="text-sm font-semibold text-slate-100">AI Instruction</p>
                            <p className="mt-2 whitespace-pre-wrap text-sm text-slate-200/90">
                              {selectedCampaign.aiInstruction}
                            </p>
                            <p className="mt-3 text-xs text-slate-300/75">
                              One AI call is made per form submission attempt.
                            </p>
                          </div>
                        </div>
                      ) : null}

                      {campaignTab === "logs" ? (
                        <div className="mt-4 space-y-4">
                          <div className="rounded-2xl border border-slate-300/15 bg-slate-950/45 p-4">
                            <p className="text-sm font-semibold text-slate-100">Campaign Level Tracking</p>
                            {selectedCampaignRun ? (
                              <div className="mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                                <div className="rounded-xl border border-slate-300/15 bg-slate-900/45 p-3 text-sm">
                                  <p className="text-slate-300/80">Run ID</p>
                                  <p className="mt-1 break-all text-slate-100">{selectedCampaignRun.runId}</p>
                                </div>
                                <div className="rounded-xl border border-slate-300/15 bg-slate-900/45 p-3 text-sm">
                                  <p className="text-slate-300/80">Progress</p>
                                  <p className="mt-1 text-slate-100">
                                    {selectedCampaignRun.processedLeads}/{selectedCampaignRun.totalLeads}
                                  </p>
                                </div>
                                <div className="rounded-xl border border-slate-300/15 bg-slate-900/45 p-3 text-sm">
                                  <p className="text-slate-300/80">Duplicates Skipped</p>
                                  <p className="mt-1 text-slate-100">{selectedCampaignRun.duplicatesSkipped}</p>
                                </div>
                                <div className="rounded-xl border border-slate-300/15 bg-slate-900/45 p-3 text-sm">
                                  <p className="text-slate-300/80">Captcha Used</p>
                                  <p className="mt-1 text-slate-100">
                                    {selectedCampaignRun.captchaCreditsUsedToday}/{selectedCampaignRun.captchaCreditsLimit}
                                  </p>
                                </div>
                                <div className="rounded-xl border border-slate-300/15 bg-slate-900/45 p-3 text-sm">
                                  <p className="text-slate-300/80">Captcha Remaining</p>
                                  <p className="mt-1 text-slate-100">{selectedCampaignRun.captchaCreditsRemaining}</p>
                                </div>
                                <div className="rounded-xl border border-slate-300/15 bg-slate-900/45 p-3 text-sm">
                                  <p className="text-slate-300/80">Status</p>
                                  <span
                                    className={`mt-1 inline-block rounded-full px-2 py-1 text-xs ${runStatusClass(
                                      selectedCampaignRun.status,
                                    )}`}
                                  >
                                    {selectedCampaignRun.status}
                                  </span>
                                </div>
                              </div>
                            ) : selectedCampaignLatestRun ? (
                              <div className="mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                                <div className="rounded-xl border border-slate-300/15 bg-slate-900/45 p-3 text-sm">
                                  <p className="text-slate-300/80">Run ID</p>
                                  <p className="mt-1 break-all text-slate-100">{selectedCampaignLatestRun.runId}</p>
                                </div>
                                <div className="rounded-xl border border-slate-300/15 bg-slate-900/45 p-3 text-sm">
                                  <p className="text-slate-300/80">Processed</p>
                                  <p className="mt-1 text-slate-100">
                                    {selectedCampaignLatestRun.processedLeads}/{selectedCampaignLatestRun.totalLeads}
                                  </p>
                                </div>
                                <div className="rounded-xl border border-slate-300/15 bg-slate-900/45 p-3 text-sm">
                                  <p className="text-slate-300/80">Duplicates Skipped</p>
                                  <p className="mt-1 text-slate-100">{selectedCampaignLatestRun.duplicatesSkipped}</p>
                                </div>
                                <div className="rounded-xl border border-slate-300/15 bg-slate-900/45 p-3 text-sm">
                                  <p className="text-slate-300/80">Captcha Used</p>
                                  <p className="mt-1 text-slate-100">
                                    {selectedCampaignLatestRun.captchaCreditsUsedToday}/{selectedCampaignLatestRun.captchaCreditsLimit}
                                  </p>
                                </div>
                                <div className="rounded-xl border border-slate-300/15 bg-slate-900/45 p-3 text-sm">
                                  <p className="text-slate-300/80">Captcha Remaining</p>
                                  <p className="mt-1 text-slate-100">
                                    {selectedCampaignLatestRun.captchaCreditsRemaining}
                                  </p>
                                </div>
                                <div className="rounded-xl border border-slate-300/15 bg-slate-900/45 p-3 text-sm">
                                  <p className="text-slate-300/80">Status</p>
                                  <span
                                    className={`mt-1 inline-block rounded-full px-2 py-1 text-xs ${runStatusClass(
                                      selectedCampaignLatestRun.status,
                                    )}`}
                                  >
                                    {selectedCampaignLatestRun.status}
                                  </span>
                                </div>
                              </div>
                            ) : (
                              <p className="mt-2 text-sm text-slate-300/75">
                                No run logs yet for this campaign.
                              </p>
                            )}
                          </div>

                          <div className="terminal-shell rounded-2xl p-4">
                            <p className="text-sm font-semibold text-slate-100">Logs</p>
                            <div className="mt-3 max-h-[320px] overflow-auto rounded-xl border border-slate-300/15 bg-slate-950/70 p-3 text-xs text-slate-200">
                              {(selectedCampaignRun?.logs ?? selectedCampaignLatestRun?.logsTail ?? []).length === 0 ? (
                                <p className="text-slate-300/70">No logs available.</p>
                              ) : (
                                (selectedCampaignRun?.logs ?? selectedCampaignLatestRun?.logsTail ?? []).map(
                                  (line, index) => (
                                    <p key={`${line.slice(0, 20)}-${index}`} className="font-mono">
                                      {line}
                                    </p>
                                  ),
                                )
                              )}
                            </div>
                          </div>
                        </div>
                      ) : null}
                    </>
                  ) : (
                    <div className="rounded-2xl border border-slate-300/20 bg-slate-900/45 p-6 text-center text-slate-300/85">
                      No campaign selected.
                    </div>
                  )}
                </div>
              </section>
            ) : null}
          </main>
        </div>
      </div>

      {showAddContactsModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4">
          <div className="glass-panel w-full max-w-2xl rounded-2xl p-5">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-xs uppercase tracking-[0.18em] text-slate-300/70">Add Contacts</p>
                <h3 className="mt-1 text-xl font-semibold text-slate-50">Create List and Upload CSV</h3>
                <p className="mt-1 text-sm text-slate-300/80">
                  Step {addContactsStep} of 2: enter list name, then upload contacts CSV.
                </p>
              </div>

              <button
                type="button"
                onClick={closeAddContactsModal}
                aria-label="Close add contacts dialog"
                title="Close add contacts dialog"
                className="rounded-lg p-1 text-slate-200 transition hover:bg-slate-800/55"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            {addContactsStep === 1 ? (
              <div className="mt-4 rounded-2xl border border-slate-300/20 bg-slate-900/45 p-4">
                <label className="text-xs uppercase tracking-[0.14em] text-slate-300/75">List Name</label>
                <input
                  value={newListName}
                  onChange={(event) => setNewListName(event.target.value)}
                  className="form-input mt-2"
                  placeholder="April SaaS Prospects"
                />

                <div className="mt-4 flex flex-wrap gap-2">
                  <button
                    type="button"
                    onClick={handleNextAddContactsStep}
                    className="inline-flex items-center gap-2 rounded-xl border border-sky-300/55 bg-sky-500/20 px-3 py-2 text-sm font-medium text-sky-100 transition hover:bg-sky-500/35"
                  >
                    <UploadCloud className="h-4 w-4" />
                    Continue to Upload
                  </button>

                  <button
                    type="button"
                    onClick={downloadCsvTemplate}
                    className="inline-flex items-center gap-2 rounded-xl border border-slate-300/35 bg-slate-900/45 px-3 py-2 text-sm text-slate-200 transition hover:bg-slate-900/65"
                  >
                    Download Template
                  </button>
                </div>
              </div>
            ) : null}

            {addContactsStep === 2 ? (
              <div className="mt-4 rounded-2xl border border-slate-300/20 bg-slate-900/45 p-4">
                <p className="text-sm text-slate-200">
                  Upload CSV into list: <span className="font-semibold">{newListName}</span>
                </p>

                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv,text/csv"
                  aria-label="Upload contacts CSV"
                  title="Upload contacts CSV"
                  onChange={handleCsvUpload}
                  disabled={isParsingCsv}
                  className="mt-3 block w-full rounded-xl border border-slate-300/30 bg-slate-950/45 p-3 text-sm text-slate-200 file:mr-3 file:rounded-lg file:border-0 file:bg-slate-700/80 file:px-3 file:py-1.5 file:text-slate-100"
                />

                <div className="mt-3 flex flex-wrap gap-2">
                  <button
                    type="button"
                    onClick={() => setAddContactsStep(1)}
                    className="rounded-xl border border-slate-300/35 bg-slate-900/45 px-3 py-2 text-sm text-slate-200 transition hover:bg-slate-900/65"
                  >
                    Back
                  </button>

                  <button
                    type="button"
                    onClick={downloadCsvTemplate}
                    className="rounded-xl border border-slate-300/35 bg-slate-900/45 px-3 py-2 text-sm text-slate-200 transition hover:bg-slate-900/65"
                  >
                    Download Template
                  </button>
                </div>
              </div>
            ) : null}

            {csvFeedback ? (
              <div className="mt-4 rounded-xl border border-slate-300/20 bg-slate-900/55 p-3 text-sm text-slate-200">
                <div className="flex items-start gap-2">
                  {csvFeedback.toLowerCase().includes("failed") ||
                  csvFeedback.toLowerCase().includes("no contacts") ? (
                    <AlertTriangle className="mt-0.5 h-4 w-4 text-amber-300" />
                  ) : (
                    <CheckCircle2 className="mt-0.5 h-4 w-4 text-emerald-300" />
                  )}
                  <p>{csvFeedback}</p>
                </div>
              </div>
            ) : null}

            {isParsingCsv ? (
              <div className="mt-4 inline-flex items-center gap-2 rounded-xl border border-sky-300/45 bg-sky-500/15 px-3 py-1.5 text-sm text-sky-100">
                <Loader2 className="h-4 w-4 animate-spin" />
                Processing contacts...
              </div>
            ) : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}
