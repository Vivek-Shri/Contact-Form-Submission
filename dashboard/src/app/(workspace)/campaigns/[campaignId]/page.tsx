"use client";

import React, { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { useSession } from "next-auth/react";
import { useParams, useRouter, useSearchParams } from "next/navigation";
import {
  Users,
  Activity,
  Settings,
  Plus,
  X,
  CheckCircle2,
  XCircle,
  Clock,
  AlertTriangle,
  Trash2,
  Download as DownloadIcon,
  ExternalLink,
  Heart,
  Database,
  Search,
  ChevronLeft,
  Play,
  Square,
  Terminal,
  RefreshCw,
} from "lucide-react";

import type {
  CampaignRecord,
  CampaignRunSummary,
  ContactRecord,
  OutreachRunSnapshot,
} from "@/lib/models";
import { formatDateTime, statusTone } from "@/lib/ui";

/* ─── Local List Types ─────────────────────────────────────── */
interface ListContact { companyName: string; contactUrl: string; }
interface ContactList { id: string; name: string; contacts: ListContact[]; createdAt: string; }

/* ─── Types ────────────────────────────────────────────────── */
interface CampaignContactsResponse { contacts: ContactRecord[]; }
interface CampaignRunsResponse { runs: CampaignRunSummary[]; }

const RUN_POLL_INTERVAL_MS = 2500;
type Tab = "contacts" | "activity" | "results" | "editor" | "settings";
type FilterMode = "all" | "success" | "fail" | "pending" | "warning";

function isActiveRun(status: string) {
  return ["running", "queued"].includes(status.trim().toLowerCase());
}

/* ─── Captcha Status Parser ────────────────────────────────── */
function parseCaptchaStatus(captchaStatus: string) {
  const s = (captchaStatus || "").toLowerCase();
  return {
    found: s.includes("found") || s.includes("detected") || s.includes("present"),
    solved: s.includes("solved") || s.includes("passed") || s.includes("success"),
    siteKeyNotFound: s.includes("site key not found") || s.includes("no site key") || s.includes("sitekey not"),
  };
}

/* ─── Status Icon Helper ───────────────────────────────────── */
const StatusIcon = ({ status }: { status: string | null }) => {
  if (!status) return <Clock size={14} style={{ color: "#9ca3af" }} />;
  if (status === "success") return <CheckCircle2 size={14} style={{ color: "#16a34a" }} />;
  if (status === "fail") return <XCircle size={14} style={{ color: "#dc2626" }} />;
  if (status === "warning") return <AlertTriangle size={14} style={{ color: "#d97706" }} />;
  return <Clock size={14} style={{ color: "#9ca3af" }} />;
};

/* ─── Normalize URL ────────────────────────────────────────── */
const normUrl = (url?: string) => {
  if (!url) return "";
  return url.toLowerCase().trim().replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/+$/, "");
};

export default function CampaignDetailPage() {
  const { data: session } = useSession();
  const userId = (session?.user as any)?.id || "";

  const router = useRouter();
  const params = useParams<{ campaignId: string }>();
  const campaignId = params.campaignId;

  /* ─── Core state ──────────────────────────────────────────── */
  const [campaign, setCampaign] = useState<CampaignRecord | null>(null);
  const [contacts, setContacts] = useState<ContactRecord[]>([]);
  const [runs, setRuns] = useState<CampaignRunSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  /* ─── Tab ─────────────────────────────────────────────────── */
  const searchParams = useSearchParams();
  const initialTab = searchParams.get("tab") as Tab | null;
  const [activeTab, setActiveTab] = useState<Tab>(initialTab || "contacts");

  // Sync tab if URL changes (optional but good for back/forward)
  useEffect(() => {
    const tab = searchParams.get("tab") as Tab | null;
    if (tab && ["contacts", "activity", "results", "editor", "settings"].includes(tab)) {
      setActiveTab(tab);
    }
  }, [searchParams]);

  /* ─── Run ─────────────────────────────────────────────────── */
  const [runSnapshot, setRunSnapshot] = useState<OutreachRunSnapshot | null>(null);
  const [startingRun, setStartingRun] = useState(false);
  const [stoppingRun, setStoppingRun] = useState(false);

  /* ─── Activity filter/search ──────────────────────────────── */
  const [filterMode, setFilterMode] = useState<FilterMode>("all");
  const [searchActivity, setSearchActivity] = useState("");

  /* ─── Contacts tab search ─────────────────────────────────── */
  const [searchContacts, setSearchContacts] = useState("");
  const [deletingContactId, setDeletingContactId] = useState<string | null>(null);
  const [togglingContactId, setTogglingContactId] = useState<string | null>(null);
  const [deletingAllContacts, setDeletingAllContacts] = useState(false);

  /* ─── Import from list ────────────────────────────────────── */
  const [showImportModal, setShowImportModal] = useState(false);
  const [availableLists, setAvailableLists] = useState<ContactList[]>([]);
  const [importingListId, setImportingListId] = useState<string | null>(null);

  /* ─── Detail modal ───────────────────────────────────────── */
  const [selectedDetail, setSelectedDetail] = useState<{
    contact: ContactRecord;
    result: { status: string; submitted: string; confirmationMsg: string; captchaStatus: string; contactUrl: string; estCostUsd?: number } | null;
  } | null>(null);

  /* ─── Logs ────────────────────────────────────────────────── */
  const [logs, setLogs] = useState<string[]>([]);
  const [logsLoading, setLogsLoading] = useState(false);
  const [showLogs, setShowLogs] = useState(false);
  const logsEndRef = React.useRef<HTMLDivElement>(null);

  /* ─── Add Steps modal ─────────────────────────────────────── */
  const [showStepsModal, setShowStepsModal] = useState(false);
  const [stepsLocal, setStepsLocal] = useState<CampaignRecord["steps"]>([]);
  const [savingSteps, setSavingSteps] = useState(false);
  const [expandedStepIndex, setExpandedStepIndex] = useState<number | null>(null);

  /* ─── Settings state ──────────────────────────────────────── */
  const [editName, setEditName] = useState("");
  const [editStatus, setEditStatus] = useState<CampaignRecord["status"]>("draft");
  const [editMaxDaily, setEditMaxDaily] = useState(100);
  const [editAiInstruction, setEditAiInstruction] = useState("");
  const [editSearchForForm, setEditSearchForForm] = useState(false);
  const [editBreakFlag, setEditBreakFlag] = useState(false);
  const [savingSettings, setSavingSettings] = useState(false);

  /* ─── Contact results map ─────────────────────────────────── */
  const contactResultsMap = useMemo(() => {
    const map = new Map<string, { status: string; submitted: string; confirmationMsg: string; captchaStatus: string; contactUrl: string }>();
    if (!runSnapshot?.results) return map;
    for (const res of runSnapshot.results) {
      const nUrl = normUrl(res.contactUrl);
      const rawName = (res.companyName || "").toLowerCase().trim();
      const nName = ["unknown", "n/a", "null", "undefined", ""].includes(rawName) ? "" : rawName;
      const payload = { status: res.status, submitted: res.submitted, confirmationMsg: res.confirmationMsg, captchaStatus: res.captchaStatus, contactUrl: res.contactUrl };
      if (nUrl) map.set(`url:${nUrl}`, payload);
      if (nName) map.set(`name:${nName}`, payload);
    }
    return map;
  }, [runSnapshot]);

  const getContactResult = useCallback((contact: ContactRecord) => {
    const nUrl = normUrl(contact.contactUrl);
    const rawName = (contact.companyName || "").toLowerCase().trim();
    const nName = ["unknown", "n/a", "null", "undefined", ""].includes(rawName) ? "" : rawName;
    return (nUrl ? contactResultsMap.get(`url:${nUrl}`) : null) || (nName ? contactResultsMap.get(`name:${nName}`) : null) || null;
  }, [contactResultsMap]);

  /* ─── Stats ───────────────────────────────────────────────── */
  const stats = useMemo(() => {
    let success = 0, fail = 0, warning = 0, pending = 0;
    for (const c of contacts) {
      const r = getContactResult(c);
      if (!r) pending++;
      else if (r.status === "success") success++;
      else if (r.status === "fail") fail++;
      else if (r.status === "warning") warning++;
      else pending++;
    }
    return { total: contacts.length, success, fail, warning, pending };
  }, [contacts, getContactResult]);

  /* ─── Filtered activity rows ──────────────────────────────── */
  const activityRows = useMemo(() => {
    return contacts.filter((c) => {
      const r = getContactResult(c);
      const matchesFilter =
        filterMode === "all" ? true :
        filterMode === "pending" ? !r :
        r?.status === filterMode;
      const search = searchActivity.trim().toLowerCase();
      const matchesSearch = !search ||
        c.companyName.toLowerCase().includes(search) ||
        c.contactUrl.toLowerCase().includes(search);
      return matchesFilter && matchesSearch;
    });
  }, [contacts, filterMode, searchActivity, getContactResult]);

  /* ─── Filtered contact rows ───────────────────────────────── */
  const filteredContacts = useMemo(() => {
    const s = searchContacts.trim().toLowerCase();
    if (!s) return contacts;
    return contacts.filter(c =>
      c.companyName.toLowerCase().includes(s) ||
      c.contactUrl.toLowerCase().includes(s) ||
      (c.domain || "").toLowerCase().includes(s)
    );
  }, [contacts, searchContacts]);

  /* ─── Load bundle ─────────────────────────────────────────── */
  const loadCampaignBundle = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [cRes, coRes, rRes] = await Promise.all([
        fetch(`/api/campaigns/${campaignId}`, { cache: "no-store" }),
        fetch(`/api/campaigns/${campaignId}/contacts`, { cache: "no-store" }),
        fetch(`/api/campaigns/${campaignId}/runs?limit=50`, { cache: "no-store" }),
      ]);
      const cPayload = await cRes.json() as CampaignRecord | { error?: string };
      const coPayload = await coRes.json() as CampaignContactsResponse | { error?: string };
      const rPayload = await rRes.json() as CampaignRunsResponse | { error?: string };

      if (!cRes.ok || !coRes.ok || !rRes.ok) {
        setError(("error" in cPayload && cPayload.error) || ("error" in coPayload && coPayload.error) || "Unable to load campaign.");
        return;
      }
      const cData = cPayload as CampaignRecord;
      const coData = coPayload as CampaignContactsResponse;
      const rData = rPayload as CampaignRunsResponse;
      setCampaign(cData);
      setContacts(coData.contacts ?? []);
      setRuns(rData.runs ?? []);
      setEditName(cData.name);
      setEditStatus(cData.status);
      setEditMaxDaily(cData.maxDailySubmissions);
      setEditAiInstruction(cData.aiInstruction || "");
      setEditSearchForForm(cData.searchForForm || false);
      setEditBreakFlag(cData.breakFlag || false);
      setStepsLocal(Array.isArray(cData.steps) ? cData.steps : []);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load campaign.");
    } finally {
      setLoading(false);
    }
  }, [campaignId]);

  /* ─── Fetch logs ───────────────────────────────────────────── */
  const fetchLogs = useCallback(async () => {
    setLogsLoading(true);
    try {
      const runId = runSnapshot?.runId;
      const url = runId ? `/api/outreach/logs?tail=300&run_id=${encodeURIComponent(runId)}` : `/api/outreach/logs?tail=300`;
      const res = await fetch(url, { cache: "no-store" });
      if (res.ok) {
        const data = await res.json() as { lines?: string[] };
        setLogs(data.lines ?? []);
        setTimeout(() => logsEndRef.current?.scrollIntoView({ behavior: "smooth" }), 50);
      }
    } catch { /* ignore */ }
    finally { setLogsLoading(false); }
  }, [runSnapshot?.runId]);

  useEffect(() => {
    if (!showLogs) return;
    void fetchLogs();
    if (!runSnapshot || !isActiveRun(runSnapshot.status)) return;
    const timer = globalThis.setInterval(() => { void fetchLogs(); }, 3000);
    return () => globalThis.clearInterval(timer);
  }, [showLogs, fetchLogs, runSnapshot]);

  useEffect(() => { void loadCampaignBundle(); }, [loadCampaignBundle]);

  /* ─── Restore run snapshot ────────────────────────────────── */
  useEffect(() => {
    if (!userId) return;
    try {
      const saved = localStorage.getItem(`run-snapshot-${userId}-${campaignId}`);
      if (saved) {
        const snap = JSON.parse(saved) as OutreachRunSnapshot;
        if (snap?.runId) setRunSnapshot(snap);
      }
    } catch { /* ignore */ }
  }, [campaignId, userId]);

  useEffect(() => {
    if (!runSnapshot || !userId) return;
    try { localStorage.setItem(`run-snapshot-${userId}-${campaignId}`, JSON.stringify(runSnapshot)); }
    catch { /* ignore */ }
  }, [runSnapshot, campaignId, userId]);

  /* ─── Poll active run ─────────────────────────────────────── */
  useEffect(() => {
    if (!runSnapshot || !isActiveRun(runSnapshot.status)) return;
    const timer = globalThis.setInterval(async () => {
      try {
        const res = await fetch(`/api/outreach/run?runId=${encodeURIComponent(runSnapshot.runId)}`, { cache: "no-store" });
        if (res.status === 404) {
          // Run no longer exists on backend — stop polling
          setRunSnapshot(prev => prev ? { ...prev, status: "completed" as OutreachRunSnapshot["status"] } : prev);
          void loadCampaignBundle();
          return;
        }
        const payload = await res.json() as OutreachRunSnapshot | { error?: string };
        if (!res.ok || !("runId" in payload)) return;
        setRunSnapshot(payload);
        if (!isActiveRun(payload.status)) void loadCampaignBundle();
      } catch { /* keep stable */ }
    }, RUN_POLL_INTERVAL_MS);
    return () => globalThis.clearInterval(timer);
  }, [loadCampaignBundle, runSnapshot]);

  /* ─── Actions ─────────────────────────────────────────────── */
  const startRun = useCallback(async () => {
    if (!campaign) return;
    if (contacts.length === 0) { setMessage("Add contacts before starting a run."); return; }
    setStartingRun(true);
    setMessage("");
    try {
      const res = await fetch("/api/outreach/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          resume: true,
          persona: { id: campaign.id, title: campaign.name, aiInstruction: campaign.aiInstruction, maxDailySubmissions: campaign.maxDailySubmissions },
          leads: contacts.map(c => ({ companyName: c.companyName, contactUrl: c.contactUrl })),
        }),
      });
      const payload = await res.json() as OutreachRunSnapshot | { error?: string };
      if (!res.ok || !("runId" in payload)) { setMessage(("error" in payload && payload.error) || "Unable to start run."); return; }
      setRunSnapshot(payload);
      setMessage(`Run ${payload.runId} started.`);
      setActiveTab("activity");
      await loadCampaignBundle();
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Unable to start run.");
    } finally { setStartingRun(false); }
  }, [campaign, contacts, loadCampaignBundle]);

  const stopRun = useCallback(async () => {
    if (!runSnapshot) return;
    setStoppingRun(true);
    setMessage("");
    try {
      const res = await fetch("/api/outreach/run/stop", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ runId: runSnapshot.runId }),
      });
      const payload = await res.json() as OutreachRunSnapshot | { error?: string };
      if (!res.ok || !("runId" in payload)) { setMessage(("error" in payload && payload.error) || "Unable to stop run."); return; }
      setRunSnapshot(payload);
      setMessage(`Stopped run ${payload.runId}.`);
      await loadCampaignBundle();
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Unable to stop run.");
    } finally { setStoppingRun(false); }
  }, [loadCampaignBundle, runSnapshot]);

  const deleteContact = useCallback(async (contactId: string) => {
    setDeletingContactId(contactId);
    try {
      const res = await fetch(`/api/campaigns/${campaignId}/contacts/${contactId}`, { method: "DELETE" });
      const payload = await res.json() as { error?: string };
      if (!res.ok) { setMessage(payload.error || "Unable to delete."); return; }
      setContacts(prev => prev.filter(c => c.id !== contactId));
      setCampaign(prev => prev ? { ...prev, contactCount: Math.max(0, prev.contactCount - 1) } : prev);
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Unable to delete.");
    } finally { setDeletingContactId(null); }
  }, [campaignId]);

  const deleteAllContacts = useCallback(async () => {
    if (!confirm("Are you sure you want to delete ALL contacts in this campaign? This cannot be undone.")) return;
    setDeletingAllContacts(true);
    setMessage("");
    try {
      const res = await fetch(`/api/campaigns/${campaignId}/contacts`, { method: "DELETE" });
      const payload = await res.json() as { error?: string };
      if (!res.ok) { setMessage(payload.error || "Unable to delete all contacts."); return; }
      setContacts([]);
      setCampaign(prev => prev ? { ...prev, contactCount: 0 } : prev);
      setMessage("Successfully deleted all contacts.");
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Unable to delete all contacts.");
    } finally { setDeletingAllContacts(false); }
  }, [campaignId]);

  const toggleInterested = useCallback(async (contact: ContactRecord) => {
    setTogglingContactId(contact.id);
    try {
      const res = await fetch(`/api/campaigns/${campaignId}/contacts/${contact.id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ isInterested: !contact.isInterested }),
      });
      const payload = await res.json() as ContactRecord | { error?: string };
      if (!res.ok || !("id" in payload)) { setMessage(("error" in payload && payload.error) || "Update failed."); return; }
      setContacts(prev => prev.map(c => c.id === contact.id ? { ...c, isInterested: !c.isInterested } : c));
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Update failed.");
    } finally { setTogglingContactId(null); }
  }, [campaignId]);

  const importFromList = useCallback(async (list: ContactList) => {
    if (!userId) return;
    setImportingListId(list.id);
    setMessage("");
    try {
      const existingUrls = new Set(contacts.map(c => normUrl(c.contactUrl)));
      const duplicates = list.contacts.filter(item => existingUrls.has(normUrl(item.contactUrl)));
      if (duplicates.length > 0) {
        if (!window.confirm(`Warning: ${duplicates.length} duplicate URLs are already in this campaign. Want to proceed?`)) {
          setImportingListId(null);
          return;
        }
      }

      const payload = {
        contacts: list.contacts.map(item => ({
          companyName: item.companyName || "Unknown",
          contactUrl: item.contactUrl
        }))
      };

      const res = await fetch(`/api/campaigns/${campaign?.id || campaignId}/contacts/bulk`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!res.ok) throw new Error("Bulk import failed returned " + res.status);
      
      setMessage(`Successfully imported ${list.contacts.length} contacts from "${list.name}".`);
      setShowImportModal(false);
      await loadCampaignBundle();
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Import failed.");
    } finally { setImportingListId(null); }
  }, [campaignId, loadCampaignBundle]);

  const saveSettings = useCallback(async () => {
    if (!campaign) return;
    if (!editName.trim()) { setMessage("Campaign name is required."); return; }
    setSavingSettings(true);
    setMessage("");
    try {
      const body = {
        name: editName,
        status: editStatus,
        maxDailySubmissions: editMaxDaily,
        aiInstruction: editAiInstruction,
        searchForForm: editSearchForForm,
        breakFlag: editBreakFlag,
        steps: stepsLocal,
      };
      const res = await fetch(`/api/campaigns/${campaign.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const payload = await res.json() as CampaignRecord | { error?: string };
      if (!res.ok || !("id" in payload)) { setMessage(("error" in payload && payload.error) || "Unable to update."); return; }
      setCampaign(payload);
      setMessage("Settings saved successfully.");
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Unable to update.");
    } finally { setSavingSettings(false); }
  }, [campaign, editName, editStatus, editMaxDaily, editAiInstruction, editSearchForForm, editBreakFlag, stepsLocal]);

  const saveSteps = useCallback(async () => {
    if (!campaign) return;
    setSavingSteps(true);
    try {
      // Ensure we only send valid step objects
      const cleanSteps = stepsLocal.map(s => {
        if (typeof s === "string") return { id: Date.now().toString(), aiInstruction: s, daySequence: 1, timeOfDay: "09:00", type: "immediate", enabled: true };
        return { id: s.id || Date.now().toString(), aiInstruction: s.aiInstruction || "", daySequence: s.daySequence || 1, timeOfDay: s.timeOfDay || "09:00", type: s.type || "immediate", enabled: s.enabled !== false };
      });
      const res = await fetch(`/api/campaigns/${campaign.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ steps: cleanSteps }),
      });
      const payload = await res.json() as CampaignRecord | { error?: string };
      if (!res.ok || !("id" in payload)) { setMessage(("error" in payload && payload.error) || "Unable to save steps."); return; }
      setCampaign(payload);
      setStepsLocal(Array.isArray(payload.steps) ? payload.steps : []);
      setMessage("Steps saved.");
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Unable to save steps.");
    } finally { setSavingSteps(false); }
  }, [campaign, stepsLocal]);

  const exportResultsToCsv = useCallback(() => {
    if (!runSnapshot?.results?.length) { alert("No results to export."); return; }
    const headers = ["Company", "Contact URL", "Status", "Captcha Status", "Submitted", "Form Found", "Confirmation"];
    const rows = runSnapshot.results.map(r => [
      `"${(r.companyName || "").replace(/"/g, '""')}"`,
      `"${(r.contactUrl || "").replace(/"/g, '""')}"`,
      `"${r.status || ""}"`,
      `"${(r.captchaStatus || "").replace(/"/g, '""')}"`,
      `"${r.submitted || ""}"`,
      `"${(r.captchaStatus || "").toLowerCase().includes("found") ? "Yes" : "No"}"`,
      `"${(r.confirmationMsg || "").replace(/"/g, '""')}"`,
    ]);
    const csv = "data:text/csv;charset=utf-8," + [headers.join(","), ...rows.map(r => r.join(","))].join("\n");
    const link = document.createElement("a");
    link.href = encodeURI(csv);
    link.download = `run-${runSnapshot.runId}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }, [runSnapshot]);

  if (loading) return <p className="panel-muted" style={{ padding: "2rem" }}>Loading campaign...</p>;
  if (error || !campaign) return <p className="panel-error" style={{ padding: "2rem" }}>{error || "Campaign not found."}</p>;

  /* Load lists for import */
  const refreshLists = useCallback(async () => {
    try {
      const res = await fetch("/api/contact-lists");
      if (res.ok) {
        const data = await res.json();
        setAvailableLists(data.lists || []);
      }
    } catch (err) {
      console.error("Failed to fetch lists", err);
    }
  }, []);

  useEffect(() => {
    void refreshLists();
  }, [refreshLists]);

  const runActive = !!runSnapshot && isActiveRun(runSnapshot.status);

  return (
    <div className="page-stack">
      {/* ─── Company Detail Modal ─────────────────────────────── */}
      {selectedDetail && (() => {
        const { contact, result } = selectedDetail;
        const captcha = parseCaptchaStatus(result?.captchaStatus || "");
        const isSuccess = result?.status === "success";
        const isFail = result?.status === "fail";
        const isWarning = result?.status === "warning";
        return (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4" onClick={() => setSelectedDetail(null)}>
            <div className="w-full max-w-lg bg-white rounded-2xl shadow-2xl overflow-hidden" onClick={e => e.stopPropagation()}>
              {/* Header */}
              <div className={`px-6 py-5 ${isSuccess ? "bg-green-50 border-b border-green-100" : isFail ? "bg-red-50 border-b border-red-100" : isWarning ? "bg-amber-50 border-b border-amber-100" : "bg-gray-50 border-b border-gray-100"}`}>
                <div className="flex items-start justify-between gap-3">
                  <div className="flex items-center gap-3">
                    <div className={`w-10 h-10 rounded-full flex items-center justify-center shrink-0 ${isSuccess ? "bg-green-100" : isFail ? "bg-red-100" : isWarning ? "bg-amber-100" : "bg-gray-100"}`}>
                      <StatusIcon status={result?.status ?? null} />
                    </div>
                    <div>
                      <h3 className="font-bold text-gray-900 text-base leading-tight">{contact.companyName}</h3>
                      <a href={contact.contactUrl} target="_blank" rel="noreferrer" className="text-xs text-blue-600 hover:underline flex items-center gap-1 mt-0.5">
                        {contact.contactUrl.length > 50 ? contact.contactUrl.slice(0, 50) + "…" : contact.contactUrl}
                        <ExternalLink size={10} />
                      </a>
                    </div>
                  </div>
                  <button type="button" onClick={() => setSelectedDetail(null)} className="p-1.5 text-gray-400 hover:text-gray-600 hover:bg-white/60 rounded-lg transition-colors shrink-0">
                    <X size={18} />
                  </button>
                </div>
              </div>

              {/* Body */}
              <div className="p-6 space-y-4">
                {/* Status + Submitted row */}
                <div className="grid grid-cols-2 gap-3">
                  <div className="bg-gray-50 rounded-xl p-3">
                    <p className="text-xs text-gray-400 uppercase tracking-wide font-medium mb-1">Status</p>
                    <span className={`text-sm font-semibold px-2.5 py-0.5 rounded-full ${isSuccess ? "bg-green-100 text-green-700" : isFail ? "bg-red-100 text-red-700" : isWarning ? "bg-amber-100 text-amber-700" : "bg-gray-100 text-gray-500"}`}>
                      {result?.status ?? "Pending"}
                    </span>
                  </div>
                  <div className="bg-gray-50 rounded-xl p-3">
                    <p className="text-xs text-gray-400 uppercase tracking-wide font-medium mb-1">Form Submitted</p>
                    <span className={`text-sm font-semibold ${result?.submitted === "Yes" ? "text-green-600" : "text-red-500"}`}>
                      {result ? (result.submitted === "Yes" ? "✅ Yes" : "❌ No") : "—"}
                    </span>
                  </div>
                </div>

                {/* Captcha row */}
                <div className="grid grid-cols-3 gap-3">
                  <div className="bg-gray-50 rounded-xl p-3 text-center">
                    <p className="text-xs text-gray-400 uppercase tracking-wide font-medium mb-1">Captcha Found</p>
                    <span className="text-lg">{result ? (captcha.found ? "✅" : "❌") : "—"}</span>
                  </div>
                  <div className="bg-gray-50 rounded-xl p-3 text-center">
                    <p className="text-xs text-gray-400 uppercase tracking-wide font-medium mb-1">Captcha Solved</p>
                    <span className="text-lg">{result ? (captcha.solved ? "✅" : "❌") : "—"}</span>
                  </div>
                  <div className="bg-gray-50 rounded-xl p-3 text-center">
                    <p className="text-xs text-gray-400 uppercase tracking-wide font-medium mb-1">Site Key ⚠️</p>
                    <span className="text-lg">{result ? (captcha.siteKeyNotFound ? "⚠️" : "—") : "—"}</span>
                  </div>
                </div>

                {/* Confirmation message */}
                {result?.confirmationMsg && (
                  <div className="bg-blue-50 border border-blue-100 rounded-xl p-4">
                    <p className="text-xs text-blue-500 uppercase tracking-wide font-medium mb-1.5">Confirmation / Details</p>
                    <p className="text-sm text-blue-900 leading-relaxed break-words">{result.confirmationMsg}</p>
                  </div>
                )}

                {/* Captcha status raw */}
                {result?.captchaStatus && result.captchaStatus !== "none" && (
                  <div className="bg-purple-50 border border-purple-100 rounded-xl p-4">
                    <p className="text-xs text-purple-500 uppercase tracking-wide font-medium mb-1.5">Captcha Detail</p>
                    <p className="text-xs text-purple-800 font-mono break-words">{result.captchaStatus}</p>
                  </div>
                )}

                {/* Cost */}
                {result?.estCostUsd != null && result.estCostUsd > 0 && (
                  <div className="flex items-center justify-between px-4 py-2.5 bg-gray-50 rounded-xl">
                    <span className="text-sm text-gray-500">Estimated AI Cost</span>
                    <span className="text-sm font-semibold text-gray-800">${result.estCostUsd.toFixed(5)}</span>
                  </div>
                )}

                {/* Added date */}
                <div className="flex items-center justify-between px-4 py-2.5 bg-gray-50 rounded-xl">
                  <span className="text-sm text-gray-500">Added to Campaign</span>
                  <span className="text-sm text-gray-700">{formatDateTime(contact.createdAt)}</span>
                </div>
              </div>

              {/* Footer */}
              <div className="px-6 pb-5 flex justify-end gap-2">
                <a
                  href={contact.contactUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="flex items-center gap-1.5 px-4 py-2 text-sm font-medium text-blue-700 bg-blue-50 hover:bg-blue-100 rounded-lg transition-colors"
                >
                  <ExternalLink size={14} /> Open Site
                </a>
                <button
                  type="button"
                  onClick={() => setSelectedDetail(null)}
                  className="px-4 py-2 text-sm font-medium text-gray-600 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        );
      })()}

      {/* ─── Add Steps Modal (REMOVED) ────────────────────────── */}

      {/* ─── Import from List Modal ───────────────────────────── */}
      {showImportModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm p-4">
          <div className="w-full max-w-lg bg-white rounded-2xl shadow-xl flex flex-col max-h-[80vh]">
            <div className="flex justify-between items-center px-6 py-4 border-b border-gray-100 shrink-0">
              <h3 className="text-lg font-semibold text-gray-900">Import from Contact List</h3>
              <button onClick={() => setShowImportModal(false)} className="p-1 text-gray-400 hover:text-gray-600"><X size={20} /></button>
            </div>
            <div className="p-6 overflow-y-auto flex-1">
              {availableLists.length === 0 ? (
                <div className="text-center py-8">
                  <Database size={40} className="mx-auto text-gray-300 mb-3" strokeWidth={1} />
                  <p className="text-gray-500 text-sm">No contact lists found. Create one from the Contact Lists page.</p>
                </div>
              ) : (
                <div className="space-y-2">
                  {availableLists.map(list => (
                    <div key={list.id} className="flex items-center justify-between p-3 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors">
                      <div>
                        <p className="font-medium text-gray-900 text-sm">{list.name}</p>
                        <p className="text-xs text-gray-500">{list.contacts.length} contacts · {new Date(list.createdAt).toLocaleDateString()}</p>
                      </div>
                      <button
                        type="button"
                        onClick={() => void importFromList(list)}
                        disabled={importingListId === list.id}
                        className="px-3 py-1.5 bg-blue-600 text-white text-xs font-medium rounded-md hover:bg-blue-700 disabled:bg-gray-300 transition-colors"
                      >
                        {importingListId === list.id ? "Importing..." : "Import"}
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* ─── Header ───────────────────────────────────────────── */}
      <section className="panel">
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", flexWrap: "wrap", gap: "12px" }}>
          <div className="flex items-center gap-3">
            <button type="button" onClick={() => router.push("/campaigns")} className="p-1.5 text-gray-400 hover:text-gray-700 hover:bg-gray-100 rounded-lg transition-colors">
              <ChevronLeft size={18} />
            </button>
            <div>
              <p className="text-xs text-gray-400 uppercase tracking-wider font-medium">Campaign</p>
              <h2 className="text-xl font-bold text-gray-900">{campaign.name}</h2>
            </div>
            <span className={`status-chip ${statusTone(campaign.status)}`}>{campaign.status}</span>
          </div>
          <div className="flex items-center gap-2 flex-wrap">
            <button
              type="button"
              onClick={() => void startRun()}
              disabled={startingRun || runActive}
              className="flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 disabled:bg-gray-300 text-white rounded-lg text-sm font-medium transition-colors"
            >
              <Play size={15} /> {startingRun ? "Starting..." : (campaign.lastRun?.status === "stopped" || campaign.lastRun?.status === "error") ? "Resume Run" : "Start Run"}
            </button>
            <button
              type="button"
              onClick={() => void stopRun()}
              disabled={!runActive || stoppingRun}
              className="flex items-center gap-2 px-4 py-2 bg-red-600 hover:bg-red-700 disabled:bg-gray-300 text-white rounded-lg text-sm font-medium transition-colors"
            >
              <Square size={15} /> {stoppingRun ? "Stopping..." : "Stop Run"}
            </button>
          </div>
        </div>

        {/* Stats strip */}
        <div style={{ display: "flex", gap: "24px", marginTop: "16px", paddingTop: "16px", borderTop: "1px solid #f3f4f6", flexWrap: "wrap" }}>
          <div>
            <p className="meta-label">Contacts</p>
            <p className="text-lg font-bold text-gray-900">{campaign.contactCount}</p>
          </div>
          <div>
            <p className="meta-label">Successful</p>
            <p className="text-lg font-bold text-green-600">{stats.success}</p>
          </div>
          <div>
            <p className="meta-label">Failed</p>
            <p className="text-lg font-bold text-red-500">{stats.fail}</p>
          </div>
          <div>
            <p className="meta-label">Remaining</p>
            <p className="text-lg font-bold text-gray-600">{stats.pending + stats.warning}</p>
          </div>
          <div>
            <p className="meta-label">Daily Limit</p>
            <p className="text-lg font-bold text-blue-600">{campaign.maxDailySubmissions}</p>
          </div>
        </div>

        {/* Stopped run reminder */}
        {campaign.lastRun && (campaign.lastRun.status === "stopped" || campaign.lastRun.status === "error") && !runActive && (
          <div className="mt-4 p-3 bg-amber-50 border border-amber-200 rounded-lg flex items-start gap-3">
            <AlertTriangle className="text-amber-600 mt-0.5 shrink-0" size={16} />
            <div>
              <p className="text-sm font-medium text-amber-800">
                Run paused at company {campaign.lastRun.processedLeads} of {campaign.lastRun.totalLeads}
              </p>
              <p className="text-xs text-amber-700 mt-0.5">
                Click &quot;Resume Run&quot; at the top to automatically continue from the next company.
              </p>
            </div>
          </div>
        )}

        {/* Run progress */}
        {runSnapshot && isActiveRun(runSnapshot.status) && (
          <div style={{ marginTop: "12px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "4px" }}>
              <p className="text-xs text-gray-500">Run progress · {runSnapshot.processedLeads}/{runSnapshot.totalLeads}</p>
              <p className="text-xs font-medium text-blue-600">{runSnapshot.progress}%</p>
            </div>
            <div className="w-full bg-gray-100 rounded-full h-2 overflow-hidden">
              <div className="bg-blue-600 h-2 rounded-full transition-all duration-500" style={{ width: `${runSnapshot.progress}%` }} />
            </div>
          </div>
        )}

        {message && <p className="panel-muted mt-3 text-sm">{message}</p>}
      </section>

      {/* ─── Tabs ─────────────────────────────────────────────── */}
      <div style={{ display: "flex", gap: "4px", padding: "0 4px" }}>
        {(["contacts", "activity", "results", "editor", "settings"] as Tab[]).map((tab) => {
          const icons: Record<Tab, React.ReactNode> = {
            contacts: <Users size={15} />,
            activity: <Activity size={15} />,
            results: <Database size={15} />,
            editor: <Terminal size={15} />,
            settings: <Settings size={15} />,
          };
          const labels: Record<Tab, string> = {
            contacts: "Contacts",
            activity: "Activity",
            results: "Results",
            editor: "Editor",
            settings: "Settings",
          };
          return (
            <button
              key={tab}
              type="button"
              onClick={() => setActiveTab(tab)}
              className={`flex items-center gap-2 px-5 py-2.5 rounded-t-lg text-sm font-medium transition-colors ${
                activeTab === tab
                  ? "bg-white border border-b-0 border-gray-200 text-blue-600"
                  : "text-gray-500 hover:text-gray-800 hover:bg-gray-100"
              }`}
            >
              {icons[tab]} {labels[tab]}
            </button>
          );
        })}
      </div>

      {/* ─── CONTACTS TAB ─────────────────────────────────────── */}
      {activeTab === "contacts" && (
        <section className="panel" style={{ borderTopLeftRadius: 0 }}>
          <div className="panel-header">
            <h3 className="font-semibold text-gray-800">Campaign Contacts</h3>
            <div className="flex items-center gap-2">
              {contacts.length > 0 && (
                <button
                  type="button"
                  onClick={() => void deleteAllContacts()}
                  disabled={deletingAllContacts}
                  className="flex items-center gap-2 px-3 py-1.5 bg-red-50 text-red-600 border border-red-200 text-sm font-medium rounded-lg hover:bg-red-100 transition-colors disabled:opacity-50"
                  title="Delete all contacts in this campaign"
                >
                  <Trash2 size={14} /> {deletingAllContacts ? "Deleting..." : "Remove All"}
                </button>
              )}
              <button
                type="button"
                onClick={() => { void refreshLists(); setShowImportModal(true); }}
                className="flex items-center gap-2 px-3 py-1.5 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors"
                title="Add contacts from lists"
              >
                <Database size={14} /> Add from Lists
              </button>
            </div>
          </div>

          <div className="flex items-center gap-2 mb-4">
            <div className="relative flex-1">
              <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
              <input
                value={searchContacts}
                onChange={(e) => setSearchContacts(e.target.value)}
                className="field-input pl-8"
                placeholder="Search contacts..."
              />
            </div>
          </div>

          {filteredContacts.length === 0 ? (
            <div className="empty-state">
              <Users size={48} strokeWidth={1} />
              <h3>No contacts yet</h3>
              <p>Import contacts from your Contact Lists to get started.</p>
              <button
                type="button"
                onClick={() => { void refreshLists(); setShowImportModal(true); }}
                className="mt-3 flex items-center gap-2 px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 mx-auto transition-colors"
              >
                <Database size={14} /> Import from Lists
              </button>
            </div>
          ) : (
            <div className="table-wrap">
              <table className="clean-table">
                <thead>
                  <tr>
                    <th>Company</th>
                    <th>Domain</th>
                    <th>Contact URL</th>
                    <th>Interested</th>
                    <th>Added</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {filteredContacts.map((contact) => (
                    <tr key={contact.id}>
                      <td className="font-medium">{contact.companyName}</td>
                      <td className="text-gray-500">{contact.domain || "—"}</td>
                      <td>
                        <a href={contact.contactUrl} target="_blank" rel="noreferrer" className="table-link flex items-center gap-1">
                          <span className="truncate max-w-[200px] inline-block">{contact.contactUrl}</span>
                          <ExternalLink size={12} />
                        </a>
                      </td>
                      <td>
                        <button
                          type="button"
                          onClick={() => void toggleInterested(contact)}
                          disabled={togglingContactId === contact.id}
                          title={contact.isInterested ? "Mark as not interested" : "Mark as interested"}
                          className={`w-6 h-6 rounded border-2 flex items-center justify-center transition-colors ${
                            contact.isInterested
                              ? "border-pink-500 bg-pink-50 text-pink-600"
                              : "border-gray-300 hover:border-pink-400"
                          }`}
                        >
                          {contact.isInterested && <Heart size={12} fill="currentColor" />}
                        </button>
                      </td>
                      <td className="text-gray-400 text-xs">{formatDateTime(contact.createdAt)}</td>
                      <td>
                        <button
                          type="button"
                          className="table-delete flex items-center gap-1"
                          onClick={() => void deleteContact(contact.id)}
                          disabled={deletingContactId === contact.id}
                        >
                          <Trash2 size={12} />
                          {deletingContactId === contact.id ? "..." : "Remove"}
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      {/* ─── ACTIVITY TAB ─────────────────────────────────────── */}
      {activeTab === "activity" && (
        <section className="panel" style={{ borderTopLeftRadius: 0 }}>


          {/* Stats row */}
          <div className="flex items-center gap-4 flex-wrap mb-4">
            {(["all", "success", "fail", "warning", "pending"] as FilterMode[]).map((mode) => {
              const count = mode === "all" ? stats.total : stats[mode as keyof typeof stats] as number;
              const colors: Record<string, string> = { all: "bg-gray-100 text-gray-700", success: "bg-green-100 text-green-700", fail: "bg-red-100 text-red-700", warning: "bg-amber-100 text-amber-700", pending: "bg-gray-100 text-gray-500" };
              return (
                <button
                  key={mode}
                  type="button"
                  onClick={() => setFilterMode(mode)}
                  className={`px-4 py-2 rounded-lg text-sm font-medium transition-all border-2 ${colors[mode]} ${filterMode === mode ? "border-current opacity-100" : "border-transparent opacity-70 hover:opacity-100"}`}
                >
                  {mode.charAt(0).toUpperCase() + mode.slice(1)}: <strong>{count}</strong>
                </button>
              );
            })}
            <div className="ml-auto flex items-center gap-2">
              {runSnapshot && (
                <button type="button" onClick={exportResultsToCsv} className="button-secondary flex items-center gap-1.5 text-xs">
                  <DownloadIcon size={13} /> Export CSV
                </button>
              )}
              {runSnapshot && (
                <button
                  type="button"
                  className="button-secondary text-xs"
                  onClick={() => {
                    try { localStorage.removeItem(`run-snapshot-${campaignId}`); } catch { /* ignore */ }
                    setRunSnapshot(null);
                  }}
                >
                  Clear Results
                </button>
              )}
            </div>
          </div>

          {/* Search */}
          <div className="relative mb-4">
            <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
            <input
              value={searchActivity}
              onChange={(e) => setSearchActivity(e.target.value)}
              className="field-input pl-8"
              placeholder="Search company or URL..."
            />
          </div>

          {!runSnapshot && (
            <div className="empty-state">
              <Activity size={48} strokeWidth={1} />
              <h3>No run results yet</h3>
              <p>Start a campaign run from the header above to see activity here.</p>
            </div>
          )}

          {runSnapshot && (
            <div className="table-wrap">
              <table className="clean-table">
                <thead>
                  <tr>
                    <th></th>
                    <th>Company</th>
                    <th>Contact URL</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {activityRows.length === 0 ? (
                    <tr><td colSpan={4} className="table-empty">No results match current filter.</td></tr>
                  ) : (
                    activityRows.map((contact) => {
                      const result = getContactResult(contact);
                      return (
                        <tr key={contact.id} className={result?.status === "success" ? "bg-green-50/30" : result?.status === "fail" ? "bg-red-50/30" : ""}>
                          <td><StatusIcon status={result?.status ?? null} /></td>
                          <td className="font-medium text-gray-800">
                            {contact.companyName}
                          </td>
                          <td>
                            <a href={contact.contactUrl} target="_blank" rel="noreferrer" className="table-link flex items-center gap-1">
                              <span className="truncate max-w-[160px] inline-block">{contact.contactUrl}</span>
                              <ExternalLink size={11} />
                            </a>
                          </td>
                          <td>
                            <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${
                              result?.status === "success" ? "bg-green-100 text-green-700" :
                              result?.status === "fail" ? "bg-red-100 text-red-700" :
                              result?.status === "warning" ? "bg-amber-100 text-amber-700" :
                              "bg-gray-100 text-gray-500"
                            }`}>
                              {result?.status === "success" ? "Site successfully submit" : result?.status === "fail" ? "Fail" : result?.status ?? "Pending"}
                            </span>
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          )}

          {/* Recent Runs */}
          {runs.length > 0 && (
            <div style={{ marginTop: "24px" }}>
              <h4 className="font-semibold text-gray-700 text-sm mb-3">Past Runs</h4>
              <div className="table-wrap">
                <table className="clean-table">
                  <thead>
                    <tr><th>Run ID</th><th>Status</th><th>Total</th><th>Processed</th><th>Duplicates Skipped</th><th>Started</th><th>Finished</th></tr>
                  </thead>
                  <tbody>
                    {runs.map((run) => (
                      <tr key={run.runId}>
                        <td className="font-mono text-xs">{run.runId}</td>
                        <td><span className={`status-chip ${statusTone(run.status)}`}>{run.status}</span></td>
                        <td>{run.totalLeads}</td>
                        <td>{run.processedLeads}</td>
                        <td>{run.duplicatesSkipped}</td>
                        <td className="text-xs text-gray-500">{formatDateTime(run.startedAt)}</td>
                        <td className="text-xs text-gray-500">{run.finishedAt ? formatDateTime(run.finishedAt) : "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </section>
      )}

      {/* ─── RESULTS TAB ──────────────────────────────────────── */}
      {activeTab === "results" && (
        <section className="panel" style={{ borderTopLeftRadius: 0 }}>
          {/* ─── Logs Panel ──────────────────────────────────────── */}
          <div style={{ marginBottom: "20px" }}>
            <div className="flex items-center justify-between mb-3">
              <button
                type="button"
                onClick={() => { setShowLogs(v => !v); if (!showLogs) void fetchLogs(); }}
                className="flex items-center gap-2 text-sm font-semibold text-gray-700 hover:text-gray-900 transition-colors"
              >
                <Terminal size={15} className="text-gray-500" />
                {showLogs ? "Hide" : "Show"} Live Logs
                <span className="text-xs text-gray-400 font-normal">(raw scraper output)</span>
              </button>
              {showLogs && (
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={() => void fetchLogs()}
                    disabled={logsLoading}
                    className="flex items-center gap-1.5 text-xs text-gray-500 hover:text-gray-700 transition-colors"
                  >
                    <RefreshCw size={12} className={logsLoading ? "animate-spin" : ""} /> Refresh
                  </button>
                  <button
                    type="button"
                    onClick={() => { void navigator.clipboard.writeText(logs.join("\n")); }}
                    className="text-xs text-gray-500 hover:text-gray-700 transition-colors"
                  >
                    Copy All
                  </button>
                </div>
              )}
            </div>
            {showLogs && (
              <div
                style={{
                  background: "#0f172a",
                  borderRadius: "10px",
                  padding: "16px",
                  maxHeight: "420px",
                  overflowY: "auto",
                  fontFamily: "'Courier New', Courier, monospace",
                  fontSize: "11.5px",
                  lineHeight: "1.6",
                  border: "1px solid #1e293b",
                }}
              >
                {logs.length === 0 ? (
                  <p style={{ color: "#64748b", margin: 0 }}>
                    {logsLoading ? "Loading logs..." : "No logs available. Start a campaign run to see live output here."}
                  </p>
                ) : (
                  logs.map((line, i) => {
                    const lower = line.toLowerCase();
                    const color =
                      lower.includes("[result]") && lower.includes('"submitted": "yes"') ? "#4ade80" :
                      lower.includes("[result]") ? "#f87171" :
                      lower.includes("[limit]") ? "#fbbf24" :
                      lower.includes("[fatal]") || lower.includes("error") ? "#f87171" :
                      lower.includes("[worker") ? "#93c5fd" :
                      lower.includes("✓") || lower.includes("success") ? "#4ade80" :
                      lower.includes("[captcha") ? "#c084fc" :
                      lower.includes("[form") ? "#67e8f9" :
                      "#94a3b8";
                    return (
                      <div key={i} style={{ color, wordBreak: "break-all", marginBottom: "2px" }}>
                        {line}
                      </div>
                    );
                  })
                )}
                <div ref={logsEndRef} />
              </div>
            )}
          </div>

          {!runSnapshot && (
            <div className="empty-state">
              <Database size={48} strokeWidth={1} />
              <h3>No detailed results</h3>
              <p>Start a campaign run to see comprehensive logs and failure reasons.</p>
            </div>
          )}

          {runSnapshot && (
            <div className="table-wrap">
              <table className="clean-table">
                <thead>
                  <tr>
                    <th></th>
                    <th>Company</th>
                    <th>Contact URL</th>
                    <th>Status</th>
                    <th>Captcha Found</th>
                    <th>Captcha Solved</th>
                    <th>Site Key Not Found</th>
                    <th>Form Found</th>
                    <th>Details</th>
                  </tr>
                </thead>
                <tbody>
                  {activityRows.length === 0 ? (
                    <tr><td colSpan={9} className="table-empty">No results match current filter.</td></tr>
                  ) : (
                    activityRows.map((contact) => {
                      const result = getContactResult(contact);
                      const captcha = parseCaptchaStatus(result?.captchaStatus || "");
                      const _conf = (result?.confirmationMsg || "").toLowerCase();
                      const _cap = (result?.captchaStatus || "").toLowerCase();
                      const _negatives = ["not found", "not filled", "could not find", "no form", "form not", "unable to find", "failed to find", "no contact"];
                      const _capFoundPositive = _cap.includes("found") && !_negatives.some(n => _cap.includes(n));
                      const _confFormPositive = (_conf.includes("form") || _conf.includes("submitted") || _conf.includes("sent") || _conf.includes("message")) && !_negatives.some(n => _conf.includes(n));
                      const formFound = _capFoundPositive || _confFormPositive;
                      return (
                        <tr key={contact.id} className={result?.status === "success" ? "bg-green-50/30" : result?.status === "fail" ? "bg-red-50/30" : ""}>
                          <td><StatusIcon status={result?.status ?? null} /></td>
                          <td
                            className="font-medium text-blue-700 cursor-pointer hover:underline hover:text-blue-900 transition-colors"
                            onClick={() => setSelectedDetail({ contact, result: result ?? null })}
                            title="Click for full details"
                          >
                            {contact.companyName}
                          </td>
                          <td>
                            <a href={contact.contactUrl} target="_blank" rel="noreferrer" className="table-link flex items-center gap-1">
                              <span className="truncate max-w-[160px] inline-block">{contact.contactUrl}</span>
                              <ExternalLink size={11} />
                            </a>
                          </td>
                          <td>
                            <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${
                              result?.status === "success" ? "bg-green-100 text-green-700" :
                              result?.status === "fail" ? "bg-red-100 text-red-700" :
                              result?.status === "warning" ? "bg-amber-100 text-amber-700" :
                              "bg-gray-100 text-gray-500"
                            }`}>
                              {result?.status ?? "pending"}
                            </span>
                          </td>
                          <td className="text-center">{result ? (captcha.found ? "✅" : "❌") : "—"}</td>
                          <td className="text-center">{result ? (captcha.solved ? "✅" : "❌") : "—"}</td>
                          <td className="text-center">{result ? (captcha.siteKeyNotFound ? "⚠️" : "—") : "—"}</td>
                          <td className="text-center">{result ? (formFound ? "✅" : "❌") : "—"}</td>
                          <td className="text-xs text-gray-400 max-w-[180px] truncate" title={result?.confirmationMsg}>
                            <button
                              type="button"
                              className="text-xs text-blue-600 hover:underline"
                              onClick={() => setSelectedDetail({ contact, result: result ?? null })}
                            >
                              {result ? "View Details" : "—"}
                            </button>
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      {/* ─── EDITOR TAB ───────────────────────────────────────── */}
      {activeTab === "editor" && (
        <section className="panel" style={{ borderTopLeftRadius: 0 }}>
          <div className="flex justify-between items-center mb-4">
            <div>
              <h3 className="font-semibold text-gray-800">Campaign Editor</h3>
              <p className="text-sm text-gray-500">Configure AI steps and submission schedules.</p>
            </div>
            <button
              type="button"
              onClick={() => {
                setStepsLocal(prev => [...prev, { id: Date.now().toString(), aiInstruction: "", daySequence: 1, timeOfDay: "09:00", type: "immediate", enabled: true }]);
                setExpandedStepIndex(stepsLocal.length);
              }}
              className="flex items-center gap-2 px-3 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg text-sm font-medium transition-colors"
            >
              <Plus size={15} /> Add Step
            </button>
          </div>

          {message && <p className="text-sm mb-3 px-3 py-2 bg-blue-50 text-blue-700 border border-blue-200 rounded-lg">{message}</p>}

          <div className="space-y-2">
            {stepsLocal.length === 0 ? (
              <div className="p-8 text-center border-2 border-dashed border-gray-200 rounded-xl bg-gray-50">
                <Terminal size={32} className="mx-auto text-gray-400 mb-2" />
                <p className="text-gray-600 font-medium text-sm">No follow-up steps configured.</p>
                <p className="text-gray-400 text-xs mt-1">Click &quot;+ Add Step&quot; to configure automated follow-up messages.</p>
              </div>
            ) : (
              stepsLocal.map((step, i) => {
                const isExpanded = expandedStepIndex === i;
                const label = (typeof step === "string" ? step : step.aiInstruction || "").trim();
                const typeLabel = step.type === "normal" ? `Scheduled (Day +${step.daySequence || 1}, ${step.timeOfDay || "09:00"})` : "Immediate Send";
                return (
                  <div key={step.id || i} className={`border ${step.enabled !== false ? "border-gray-200 bg-white" : "border-gray-100 bg-gray-50 opacity-70"} rounded-xl shadow-sm transition-all overflow-hidden`}>
                    {/* ── Collapsed row header ── */}
                    <button
                      type="button"
                      onClick={() => setExpandedStepIndex(isExpanded ? null : i)}
                      className="w-full flex items-center gap-3 px-4 py-3 hover:bg-gray-50/50 transition-colors text-left"
                    >
                      <div className="w-7 h-7 rounded-full bg-blue-100 text-blue-700 text-xs font-bold flex items-center justify-center shrink-0">
                        {i + 1}
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-medium text-gray-800 truncate">{label || <span className="italic text-gray-400">No instruction yet</span>}</p>
                        <p className="text-xs text-gray-400 mt-0.5">{typeLabel}</p>
                      </div>
                      <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full ${step.enabled !== false ? "bg-green-100 text-green-700" : "bg-gray-200 text-gray-500"}`}>
                        {step.enabled !== false ? "ON" : "OFF"}
                      </span>
                      <ChevronLeft size={16} className={`text-gray-400 transition-transform ${isExpanded ? "-rotate-90" : ""}`} />
                    </button>

                    {/* ── Expanded detail panel ── */}
                    {isExpanded && (
                      <div className="px-4 pb-4 pt-2 border-t border-gray-100 space-y-4">
                        <div className="flex flex-wrap gap-4 items-start">
                          <label className="flex-1 min-w-[120px]">
                            <span className="block text-xs font-semibold text-gray-500 mb-1 uppercase tracking-wider">Type</span>
                            <select
                              value={step.type || "immediate"}
                              onChange={e => setStepsLocal(prev => prev.map((s, idx) => idx === i ? { ...s, type: e.target.value } : s))}
                              className="field-input text-sm py-1.5"
                            >
                              <option value="immediate">Immediate Send</option>
                              <option value="normal">Scheduled Send</option>
                            </select>
                          </label>
                          
                          {step.type === "normal" && (
                            <>
                              <label className="w-24">
                                <span className="block text-xs font-semibold text-gray-500 mb-1 uppercase tracking-wider">Day +</span>
                                <input
                                  type="number"
                                  min={1}
                                  value={step.daySequence || 1}
                                  onChange={e => setStepsLocal(prev => prev.map((s, idx) => idx === i ? { ...s, daySequence: Number(e.target.value) } : s))}
                                  className="field-input text-sm py-1.5"
                                />
                              </label>
                              <label className="w-32">
                                <span className="block text-xs font-semibold text-gray-500 mb-1 uppercase tracking-wider">Time</span>
                                <div className="relative">
                                  <Clock size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
                                  <input
                                    type="time"
                                    value={step.timeOfDay || "09:00"}
                                    onChange={e => setStepsLocal(prev => prev.map((s, idx) => idx === i ? { ...s, timeOfDay: e.target.value } : s))}
                                    className="field-input text-sm py-1.5 pl-8"
                                  />
                                </div>
                              </label>
                            </>
                          )}
                        </div>

                        <div>
                          <span className="block text-xs font-semibold text-gray-500 mb-2 uppercase tracking-wider">AI Instruction</span>
                          <textarea
                            value={step.aiInstruction || (typeof step === "string" ? step : "")}
                            onChange={e => setStepsLocal(prev => prev.map((s, idx) => idx === i ? { ...s, aiInstruction: e.target.value } : s))}
                            rows={4}
                            className="field-input field-textarea text-sm w-full"
                            placeholder="What should the AI do in this step?"
                          />
                        </div>

                        <div className="flex items-center justify-between pt-2 border-t border-gray-100">
                          <button
                            type="button"
                            onClick={() => setStepsLocal(prev => prev.map((s, idx) => idx === i ? { ...s, enabled: s.enabled === false ? true : false } : s))}
                            className={`text-xs font-medium px-3 py-1.5 rounded-md transition-colors ${step.enabled !== false ? "bg-green-100 text-green-700 hover:bg-green-200" : "bg-gray-200 text-gray-600 hover:bg-gray-300"}`}
                          >
                            {step.enabled !== false ? "Enabled" : "Disabled"}
                          </button>
                          <button
                            type="button"
                            onClick={() => { setStepsLocal(prev => prev.filter((_, idx) => idx !== i)); setExpandedStepIndex(null); }}
                            className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-red-500 hover:text-red-700 hover:bg-red-50 rounded-md transition-colors"
                          >
                            <Trash2 size={14} /> Remove Step
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })
            )}
            
            <div className="pt-4 border-t border-gray-100 flex justify-end">
              <button
                type="button"
                onClick={() => void saveSteps()}
                disabled={savingSteps}
                className="px-6 py-2.5 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-300 text-white text-sm font-medium rounded-lg transition-colors"
              >
                {savingSteps ? "Saving..." : "Save Steps"}
              </button>
            </div>
          </div>
        </section>
      )}

      {/* ─── SETTINGS TAB ─────────────────────────────────────── */}
      {activeTab === "settings" && (
        <section className="panel" style={{ borderTopLeftRadius: 0 }}>
          <h3 className="font-semibold text-gray-800 mb-4">Campaign Settings</h3>
          {message && <p className="text-sm mb-3 px-3 py-2 bg-blue-50 text-blue-700 border border-blue-200 rounded-lg">{message}</p>}
          <div className="form-grid">
            <label className="field-block">
              Campaign Name
              <input value={editName} onChange={(e) => setEditName(e.target.value)} className="field-input" />
            </label>
            <label className="field-block">
              Status
              <select
                value={editStatus}
                onChange={(e) => setEditStatus(e.target.value as CampaignRecord["status"])}
                className="field-input"
              >
                {["draft", "active", "paused", "archived"].map(s => (
                  <option key={s} value={s}>{s.charAt(0).toUpperCase() + s.slice(1)}</option>
                ))}
              </select>
            </label>
            <label className="field-block">
              <span className="font-medium text-gray-700 text-sm">Daily Successful Submissions Limit</span>
              <p className="text-xs text-gray-400 mb-1">Automation stops after this many successful (submitted = Yes) contacts. Failures don&apos;t count towards this limit.</p>
              <input
                type="number"
                min={1}
                max={5000}
                value={editMaxDaily}
                onChange={(e) => setEditMaxDaily(Number(e.target.value || 1))}
                className="field-input"
              />
            </label>
            <label className="field-block">
              <span className="font-medium text-gray-700 text-sm">Contact Form Strategy</span>
              <p className="text-xs text-gray-400 mb-1">How should outreach find the contact form?</p>
              <select
                value={editSearchForForm ? "search" : "exact"}
                onChange={(e) => setEditSearchForForm(e.target.value === "search")}
                className="field-input"
              >
                <option value="exact">Use exact URL provided — contact form is directly at that URL</option>
                <option value="search">Search entire domain — outreach will look for contact page</option>
              </select>
            </label>

            <label className="field-block">
              <div className="flex items-center gap-2">
                <input
                  type="checkbox"
                  id="breakFlag"
                  checked={editBreakFlag}
                  onChange={(e) => setEditBreakFlag(e.target.checked)}
                  className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                />
                <span className="font-medium text-gray-700 text-sm">Break/Stop on Failure</span>
              </div>
              <p className="text-xs text-gray-400 mt-1 ml-6">If enabled, the outreach run will immediately stop if a submission step fails or requires manual intervention.</p>
            </label>

            {/* Scheduling fields */}
            <label className="field-block">
              <span className="font-medium text-gray-700 text-sm">Schedule Outreach Run</span>
              <div className="flex gap-2 items-center mt-1">
                <span className="text-xs text-gray-500">Day of Week:</span>
                <select
                  value={campaign?.scheduleDay || "monday"}
                  onChange={e => setCampaign(c => c ? { ...c, scheduleDay: e.target.value } : c)}
                  className="field-input text-sm py-1.5 w-32"
                >
                  <option value="monday">Monday</option>
                  <option value="tuesday">Tuesday</option>
                  <option value="wednesday">Wednesday</option>
                  <option value="thursday">Thursday</option>
                  <option value="friday">Friday</option>
                  <option value="saturday">Saturday</option>
                  <option value="sunday">Sunday</option>
                </select>
                <span className="text-xs text-gray-500 ml-4">Time:</span>
                <input
                  type="time"
                  value={campaign?.scheduleTime || "09:00"}
                  onChange={e => setCampaign(c => c ? { ...c, scheduleTime: e.target.value } : c)}
                  className="field-input text-sm py-1.5 w-28"
                />
              </div>
              <p className="text-xs text-gray-400 mt-1">Outreach will automatically run at the scheduled day and time.</p>
            </label>

            <div className="full">
              <button
                type="button"
                onClick={() => void saveSettings()}
                disabled={savingSettings}
                className="px-6 py-2.5 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-300 text-white text-sm font-medium rounded-lg transition-colors"
              >
                {savingSettings ? "Saving..." : "Save Settings"}
              </button>
            </div>
          </div>
        </section>
      )}
    </div>
  );
}
