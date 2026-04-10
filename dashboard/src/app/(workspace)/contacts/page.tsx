"use client";

import Link from "next/link";
import React, {
  FormEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useSession } from "next-auth/react";
import Papa from "papaparse";
import {
  Plus,
  X,
  Database,
  Bookmark,
  Users,
  ChevronDown,
  ChevronRight,
  Building2,
  Trash2,
  Check,
  UploadCloud,
  Download,
  CheckCircle2,
  AlertCircle,
  Send,
} from "lucide-react";
import { AddContactsModal } from "./AddContactsModal";

import type { ContactRecord, PaginationMeta } from "@/lib/models";
import { formatDateTime } from "@/lib/ui";

/* ─── Local List Types ─────────────────────────────────────────── */
interface ListContact { companyName: string; contactUrl: string; }
interface ContactList { id: string; name: string; contacts: ListContact[]; createdAt: string; }
interface CampaignOption { id: string; name: string; }

// Lists are now managed via DB

/* ─── Contacts API Types ────────────────────────────────────────── */
interface ContactListResponse { contacts: ContactRecord[]; pagination?: PaginationMeta; }

const PAGE_SIZE = 50;
type PageTab = "contacts" | "lists";

export default function ContactsPage() {
  const { data: session } = useSession();
  const userId = (session?.user as any)?.id || "";

  const [activeTab, setActiveTab] = useState<PageTab>("contacts");

  /* ══════════════════════════════════════════════════════════════
     CONTACTS TAB STATE
  ══════════════════════════════════════════════════════════════ */
  const [contacts, setContacts] = useState<ContactRecord[]>([]);
  const [pagination, setPagination] = useState<PaginationMeta>({ page: 1, limit: PAGE_SIZE, total: 0, totalPages: 1 });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [searchInput, setSearchInput] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [page, setPage] = useState(1);
  const [isImportModalOpen, setIsImportModalOpen] = useState(false);
  const [loadingAll, setLoadingAll] = useState(false);

  /* Save to List modal */
  const [saveTargets, setSaveTargets] = useState<ContactRecord[] | null>(null);
  const [saveMode, setSaveMode] = useState<"existing" | "new">("existing");
  const [selectedListId, setSelectedListId] = useState("");
  const [newListNameForSave, setNewListNameForSave] = useState("");
  const [savedFeedback, setSavedFeedback] = useState<string | null>(null);

  /* ══════════════════════════════════════════════════════════════
     LISTS TAB STATE
  ══════════════════════════════════════════════════════════════ */
  const [lists, setLists] = useState<ContactList[]>([]);
  const [expandedListId, setExpandedListId] = useState<string | null>(null);
  const [listSearchQuery, setListSearchQuery] = useState("");

  /* Create new list */
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [newListName, setNewListName] = useState("");
  const [createStep, setCreateStep] = useState<1 | 2 | 3>(1);
  const [parsedLeads, setParsedLeads] = useState<ListContact[]>([]);
  const [fileName, setFileName] = useState("");
  const [createError, setCreateError] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);

  /* Send to campaign */
  const [showSendModal, setShowSendModal] = useState(false);
  const [sendingList, setSendingList] = useState<ContactList | null>(null);
  const [campaigns, setCampaigns] = useState<CampaignOption[]>([]);
  const [selectedCampaignId, setSelectedCampaignId] = useState("");
  const [sendingProgress, setSendingProgress] = useState(0);
  const [sendingCount, setSendingCount] = useState(0);
  const [sendingDone, setSendingDone] = useState(false);
  const [sendingActive, setSendingActive] = useState(false);

  /* ──────────────────────────────────────────────────────────────
     Load
  ────────────────────────────────────────────────────────────── */
  const loadLists = useCallback(async () => {
    if (!userId) return;
    try {
      const res = await fetch("/api/contact-lists", { cache: "no-store" });
      if (res.ok) {
        const data = await res.json();
        setLists(data.lists || []);
      }
    } catch (err) {
      console.error("Failed to load lists", err);
    }
  }, [userId]);

  useEffect(() => { 
    void loadLists();
  }, [loadLists]);

  const totalPages = useMemo(() => {
    const candidate = pagination.totalPages ?? pagination.total_pages ?? 1;
    return Math.max(1, candidate);
  }, [pagination]);

  const loadContacts = useCallback(async () => {
    setLoading(true);
    setError("");
    const params = new URLSearchParams();
    params.set("page", String(page));
    params.set("limit", String(PAGE_SIZE));
    if (searchQuery.trim()) params.set("q", searchQuery.trim());
    try {
      const res = await fetch(`/api/contacts?${params.toString()}`, { cache: "no-store" });
      const payload = (await res.json()) as ContactListResponse | { error?: string };
      if (!res.ok) { setError(("error" in payload && payload.error) || "Unable to load contacts."); return; }
      const data = payload as ContactListResponse;
      setContacts(data.contacts ?? []);
      const inc = data.pagination;
      setPagination({
        page: inc?.page ?? page,
        limit: inc?.limit ?? PAGE_SIZE,
        total: inc?.total ?? (data.contacts ?? []).length,
        total_pages: inc?.total_pages,
        totalPages: inc?.totalPages ?? inc?.total_pages ?? 1,
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load contacts.");
    } finally { setLoading(false); }
  }, [page, searchQuery]);

  const loadCampaigns = useCallback(async () => {
    try {
      const res = await fetch("/api/campaigns?limit=200", { cache: "no-store" });
      if (res.ok) {
        const data = await res.json();
        console.log("Loaded campaigns:", data.campaigns);
        setCampaigns(data.campaigns || []);
      } else {
        console.error("Failed to load campaigns:", res.status);
      }
    } catch (e) { console.error("Failed to load campaigns", e); }
  }, []);

  useEffect(() => { 
    void loadContacts(); 
    void loadCampaigns();
  }, [loadContacts, loadCampaigns]);

  const submitSearch = useCallback((e: FormEvent<HTMLFormElement>) => {
    e.preventDefault(); setPage(1); setSearchQuery(searchInput.trim());
  }, [searchInput]);

  const clearSearch = useCallback(() => { setSearchInput(""); setSearchQuery(""); setPage(1); }, []);

  const deleteContactGlobal = async (contactId: string) => {
    if (!globalThis.confirm("Are you sure you want to permanently delete this contact?")) return;
    try {
      const res = await fetch(`/api/contacts/${contactId}`, { method: "DELETE" });
      if (!res.ok) throw new Error("Failed to delete contact");
      void loadContacts();
    } catch {
      globalThis.alert("Failed to delete contact.");
    }
  };

  const deleteAllContactsGlobal = async () => {
    if (!globalThis.confirm("DANGER: Are you sure you want to permanently delete ALL contacts in the database? This cannot be undone.")) return;
    try {
      const res = await fetch(`/api/contacts`, { method: "DELETE" });
      if (!res.ok) throw new Error("Failed to delete contacts");
      void loadContacts();
    } catch {
      globalThis.alert("Failed to delete contacts.");
    }
  };

  /* Fetch ALL contacts from DB (ignoring pagination) for "Add All to List" */
  const fetchAllAndSaveToList = async () => {
    setLoadingAll(true);
    try {
      // First get total count from current pagination
      const total = pagination.total || 0;
      if (total === 0) { setLoadingAll(false); return; }

      // Fetch all contacts in one request with a large limit
      const params = new URLSearchParams();
      params.set("page", "1");
      params.set("limit", String(Math.max(total, 100000)));
      if (searchQuery.trim()) params.set("q", searchQuery.trim());

      const res = await fetch(`/api/contacts?${params.toString()}`, { cache: "no-store" });
      const payload = (await res.json()) as ContactListResponse | { error?: string };
      if (!res.ok) {
        globalThis.alert("Failed to fetch all contacts.");
        setLoadingAll(false);
        return;
      }
      const data = payload as ContactListResponse;
      const allContacts = data.contacts ?? [];
      if (allContacts.length === 0) {
        globalThis.alert("No contacts found.");
        setLoadingAll(false);
        return;
      }
      openSaveToList(allContacts);
    } catch (err) {
      globalThis.alert("Failed to fetch all contacts.");
    } finally {
      setLoadingAll(false);
    }
  };


  /* ──────────────────────────────────────────────────────────────
     Save Contact to List
  ────────────────────────────────────────────────────────────── */
  const openSaveToList = async (contactTargets: ContactRecord[]) => {
    if (contactTargets.length === 0) return;
    
    let fetchLists: ContactList[] = [];
    if (userId) {
      try {
        const res = await fetch("/api/contact-lists");
        if (res.ok) {
          const d = await res.json();
          fetchLists = d.lists || [];
        }
      } catch (e) {
        // failed to fetch lists
      }
    }
    setLists(fetchLists);
    setSaveTargets(contactTargets);
    setSaveMode(fetchLists.length > 0 ? "new" : "new"); // Forced "new" for now due to backend API
    setSelectedListId("");
    setNewListNameForSave("");
    setSavedFeedback(null);
  };


  const confirmSaveToList = async () => {
    if (!saveTargets || saveTargets.length === 0) return;
    const items: ListContact[] = saveTargets.map(t => ({ companyName: t.companyName || "Unknown", contactUrl: t.contactUrl }));

    let listName = "";

    try {
      if (saveMode === "existing" && selectedListId) {
        const res = await fetch(`/api/contact-lists/${selectedListId}`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ contacts: items })
        });
        if (!res.ok) throw new Error("Failed to update list");
        listName = lists.find(l => l.id === selectedListId)?.name || "Selected List";
      } else if (saveMode === "new" && newListNameForSave.trim()) {
        const res = await fetch("/api/contact-lists", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name: newListNameForSave.trim(), contacts: items })
        });
        if (!res.ok) throw new Error("Failed to save list to backend");
        listName = newListNameForSave.trim();
        void loadLists();
      } else return;
  
      setSavedFeedback(`Saved ${items.length} contact${items.length !== 1 ? "s" : ""} to "${listName}"`);
      setTimeout(() => { setSaveTargets(null); setSavedFeedback(null); }, 1200);
    } catch {
      globalThis.alert("Failed to save list.");
    }
  };

  /* ──────────────────────────────────────────────────────────────
     Lists Tab — Create Flow
  ────────────────────────────────────────────────────────────── */
  const filteredLists = useMemo(() =>
    lists.filter(l => !listSearchQuery.trim() || l.name.toLowerCase().includes(listSearchQuery.toLowerCase()))
  , [lists, listSearchQuery]);

  const resetCreateFlow = useCallback(() => {
    setNewListName(""); setCreateStep(1); setParsedLeads([]);
    setFileName(""); setCreateError(""); setShowCreateModal(false);
  }, []);

  const handleNextName = () => {
    if (!newListName.trim()) { setCreateError("Please enter a list name"); return; }
    setCreateError(""); setCreateStep(2);
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    if (!file.name.endsWith(".csv")) { setCreateError("Please upload a .csv file"); return; }
    setFileName(file.name); setCreateError("");
    Papa.parse(file, {
      header: true, skipEmptyLines: true,
      complete: (results) => {
        if (!results.data || results.data.length === 0) { setCreateError("CSV is empty."); return; }
        const data = results.data as Record<string, string>[];
        const headers = results.meta.fields || [];
        let urlCol = "", nameCol = "";
        for (const h of headers) {
          const l = h.toLowerCase();
          if (l.includes("url") || l.includes("link") || l.includes("website") || l.includes("sites")) { urlCol = h; break; }
        }
        if (!urlCol && data.length > 0) {
          for (const key of Object.keys(data[0])) {
            const val = String(data[0][key]).toLowerCase();
            if (val.startsWith("http") || val.includes(".com") || val.includes(".org")) { urlCol = key; break; }
          }
        }
        for (const h of headers) {
          const l = h.toLowerCase();
          if (l.includes("company") || l.includes("name") || l.includes("business")) { nameCol = h; break; }
        }
        if (!nameCol) { for (const key of headers) { if (key !== urlCol) { nameCol = key; break; } } }
        if (!urlCol) { setCreateError("Could not detect a Website/URL column."); return; }
        const valid = data.filter(r => r[urlCol]);
        if (!valid.length) { setCreateError("No valid URLs found."); return; }
        setParsedLeads(valid.map(r => ({ companyName: nameCol ? r[nameCol] || "Unknown" : "Unknown", contactUrl: r[urlCol] })));
        setCreateStep(3);
      },
      error: (err) => setCreateError(`Parse error: ${err.message}`),
    });
  };

  const saveNewList = async () => {
    if (!newListName.trim()) return;
    setCreateError("");
    try {
      const res = await fetch("/api/contact-lists", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: newListName.trim(), contacts: parsedLeads })
      });
      if (!res.ok) throw new Error("Failed to create list");
      void loadLists();
      resetCreateFlow();
    } catch (err) {
      setCreateError("Failed to save list to backend.");
    }
  };

  const deleteList = async (listId: string) => {
    if (!globalThis.confirm("Delete this list?")) return;
    try {
      const res = await fetch(`/api/contact-lists/${listId}`, { method: "DELETE" });
      if (!res.ok) throw new Error("Failed to delete list");
      void loadLists();
      if (expandedListId === listId) setExpandedListId(null);
    } catch {
      globalThis.alert("Failed to delete list.");
    }
  };

  const downloadListCsv = (list: ContactList) => {
    const rows = list.contacts.map(c => [`"${c.companyName.replace(/"/g, '""')}"`, `"${c.contactUrl.replace(/"/g, '""')}"`]);
    const csv = "data:text/csv;charset=utf-8," + ["Company Name,Contact URL", ...rows.map(r => r.join(","))].join("\n");
    const link = document.createElement("a");
    link.href = encodeURI(csv);
    link.download = `${list.name.replace(/[^a-z0-9]/gi, "_")}.csv`;
    document.body.appendChild(link); link.click(); document.body.removeChild(link);
  };

  const downloadTemplate = (e: React.MouseEvent) => {
    e.preventDefault();
    const csv = "data:text/csv;charset=utf-8,Company Name,Website URL\nAcme Corp,https://acme.com\nGlobex,https://globex.io\n";
    const link = document.createElement("a");
    link.href = encodeURI(csv); link.download = "sites_template.csv";
    document.body.appendChild(link); link.click(); document.body.removeChild(link);
  };

  /* ──────────────────────────────────────────────────────────────
     Save List to Global Contacts Database
  ────────────────────────────────────────────────────────────── */
  const openSendModal = useCallback((list: ContactList) => {
    setSendingList(list); setSendingDone(false); setSendingActive(false);
    setSendingProgress(0); setSendingCount(0); setShowSendModal(true);
  }, []);

  const sendToDatabase = useCallback(async () => {
    if (!sendingList) return;
    setSendingActive(true); setSendingProgress(0); setSendingCount(0);
    try {
      await fetch(`/api/contacts/bulk`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ 
          contacts: sendingList.contacts,
          campaign_id: selectedCampaignId || undefined
        }),
      });
      setSendingCount(sendingList.contacts.length);
      setSendingProgress(100);
      void loadContacts();
    } catch {
      globalThis.alert("Failed to save some or all contacts to the database.");
    } finally {
      setSendingDone(true); setSendingActive(false);
    }
  }, [sendingList, loadContacts, selectedCampaignId]);

  /* ══════════════════════════════════════════════════════════════
     RENDER
  ══════════════════════════════════════════════════════════════ */
  return (
    <div className="page-stack">

      {/* ─── Import Contacts Modal ──────────────────────────────── */}
      <AddContactsModal
        isOpen={isImportModalOpen}
        onClose={() => setIsImportModalOpen(false)}
        onComplete={() => { setIsImportModalOpen(false); void loadContacts(); }}
      />

      {/* ─── Save to List Modal ─────────────────────────────────── */}
      {saveTargets && saveTargets.length > 0 && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm p-4">
          <div className="w-full max-w-md bg-white rounded-2xl shadow-xl flex flex-col">
            <div className="flex justify-between items-center px-6 py-4 border-b border-gray-100">
              <div>
                <h3 className="text-base font-semibold text-gray-900">Save to Contact List</h3>
                <p className="text-xs text-gray-500 mt-0.5 truncate">
                  {saveTargets.length === 1 ? saveTargets[0].companyName : `Saving ${saveTargets.length} contacts`}
                </p>
              </div>
              {!savedFeedback && <button onClick={() => setSaveTargets(null)} className="p-1 text-gray-400 hover:text-gray-600"><X size={18} /></button>}
            </div>
            <div className="p-6 space-y-4">
              {savedFeedback ? (
                <div className="flex flex-col items-center gap-3 py-4 text-center">
                  <div className="w-12 h-12 bg-green-100 text-green-600 rounded-full flex items-center justify-center"><Check size={24} /></div>
                  <p className="font-medium text-gray-800">{savedFeedback}</p>
                </div>
              ) : (
                <>
                  <div className="flex gap-2">
                    <button type="button" onClick={() => setSaveMode("existing")}
                      className={`flex-1 py-2 rounded-lg text-sm font-medium border-2 transition-colors ${saveMode === "existing" ? "border-blue-500 bg-blue-50 text-blue-700" : "border-gray-200 text-gray-600 hover:border-gray-300"}`}>
                      Existing List
                    </button>
                    <button type="button" onClick={() => setSaveMode("new")}
                      className={`flex-1 py-2 rounded-lg text-sm font-medium border-2 transition-colors ${saveMode === "new" ? "border-blue-500 bg-blue-50 text-blue-700" : "border-gray-200 text-gray-600 hover:border-gray-300"}`}>
                      + Create New List
                    </button>
                  </div>
                  {saveMode === "existing" && (
                    lists.length === 0 ? (
                      <p className="text-sm text-gray-500 text-center py-2">No lists yet. Switch to &quot;Create New List&quot;.</p>
                    ) : (
                      <div className="space-y-1 max-h-52 overflow-y-auto">
                        {lists.map(l => (
                          <button key={l.id} type="button" onClick={() => setSelectedListId(l.id)}
                            className={`w-full flex items-center justify-between px-3 py-2.5 rounded-lg border-2 text-left transition-colors ${selectedListId === l.id ? "border-blue-500 bg-blue-50" : "border-gray-200 hover:border-gray-300"}`}>
                            <div>
                              <p className="font-medium text-gray-900 text-sm">{l.name}</p>
                              <p className="text-xs text-gray-500">{l.contacts.length} contacts</p>
                            </div>
                            {selectedListId === l.id && <Check size={16} className="text-blue-600 shrink-0" />}
                          </button>
                        ))}
                      </div>
                    )
                  )}
                  {saveMode === "new" && (
                    <input value={newListNameForSave} onChange={(e) => setNewListNameForSave(e.target.value)}
                      className="field-input" placeholder='New list name e.g. "Tech Q2"' autoFocus />
                  )}
                  <div className="flex gap-3 pt-1">
                    <button type="button" onClick={confirmSaveToList}
                      disabled={saveMode === "existing" ? !selectedListId : !newListNameForSave.trim()}
                      className="flex-1 px-4 py-2.5 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed text-white text-sm font-medium rounded-lg transition-colors">
                      Save to List
                    </button>
                    <button type="button" onClick={() => setSaveTargets(null)}
                      className="px-4 py-2.5 border border-gray-300 text-gray-700 text-sm rounded-lg hover:bg-gray-50">
                      Cancel
                    </button>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      )}

      {/* ─── Save List to Contacts Modal ─────────────────────────────── */}
      {showSendModal && sendingList && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm p-4">
          <div className="w-full max-w-md bg-white rounded-2xl shadow-xl flex flex-col">
            <div className="flex justify-between items-center px-6 py-4 border-b border-gray-100">
              <div>
                <h3 className="text-lg font-semibold text-gray-900">Add to Campaign</h3>
                <p className="text-xs text-gray-500 mt-0.5">{sendingList.contacts.length} contacts · &quot;{sendingList.name}&quot;</p>
              </div>
              {!sendingActive && <button onClick={() => setShowSendModal(false)} className="p-1 text-gray-400 hover:text-gray-600"><X size={20} /></button>}
            </div>
            <div className="p-6 space-y-4">
              {!sendingDone && !sendingActive && (
                <>
                  <p className="text-sm text-gray-600">
                    Select a campaign to add {sendingList.contacts.length} contacts from &quot;{sendingList.name}&quot;.
                  </p>

                  <div className="space-y-1.5">
                    <label className="text-sm font-medium text-gray-700">Select Campaign</label>
                    <select
                      value={selectedCampaignId}
                      onChange={(e) => setSelectedCampaignId(e.target.value)}
                      className="field-input"
                    >
                      <option value="">No Campaign (Global List)</option>
                      {campaigns.map(c => (
                        <option key={c.id} value={c.id}>{c.name}</option>
                      ))}
                    </select>
                  </div>

                  <div className="flex gap-3 pt-2">
                    <button type="button" onClick={() => void sendToDatabase()}
                      className="flex-1 px-5 py-2.5 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg transition-colors flex items-center justify-center gap-2">
                      <Send size={15} /> Add to Campaign
                    </button>
                    <button type="button" onClick={() => setShowSendModal(false)} className="px-5 py-2.5 border border-gray-300 text-gray-700 text-sm rounded-lg hover:bg-gray-50">Cancel</button>
                  </div>
                </>
              )}
              {sendingActive && (
                <div className="space-y-3 text-center py-2">
                  <p className="font-medium text-gray-900 text-sm">Sending contacts…</p>
                  <p className="text-gray-500 text-xs">{sendingCount} of {sendingList.contacts.length} sent</p>
                  <div className="w-full bg-gray-100 rounded-full h-2 overflow-hidden">
                    <div className="bg-blue-600 h-2 rounded-full transition-all duration-300" style={{ width: `${sendingProgress}%` }} />
                  </div>
                  <p className="text-sm font-medium text-blue-600">{sendingProgress}%</p>
                </div>
              )}
              {sendingDone && (
                <div className="text-center py-4 space-y-3">
                  <div className="w-14 h-14 bg-green-100 text-green-600 rounded-full flex items-center justify-center mx-auto"><CheckCircle2 size={28} /></div>
                  <p className="font-medium text-gray-900">Done! {sendingCount} contacts sent.</p>
                  <button type="button" onClick={() => setShowSendModal(false)} className="px-5 py-2 bg-gray-900 text-white text-sm rounded-lg hover:bg-gray-800">Close</button>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* ─── Create List Modal ───────────────────────────────────── */}
      {showCreateModal && (
        <div className="modal-overlay" onClick={resetCreateFlow}>
          <div className="modal-box" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>Create New List</h2>
              <button type="button" className="modal-close" onClick={resetCreateFlow}><X size={20} /></button>
            </div>
            <div className="step-indicator">
              <div className={`step-dot ${createStep >= 1 ? "active" : ""}`}>1</div>
              <div className="step-line" />
              <div className={`step-dot ${createStep >= 2 ? "active" : ""}`}>2</div>
              <div className="step-line" />
              <div className={`step-dot ${createStep >= 3 ? "active" : ""}`}>3</div>
            </div>
            {createError && <div className="modal-error"><AlertCircle size={16} /> {createError}</div>}

            {createStep === 1 && (
              <div className="modal-body">
                <label className="field-block">
                  List Name
                  <input value={newListName} onChange={(e) => setNewListName(e.target.value)} className="field-input"
                    placeholder='e.g. "Tech Companies Q2"' autoFocus onKeyDown={(e) => e.key === "Enter" && handleNextName()} />
                </label>
                <div style={{ marginTop: "16px" }}>
                  <button type="button" className="button-primary" onClick={handleNextName}>Next → Upload CSV</button>
                </div>
              </div>
            )}

            {createStep === 2 && (
              <div className="modal-body">
                <h3 style={{ margin: "0 0 8px", fontSize: "16px" }}>Upload Companies CSV</h3>
                <div className="upload-zone" onClick={() => fileInputRef.current?.click()}>
                  <div className="upload-zone-icon"><UploadCloud size={28} /></div>
                  <div style={{ textAlign: "center" }}>
                    <span style={{ fontWeight: 600, color: "#2563eb" }}>Click to browse</span>
                    <span style={{ color: "#6b7280" }}> or drag &amp; drop</span>
                    <p style={{ fontSize: "11px", color: "#9ca3af", marginTop: "8px", textTransform: "uppercase" }}>.CSV Supported</p>
                  </div>
                  <input type="file" accept=".csv" style={{ display: "none" }} ref={fileInputRef} onChange={handleFileChange} />
                </div>
                <div style={{ marginTop: "12px", fontSize: "13px" }}>
                  <span style={{ color: "#6b7280" }}>Need the template? </span>
                  <a href="#" onClick={downloadTemplate} style={{ color: "#2563eb", display: "inline-flex", alignItems: "center", gap: "4px", fontWeight: 500 }}>
                    <Download size={14} /> Download Template
                  </a>
                </div>
              </div>
            )}

            {createStep === 3 && (
              <div className="modal-body">
                <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "16px" }}>
                  <div style={{ width: "40px", height: "40px", background: "#dcfce7", color: "#16a34a", borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center" }}>
                    <CheckCircle2 size={20} />
                  </div>
                  <div>
                    <h3 style={{ margin: 0, fontSize: "16px" }}>{parsedLeads.length} companies found</h3>
                    <p style={{ margin: 0, fontSize: "13px", color: "#6b7280" }}>from {fileName}</p>
                  </div>
                </div>
                <div className="preview-box">
                  <h4 style={{ margin: "0 0 8px", fontSize: "13px", color: "#374151" }}>Preview</h4>
                  {parsedLeads.slice(0, 5).map((l, i) => (
                    <div key={i} className="preview-row">
                      <span style={{ fontWeight: 500, fontSize: "13px" }}>{l.companyName}</span>
                      <span style={{ fontSize: "12px", color: "#6b7280", overflow: "hidden", textOverflow: "ellipsis" }}>{l.contactUrl}</span>
                    </div>
                  ))}
                  {parsedLeads.length > 5 && <p style={{ fontSize: "12px", color: "#9ca3af", fontStyle: "italic", margin: "8px 0 0" }}>…and {parsedLeads.length - 5} more</p>}
                </div>
                <div style={{ marginTop: "16px" }}>
                  <button type="button" className="button-primary" onClick={saveNewList}
                    style={{ width: "100%", display: "flex", alignItems: "center", justifyContent: "center", gap: "8px", padding: "10px" }}>
                    <CheckCircle2 size={16} /> Save List &ldquo;{newListName}&rdquo;
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* ─── Tabs ────────────────────────────────────────────────── */}
      <div style={{ display: "flex", gap: "4px" }}>
        {(["contacts", "lists"] as PageTab[]).map((tab) => {
          const icons: Record<PageTab, React.ReactNode> = {
            contacts: <Users size={15} />,
            lists: <Database size={15} />,
          };
          const labels: Record<PageTab, string> = {
            contacts: "Contacts",
            lists: "Lists",
          };
          return (
            <button key={tab} type="button" onClick={() => setActiveTab(tab)}
              className={`flex items-center gap-2 px-5 py-2.5 rounded-t-lg text-sm font-medium transition-colors ${
                activeTab === tab ? "bg-white border border-b-0 border-gray-200 text-blue-600" : "text-gray-500 hover:text-gray-800 hover:bg-gray-100"
              }`}>
              {icons[tab]} {labels[tab]}
            </button>
          );
        })}
      </div>

      {/* ══════════════════════════════════════════════════════════
          CONTACTS TAB
      ══════════════════════════════════════════════════════════ */}
      {activeTab === "contacts" && (
        <section className="panel" style={{ borderTopLeftRadius: 0 }}>
          <div className="panel-header">
            <div className="flex items-center gap-3">
              <h2>All Contacts</h2>
              {contacts.length > 0 && (
                <>
                  <button type="button" onClick={() => void fetchAllAndSaveToList()}
                    disabled={loadingAll}
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-100 text-gray-700 rounded-lg text-xs font-medium hover:bg-gray-200 transition-colors border border-gray-200 disabled:opacity-50 disabled:cursor-wait"
                    title={`Add all ${pagination.total.toLocaleString()} contacts to a list`}>
                    <Bookmark size={13} /> {loadingAll ? `Loading ${pagination.total.toLocaleString()} contacts…` : `Add All (${pagination.total.toLocaleString()}) to List`}
                  </button>
                  <button type="button" onClick={deleteAllContactsGlobal}
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-red-50 text-red-600 rounded-lg text-xs font-medium hover:bg-red-100 transition-colors border border-red-200"
                    title="Delete ALL contacts in the database globally">
                    <Trash2 size={13} /> Delete All
                  </button>
                </>
              )}
            </div>
            <button type="button" onClick={() => setIsImportModalOpen(true)}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition shadow-sm">
              <Plus size={16} /> Add Contacts
            </button>
          </div>

          <form onSubmit={submitSearch} className="button-row search-toolbar">
            <input value={searchInput} onChange={(e) => setSearchInput(e.target.value)}
              className="field-input search-input-wide" placeholder="Search by company, domain, URL, or campaign" />
            <button type="submit" className="button-secondary">Search</button>
            <button type="button" className="button-secondary" onClick={clearSearch}>Clear</button>
          </form>

          {loading && <p className="panel-muted">Loading contacts...</p>}
          {error && <p className="panel-error">{error}</p>}

          <div className="table-wrap">
            <table className="clean-table">
              <thead>
                <tr>
                  <th>Company</th>
                  <th>Domain</th>
                  <th>Campaign</th>
                  <th>Contact URL</th>
                  <th>Updated</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {!loading && contacts.length === 0 ? (
                  <tr><td colSpan={6} className="table-empty">No contacts available.</td></tr>
                ) : (
                  contacts.map((contact) => (
                    <tr key={contact.id}>
                      <td className="font-medium">{contact.companyName}</td>
                      <td className="text-gray-500">{contact.domain || "—"}</td>
                      <td>
                        {contact.campaignId ? (
                          <Link href={`/campaigns/${contact.campaignId}`} className="table-link">{contact.campaignName || contact.campaignId}</Link>
                        ) : "—"}
                      </td>
                      <td>
                        <a href={contact.contactUrl} target="_blank" rel="noreferrer" className="table-link block truncate max-w-[200px]">{contact.contactUrl}</a>
                      </td>
                      <td className="text-gray-400 text-xs">{formatDateTime(contact.updatedAt)}</td>
                      <td>
                        <div className="flex items-center gap-2">
                          <button type="button" onClick={() => openSaveToList([contact])}
                            className="flex items-center gap-1 text-xs font-medium px-2.5 py-1.5 border border-gray-300 text-gray-600 rounded-md hover:bg-blue-50 hover:border-blue-400 hover:text-blue-600 transition-colors">
                            <Bookmark size={12} /> Save
                          </button>
                          <button type="button" onClick={() => deleteContactGlobal(contact.id)}
                            className="flex items-center gap-1 text-xs font-medium px-2.5 py-1.5 border border-gray-300 text-red-600 rounded-md hover:bg-red-50 hover:border-red-400 transition-colors">
                            <Trash2 size={12} /> Delete
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>

          <div className="button-row pagination-row">
            <p className="panel-muted pagination-summary">Showing {contacts.length} of {pagination.total} contact(s)</p>
            <div className="button-row">
              <button type="button" className="button-secondary" onClick={() => setPage(p => Math.max(1, p - 1))} disabled={loading || page <= 1}>Previous</button>
              <p className="panel-muted pagination-label">Page {page} of {totalPages}</p>
              <button type="button" className="button-secondary" onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={loading || page >= totalPages}>Next</button>
            </div>
          </div>
        </section>
      )}

      {/* ══════════════════════════════════════════════════════════
          LISTS TAB
      ══════════════════════════════════════════════════════════ */}
      {activeTab === "lists" && (
        <section className="panel" style={{ borderTopLeftRadius: 0 }}>
          <div className="panel-header">
            <div className="flex items-center gap-2">
              <Database size={18} />
              <h2>Contact Lists</h2>
            </div>
            <button type="button" className="button-primary" onClick={() => setShowCreateModal(true)}
              style={{ display: "flex", alignItems: "center", gap: "6px" }}>
              <Plus size={15} /> Create New List
            </button>
          </div>

          <p className="panel-muted" style={{ marginBottom: "12px" }}>
            Organize companies into named lists. Upload CSVs and send them to any campaign.
          </p>

          <div className="button-row search-toolbar">
            <input value={listSearchQuery} onChange={(e) => setListSearchQuery(e.target.value)}
              className="field-input search-input-wide" placeholder="Search lists by name..." />
          </div>

          {filteredLists.length === 0 ? (
            <div className="empty-state">
              <Database size={48} strokeWidth={1} />
              <h3>No lists yet</h3>
              <p>Create your first contact list by uploading a CSV file.</p>
            </div>
          ) : (
            <div className="lists-container">
              {filteredLists.map((list) => (
                <div key={list.id} className="list-card">
                  <div className="list-card-header" onClick={async () => {
                    if (expandedListId === list.id) {
                      setExpandedListId(null);
                    } else {
                      setExpandedListId(list.id);
                      if (!list.contacts || list.contacts.length === 0) {
                        try {
                          const res = await fetch(`/api/contact-lists/${list.id}`);
                          if (res.ok) {
                            const data = await res.json();
                            setLists(prev => prev.map(l => l.id === list.id ? { ...l, contacts: data.contacts || [] } : l));
                          }
                        } catch (err) { console.error("Failed to load list contacts", err); }
                      }
                    }
                  }}>
                    <div className="list-card-info">
                      <div className="list-card-chevron">
                        {expandedListId === list.id ? <ChevronDown size={18} /> : <ChevronRight size={18} />}
                      </div>
                      <div>
                        <h3 className="list-card-name">{list.name}</h3>
                        <p className="list-card-meta">
                          <Building2 size={13} /> {(list as any).contactCount ?? list.contacts?.length ?? 0} companies &nbsp;·&nbsp;
                          {new Date(list.createdAt).toLocaleDateString()}
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-2" onClick={(e) => e.stopPropagation()}>
                      <button type="button" onClick={() => downloadListCsv(list)}
                        className="flex items-center gap-1 text-xs font-medium px-3 py-1.5 border border-gray-300 text-gray-600 rounded-md hover:bg-gray-50 transition-colors">
                        <Download size={12} /> Download
                      </button>
                      <button type="button" onClick={() => void openSendModal(list)}
                        className="flex items-center gap-1 text-xs font-medium px-3 py-1.5 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors">
                        <Send size={12} /> Send to Campaign
                      </button>
                      <button type="button" className="table-delete flex items-center gap-1" onClick={() => deleteList(list.id)}>
                        <Trash2 size={13} /> Delete
                      </button>
                    </div>
                  </div>

                  {expandedListId === list.id && (
                    <div className="list-card-body">
                      {!list.contacts || list.contacts.length === 0 ? (
                        <p className="text-sm text-gray-400 p-4 text-center">Loading contacts…</p>
                      ) : (
                      <div className="table-wrap">
                        <table className="clean-table">
                          <thead>
                            <tr><th>#</th><th>Company Name</th><th>Contact URL</th></tr>
                          </thead>
                          <tbody>
                            {list.contacts.map((contact, i) => (
                              <tr key={i}>
                                <td style={{ color: "#9ca3af", width: "40px" }}>{i + 1}</td>
                                <td style={{ fontWeight: 500 }}>{contact.companyName}</td>
                                <td>
                                  <a href={contact.contactUrl} target="_blank" rel="noreferrer" className="table-link">
                                    {contact.contactUrl.length > 50 ? `${contact.contactUrl.slice(0, 50)}…` : contact.contactUrl}
                                  </a>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      )}
                    </div>
                  )}

                </div>
              ))}
            </div>
          )}
        </section>
      )}
    </div>
  );
}
