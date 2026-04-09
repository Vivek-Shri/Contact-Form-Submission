"use client";

import React, { useState, useRef, useEffect, useCallback } from "react";
import Papa from "papaparse";
import {
  UploadCloud,
  CheckCircle2,
  AlertCircle,
  Download,
  Trash2,
  ChevronDown,
  ChevronRight,
  Plus,
  Database,
  Building2,
  X,
  Send,
} from "lucide-react";

/* ─── Types ─────────────────────────────────────────────────────── */

interface ListContact {
  companyName: string;
  contactUrl: string;
}

interface ContactList {
  id: string;
  name: string;
  contacts: ListContact[];
  createdAt: string;
}

interface CampaignOption {
  id: string;
  name: string;
}

/* ─── Helpers ───────────────────────────────────────────────────── */

const LS_KEY = "outreach-contact-lists";

function loadLists(): ContactList[] {
  try {
    const raw = localStorage.getItem(LS_KEY);
    return raw ? (JSON.parse(raw) as ContactList[]) : [];
  } catch {
    return [];
  }
}

function saveLists(lists: ContactList[]) {
  localStorage.setItem(LS_KEY, JSON.stringify(lists));
}

function generateId() {
  return `list-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

/* ─── Component ────────────────────────────────────────────────── */

export default function ListsPage() {
  const [lists, setLists] = useState<ContactList[]>([]);
  const [expandedListId, setExpandedListId] = useState<string | null>(null);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [error, setError] = useState("");
  const [existingUrls, setExistingUrls] = useState<Set<string>>(new Set());

  // Create flow
  const [newListName, setNewListName] = useState("");
  const [createStep, setCreateStep] = useState<1 | 2 | 3>(1);
  const [parsedLeads, setParsedLeads] = useState<ListContact[]>([]);
  const [fileName, setFileName] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Search
  const [searchQuery, setSearchQuery] = useState("");

  // Send to Campaign
  const [showSendModal, setShowSendModal] = useState(false);
  const [sendingList, setSendingList] = useState<ContactList | null>(null);
  const [campaigns, setCampaigns] = useState<CampaignOption[]>([]);
  const [selectedCampaignId, setSelectedCampaignId] = useState("");
  const [sendingProgress, setSendingProgress] = useState(0);
  const [sendingCount, setSendingCount] = useState(0);
  const [sendingDone, setSendingDone] = useState(false);
  const [sendingActive, setSendingActive] = useState(false);

  useEffect(() => {
    setLists(loadLists());
  }, []);

  const filteredLists = lists.filter(
    (l) =>
      !searchQuery.trim() ||
      l.name.toLowerCase().includes(searchQuery.toLowerCase())
  );

  /* ─ Download list as CSV ─ */
  const downloadListCsv = useCallback((list: ContactList) => {
    const headers = ["Company Name", "Contact URL"];
    const rows = list.contacts.map(c => [
      `"${(c.companyName || "").replace(/"/g, '""')}"`,
      `"${(c.contactUrl || "").replace(/"/g, '""')}"`,
    ]);
    const csv = "data:text/csv;charset=utf-8," + [headers.join(","), ...rows.map(r => r.join(","))].join("\n");
    const link = document.createElement("a");
    link.href = encodeURI(csv);
    link.download = `${list.name.replace(/[^a-z0-9]/gi, "_")}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }, []);

  /* ─ Open Send to Campaign modal ─ */
  const openSendModal = useCallback(async (list: ContactList) => {
    setSendingList(list);
    setSendingDone(false);
    setSendingActive(false);
    setSendingProgress(0);
    setSendingCount(0);
    setSelectedCampaignId("");
    setShowSendModal(true);
    try {
      const res = await fetch("/api/campaigns?limit=100");
      const data = await res.json() as { campaigns?: CampaignOption[] };
      const cList = (data.campaigns || []).map(c => ({ id: c.id, name: c.name }));
      setCampaigns(cList);
      if (cList.length > 0) setSelectedCampaignId(cList[0].id);
    } catch {
      setCampaigns([]);
    }
  }, []);

  const sendToCampaign = useCallback(async () => {
    if (!sendingList || !selectedCampaignId) return;
    setSendingActive(true);
    setSendingProgress(0);
    setSendingCount(0);
    let done = 0;
    for (const item of sendingList.contacts) {
      try {
        await fetch(`/api/campaigns/${selectedCampaignId}/contacts`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ companyName: item.companyName || "Unknown", contactUrl: item.contactUrl }),
        });
      } catch { /* skip */ }
      done++;
      setSendingCount(done);
      setSendingProgress(Math.floor((done / sendingList.contacts.length) * 100));
    }
    setSendingDone(true);
    setSendingActive(false);
  }, [sendingList, selectedCampaignId]);

  /* ─ Create list flow ─ */

  const resetCreateFlow = useCallback(() => {
    setNewListName("");
    setCreateStep(1);
    setParsedLeads([]);
    setFileName("");
    setError("");
    setShowCreateModal(false);
  }, []);

  const handleNextName = () => {
    if (!newListName.trim()) {
      setError("Please enter a list name");
      return;
    }
    setError("");
    setCreateStep(2);
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    if (!file.name.endsWith(".csv")) {
      setError("Please upload a .csv file");
      return;
    }

    setFileName(file.name);
    setError("");

    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      complete: (results) => {
        if (!results.data || results.data.length === 0) {
          setError("CSV appears to be empty.");
          return;
        }

        const data = results.data as Record<string, string>[];
        const headers = results.meta.fields || [];

        let urlCol = "";
        let nameCol = "";

        for (const h of headers) {
          const lower = h.toLowerCase();
          if (lower.includes("url") || lower.includes("link") || lower.includes("website") || lower.includes("sites")) {
            urlCol = h;
            break;
          }
        }
        if (!urlCol && data.length > 0) {
          for (const key of Object.keys(data[0])) {
            const val = String(data[0][key]).toLowerCase();
            if (val.startsWith("http") || val.includes(".com") || val.includes(".org")) {
              urlCol = key;
              break;
            }
          }
        }

        for (const h of headers) {
          const lower = h.toLowerCase();
          if (lower.includes("company") || lower.includes("name") || lower.includes("business")) {
            nameCol = h;
            break;
          }
        }
        if (!nameCol) {
          for (const key of headers) {
            if (key !== urlCol) { nameCol = key; break; }
          }
        }

        if (!urlCol) {
          setError("Could not detect a Website/URL column in the CSV.");
          return;
        }

        const validRows = data.filter((row) => row[urlCol]);
        if (validRows.length === 0) {
          setError("No valid data found in the detected URL column.");
          return;
        }

        const normalized = validRows.map((r) => ({
          companyName: nameCol ? r[nameCol] || "Unknown" : "Unknown",
          contactUrl: r[urlCol],
        }));

        setParsedLeads(normalized);
        setCreateStep(3);
      },
      error: (err) => {
        setError(`Failed to parse CSV: ${err.message}`);
      },
    });
  };

  const saveList = () => {
    const newList: ContactList = {
      id: generateId(),
      name: newListName.trim(),
      contacts: parsedLeads,
      createdAt: new Date().toISOString(),
    };
    const updated = [newList, ...lists];
    setLists(updated);
    saveLists(updated);
    resetCreateFlow();
  };

  const deleteList = (listId: string) => {
    const confirmed = globalThis.confirm("Delete this list and all its contacts?");
    if (!confirmed) return;
    const updated = lists.filter((l) => l.id !== listId);
    setLists(updated);
    saveLists(updated);
    if (expandedListId === listId) setExpandedListId(null);
  };

  const downloadTemplate = (e: React.MouseEvent) => {
    e.preventDefault();
    const csvContent = "data:text/csv;charset=utf-8,Company Name,Website URL\nAcme Corp,https://acme.com\nGlobex,https://globex.io\n";
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement("a");
    link.href = encodedUri;
    link.download = "sites_template.csv";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const checkUrls = async (urls: string[]) => {
    // Check URLs against backend and add to existingUrls set
    try {
      const chunkSize = 2000;
      const allExisting = new Set<string>(existingUrls);

      for (let i = 0; i < urls.length; i += chunkSize) {
        const chunk = urls.slice(i, i + chunkSize);
        const res = await fetch("/api/contacts/check-exists", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ urls: chunk }),
        });
        if (res.ok) {
          const data = await res.json();
          (data.existing_urls || []).forEach((u: string) => allExisting.add(u));
        }
      }
      setExistingUrls(allExisting);
    } catch {
      // Ignored
    }
  };

  const toggleExpand = (listId: string) => {
    setExpandedListId((prev) => {
      const next = prev === listId ? null : listId;
      if (next) {
        const list = lists.find(l => l.id === next);
        if (list) checkUrls(list.contacts.map(c => c.contactUrl));
      }
      return next;
    });
  };

  /* ─── Render ──────────────────────────────────────────────────── */

  return (
    <div className="page-stack">
      {/* ─── Send to Campaign Modal ───────────────────────────────── */}
      {showSendModal && sendingList && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm p-4">
          <div className="w-full max-w-md bg-white rounded-2xl shadow-xl flex flex-col">
            <div className="flex justify-between items-center px-6 py-4 border-b border-gray-100">
              <div>
                <h3 className="text-lg font-semibold text-gray-900">Send to Campaign</h3>
                <p className="text-xs text-gray-500 mt-0.5">{sendingList.contacts.length} contacts from &quot;{sendingList.name}&quot;</p>
              </div>
              {!sendingActive && <button onClick={() => setShowSendModal(false)} className="p-1 text-gray-400 hover:text-gray-600"><X size={20} /></button>}
            </div>
            <div className="p-6 space-y-4">
              {!sendingDone && !sendingActive && (
                <>
                  {campaigns.length === 0 ? (
                    <p className="text-sm text-gray-500 text-center py-4">No campaigns found. Create one first.</p>
                  ) : (
                    <label className="block">
                      <span className="text-sm font-medium text-gray-700">Select Campaign</span>
                      <select
                        value={selectedCampaignId}
                        onChange={(e) => setSelectedCampaignId(e.target.value)}
                        className="field-input mt-1"
                      >
                        {campaigns.map(c => (
                          <option key={c.id} value={c.id}>{c.name}</option>
                        ))}
                      </select>
                    </label>
                  )}
                  <div className="flex gap-3">
                    <button
                      type="button"
                      onClick={() => void sendToCampaign()}
                      disabled={!selectedCampaignId}
                      className="flex-1 px-5 py-2.5 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-300 text-white text-sm font-medium rounded-lg transition-colors flex items-center justify-center gap-2"
                    >
                      <Send size={15} /> Send Contacts
                    </button>
                    <button type="button" onClick={() => setShowSendModal(false)} className="px-5 py-2.5 border border-gray-300 text-gray-700 text-sm rounded-lg hover:bg-gray-50">
                      Cancel
                    </button>
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
                  <div className="w-14 h-14 bg-green-100 text-green-600 rounded-full flex items-center justify-center mx-auto">
                    <CheckCircle2 size={28} />
                  </div>
                  <p className="font-medium text-gray-900">Done! {sendingCount} contacts sent.</p>
                  <button type="button" onClick={() => setShowSendModal(false)} className="px-5 py-2 bg-gray-900 text-white text-sm rounded-lg hover:bg-gray-800 transition-colors">
                    Close
                  </button>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Header */}
      <section className="panel">
        <div className="panel-header">
          <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
            <Database size={20} />
            <h2>Contact Lists</h2>
          </div>
          <button
            type="button"
            className="button-primary"
            onClick={() => setShowCreateModal(true)}
            style={{ display: "flex", alignItems: "center", gap: "6px" }}
          >
            <Plus size={15} /> Create New List
          </button>
        </div>

        <p className="panel-muted" style={{ marginBottom: "12px" }}>
          Organize companies into named lists. Upload CSVs and send them directly to any campaign.
        </p>

        {/* Search */}
        <div className="button-row search-toolbar">
          <input
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="field-input search-input-wide"
            placeholder="Search lists by name..."
          />
        </div>

        {/* Lists grid */}
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
                <div
                  className="list-card-header"
                  onClick={() => toggleExpand(list.id)}
                >
                  <div className="list-card-info">
                    <div className="list-card-chevron">
                      {expandedListId === list.id ? (
                        <ChevronDown size={18} />
                      ) : (
                        <ChevronRight size={18} />
                      )}
                    </div>
                    <div>
                      <h3 className="list-card-name">{list.name}</h3>
                      <p className="list-card-meta">
                        <Building2 size={13} /> {list.contacts.length} companies
                        &nbsp;·&nbsp;
                        {new Date(list.createdAt).toLocaleDateString()}
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2" onClick={(e) => e.stopPropagation()}>
                    <button
                      type="button"
                      onClick={() => downloadListCsv(list)}
                      className="flex items-center gap-1 text-xs font-medium px-3 py-1.5 border border-gray-300 text-gray-600 rounded-md hover:bg-gray-50 transition-colors"
                    >
                      <Download size={12} /> Download
                    </button>
                    <button
                      type="button"
                      onClick={() => void openSendModal(list)}
                      className="flex items-center gap-1 text-xs font-medium px-3 py-1.5 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
                    >
                      <Send size={12} /> Send to Campaign
                    </button>
                    <button
                      type="button"
                      className="table-delete"
                      onClick={() => deleteList(list.id)}
                      style={{ display: "flex", alignItems: "center", gap: "4px" }}
                    >
                      <Trash2 size={13} /> Delete
                    </button>
                  </div>
                </div>

                {expandedListId === list.id && (
                  <div className="list-card-body">
                    <div className="table-wrap">
                      <table className="clean-table">
                        <thead>
                          <tr>
                            <th>#</th>
                            <th>Company Name</th>
                            <th>Contact URL</th>
                          </tr>
                        </thead>
                        <tbody>
                          {list.contacts.map((contact, i) => {
                            const exists = existingUrls.has(contact.contactUrl);
                            return (
                              <tr key={i} className={exists ? "bg-amber-50" : ""}>
                                <td style={{ color: "#9ca3af", width: "40px" }}>
                                  {i + 1}
                                </td>
                                <td style={{ fontWeight: 500 }}>
                                  {contact.companyName}
                                </td>
                                <td>
                                  <div className="flex items-center gap-2">
                                    <a
                                      href={contact.contactUrl}
                                      target="_blank"
                                      rel="noreferrer"
                                      className="table-link"
                                    >
                                      {contact.contactUrl.length > 50
                                        ? `${contact.contactUrl.slice(0, 50)}…`
                                        : contact.contactUrl}
                                    </a>
                                    {exists && (
                                      <span
                                        className="inline-flex items-center text-xs font-medium text-amber-700 bg-amber-100 px-1.5 py-0.5 rounded gap-1 whitespace-nowrap"
                                        title="This company is already visited by our automation."
                                      >
                                        <AlertCircle size={12} /> Already companies exists
                                      </span>
                                    )}
                                  </div>
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </section>

      {/* ─── Create Modal ─────────────────────────────────────────── */}
      {showCreateModal && (
        <div className="modal-overlay" onClick={resetCreateFlow}>
          <div className="modal-box" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>Create New List</h2>
              <button
                type="button"
                className="modal-close"
                onClick={resetCreateFlow}
              >
                <X size={20} />
              </button>
            </div>

            {/* Step indicator */}
            <div className="step-indicator">
              <div className={`step-dot ${createStep >= 1 ? "active" : ""}`}>
                1
              </div>
              <div className="step-line" />
              <div className={`step-dot ${createStep >= 2 ? "active" : ""}`}>
                2
              </div>
              <div className="step-line" />
              <div className={`step-dot ${createStep >= 3 ? "active" : ""}`}>
                3
              </div>
            </div>

            {error && (
              <div className="modal-error">
                <AlertCircle size={16} /> {error}
              </div>
            )}

            {/* Step 1: Name */}
            {createStep === 1 && (
              <div className="modal-body">
                <label className="field-block">
                  List Name
                  <input
                    value={newListName}
                    onChange={(e) => setNewListName(e.target.value)}
                    className="field-input"
                    placeholder='e.g. "Skyvern Folder" or "Tech Companies Q2"'
                    autoFocus
                    onKeyDown={(e) => e.key === "Enter" && handleNextName()}
                  />
                </label>
                <p style={{ fontSize: "13px", color: "#6b7280", margin: "8px 0 0" }}>
                  Give your list a descriptive name so you can find it easily later.
                </p>
                <div style={{ marginTop: "16px" }}>
                  <button
                    type="button"
                    className="button-primary"
                    onClick={handleNextName}
                  >
                    Next → Upload CSV
                  </button>
                </div>
              </div>
            )}

            {/* Step 2: Upload */}
            {createStep === 2 && (
              <div className="modal-body">
                <h3 style={{ margin: "0 0 8px", fontSize: "16px" }}>
                  Upload Companies CSV
                </h3>
                <p style={{ fontSize: "13px", color: "#6b7280", margin: "0 0 16px" }}>
                  Upload a CSV with company names and website URLs.
                </p>

                <div
                  className="upload-zone"
                  onClick={() => fileInputRef.current?.click()}
                >
                  <div className="upload-zone-icon">
                    <UploadCloud size={28} />
                  </div>
                  <div style={{ textAlign: "center" }}>
                    <span style={{ fontWeight: 600, color: "#2563eb" }}>
                      Click to browse
                    </span>
                    <span style={{ color: "#6b7280" }}> or drag &amp; drop</span>
                    <p
                      style={{
                        fontSize: "11px",
                        color: "#9ca3af",
                        marginTop: "8px",
                        textTransform: "uppercase",
                        letterSpacing: "0.05em",
                      }}
                    >
                      .CSV Supported
                    </p>
                  </div>
                  <input
                    type="file"
                    accept=".csv"
                    className="hidden"
                    ref={fileInputRef}
                    onChange={handleFileChange}
                    style={{ display: "none" }}
                  />
                </div>

                <div style={{ marginTop: "12px", fontSize: "13px" }}>
                  <span style={{ color: "#6b7280" }}>Need the template? </span>
                  <a
                    href="#"
                    onClick={downloadTemplate}
                    style={{
                      color: "#2563eb",
                      display: "inline-flex",
                      alignItems: "center",
                      gap: "4px",
                      fontWeight: 500,
                    }}
                  >
                    <Download size={14} /> Download Template
                  </a>
                </div>
              </div>
            )}

            {/* Step 3: Preview & Save */}
            {createStep === 3 && (
              <div className="modal-body">
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "10px",
                    marginBottom: "16px",
                  }}
                >
                  <div
                    style={{
                      width: "40px",
                      height: "40px",
                      background: "#dcfce7",
                      color: "#16a34a",
                      borderRadius: "50%",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                    }}
                  >
                    <CheckCircle2 size={20} />
                  </div>
                  <div>
                    <h3 style={{ margin: 0, fontSize: "16px" }}>
                      {parsedLeads.length} companies found
                    </h3>
                    <p style={{ margin: 0, fontSize: "13px", color: "#6b7280" }}>
                      from {fileName}
                    </p>
                  </div>
                </div>

                <div className="preview-box">
                  <h4
                    style={{
                      margin: "0 0 8px",
                      fontSize: "13px",
                      color: "#374151",
                    }}
                  >
                    Preview
                  </h4>
                  {parsedLeads.slice(0, 5).map((l, i) => (
                    <div key={i} className="preview-row">
                      <span style={{ fontWeight: 500, fontSize: "13px" }}>
                        {l.companyName}
                      </span>
                      <span
                        style={{
                          fontSize: "12px",
                          color: "#6b7280",
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                        }}
                      >
                        {l.contactUrl}
                      </span>
                    </div>
                  ))}
                  {parsedLeads.length > 5 && (
                    <p
                      style={{
                        fontSize: "12px",
                        color: "#9ca3af",
                        fontStyle: "italic",
                        margin: "8px 0 0",
                      }}
                    >
                      ...and {parsedLeads.length - 5} more
                    </p>
                  )}
                </div>

                <div style={{ marginTop: "16px" }}>
                  <button
                    type="button"
                    className="button-primary"
                    onClick={saveList}
                    style={{
                      width: "100%",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      gap: "8px",
                      padding: "10px",
                    }}
                  >
                    <CheckCircle2 size={16} /> Save List &ldquo;{newListName}&rdquo;
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
