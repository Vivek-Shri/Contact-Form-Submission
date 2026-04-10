"use client";

import React, { useState, useRef, useEffect } from "react";
import { useSession } from "next-auth/react";
import Papa from "papaparse";
import {
  UploadCloud,
  CheckCircle2,
  AlertCircle,
  X,
  Download,
  Filter,
  Trash2,
  List as ListIcon,
  Check,
} from "lucide-react";

/* ─── Types ─────────────────────────────────────────────────────── */

interface ParsedRow {
  companyName: string;
  contactUrl: string;
  domain: string;
}

interface AddContactsModalProps {
  isOpen: boolean;
  onClose: () => void;
  onComplete: () => void;
}

interface CampaignOption { id: string; name: string; }

interface ContactList {
  id: string;
  name: string;
  contacts: { companyName: string; contactUrl: string }[];
  createdAt: string;
}

/* ─── Helpers ───────────────────────────────────────────────────── */

function extractDomain(url: string): string {
  try {
    const candidate = url.startsWith("http") ? url : `https://${url}`;
    const parsed = new URL(candidate);
    return (parsed.hostname || "").replace(/^www\./, "").toLowerCase();
  } catch {
    return "";
  }
}

function dedupeByDomain(rows: ParsedRow[]): {
  kept: ParsedRow[];
  duplicatesRemoved: number;
  noDomainRemoved: number;
} {
  const domainMap = new Map<string, ParsedRow>();
  let duplicatesRemoved = 0;
  let noDomainRemoved = 0;

  for (const row of rows) {
    if (!row.domain) {
      noDomainRemoved++;
      continue;
    }
    if (domainMap.has(row.domain)) {
      duplicatesRemoved++;
      continue;
    }
    domainMap.set(row.domain, row);
  }

  return {
    kept: Array.from(domainMap.values()),
    duplicatesRemoved,
    noDomainRemoved,
  };
}

// We now use PostgreSQL backend APIs for saving/loading lists.

/* ─── Component ────────────────────────────────────────────────── */

export function AddContactsModal({
  isOpen,
  onClose,
  onComplete,
}: AddContactsModalProps) {
  const { data: session } = useSession();
  const userId = (session?.user as any)?.id || "";

  // Steps: 1 = upload CSV, 2 = review filtered + multi-select, 3 = save options, 4 = done
  const [step, setStep] = useState<number>(1);
  const [error, setError] = useState("");
  const [fileName, setFileName] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Parsed & filtered data
  const [filteredRows, setFilteredRows] = useState<ParsedRow[]>([]);
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set());
  const [duplicatesRemoved, setDuplicatesRemoved] = useState(0);
  const [noDomainRemoved, setNoDomainRemoved] = useState(0);
  const [totalRawRows, setTotalRawRows] = useState(0);
  
  // Existing contacts checking
  const [existingUrls, setExistingUrls] = useState<Set<string>>(new Set());
  const [isChecking, setIsChecking] = useState(false);

  // Lists state
  const [savedCount, setSavedCount] = useState(0);
  const [lists, setLists] = useState<ContactList[]>([]);
  const [saveMode, setSaveMode] = useState<"existing" | "new">("new");
  const [selectedListId, setSelectedListId] = useState("");
  const [newListName, setNewListName] = useState("");

  // Campaign assignment
  const [campaigns, setCampaigns] = useState<CampaignOption[]>([]);
  const [selectedCampaignId, setSelectedCampaignId] = useState("");
  const [isSavingToDb, setIsSavingToDb] = useState(false);

  const loadCampaigns = async () => {
    try {
      const res = await fetch("/api/campaigns?limit=200", { cache: "no-store" });
      if (res.ok) {
        const data = await res.json();
        setCampaigns(data.campaigns || []);
      }
    } catch (e) { console.error("Failed to load campaigns", e); }
  };

  useEffect(() => {
    if (isOpen) {
      if (userId) {
        fetch("/api/contact-lists")
          .then((r) => r.ok ? r.json() : Promise.reject())
          .then((d) => setLists(d.lists || []))
          .catch(() => setLists([]));
      }
      void loadCampaigns();
    } else {
      setStep(1);
      setError("");
      setFileName("");
      setFilteredRows([]);
      setSelectedIds(new Set());
      setDuplicatesRemoved(0);
      setNoDomainRemoved(0);
      setTotalRawRows(0);
      setSavedCount(0);
      setExistingUrls(new Set());
      setIsChecking(false);
      setSaveMode("new");
      setNewListName("");
      setSelectedListId("");
      setSelectedCampaignId("");
    }
  }, [isOpen]);

  if (!isOpen) return null;

  /* ─ CSV Upload & Parse ─ */

  const downloadTemplate = (e: React.MouseEvent) => {
    e.preventDefault();
    const csvContent =
      "data:text/csv;charset=utf-8,Company Name,Website URL\nAcme Corp,https://acme.com/contact\nGlobex Inc,https://globex.io/contact\n";
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement("a");
    link.setAttribute("href", encodedUri);
    link.setAttribute("download", "contacts_template.csv");
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const checkExistingContacts = async (rows: ParsedRow[]) => {
    setIsChecking(true);
    try {
      const urls = rows.map((r) => r.contactUrl);
      const chunkSize = 2000;
      const allExisting = new Set<string>();

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
    } finally {
      setIsChecking(false);
    }
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

        // Detect URL column
        let urlCol = "";
        for (const h of headers) {
          const lower = h.toLowerCase();
          if (
            lower.includes("url") ||
            lower.includes("link") ||
            lower.includes("website") ||
            lower.includes("site")
          ) {
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

        // Detect Name column
        let nameCol = "";
        for (const h of headers) {
          const lower = h.toLowerCase();
          if (lower.includes("company") || lower.includes("name") || lower.includes("business")) {
            nameCol = h;
            break;
          }
        }
        if (!nameCol) {
          for (const key of headers) {
            if (key !== urlCol) {
              nameCol = key;
              break;
            }
          }
        }

        if (!urlCol) {
          setError("Could not detect a Website/URL column in the CSV.");
          return;
        }

        // Build parsed rows with domains
        const rawRows: ParsedRow[] = data
          .filter((row) => row[urlCol]?.trim())
          .map((row) => {
            const contactUrl = row[urlCol].trim();
            return {
              companyName: nameCol ? row[nameCol]?.trim() || "Unknown" : "Unknown",
              contactUrl,
              domain: extractDomain(contactUrl),
            };
          });

        setTotalRawRows(rawRows.length);

        // Apply domain dedup + no-domain filter
        const { kept, duplicatesRemoved: dupes, noDomainRemoved: noDomain } =
          dedupeByDomain(rawRows);

        setFilteredRows(kept);
        setDuplicatesRemoved(dupes);
        setNoDomainRemoved(noDomain);

        // Select all by default
        setSelectedIds(new Set(kept.map((_, i) => i)));
        setStep(2);
        
        // Also fire off api to check if existing
        void checkExistingContacts(kept);
      },
      error: (err) => {
        setError(`Failed to parse CSV: ${err.message}`);
      },
    });
  };

  /* ─ Selection ─ */

  const toggleSelect = (index: number) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(index)) {
        next.delete(index);
      } else {
        next.add(index);
      }
      return next;
    });
  };

  const selectAll = () => {
    setSelectedIds(new Set(filteredRows.map((_, i) => i)));
  };

  const deselectAll = () => {
    setSelectedIds(new Set());
  };

  /* ─ Save selected contacts ─ */
  const handleProceedToSave = () => {
    if (selectedIds.size === 0) {
      setError("Please select at least one company.");
      return;
    }
    setError("");
    setSaveMode(lists.length > 0 ? "existing" : "new");
    setSelectedListId(lists[0]?.id ?? "");
    setNewListName("");
    setStep(3);
  };

  const confirmSaveToList = async () => {
    setError("");
    setIsSavingToDb(true);
    const selected = filteredRows.filter((_, i) => selectedIds.has(i));
    const items = selected.map(r => ({ companyName: r.companyName, contactUrl: r.contactUrl }));

    // Save to backend database as well with campaign_id
    try {
      const CHUNK_SIZE = 2000;
      for (let i = 0; i < items.length; i += CHUNK_SIZE) {
        const chunk = items.slice(i, i + CHUNK_SIZE);
        await fetch(`/api/contacts/bulk`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ 
            contacts: chunk,
            campaign_id: selectedCampaignId || undefined
          }),
        });
      }
    } catch (e) {
      console.error("Backend save failed, but proceeding with local list save", e);
    } finally {
      setIsSavingToDb(false);
    }

    try {
      if (saveMode === "existing" && selectedListId) {
        // Not currently implemented in the proxy, treating as error for now or fallback to new list mode via ui
        setError("Appending to existing lists is currently disabled via API. Create a new list.");
        return;
      } else if (saveMode === "new" && newListName.trim()) {
        const res = await fetch("/api/contact-lists", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name: newListName.trim(), contacts: items })
        });
        if (!res.ok) throw new Error("Failed to save list to backend");
      } else {
        setError("Please choose a valid list.");
        return;
      }
    } catch(err) {
      setError(err instanceof Error ? err.message : "Failed to save contacts");
      return;
    }

    setSavedCount(selected.length);
    setStep(4);
  };

  /* ─── Render ──────────────────────────────────────────────────── */

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm p-4">
      <div className="w-full max-w-3xl bg-white rounded-2xl shadow-xl overflow-hidden flex flex-col max-h-[90vh]">
        {/* Header */}
        <div className="flex justify-between items-center px-6 py-4 border-b border-gray-100 shrink-0">
          <h3 className="text-lg font-semibold text-gray-900">Import Contacts</h3>
          <button
            onClick={onClose}
            className="p-1 text-gray-400 hover:text-gray-600 transition-colors"
          >
            <X size={20} />
          </button>
        </div>

        {/* Body */}
        <div className="p-6 overflow-y-auto">
          {error && (
            <div className="mb-4 flex items-center gap-2 p-3 text-sm text-red-700 bg-red-50 border border-red-200 rounded-lg">
              <AlertCircle size={16} />
              {error}
            </div>
          )}

          {/* ─── Step 1: Upload CSV ─── */}
          {step === 1 && (
            <div className="space-y-4 text-center">
              <h4 className="font-medium text-gray-900">Upload CSV</h4>
              <p className="text-sm text-gray-500 mb-4 px-4">
                Upload your list of companies. We&apos;ll auto-filter:
                <strong> one domain = one entry</strong>, and remove entries
                without a valid domain.
              </p>

              <div
                className="border-2 border-dashed border-gray-200 rounded-xl p-8 hover:bg-gray-50 hover:border-blue-400 cursor-pointer transition-all flex flex-col items-center gap-3"
                onClick={() => fileInputRef.current?.click()}
              >
                <div className="w-12 h-12 bg-blue-50 text-blue-600 rounded-full flex items-center justify-center">
                  <UploadCloud size={24} />
                </div>
                <div>
                  <span className="font-medium text-blue-600">Click to browse</span>
                  <span className="text-gray-500"> or drag and drop</span>
                  <p className="text-xs text-gray-400 mt-1">.csv supported</p>
                </div>
                <input
                  type="file"
                  accept=".csv"
                  className="hidden"
                  ref={fileInputRef}
                  onChange={handleFileChange}
                />
              </div>

              <div className="pt-2 text-sm">
                <span className="text-gray-500">Need the correct format? </span>
                <a
                  href="#"
                  onClick={downloadTemplate}
                  className="text-blue-600 hover:underline inline-flex items-center gap-1"
                >
                  <Download size={14} /> Download Template
                </a>
              </div>
            </div>
          )}

          {/* ─── Step 2: Filtered results + Multi-select ─── */}
          {step === 2 && (
            <div className="space-y-4">
              {/* Filter summary */}
              <div className="flex items-start gap-3 p-4 bg-blue-50 border border-blue-200 rounded-lg">
                <Filter size={18} className="text-blue-600 mt-0.5 shrink-0" />
                <div className="text-sm text-blue-900">
                  <p className="font-semibold mb-1">
                    Filtered: {filteredRows.length} companies kept from {totalRawRows} rows
                  </p>
                  <ul className="list-disc list-inside text-blue-700 space-y-0.5">
                    {noDomainRemoved > 0 && (
                      <li>
                        <Trash2 size={12} className="inline mr-1" />
                        {noDomainRemoved} removed (no valid domain)
                      </li>
                    )}
                    {duplicatesRemoved > 0 && (
                      <li>
                        {duplicatesRemoved} removed (duplicate domain)
                      </li>
                    )}
                  </ul>
                </div>
              </div>

              {/* Select controls */}
              <div className="flex items-center justify-between">
                <p className="text-sm font-medium text-gray-700">
                  {selectedIds.size} of {filteredRows.length} selected
                </p>
                <div className="flex gap-2">
                  <button
                    type="button"
                    onClick={selectAll}
                    className="text-xs font-medium text-blue-600 hover:text-blue-800 transition-colors px-2 py-1 rounded hover:bg-blue-50"
                  >
                    Select All
                  </button>
                  <button
                    type="button"
                    onClick={deselectAll}
                    className="text-xs font-medium text-gray-500 hover:text-gray-700 transition-colors px-2 py-1 rounded hover:bg-gray-100"
                  >
                    Deselect All
                  </button>
                </div>
              </div>

              {/* Table */}
              <div className="border border-gray-200 rounded-lg overflow-auto max-h-[340px]">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 sticky top-0 z-10">
                    <tr>
                      <th className="w-10 px-3 py-2 text-left">
                        <input
                          type="checkbox"
                          checked={selectedIds.size === filteredRows.length}
                          onChange={() =>
                            selectedIds.size === filteredRows.length
                              ? deselectAll()
                              : selectAll()
                          }
                          className="rounded"
                        />
                      </th>
                      <th className="px-3 py-2 text-left font-medium text-gray-600">Company</th>
                      <th className="px-3 py-2 text-left font-medium text-gray-600">Domain</th>
                      <th className="px-3 py-2 text-left font-medium text-gray-600">Contact URL</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {filteredRows.map((row, i) => {
                      const exists = existingUrls.has(row.contactUrl);
                      return (
                        <tr
                          key={i}
                          className={`cursor-pointer transition-colors ${
                            selectedIds.has(i) ? "bg-blue-50/60" : "hover:bg-gray-50"
                          } ${exists ? "bg-amber-50/30" : ""}`}
                          onClick={() => toggleSelect(i)}
                        >
                          <td className="px-3 py-2">
                            <input
                              type="checkbox"
                              checked={selectedIds.has(i)}
                              onChange={() => toggleSelect(i)}
                              className="rounded"
                            />
                          </td>
                          <td className="px-3 py-2 font-medium text-gray-900">
                            {row.companyName}
                          </td>
                          <td className="px-3 py-2 text-gray-500">
                            {row.domain}
                          </td>
                          <td className="px-3 py-2 text-gray-500">
                            <div className="flex items-center gap-2">
                              <span className="truncate max-w-[180px]">{row.contactUrl}</span>
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

              {/* Action */}
              <div className="pt-2 space-y-3">
                <button
                  type="button"
                  onClick={handleProceedToSave}
                  disabled={selectedIds.size === 0 || isChecking}
                  className="w-full px-5 py-2.5 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed text-white text-sm font-medium rounded-lg transition-colors flex items-center justify-center gap-2"
                >
                  <ListIcon size={16} />
                  Proceed with {selectedIds.size} Selected Contact{selectedIds.size !== 1 ? "s" : ""}
                </button>
              </div>
            </div>
          )}

          {/* ─── Step 3: Choose List ─── */}
          {step === 3 && (
            <div className="space-y-6">
              <div className="text-center">
                <div className="w-12 h-12 bg-blue-100 text-blue-600 rounded-full flex items-center justify-center mx-auto mb-3">
                  <ListIcon size={24} />
                </div>
                <h4 className="text-lg font-medium text-gray-900">Save to Contact List</h4>
                <p className="text-sm text-gray-500 mt-1">
                  You are saving <strong>{selectedIds.size}</strong> contacts. Select or create a list to add them to.
                </p>
              </div>

              <div className="bg-gray-50 rounded-xl p-5 border border-gray-200">
                <div className="flex gap-2 mb-4">
                  <button
                    type="button"
                    onClick={() => setSaveMode("existing")}
                    className={`flex-1 py-2.5 rounded-lg text-sm font-medium border-2 transition-colors ${
                      saveMode === "existing"
                        ? "border-blue-500 bg-blue-50 text-blue-700"
                        : "border-gray-200 text-gray-600 hover:border-gray-300 bg-white"
                    }`}
                  >
                    Existing List
                  </button>
                  <button
                    type="button"
                    onClick={() => setSaveMode("new")}
                    className={`flex-1 py-2.5 rounded-lg text-sm font-medium border-2 transition-colors ${
                      saveMode === "new"
                        ? "border-blue-500 bg-blue-50 text-blue-700"
                        : "border-gray-200 text-gray-600 hover:border-gray-300 bg-white"
                    }`}
                  >
                    + Create New List
                  </button>
                </div>

                {saveMode === "existing" && (
                  lists.length === 0 ? (
                    <p className="text-sm text-gray-500 text-center py-4 bg-white border border-gray-200 rounded-lg">
                      No lists yet. Please switch to &quot;Create New List&quot;.
                    </p>
                  ) : (
                    <div className="space-y-2 max-h-52 overflow-y-auto pr-1">
                      {lists.map((l) => (
                        <button
                          key={l.id}
                          type="button"
                          onClick={() => setSelectedListId(l.id)}
                          className={`w-full flex items-center justify-between px-3 py-3 rounded-lg border-2 text-left transition-colors bg-white ${
                            selectedListId === l.id
                              ? "border-blue-500 bg-blue-50"
                              : "border-gray-200 hover:border-gray-300"
                          }`}
                        >
                          <div>
                            <p className="font-medium text-gray-900 text-sm">{l.name}</p>
                            <p className="text-xs text-gray-500">{l.contacts.length} current contacts</p>
                          </div>
                          {selectedListId === l.id && <Check size={18} className="text-blue-600 shrink-0" />}
                        </button>
                      ))}
                    </div>
                  )
                )}

                {saveMode === "new" && (
                  <div className="bg-white p-4 rounded-lg border border-gray-200 shadow-sm">
                    <label className="block text-sm font-medium text-gray-700 mb-1">New List Name</label>
                    <input
                      value={newListName}
                      onChange={(e) => setNewListName(e.target.value)}
                      className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm text-gray-900 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      placeholder='e.g. "Tech Companies Q2"'
                      autoFocus
                    />
                  </div>
                )}

                {/* Campaign Selection */}
                <div className="mt-4 pt-4 border-t border-gray-200">
                  <label className="block text-sm font-medium text-gray-700 mb-1.5">Assign to Campaign (Optional)</label>
                  <select
                    value={selectedCampaignId}
                    onChange={(e) => setSelectedCampaignId(e.target.value)}
                    className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm text-gray-900 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white"
                  >
                    <option value="">No Campaign (Global Only)</option>
                    {campaigns.map(c => (
                      <option key={c.id} value={c.id}>{c.name}</option>
                    ))}
                  </select>
                  <p className="text-[11px] text-gray-500 mt-1.5 px-0.5">
                    Connecting these contacts to a campaign will make them available for automatic outreach steps.
                  </p>
                </div>
              </div>

              <div className="flex gap-3 pt-2">
                <button
                  type="button"
                  onClick={() => setStep(2)}
                  className="px-5 py-2.5 border border-gray-300 text-gray-700 text-sm font-medium rounded-lg hover:bg-gray-50 transition-colors"
                >
                  Back
                </button>
                <button
                  type="button"
                  onClick={confirmSaveToList}
                  disabled={isSavingToDb || (saveMode === "existing" ? !selectedListId : !newListName.trim())}
                  className="flex-1 px-5 py-2.5 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed text-white text-sm font-medium rounded-lg transition-colors flex items-center justify-center gap-2"
                >
                  {isSavingToDb ? (
                    <span className="flex items-center gap-2">Saving...</span>
                  ) : (
                    <>
                      <CheckCircle2 size={16} />
                      Confirm Save
                    </>
                  )}
                </button>
              </div>
            </div>
          )}

          {/* ─── Step 4: Done ─── */}
          {step === 4 && (
            <div className="space-y-4 text-center py-6">
              <div className="w-16 h-16 bg-green-100 text-green-600 rounded-full flex items-center justify-center mx-auto shadow-sm">
                <CheckCircle2 size={32} />
              </div>
              <div>
                <h4 className="text-lg font-medium text-gray-900">
                  Import Complete
                </h4>
                <p className="text-sm text-gray-500 mt-1">
                  Successfully imported {savedCount} contacts to your list.
                </p>
              </div>
              <div className="pt-4">
                <button
                  onClick={() => {
                    onClose();
                    onComplete();
                  }}
                  className="w-full px-5 py-2.5 bg-gray-900 hover:bg-gray-800 text-white text-sm font-medium rounded-lg transition-colors shadow-sm"
                >
                  View Contacts / Close
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
