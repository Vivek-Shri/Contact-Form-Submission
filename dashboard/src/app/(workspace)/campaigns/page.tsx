"use client";

import Link from "next/link";
import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { Plus, Trash2, X, FolderKanban } from "lucide-react";

import type { CampaignRecord, PaginationMeta } from "@/lib/models";
import { formatDateTime, statusTone } from "@/lib/ui";

interface CampaignListResponse {
  campaigns: CampaignRecord[];
  pagination?: PaginationMeta;
}

const PAGE_SIZE = 25;

export default function CampaignsPage() {
  const [campaigns, setCampaigns] = useState<CampaignRecord[]>([]);
  const [pagination, setPagination] = useState<PaginationMeta>({
    page: 1,
    limit: PAGE_SIZE,
    total: 0,
    totalPages: 1,
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [busyCampaignId, setBusyCampaignId] = useState<string | null>(null);
  const [searchInput, setSearchInput] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [page, setPage] = useState(1);

  // New campaign modal
  const [showNewModal, setShowNewModal] = useState(false);
  const [newName, setNewName] = useState("");
  const [newAiInstruction, setNewAiInstruction] = useState("");
  const [newMaxDaily, setNewMaxDaily] = useState(100);
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState("");

  const totalPages = useMemo(() => {
    const candidate = pagination.totalPages ?? pagination.total_pages ?? 1;
    return Math.max(1, candidate);
  }, [pagination]);

  const loadCampaigns = useCallback(async () => {
    setLoading(true);
    setError("");
    const params = new URLSearchParams();
    params.set("page", String(page));
    params.set("limit", String(PAGE_SIZE));
    if (searchQuery.trim()) params.set("q", searchQuery.trim());
    try {
      const res = await fetch(`/api/campaigns?${params.toString()}`, { cache: "no-store" });
      const payload = (await res.json()) as CampaignListResponse | { error?: string };
      if (!res.ok) {
        setError(("error" in payload && payload.error) || "Unable to load campaigns.");
        return;
      }
      const data = payload as CampaignListResponse;
      setCampaigns(data.campaigns ?? []);
      const inc = data.pagination;
      setPagination({
        page: inc?.page ?? page,
        limit: inc?.limit ?? PAGE_SIZE,
        total: inc?.total ?? (data.campaigns ?? []).length,
        total_pages: inc?.total_pages,
        totalPages: inc?.totalPages ?? inc?.total_pages ?? 1,
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load campaigns.");
    } finally {
      setLoading(false);
    }
  }, [page, searchQuery]);

  useEffect(() => { void loadCampaigns(); }, [loadCampaigns]);

  const submitSearch = useCallback((e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setPage(1);
    setSearchQuery(searchInput.trim());
  }, [searchInput]);

  const clearSearch = useCallback(() => {
    setSearchInput("");
    setSearchQuery("");
    setPage(1);
  }, []);

  const deleteCampaign = useCallback(async (campaignId: string) => {
    if (!globalThis.confirm("Delete this campaign and all its contacts?")) return;
    setBusyCampaignId(campaignId);
    try {
      const res = await fetch(`/api/campaigns/${campaignId}`, { method: "DELETE" });
      const payload = (await res.json()) as { error?: string };
      if (!res.ok) { setError(payload.error || "Unable to delete campaign."); return; }
      await loadCampaigns();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to delete campaign.");
    } finally {
      setBusyCampaignId(null);
    }
  }, [loadCampaigns]);

  const createCampaign = useCallback(async () => {
    if (!newName.trim()) { setCreateError("Campaign name is required."); return; }
    setCreating(true);
    setCreateError("");
    try {
      const res = await fetch("/api/campaigns", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: newName.trim(),
          aiInstruction: newAiInstruction.trim(),
          maxDailySubmissions: Math.max(1, Math.round(newMaxDaily || 1)),
          status: "draft",
        }),
      });
      const payload = (await res.json()) as CampaignRecord | { error?: string };
      if (!res.ok || !("id" in payload)) {
        setCreateError(("error" in payload && payload.error) || "Unable to create campaign.");
        return;
      }
      setShowNewModal(false);
      setNewName("");
      setNewAiInstruction("");
      setNewMaxDaily(100);
      await loadCampaigns();
    } catch (err) {
      setCreateError(err instanceof Error ? err.message : "Unable to create campaign.");
    } finally {
      setCreating(false);
    }
  }, [newName, newAiInstruction, newMaxDaily, loadCampaigns]);

  return (
    <div className="page-stack">
      {/* New Campaign Modal */}
      {showNewModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm p-4">
          <div className="w-full max-w-xl bg-white rounded-2xl shadow-xl overflow-hidden flex flex-col">
            <div className="flex justify-between items-center px-6 py-4 border-b border-gray-100">
              <h3 className="text-lg font-semibold text-gray-900">New Campaign</h3>
              <button onClick={() => setShowNewModal(false)} className="p-1 text-gray-400 hover:text-gray-600">
                <X size={20} />
              </button>
            </div>
            <div className="p-6 space-y-4">
              {createError && (
                <p className="text-sm text-red-600 bg-red-50 border border-red-200 rounded-lg px-3 py-2">{createError}</p>
              )}
              <label className="block">
                <span className="text-sm font-medium text-gray-700">Campaign Name *</span>
                <input
                  value={newName}
                  onChange={(e) => setNewName(e.target.value)}
                  className="field-input mt-1"
                  placeholder='e.g. "Q2 Tech Outreach"'
                  autoFocus
                  onKeyDown={(e) => e.key === "Enter" && void createCampaign()}
                />
              </label>
              <label className="block">
                <span className="text-sm font-medium text-gray-700">Daily Successful Submissions Limit</span>
                <input
                  type="number"
                  min={1}
                  max={100000}
                  value={newMaxDaily}
                  onChange={(e) => setNewMaxDaily(Number(e.target.value || 1))}
                  className="field-input mt-1"
                />
              </label>
              <label className="block">
                <span className="text-sm font-medium text-gray-700">AI Instruction (Step 1)</span>
                <textarea
                  value={newAiInstruction}
                  onChange={(e) => setNewAiInstruction(e.target.value)}
                  rows={4}
                  className="field-input field-textarea mt-1"
                  placeholder="Describe what the AI should say when filling out contact forms..."
                />
              </label>
              <div className="flex gap-3 pt-2">
                <button
                  type="button"
                  onClick={() => void createCampaign()}
                  disabled={creating}
                  className="flex-1 px-5 py-2.5 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-300 text-white text-sm font-medium rounded-lg transition-colors"
                >
                  {creating ? "Creating..." : "Create Campaign"}
                </button>
                <button
                  type="button"
                  onClick={() => setShowNewModal(false)}
                  className="px-5 py-2.5 border border-gray-300 text-gray-700 text-sm font-medium rounded-lg hover:bg-gray-50 transition-colors"
                >
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      <section className="panel">
        <div className="panel-header">
          <div className="flex items-center gap-2">
            <FolderKanban size={20} />
            <h2>Campaigns</h2>
          </div>
          <button
            type="button"
            onClick={() => setShowNewModal(true)}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition shadow-sm"
          >
            <Plus size={16} /> New Campaign
          </button>
        </div>

        <form onSubmit={submitSearch} className="button-row search-toolbar">
          <input
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            className="field-input search-input-wide"
            placeholder="Search campaigns by name or status..."
          />
          <button type="submit" className="button-secondary">Search</button>
          <button type="button" className="button-secondary" onClick={clearSearch}>Clear</button>
        </form>

        {loading && <p className="panel-muted">Loading campaigns...</p>}
        {error && <p className="panel-error">{error}</p>}

        <div className="table-wrap">
          <table className="clean-table">
            <thead>
              <tr>
                <th>Campaign Name</th>
                <th>Status</th>
                <th>Daily Limit (Successes)</th>
                <th>Contacts</th>
                <th>Updated At</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {!loading && campaigns.length === 0 ? (
                <tr>
                  <td colSpan={6} className="table-empty">
                    No campaigns yet. Create your first one above.
                  </td>
                </tr>
              ) : (
                campaigns.map((campaign) => (
                  <tr key={campaign.id}>
                    <td>
                      <Link href={`/campaigns/${campaign.id}`} className="table-link font-medium">
                        {campaign.name}
                      </Link>
                    </td>
                    <td>
                      <span className={`status-chip ${statusTone(campaign.status)}`}>{campaign.status}</span>
                    </td>
                    <td>{campaign.maxDailySubmissions}</td>
                    <td>{campaign.contactCount}</td>
                    <td>{formatDateTime(campaign.updatedAt)}</td>
                    <td>
                      <div className="flex items-center gap-2">
                        <Link
                          href={`/campaigns/${campaign.id}`}
                          className="text-xs font-medium px-3 py-1.5 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition"
                        >
                          Open
                        </Link>
                        <button
                          type="button"
                          className="table-delete flex items-center gap-1"
                          onClick={() => void deleteCampaign(campaign.id)}
                          disabled={busyCampaignId === campaign.id}
                        >
                          <Trash2 size={13} />
                          {busyCampaignId === campaign.id ? "Deleting..." : "Delete"}
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
          <p className="panel-muted pagination-summary">
            Showing {campaigns.length} of {pagination.total} campaign(s)
          </p>
          <div className="button-row">
            <button
              type="button"
              className="button-secondary"
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={loading || page <= 1}
            >
              Previous
            </button>
            <p className="panel-muted pagination-label">Page {page} of {totalPages}</p>
            <button
              type="button"
              className="button-secondary"
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={loading || page >= totalPages}
            >
              Next
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}
