"use client";

import Link from "next/link";
import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";

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
    if (searchQuery.trim()) {
      params.set("q", searchQuery.trim());
    }

    try {
      const response = await fetch(`/api/campaigns?${params.toString()}`, {
        cache: "no-store",
      });
      const payload = (await response.json()) as CampaignListResponse | { error?: string };

      if (!response.ok) {
        const message = "error" in payload && payload.error ? payload.error : "Unable to load campaigns.";
        setError(message);
        return;
      }

      const data = payload as CampaignListResponse;
      setCampaigns(data.campaigns ?? []);
      const incoming = data.pagination;
      setPagination({
        page: incoming?.page ?? page,
        limit: incoming?.limit ?? PAGE_SIZE,
        total: incoming?.total ?? (data.campaigns ?? []).length,
        total_pages: incoming?.total_pages,
        totalPages: incoming?.totalPages ?? incoming?.total_pages ?? 1,
      });
    } catch (requestError) {
      const message = requestError instanceof Error ? requestError.message : "Unable to load campaigns.";
      setError(message);
    } finally {
      setLoading(false);
    }
  }, [page, searchQuery]);

  useEffect(() => {
    void loadCampaigns();
  }, [loadCampaigns]);

  const submitSearch = useCallback((event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setPage(1);
    setSearchQuery(searchInput.trim());
  }, [searchInput]);

  const clearSearch = useCallback(() => {
    setSearchInput("");
    setSearchQuery("");
    setPage(1);
  }, []);

  const deleteCampaign = useCallback(
    async (campaignId: string) => {
      const confirmed = globalThis.confirm("Delete this campaign and all campaign contacts?");
      if (!confirmed) {
        return;
      }

      setBusyCampaignId(campaignId);
      try {
        const response = await fetch(`/api/campaigns/${campaignId}`, {
          method: "DELETE",
        });
        const payload = (await response.json()) as { error?: string };

        if (!response.ok) {
          setError(payload.error || "Unable to delete campaign.");
          return;
        }

        await loadCampaigns();
      } catch (requestError) {
        const message = requestError instanceof Error ? requestError.message : "Unable to delete campaign.";
        setError(message);
      } finally {
        setBusyCampaignId(null);
      }
    },
    [loadCampaigns],
  );

  return (
    <div className="page-stack">
      <section className="panel">
        <div className="panel-header">
          <h2>Campaign Table</h2>
          <Link href="/campaigns/new" className="button-link">
            Add campaign
          </Link>
        </div>

        <form onSubmit={submitSearch} className="button-row search-toolbar">
          <input
            value={searchInput}
            onChange={(event) => setSearchInput(event.target.value)}
            className="field-input search-input-wide"
            placeholder="Search by campaign name, status, or description"
          />
          <button type="submit" className="button-secondary">
            Search
          </button>
          <button type="button" className="button-secondary" onClick={clearSearch}>
            Clear
          </button>
        </form>

        {loading ? <p className="panel-muted">Loading campaigns...</p> : null}
        {error ? <p className="panel-error">{error}</p> : null}

        <div className="table-wrap">
          <table className="clean-table">
            <thead>
              <tr>
                <th>Campaign Name</th>
                <th>Status</th>
                <th>Daily Limit</th>
                <th>Contacts</th>
                <th>Updated At</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {!loading && campaigns.length === 0 ? (
                <tr>
                  <td colSpan={6} className="table-empty">
                    No campaigns available.
                  </td>
                </tr>
              ) : (
                campaigns.map((campaign) => (
                  <tr key={campaign.id}>
                    <td>
                      <Link href={`/campaigns/${campaign.id}`} className="table-link">
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
                      <button
                        type="button"
                        className="table-delete"
                        onClick={() => void deleteCampaign(campaign.id)}
                        disabled={busyCampaignId === campaign.id}
                      >
                        {busyCampaignId === campaign.id ? "Deleting..." : "Delete"}
                      </button>
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
              onClick={() => setPage((previous) => Math.max(1, previous - 1))}
              disabled={loading || page <= 1}
            >
              Previous
            </button>
            <p className="panel-muted pagination-label">
              Page {page} of {totalPages}
            </p>
            <button
              type="button"
              className="button-secondary"
              onClick={() => setPage((previous) => Math.min(totalPages, previous + 1))}
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
