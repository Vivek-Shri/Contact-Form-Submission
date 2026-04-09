"use client";

import Link from "next/link";
import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";

import type { ContactRecord, PaginationMeta } from "@/lib/models";
import { formatDateTime } from "@/lib/ui";

interface ContactListResponse {
  contacts: ContactRecord[];
  pagination?: PaginationMeta;
}

const PAGE_SIZE = 50;

export default function ContactsPage() {
  const [contacts, setContacts] = useState<ContactRecord[]>([]);
  const [pagination, setPagination] = useState<PaginationMeta>({
    page: 1,
    limit: PAGE_SIZE,
    total: 0,
    totalPages: 1,
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [searchInput, setSearchInput] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [page, setPage] = useState(1);

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
    if (searchQuery.trim()) {
      params.set("q", searchQuery.trim());
    }

    try {
      const response = await fetch(`/api/contacts?${params.toString()}`, { cache: "no-store" });
      const payload = (await response.json()) as ContactListResponse | { error?: string };

      if (!response.ok) {
        setError(("error" in payload && payload.error) || "Unable to load contacts.");
        return;
      }

      const data = payload as ContactListResponse;
      setContacts(data.contacts ?? []);
      const incoming = data.pagination;
      setPagination({
        page: incoming?.page ?? page,
        limit: incoming?.limit ?? PAGE_SIZE,
        total: incoming?.total ?? (data.contacts ?? []).length,
        total_pages: incoming?.total_pages,
        totalPages: incoming?.totalPages ?? incoming?.total_pages ?? 1,
      });
    } catch (requestError) {
      const message = requestError instanceof Error ? requestError.message : "Unable to load contacts.";
      setError(message);
    } finally {
      setLoading(false);
    }
  }, [page, searchQuery]);

  useEffect(() => {
    void loadContacts();
  }, [loadContacts]);

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

  return (
    <div className="page-stack">
      <section className="panel">
        <div className="panel-header">
          <h2>All Contacts</h2>
        </div>

        <form onSubmit={submitSearch} className="button-row search-toolbar">
          <input
            value={searchInput}
            onChange={(event) => setSearchInput(event.target.value)}
            className="field-input search-input-wide"
            placeholder="Search by company, domain, URL, or campaign"
          />
          <button type="submit" className="button-secondary">
            Search
          </button>
          <button type="button" className="button-secondary" onClick={clearSearch}>
            Clear
          </button>
        </form>

        {loading ? <p className="panel-muted">Loading contacts...</p> : null}
        {error ? <p className="panel-error">{error}</p> : null}

        <div className="table-wrap">
          <table className="clean-table">
            <thead>
              <tr>
                <th>Company</th>
                <th>Domain</th>
                <th>Campaign</th>
                <th>Contact URL</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {!loading && contacts.length === 0 ? (
                <tr>
                  <td colSpan={5} className="table-empty">
                    No contacts available.
                  </td>
                </tr>
              ) : (
                contacts.map((contact) => (
                  <tr key={contact.id}>
                    <td>{contact.companyName}</td>
                    <td>{contact.domain || "-"}</td>
                    <td>
                      {contact.campaignId ? (
                        <Link href={`/campaigns/${contact.campaignId}`} className="table-link">
                          {contact.campaignName || contact.campaignId}
                        </Link>
                      ) : (
                        "-"
                      )}
                    </td>
                    <td>
                      <a href={contact.contactUrl} target="_blank" rel="noreferrer" className="table-link">
                        {contact.contactUrl}
                      </a>
                    </td>
                    <td>{formatDateTime(contact.updatedAt)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        <div className="button-row pagination-row">
          <p className="panel-muted pagination-summary">
            Showing {contacts.length} of {pagination.total} contact(s)
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
