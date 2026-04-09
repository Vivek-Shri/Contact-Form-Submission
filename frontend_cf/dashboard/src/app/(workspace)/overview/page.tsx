"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import type { CampaignRecord, ContactRecord } from "@/lib/models";
import { formatDateTime, statusTone } from "@/lib/ui";

interface CampaignListResponse {
  campaigns: CampaignRecord[];
}

interface ContactListResponse {
  contacts: ContactRecord[];
}

export default function OverviewPage() {
  const [campaigns, setCampaigns] = useState<CampaignRecord[]>([]);
  const [contacts, setContacts] = useState<ContactRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      setError("");

      try {
        const [campaignsRes, contactsRes] = await Promise.all([
          fetch("/api/campaigns", { cache: "no-store" }),
          fetch("/api/contacts", { cache: "no-store" }),
        ]);

        const campaignsPayload = (await campaignsRes.json()) as CampaignListResponse | { error?: string };
        const contactsPayload = (await contactsRes.json()) as ContactListResponse | { error?: string };

        if (!campaignsRes.ok || !contactsRes.ok) {
          const message =
            ("error" in campaignsPayload && campaignsPayload.error) ||
            ("error" in contactsPayload && contactsPayload.error) ||
            "Unable to load overview data.";
          setError(message);
          return;
        }

        const campaignData = campaignsPayload as CampaignListResponse;
        const contactData = contactsPayload as ContactListResponse;
        setCampaigns(campaignData.campaigns ?? []);
        setContacts(contactData.contacts ?? []);
      } catch (requestError) {
        const message =
          requestError instanceof Error ? requestError.message : "Unable to load overview data.";
        setError(message);
      } finally {
        setLoading(false);
      }
    };

    void load();
  }, []);

  const activeCampaigns = useMemo(
    () => campaigns.filter((campaign) => campaign.status === "active").length,
    [campaigns],
  );

  const campaignsWithRuns = useMemo(
    () => campaigns.filter((campaign) => Boolean(campaign.lastRun)).length,
    [campaigns],
  );

  if (loading) {
    return <p className="panel-muted">Loading overview...</p>;
  }

  if (error) {
    return <p className="panel-error">{error}</p>;
  }

  return (
    <div className="page-stack">
      <section className="grid-cards">
        <article className="stat-card">
          <p>Total Campaigns</p>
          <h3>{campaigns.length}</h3>
        </article>
        <article className="stat-card">
          <p>Active Campaigns</p>
          <h3>{activeCampaigns}</h3>
        </article>
        <article className="stat-card">
          <p>Total Contacts</p>
          <h3>{contacts.length}</h3>
        </article>
        <article className="stat-card">
          <p>Campaigns With Runs</p>
          <h3>{campaignsWithRuns}</h3>
        </article>
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2>Recent Campaigns</h2>
          <Link href="/campaigns" className="button-link">
            Open campaigns
          </Link>
        </div>

        <div className="table-wrap">
          <table className="clean-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Status</th>
                <th>Contacts</th>
                <th>Last Updated</th>
                <th>Latest Run</th>
              </tr>
            </thead>
            <tbody>
              {campaigns.length === 0 ? (
                <tr>
                  <td colSpan={5} className="table-empty">
                    No campaigns yet.
                  </td>
                </tr>
              ) : (
                campaigns.slice(0, 8).map((campaign) => (
                  <tr key={campaign.id}>
                    <td>
                      <Link href={`/campaigns/${campaign.id}`} className="table-link">
                        {campaign.name}
                      </Link>
                    </td>
                    <td>
                      <span className={`status-chip ${statusTone(campaign.status)}`}>
                        {campaign.status}
                      </span>
                    </td>
                    <td>{campaign.contactCount}</td>
                    <td>{formatDateTime(campaign.updatedAt)}</td>
                    <td>{campaign.lastRun ? `${campaign.lastRun.status} (${campaign.lastRun.runId})` : "-"}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
