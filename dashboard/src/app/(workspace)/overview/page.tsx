"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import type { CampaignRecord, ContactRecord, OutreachRunSnapshot } from "@/lib/models";
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
  const [liveRun, setLiveRun] = useState<OutreachRunSnapshot | null>(null);
  const [stoppingRun, setStoppingRun] = useState(false);

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

  // Poll live run status every 3s
  useEffect(() => {
    const poll = async () => {
      try {
        const res = await fetch("/api/outreach/run?runId=current", { cache: "no-store" }).catch(() => null);
        if (res && res.ok) {
          const data = await res.json() as OutreachRunSnapshot;
          if (data && data.runId) {
            setLiveRun(data);
            return;
          }
        }
        // Fallback to status endpoint
        const statusRes = await fetch("/api/outreach/status", { cache: "no-store" }).catch(() => null);
        if (statusRes && statusRes.ok) {
          const data = await statusRes.json() as any;
          if (data && data.run_id && (data.status === "running" || data.running)) {
            setLiveRun({
              runId: data.run_id,
              status: data.status || "running",
              progress: data.progress ?? 0,
              totalLeads: data.total_leads ?? 0,
              processedLeads: data.processed_leads ?? 0,
              currentLead: data.current_lead ?? "-",
              logs: data.logs ?? [],
              results: data.results ?? [],
              duplicatesSkipped: data.duplicates_skipped ?? 0,
              startedAt: data.started_at ?? "",
            });
          } else {
            setLiveRun(null);
          }
        }
      } catch (err) {
        console.error("Live run poll error", err);
      }
    };

    void poll();
    const timer = setInterval(() => void poll(), 3000);
    return () => clearInterval(timer);
  }, []);

  const stopRun = useCallback(async () => {
    if (!liveRun) return;
    setStoppingRun(true);
    try {
      const response = await fetch("/api/outreach/run/stop", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ runId: liveRun.runId }),
      });
      const payload = await response.json() as OutreachRunSnapshot | { error?: string };
      if ("runId" in payload) {
        setLiveRun(payload as OutreachRunSnapshot);
      }
    } catch (err) {
      console.error("Stop run error", err);
    } finally {
      setStoppingRun(false);
    }
  }, [liveRun]);

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
      {/* Live Run Banner */}
      {liveRun && (liveRun.status === "running" || liveRun.status === "queued") && (
        <section className="panel" style={{ borderLeft: "4px solid #22c55e", background: "linear-gradient(135deg, #f0fdf4, #dcfce7)" }}>
          <div className="panel-header" style={{ gap: "1rem" }}>
            <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
              <span style={{ width: 10, height: 10, borderRadius: "50%", background: "#22c55e", display: "inline-block", animation: "pulse 1.5s infinite" }} />
              <h2 style={{ color: "#166534", margin: 0 }}>Run In Progress</h2>
            </div>
            <button
              type="button"
              onClick={() => void stopRun()}
              disabled={stoppingRun}
              style={{
                background: stoppingRun ? "#dc2626" : "#ef4444",
                color: "#fff",
                border: "none",
                borderRadius: "8px",
                padding: "0.5rem 1.25rem",
                fontWeight: 600,
                cursor: stoppingRun ? "not-allowed" : "pointer",
                opacity: stoppingRun ? 0.7 : 1,
                fontSize: "0.875rem",
              }}
            >
              {stoppingRun ? "Stopping..." : "⏹ Stop Run"}
            </button>
          </div>
          <div style={{ padding: "0.5rem 1.5rem 1rem", display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))", gap: "1rem" }}>
            <div>
              <p className="meta-label">Run ID</p>
              <p style={{ fontFamily: "monospace", fontSize: "0.8rem", wordBreak: "break-all" }}>{liveRun.runId}</p>
            </div>
            <div>
              <p className="meta-label">Progress</p>
              <p style={{ fontWeight: 600, color: "#166534" }}>{liveRun.processedLeads} / {liveRun.totalLeads}</p>
              <div style={{ background: "#bbf7d0", borderRadius: 4, height: 6, marginTop: 4 }}>
                <div style={{ background: "#16a34a", height: 6, borderRadius: 4, width: `${liveRun.progress ?? 0}%`, transition: "width 0.4s" }} />
              </div>
            </div>
            <div>
              <p className="meta-label">Current Lead</p>
              <p style={{ fontSize: "0.8rem", wordBreak: "break-all" }}>{liveRun.currentLead}</p>
            </div>
            <div>
              <p className="meta-label">Started</p>
              <p>{formatDateTime(liveRun.startedAt)}</p>
            </div>
          </div>
        </section>
      )}
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
