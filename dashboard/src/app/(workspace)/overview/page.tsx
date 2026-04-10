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
        const statusRes = await fetch("/api/outreach/status", { cache: "no-store" }).catch(() => null);
        if (statusRes && statusRes.ok) {
          const data = await statusRes.json() as {
            run_id?: string;
            status?: string;
            running?: boolean;
            exit_code?: number;
            progress?: number;
            total_leads?: number;
            processed_leads?: number;
            current_lead?: string;
            logs?: string[];
            results?: unknown[];
            duplicates_skipped?: number;
            started_at?: string;
          };
          if (data && data.run_id) {
            let status = data.status || "idle";
            const runningFlag = Boolean(data.running);
            if (status === "running" && !runningFlag) {
              if (data.exit_code === 0) status = "completed";
              else if (data.exit_code !== undefined && data.exit_code !== null) status = "failed";
              else status = "stopped";
            }

            setLiveRun({
              runId: data.run_id,
              status: status,
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

  const getBannerConfig = (status: string) => {
    switch (status) {
      case "completed":
        return {
          title: "Run Completed",
          border: "#3b82f6",
          bg: "linear-gradient(135deg, #eff6ff, #dbeafe)",
          titleColor: "#1e3a8a",
          showStop: false,
          progressBg: "#bfdbfe",
          progressFill: "#2563eb",
        };
      case "failed":
        return {
          title: "Run Failed",
          border: "#ef4444",
          bg: "linear-gradient(135deg, #fef2f2, #fee2e2)",
          titleColor: "#991b1b",
          showStop: false,
          progressBg: "#fecaca",
          progressFill: "#dc2626",
        };
      case "stopped":
      case "stopping":
      case "cancelled":
        return {
          title: "Run Stopped",
          border: "#f59e0b",
          bg: "linear-gradient(135deg, #fffbeb, #fef3c7)",
          titleColor: "#92400e",
          showStop: false,
          progressBg: "#fde68a",
          progressFill: "#d97706",
        };
      default:
        return {
          title: "Run In Progress",
          border: "#22c55e",
          bg: "linear-gradient(135deg, #f0fdf4, #dcfce7)",
          titleColor: "#166534",
          showStop: true,
          progressBg: "#bbf7d0",
          progressFill: "#16a34a",
        };
    }
  };

  if (loading) {
    return <p className="panel-muted">Loading overview...</p>;
  }

  if (error) {
    return <p className="panel-error">{error}</p>;
  }

  const runBanner = liveRun ? getBannerConfig(liveRun.status) : null;

  return (
    <div className="page-stack">
      {/* Live Run Banner */}
      {liveRun && runBanner && (
        <section className="panel" style={{ borderLeft: `4px solid ${runBanner.border}`, background: runBanner.bg }}>
          <div className="panel-header" style={{ gap: "1rem" }}>
            <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
              {runBanner.showStop && (
                <span style={{ width: 10, height: 10, borderRadius: "50%", background: runBanner.border, display: "inline-block", animation: "pulse 1.5s infinite" }} />
              )}
              {!runBanner.showStop && (
                <span style={{ width: 10, height: 10, borderRadius: "50%", background: runBanner.border, display: "inline-block" }} />
              )}
              <h2 style={{ color: runBanner.titleColor, margin: 0 }}>{runBanner.title}</h2>
            </div>
            {runBanner.showStop && (
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
            )}
          </div>
          <div style={{ padding: "0.5rem 1.5rem 1rem", display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))", gap: "1rem" }}>
            <div>
              <p className="meta-label">Run ID</p>
              <p style={{ fontFamily: "monospace", fontSize: "0.8rem", wordBreak: "break-all" }}>{liveRun.runId}</p>
            </div>
            <div>
              <p className="meta-label">Progress</p>
              <p style={{ fontWeight: 600, color: runBanner.titleColor }}>{liveRun.processedLeads} / {liveRun.totalLeads}</p>
              <div style={{ background: runBanner.progressBg, borderRadius: 4, height: 6, marginTop: 4 }}>
                <div style={{ background: runBanner.progressFill, height: 6, borderRadius: 4, width: `${liveRun.progress ?? 0}%`, transition: "width 0.4s" }} />
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
