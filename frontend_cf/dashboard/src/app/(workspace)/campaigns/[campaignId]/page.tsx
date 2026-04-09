"use client";

import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { useParams, useRouter } from "next/navigation";

import type {
  CampaignRecord,
  CampaignRunSummary,
  ContactRecord,
  OutreachRunSnapshot,
} from "@/lib/models";
import { formatDateTime, statusTone } from "@/lib/ui";

interface CampaignContactsResponse {
  contacts: ContactRecord[];
}

interface CampaignRunsResponse {
  runs: CampaignRunSummary[];
}

const RUN_POLL_INTERVAL_MS = 2500;
const CAMPAIGN_STATUS_OPTIONS = ["draft", "active", "paused", "archived"] as const;

function isActiveRun(status: string): boolean {
  const normalized = status.trim().toLowerCase();
  return normalized === "running" || normalized === "queued";
}

export default function CampaignDetailPage() {
  const router = useRouter();
  const params = useParams<{ campaignId: string }>();
  const campaignId = params.campaignId;

  const [campaign, setCampaign] = useState<CampaignRecord | null>(null);
  const [contacts, setContacts] = useState<ContactRecord[]>([]);
  const [runs, setRuns] = useState<CampaignRunSummary[]>([]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const [runSnapshot, setRunSnapshot] = useState<OutreachRunSnapshot | null>(null);
  const [startingRun, setStartingRun] = useState(false);
  const [stoppingRun, setStoppingRun] = useState(false);

  const [companyName, setCompanyName] = useState("");
  const [contactUrl, setContactUrl] = useState("");
  const [location, setLocation] = useState("");
  const [industry, setIndustry] = useState("");
  const [notes, setNotes] = useState("");
  const [addingContact, setAddingContact] = useState(false);
  const [deletingContactId, setDeletingContactId] = useState<string | null>(null);

  const [editingCampaign, setEditingCampaign] = useState(false);
  const [savingCampaign, setSavingCampaign] = useState(false);
  const [editName, setEditName] = useState("");
  const [editStatus, setEditStatus] = useState<CampaignRecord["status"]>("draft");
  const [editMaxDaily, setEditMaxDaily] = useState(100);
  const [editDescription, setEditDescription] = useState("");
  const [editAiInstruction, setEditAiInstruction] = useState("");

  const loadCampaignBundle = useCallback(async () => {
    setLoading(true);
    setError("");

    try {
      const [campaignRes, contactsRes, runsRes] = await Promise.all([
        fetch(`/api/campaigns/${campaignId}`, { cache: "no-store" }),
        fetch(`/api/campaigns/${campaignId}/contacts`, { cache: "no-store" }),
        fetch(`/api/campaigns/${campaignId}/runs?limit=50`, { cache: "no-store" }),
      ]);

      const campaignPayload = (await campaignRes.json()) as CampaignRecord | { error?: string };
      const contactsPayload = (await contactsRes.json()) as CampaignContactsResponse | { error?: string };
      const runsPayload = (await runsRes.json()) as CampaignRunsResponse | { error?: string };

      if (!campaignRes.ok || !contactsRes.ok || !runsRes.ok) {
        const messageText =
          ("error" in campaignPayload && campaignPayload.error) ||
          ("error" in contactsPayload && contactsPayload.error) ||
          ("error" in runsPayload && runsPayload.error) ||
          "Unable to load campaign details.";
        setError(messageText);
        return;
      }

      const campaignData = campaignPayload as CampaignRecord;
      const contactData = contactsPayload as CampaignContactsResponse;
      const runData = runsPayload as CampaignRunsResponse;
      setCampaign(campaignData);
      setContacts(contactData.contacts ?? []);
      setRuns(runData.runs ?? []);
    } catch (requestError) {
      const messageText =
        requestError instanceof Error ? requestError.message : "Unable to load campaign details.";
      setError(messageText);
    } finally {
      setLoading(false);
    }
  }, [campaignId]);

  useEffect(() => {
    void loadCampaignBundle();
  }, [loadCampaignBundle]);

  useEffect(() => {
    if (!campaign || editingCampaign) {
      return;
    }

    setEditName(campaign.name);
    setEditStatus(campaign.status);
    setEditMaxDaily(campaign.maxDailySubmissions);
    setEditDescription(campaign.description || "");
    setEditAiInstruction(campaign.aiInstruction || "");
  }, [campaign, editingCampaign]);

  const inspectRun = useCallback(async (runId: string) => {
    try {
      const response = await fetch(`/api/outreach/run?runId=${encodeURIComponent(runId)}`, {
        cache: "no-store",
      });
      const payload = (await response.json()) as OutreachRunSnapshot | { error?: string };
      if (!response.ok || !("runId" in payload)) {
        setMessage(("error" in payload && payload.error) || "Unable to inspect run.");
        return;
      }
      setRunSnapshot(payload);
    } catch (requestError) {
      const messageText = requestError instanceof Error ? requestError.message : "Unable to inspect run.";
      setMessage(messageText);
    }
  }, []);

  useEffect(() => {
    if (!runSnapshot || !isActiveRun(runSnapshot.status)) {
      return;
    }

    const timer = globalThis.setInterval(async () => {
      try {
        const response = await fetch(`/api/outreach/run?runId=${encodeURIComponent(runSnapshot.runId)}`, {
          cache: "no-store",
        });
        const payload = (await response.json()) as OutreachRunSnapshot | { error?: string };
        if (!response.ok || !("runId" in payload)) {
          return;
        }

        setRunSnapshot(payload);
        if (!isActiveRun(payload.status)) {
          void loadCampaignBundle();
        }
      } catch {
        // keep current run panel stable on transient polling failures
      }
    }, RUN_POLL_INTERVAL_MS);

    return () => {
      globalThis.clearInterval(timer);
    };
  }, [loadCampaignBundle, runSnapshot]);

  const saveCampaign = useCallback(async () => {
    if (!campaign) {
      return;
    }

    const normalizedName = editName.trim();
    if (!normalizedName) {
      setMessage("Campaign name is required.");
      return;
    }

    setSavingCampaign(true);
    setMessage("");

    try {
      const response = await fetch(`/api/campaigns/${campaign.id}`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          name: normalizedName,
          status: editStatus,
          maxDailySubmissions: Math.max(1, Math.round(editMaxDaily || 1)),
          description: editDescription,
          aiInstruction: editAiInstruction,
        }),
      });

      const payload = (await response.json()) as CampaignRecord | { error?: string };
      if (!response.ok || !("id" in payload)) {
        const messageText = ("error" in payload && payload.error) || "Unable to update campaign.";
        setMessage(messageText);
        return;
      }

      setCampaign(payload);
      setEditingCampaign(false);
      setMessage("Campaign updated.");
    } catch (requestError) {
      const messageText = requestError instanceof Error ? requestError.message : "Unable to update campaign.";
      setMessage(messageText);
    } finally {
      setSavingCampaign(false);
    }
  }, [campaign, editAiInstruction, editDescription, editMaxDaily, editName, editStatus]);

  const startRun = useCallback(async () => {
    if (!campaign) {
      return;
    }

    if (contacts.length === 0) {
      setMessage("Add contacts before starting a run.");
      return;
    }

    setStartingRun(true);
    setMessage("");

    try {
      const response = await fetch("/api/outreach/run", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          resume: true,
          persona: {
            id: campaign.id,
            title: campaign.name,
            aiInstruction: campaign.aiInstruction,
            maxDailySubmissions: campaign.maxDailySubmissions,
            ...campaign.persona,
          },
          leads: contacts.map((contact) => ({
            companyName: contact.companyName,
            contactUrl: contact.contactUrl,
          })),
        }),
      });

      const payload = (await response.json()) as OutreachRunSnapshot | { error?: string };

      if (!response.ok || !("runId" in payload)) {
        const messageText = ("error" in payload && payload.error) || "Unable to start campaign run.";
        setMessage(messageText);
        return;
      }

      setRunSnapshot(payload);
      setMessage(`Run ${payload.runId} started.`);
      await loadCampaignBundle();
    } catch (requestError) {
      const messageText = requestError instanceof Error ? requestError.message : "Unable to start campaign run.";
      setMessage(messageText);
    } finally {
      setStartingRun(false);
    }
  }, [campaign, contacts, loadCampaignBundle]);

  const stopRun = useCallback(async () => {
    if (!runSnapshot) {
      return;
    }

    setStoppingRun(true);
    setMessage("");

    try {
      const response = await fetch("/api/outreach/run/stop", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ runId: runSnapshot.runId }),
      });

      const payload = (await response.json()) as OutreachRunSnapshot | { error?: string };
      if (!response.ok || !("runId" in payload)) {
        const messageText = ("error" in payload && payload.error) || "Unable to stop run.";
        setMessage(messageText);
        return;
      }

      setRunSnapshot(payload);
      setMessage(`Stop requested for ${payload.runId}.`);
      await loadCampaignBundle();
    } catch (requestError) {
      const messageText = requestError instanceof Error ? requestError.message : "Unable to stop run.";
      setMessage(messageText);
    } finally {
      setStoppingRun(false);
    }
  }, [loadCampaignBundle, runSnapshot]);

  const addContact = useCallback(
    async (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      setMessage("");

      if (!companyName.trim() || !contactUrl.trim()) {
        setMessage("Company name and contact URL are required.");
        return;
      }

      setAddingContact(true);
      try {
        const response = await fetch(`/api/campaigns/${campaignId}/contacts`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            companyName,
            contactUrl,
            location,
            industry,
            notes,
          }),
        });

        const payload = (await response.json()) as ContactRecord | { error?: string };

        if (!response.ok || !("id" in payload)) {
          setMessage(("error" in payload && payload.error) || "Unable to add contact.");
          return;
        }

        setContacts((previous) => [payload, ...previous]);
        setCampaign((previous) =>
          previous
            ? {
                ...previous,
                contactCount: previous.contactCount + 1,
              }
            : previous,
        );

        setCompanyName("");
        setContactUrl("");
        setLocation("");
        setIndustry("");
        setNotes("");
      } catch (requestError) {
        const messageText = requestError instanceof Error ? requestError.message : "Unable to add contact.";
        setMessage(messageText);
      } finally {
        setAddingContact(false);
      }
    },
    [campaignId, companyName, contactUrl, industry, location, notes],
  );

  const deleteContact = useCallback(
    async (contactId: string) => {
      setDeletingContactId(contactId);
      setMessage("");
      try {
        const response = await fetch(`/api/campaigns/${campaignId}/contacts/${contactId}`, {
          method: "DELETE",
        });
        const payload = (await response.json()) as { error?: string };

        if (!response.ok) {
          setMessage(payload.error || "Unable to delete contact.");
          return;
        }

        setContacts((previous) => previous.filter((contact) => contact.id !== contactId));
        setCampaign((previous) =>
          previous
            ? {
                ...previous,
                contactCount: Math.max(0, previous.contactCount - 1),
              }
            : previous,
        );
      } catch (requestError) {
        const messageText = requestError instanceof Error ? requestError.message : "Unable to delete contact.";
        setMessage(messageText);
      } finally {
        setDeletingContactId(null);
      }
    },
    [campaignId],
  );

  const runProgress = useMemo(() => {
    if (!runSnapshot || runSnapshot.totalLeads <= 0) {
      return 0;
    }
    return Math.max(0, Math.min(100, runSnapshot.progress));
  }, [runSnapshot]);

  if (loading) {
    return <p className="panel-muted">Loading campaign details...</p>;
  }

  if (error || !campaign) {
    return <p className="panel-error">{error || "Campaign not found."}</p>;
  }

  return (
    <div className="page-stack">
      <section className="panel">
        <div className="panel-header">
          <h2>{campaign.name}</h2>
          <div className="button-row">
            <button type="button" className="button-primary" onClick={() => void startRun()} disabled={startingRun}>
              {startingRun ? "Starting..." : "Start Campaign Run"}
            </button>
            <button
              type="button"
              className="button-secondary"
              onClick={() => void stopRun()}
              disabled={!runSnapshot || !isActiveRun(runSnapshot.status) || stoppingRun}
            >
              {stoppingRun ? "Stopping..." : "Stop Run"}
            </button>
            {!editingCampaign ? (
              <button
                type="button"
                className="button-secondary"
                onClick={() => {
                  setEditingCampaign(true);
                  setMessage("");
                }}
              >
                Edit Campaign
              </button>
            ) : (
              <>
                <button type="button" className="button-primary" onClick={() => void saveCampaign()} disabled={savingCampaign}>
                  {savingCampaign ? "Saving..." : "Save Campaign"}
                </button>
                <button
                  type="button"
                  className="button-secondary"
                  onClick={() => {
                    setEditingCampaign(false);
                    setMessage("");
                  }}
                  disabled={savingCampaign}
                >
                  Cancel Edit
                </button>
              </>
            )}
            <button type="button" className="button-secondary" onClick={() => router.push("/campaigns")}>Back</button>
          </div>
        </div>

        <div className="details-grid">
          <div>
            <p className="meta-label">Status</p>
            <span className={`status-chip ${statusTone(campaign.status)}`}>{campaign.status}</span>
          </div>
          <div>
            <p className="meta-label">Max Daily</p>
            <p>{campaign.maxDailySubmissions}</p>
          </div>
          <div>
            <p className="meta-label">Contacts</p>
            <p>{campaign.contactCount}</p>
          </div>
          <div>
            <p className="meta-label">Updated At</p>
            <p>{formatDateTime(campaign.updatedAt)}</p>
          </div>
        </div>

        {!editingCampaign ? (
          <>
            {campaign.description ? (
              <div className="detail-block">
                <p className="meta-label">Description</p>
                <p>{campaign.description}</p>
              </div>
            ) : null}

            {campaign.aiInstruction ? (
              <div className="detail-block">
                <p className="meta-label">AI Instruction</p>
                <pre className="instruction-box">{campaign.aiInstruction}</pre>
              </div>
            ) : null}
          </>
        ) : (
          <form
            className="form-grid"
            onSubmit={(event) => {
              event.preventDefault();
              void saveCampaign();
            }}
          >
            <label className="field-block">
              Campaign Name
              <input value={editName} onChange={(event) => setEditName(event.target.value)} className="field-input" />
            </label>
            <label className="field-block">
              Status
              <select
                value={editStatus}
                onChange={(event) => setEditStatus(event.target.value as CampaignRecord["status"])}
                className="field-input"
              >
                {CAMPAIGN_STATUS_OPTIONS.map((option) => (
                  <option key={option} value={option}>
                    {option}
                  </option>
                ))}
              </select>
            </label>
            <label className="field-block">
              Max Daily Submissions
              <input
                type="number"
                min={1}
                max={5000}
                value={editMaxDaily}
                onChange={(event) => setEditMaxDaily(Number(event.target.value || 1))}
                className="field-input"
              />
            </label>
            <label className="field-block full">
              Description
              <textarea
                value={editDescription}
                onChange={(event) => setEditDescription(event.target.value)}
                className="field-input field-textarea"
              />
            </label>
            <label className="field-block full">
              AI Instruction
              <textarea
                value={editAiInstruction}
                onChange={(event) => setEditAiInstruction(event.target.value)}
                className="field-input field-textarea field-textarea-lg"
              />
            </label>
            <div className="full">
              <button type="submit" className="button-primary" disabled={savingCampaign}>
                {savingCampaign ? "Saving..." : "Save Campaign"}
              </button>
            </div>
          </form>
        )}
      </section>

      {message ? <p className="panel-muted">{message}</p> : null}

      <section className="panel">
        <div className="panel-header">
          <h2>Run Details</h2>
        </div>

        {runSnapshot ? (
          <>
            <div className="details-grid">
              <div>
                <p className="meta-label">Run ID</p>
                <p>{runSnapshot.runId}</p>
              </div>
              <div>
                <p className="meta-label">Status</p>
                <span className={`status-chip ${statusTone(runSnapshot.status)}`}>{runSnapshot.status}</span>
              </div>
              <div>
                <p className="meta-label">Progress</p>
                <p>
                  {runSnapshot.processedLeads}/{runSnapshot.totalLeads}
                </p>
              </div>
              <div>
                <p className="meta-label">Started</p>
                <p>{formatDateTime(runSnapshot.startedAt)}</p>
              </div>
              <div>
                <p className="meta-label">Pre-run Skips</p>
                <p>
                  dup {runSnapshot.duplicatesSkipped} | social {runSnapshot.socialSkippedLeads ?? 0} | resume{" "}
                  {runSnapshot.resumeSkippedLeads ?? 0}
                </p>
              </div>
              {runSnapshot.resumedFromRunId ? (
                <div>
                  <p className="meta-label">Resumed From</p>
                  <p>{runSnapshot.resumedFromRunId}</p>
                </div>
              ) : null}
            </div>

            <progress value={runProgress} max={100} className="run-progress" />

            <div className="log-box">
              {(runSnapshot.logs || []).slice(-25).map((line, index) => (
                <p key={`${line.slice(0, 30)}-${index}`}>{line}</p>
              ))}
            </div>
          </>
        ) : (
          <p className="panel-muted">Select a run from history to inspect full details.</p>
        )}
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2>Contacts</h2>
        </div>

        <div className="table-wrap">
          <table className="clean-table">
            <thead>
              <tr>
                <th>Company</th>
                <th>Domain</th>
                <th>Contact URL</th>
                <th>Updated</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {contacts.length === 0 ? (
                <tr>
                  <td colSpan={5} className="table-empty">
                    No contacts in this campaign.
                  </td>
                </tr>
              ) : (
                contacts.map((contact) => (
                  <tr key={contact.id}>
                    <td>{contact.companyName}</td>
                    <td>{contact.domain || "-"}</td>
                    <td>
                      <a href={contact.contactUrl} target="_blank" rel="noreferrer" className="table-link">
                        {contact.contactUrl}
                      </a>
                    </td>
                    <td>{formatDateTime(contact.updatedAt)}</td>
                    <td>
                      <button
                        type="button"
                        className="table-delete"
                        onClick={() => void deleteContact(contact.id)}
                        disabled={deletingContactId === contact.id}
                      >
                        {deletingContactId === contact.id ? "Deleting..." : "Delete"}
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        <form className="form-grid" onSubmit={(event) => void addContact(event)}>
          <label className="field-block">
            Company Name
            <input value={companyName} onChange={(event) => setCompanyName(event.target.value)} className="field-input" />
          </label>
          <label className="field-block">
            Contact URL
            <input value={contactUrl} onChange={(event) => setContactUrl(event.target.value)} className="field-input" />
          </label>
          <label className="field-block">
            Location
            <input value={location} onChange={(event) => setLocation(event.target.value)} className="field-input" />
          </label>
          <label className="field-block">
            Industry
            <input value={industry} onChange={(event) => setIndustry(event.target.value)} className="field-input" />
          </label>
          <label className="field-block full">
            Notes
            <textarea value={notes} onChange={(event) => setNotes(event.target.value)} className="field-input field-textarea" />
          </label>
          <div className="full">
            <button type="submit" className="button-primary" disabled={addingContact}>
              {addingContact ? "Adding..." : "Add Contact"}
            </button>
          </div>
        </form>
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2>Run History</h2>
        </div>

        <div className="table-wrap">
          <table className="clean-table">
            <thead>
              <tr>
                <th>Run ID</th>
                <th>Status</th>
                <th>Processed</th>
                <th>Started</th>
                <th>Finished</th>
                <th>Details</th>
              </tr>
            </thead>
            <tbody>
              {runs.length === 0 ? (
                <tr>
                  <td colSpan={6} className="table-empty">
                    No runs for this campaign yet.
                  </td>
                </tr>
              ) : (
                runs.map((run) => (
                  <tr key={run.runId}>
                    <td>{run.runId}</td>
                    <td>
                      <span className={`status-chip ${statusTone(run.status)}`}>{run.status}</span>
                    </td>
                    <td>
                      {run.processedLeads}/{run.totalLeads}
                    </td>
                    <td>{formatDateTime(run.startedAt)}</td>
                    <td>{formatDateTime(run.finishedAt)}</td>
                    <td>
                      <button type="button" className="button-small" onClick={() => void inspectRun(run.runId)}>
                        View
                      </button>
                    </td>
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
