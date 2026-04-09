"use client";

import { FormEvent, useState } from "react";
import { useRouter } from "next/navigation";

export default function NewCampaignPage() {
  const router = useRouter();

  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [aiInstruction, setAiInstruction] = useState("");
  const [status, setStatus] = useState("draft");
  const [maxDailySubmissions, setMaxDailySubmissions] = useState("100");

  const [firstName, setFirstName] = useState("");
  const [lastName, setLastName] = useState("");
  const [jobTitle, setJobTitle] = useState("");
  const [professionalEmail, setProfessionalEmail] = useState("");
  const [verifiedPhone, setVerifiedPhone] = useState("");
  const [company, setCompany] = useState("");
  const [website, setWebsite] = useState("");
  const [zipCode, setZipCode] = useState("");
  const [pitchMessage, setPitchMessage] = useState("");

  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  const onSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError("");

    if (!name.trim()) {
      setError("Campaign name is required.");
      return;
    }

    setSaving(true);

    try {
      const response = await fetch("/api/campaigns", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          name,
          description,
          aiInstruction,
          status,
          maxDailySubmissions: Number(maxDailySubmissions) || 100,
          firstName,
          lastName,
          jobTitle,
          professionalEmail,
          verifiedPhone,
          company,
          website,
          zipCode,
          pitchMessage,
        }),
      });

      const payload = (await response.json()) as { error?: string; id?: string };

      if (!response.ok) {
        setError(payload.error || "Unable to create campaign.");
        return;
      }

      router.push(`/campaigns/${payload.id}`);
      router.refresh();
    } catch (requestError) {
      const message = requestError instanceof Error ? requestError.message : "Unable to create campaign.";
      setError(message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="page-stack">
      <section className="panel">
        <div className="panel-header">
          <h2>Create Campaign</h2>
        </div>

        <form className="form-grid" onSubmit={onSubmit}>
          <label className="field-block">
            Campaign Name
            <input value={name} onChange={(event) => setName(event.target.value)} className="field-input" />
          </label>

          <label className="field-block">
            Status
            <select value={status} onChange={(event) => setStatus(event.target.value)} className="field-input">
              <option value="draft">draft</option>
              <option value="active">active</option>
              <option value="paused">paused</option>
              <option value="archived">archived</option>
            </select>
          </label>

          <label className="field-block">
            Max Daily Submissions
            <input
              value={maxDailySubmissions}
              onChange={(event) => setMaxDailySubmissions(event.target.value)}
              className="field-input"
              inputMode="numeric"
            />
          </label>

          <label className="field-block full">
            Description
            <textarea
              value={description}
              onChange={(event) => setDescription(event.target.value)}
              className="field-input field-textarea"
            />
          </label>

          <label className="field-block full">
            AI Instruction
            <textarea
              value={aiInstruction}
              onChange={(event) => setAiInstruction(event.target.value)}
              className="field-input field-textarea"
            />
          </label>

          <label className="field-block">
            First Name
            <input value={firstName} onChange={(event) => setFirstName(event.target.value)} className="field-input" />
          </label>

          <label className="field-block">
            Last Name
            <input value={lastName} onChange={(event) => setLastName(event.target.value)} className="field-input" />
          </label>

          <label className="field-block">
            Job Title
            <input value={jobTitle} onChange={(event) => setJobTitle(event.target.value)} className="field-input" />
          </label>

          <label className="field-block">
            Professional Email
            <input
              value={professionalEmail}
              onChange={(event) => setProfessionalEmail(event.target.value)}
              className="field-input"
            />
          </label>

          <label className="field-block">
            Verified Phone
            <input value={verifiedPhone} onChange={(event) => setVerifiedPhone(event.target.value)} className="field-input" />
          </label>

          <label className="field-block">
            Company
            <input value={company} onChange={(event) => setCompany(event.target.value)} className="field-input" />
          </label>

          <label className="field-block">
            Website
            <input value={website} onChange={(event) => setWebsite(event.target.value)} className="field-input" />
          </label>

          <label className="field-block">
            Zip Code
            <input value={zipCode} onChange={(event) => setZipCode(event.target.value)} className="field-input" />
          </label>

          <label className="field-block full">
            Pitch Message
            <textarea
              value={pitchMessage}
              onChange={(event) => setPitchMessage(event.target.value)}
              className="field-input field-textarea"
            />
          </label>

          {error ? <p className="panel-error full">{error}</p> : null}

          <div className="full">
            <button type="submit" className="button-primary" disabled={saving}>
              {saving ? "Creating..." : "Create Campaign"}
            </button>
          </div>
        </form>
      </section>
    </div>
  );
}
