export type CampaignStatus = "draft" | "active" | "paused" | "archived";

export interface CampaignPersona {
  firstName?: string;
  lastName?: string;
  jobTitle?: string;
  professionalEmail?: string;
  verifiedPhone?: string;
  company?: string;
  website?: string;
  zipCode?: string;
  pitchMessage?: string;
}

export interface PaginationMeta {
  page: number;
  limit: number;
  total: number;
  total_pages?: number;
  totalPages?: number;
}

export interface CampaignRunSummary {
  runId: string;
  status: string;
  startedAt: string;
  finishedAt?: string;
  exitCode?: number;
  totalLeads: number;
  processedLeads: number;
  duplicatesSkipped: number;
}

export interface CampaignRecord {
  id: string;
  name: string;
  description: string;
  status: CampaignStatus;
  aiInstruction: string;
  maxDailySubmissions: number;
  contactCount: number;
  createdAt: string;
  updatedAt: string;
  persona: CampaignPersona;
  lastRun?: CampaignRunSummary;
}

export interface ContactRecord {
  id: string;
  campaignId: string;
  campaignName?: string;
  companyName: string;
  contactUrl: string;
  domain: string;
  location?: string;
  industry?: string;
  notes?: string;
  createdAt: string;
  updatedAt: string;
}

export interface OutreachRunSnapshot {
  runId: string;
  status: string;
  progress: number;
  totalLeads: number;
  processedLeads: number;
  currentLead: string;
  logs: string[];
  results: Array<{
    campaignId?: string;
    campaignTitle?: string;
    companyName: string;
    contactUrl: string;
    submitted: "Yes" | "No";
    status: "success" | "warning" | "fail";
    captchaStatus: string;
    confirmationMsg: string;
    estCostUsd: number;
  }>;
  duplicatesSkipped: number;
  resumeSkippedLeads?: number;
  socialSkippedLeads?: number;
  resumedFromRunId?: string;
  startedAt: string;
  endedAt?: string;
  error?: string;
}
