"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { BarChart3, FolderKanban, PlusCircle, Users } from "lucide-react";

const NAV_ITEMS = [
  {
    href: "/overview",
    label: "Overview",
    icon: BarChart3,
  },
  {
    href: "/campaigns",
    label: "Campaigns",
    icon: FolderKanban,
  },
  {
    href: "/campaigns/new",
    label: "New Campaign",
    icon: PlusCircle,
  },
  {
    href: "/contacts",
    label: "Contacts",
    icon: Users,
  },
];

export default function WorkspaceLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const pathname = usePathname();

  return (
    <div className="studio-shell">
      <aside className="studio-sidebar">
        <div>
          <p className="studio-eyebrow">Contact Form Submission</p>
          <h1 className="studio-title">Outreach Studio</h1>
          <p className="studio-subtitle">Campaign routes, details, and backend run control in one place.</p>
        </div>

        <nav className="studio-nav">
          {NAV_ITEMS.map((item) => {
            const Icon = item.icon;
            const active = pathname === item.href || pathname.startsWith(`${item.href}/`);
            return (
              <Link
                key={item.href}
                href={item.href}
                className={`studio-nav-link ${active ? "is-active" : ""}`}
              >
                <Icon size={16} />
                <span>{item.label}</span>
              </Link>
            );
          })}
        </nav>
      </aside>

      <div className="studio-main">
        <div className="studio-topbar">
          <p>Route-based frontend connected to backend APIs and MongoDB collections.</p>
        </div>
        <main className="studio-content">{children}</main>
      </div>
    </div>
  );
}
