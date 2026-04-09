"use client";

import Link from "next/link";
import { usePathname, redirect } from "next/navigation";
import { BarChart3, FolderKanban, Users, LogOut } from "lucide-react";
import { useSession, signOut } from "next-auth/react";

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
  const { status, data: session } = useSession({
    required: true,
    onUnauthenticated() {
      redirect("/login");
    },
  });

  if (status === "loading") {
    return <div className="flex h-screen items-center justify-center">Loading...</div>;
  }

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
        
        <div className="mt-auto p-4 border-t border-zinc-200">
          <div className="flex items-center gap-3 mb-4">
            <div className="h-8 w-8 rounded-full bg-indigo-100 flex items-center justify-center text-indigo-700 font-bold text-sm">
              {session?.user?.name?.charAt(0).toUpperCase() || 'U'}
            </div>
            <div className="flex-1 truncate">
              <p className="text-sm font-medium text-zinc-900 truncate">{session?.user?.name}</p>
              <p className="text-xs text-zinc-500 truncate">{session?.user?.email}</p>
            </div>
          </div>
          <button 
            onClick={() => signOut({ callbackUrl: '/login' })}
            className="flex w-full items-center gap-3 rounded-md px-3 py-2 text-sm font-medium text-zinc-700 transition-colors hover:bg-zinc-100 hover:text-zinc-900"
          >
            <LogOut size={16} />
            <span>Log out</span>
          </button>
        </div>
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
