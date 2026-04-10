"use client";

import React, { useCallback, useEffect, useState } from "react";
import { useSession } from "next-auth/react";
import { useRouter } from "next/navigation";
import {
  Shield,
  ShieldCheck,
  User,
  Trash2,
  ChevronDown,
  AlertTriangle,
  CheckCircle2,
  Crown,
  Mail,
  Calendar,
} from "lucide-react";

/* ─── Types ──────────────────────────────────────────────────── */

interface UserRecord {
  id: string;
  email: string;
  name: string;
  role: "admin" | "user";
  isAdmin: boolean;
  createdAt: string;
}

/* ─── Component ──────────────────────────────────────────────── */

export default function UsersPage() {
  const { data: session, status } = useSession();
  const router = useRouter();
  const currentUserId = (session?.user as any)?.id || "";
  const isAdmin = (session?.user as any)?.isAdmin || false;

  const [users, setUsers] = useState<UserRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [updatingId, setUpdatingId] = useState<string | null>(null);
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const [confirmDeleteId, setConfirmDeleteId] = useState<string | null>(null);

  /* ─── Load users ───────────────────────────────────────────── */
  const loadUsers = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const res = await fetch("/api/users", { cache: "no-store" });
      if (res.status === 403) {
        setError("You do not have permission to view this page.");
        return;
      }
      const data = await res.json();
      if (!res.ok) {
        setError(data.error || "Unable to load users.");
        return;
      }
      setUsers(data.users || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load users.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (status === "authenticated") {
      if (!isAdmin) {
        router.replace("/overview");
        return;
      }
      void loadUsers();
    }
  }, [status, isAdmin, loadUsers, router]);

  /* ─── Toggle role ──────────────────────────────────────────── */
  const toggleRole = async (user: UserRecord) => {
    if (user.id === currentUserId) {
      setMessage("You cannot change your own role.");
      setTimeout(() => setMessage(""), 3000);
      return;
    }

    const newRole = user.role === "admin" ? "user" : "admin";
    setUpdatingId(user.id);
    setMessage("");

    try {
      const res = await fetch(`/api/users/${user.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ role: newRole }),
      });
      const data = await res.json();
      if (!res.ok) {
        setMessage(data.error || "Failed to update role.");
        return;
      }
      setUsers((prev) =>
        prev.map((u) =>
          u.id === user.id
            ? { ...u, role: data.role, isAdmin: data.isAdmin }
            : u
        )
      );
      setMessage(
        `${user.name} is now ${newRole === "admin" ? "an Admin" : "a User"}.`
      );
      setTimeout(() => setMessage(""), 3000);
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Failed to update role.");
    } finally {
      setUpdatingId(null);
    }
  };

  /* ─── Delete user ──────────────────────────────────────────── */
  const deleteUser = async (user: UserRecord) => {
    if (user.id === currentUserId) {
      setMessage("You cannot delete yourself.");
      setTimeout(() => setMessage(""), 3000);
      return;
    }

    setDeletingId(user.id);
    setMessage("");

    try {
      const res = await fetch(`/api/users/${user.id}`, { method: "DELETE" });
      const data = await res.json();
      if (!res.ok) {
        setMessage(data.error || "Failed to delete user.");
        return;
      }
      setUsers((prev) => prev.filter((u) => u.id !== user.id));
      setMessage(`User "${user.name}" has been deleted.`);
      setConfirmDeleteId(null);
      setTimeout(() => setMessage(""), 3000);
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Failed to delete user.");
    } finally {
      setDeletingId(null);
    }
  };

  /* ─── Render ───────────────────────────────────────────────── */
  if (status === "loading" || loading) {
    return (
      <div className="page-stack">
        <section className="panel">
          <p className="panel-muted" style={{ padding: "2rem", textAlign: "center" }}>
            Loading users...
          </p>
        </section>
      </div>
    );
  }

  if (false) {
    return null;
  }

  const adminCount = users.filter((u) => u.role === "admin").length;
  const userCount = users.filter((u) => u.role === "user").length;

  return (
    <div className="page-stack">
      <section className="panel">
        {/* Header */}
        <div className="panel-header">
          <div className="flex items-center gap-3">
            <div
              className="flex items-center justify-center rounded-xl"
              style={{
                width: 40,
                height: 40,
                background: "linear-gradient(135deg, #6366f1, #8b5cf6)",
              }}
            >
              <Shield size={20} color="white" />
            </div>
            <div>
              <h2 style={{ margin: 0 }}>User Management</h2>
              <p
                className="panel-muted"
                style={{ margin: 0, fontSize: "13px" }}
              >
                Manage roles and access for all users
              </p>
            </div>
          </div>
        </div>

        {/* Stats Bar */}
        <div
          style={{
            display: "flex",
            gap: "16px",
            padding: "16px 0",
            borderBottom: "1px solid #f3f4f6",
            flexWrap: "wrap",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              padding: "8px 16px",
              background: "#faf5ff",
              borderRadius: "10px",
              border: "1px solid #e9d5ff",
            }}
          >
            <Crown size={16} style={{ color: "#7c3aed" }} />
            <span style={{ fontSize: "13px", color: "#7c3aed", fontWeight: 600 }}>
              {adminCount} Admin{adminCount !== 1 ? "s" : ""}
            </span>
          </div>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              padding: "8px 16px",
              background: "#f0f9ff",
              borderRadius: "10px",
              border: "1px solid #bae6fd",
            }}
          >
            <User size={16} style={{ color: "#0284c7" }} />
            <span style={{ fontSize: "13px", color: "#0284c7", fontWeight: 600 }}>
              {userCount} User{userCount !== 1 ? "s" : ""}
            </span>
          </div>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              padding: "8px 16px",
              background: "#f8fafc",
              borderRadius: "10px",
              border: "1px solid #e2e8f0",
            }}
          >
            <span style={{ fontSize: "13px", color: "#64748b", fontWeight: 500 }}>
              {users.length} Total
            </span>
          </div>
        </div>

        {/* Messages */}
        {error && (
          <div
            style={{
              margin: "12px 0",
              padding: "12px 16px",
              background: "#fef2f2",
              border: "1px solid #fecaca",
              borderRadius: "10px",
              color: "#b91c1c",
              fontSize: "13px",
              display: "flex",
              alignItems: "center",
              gap: "8px",
            }}
          >
            <AlertTriangle size={16} />
            {error}
          </div>
        )}

        {message && (
          <div
            style={{
              margin: "12px 0",
              padding: "12px 16px",
              background: "#f0fdf4",
              border: "1px solid #bbf7d0",
              borderRadius: "10px",
              color: "#15803d",
              fontSize: "13px",
              display: "flex",
              alignItems: "center",
              gap: "8px",
            }}
          >
            <CheckCircle2 size={16} />
            {message}
          </div>
        )}

        {/* RBAC Info */}
        <div
          style={{
            margin: "16px 0",
            padding: "14px 18px",
            background: "linear-gradient(135deg, #eff6ff, #f5f3ff)",
            border: "1px solid #ddd6fe",
            borderRadius: "12px",
          }}
        >
          <p
            style={{
              margin: 0,
              fontSize: "13px",
              color: "#4338ca",
              fontWeight: 600,
              marginBottom: "6px",
            }}
          >
            🔐 Role-Based Access Control
          </p>
          <ul
            style={{
              margin: 0,
              paddingLeft: "18px",
              fontSize: "12px",
              color: "#6366f1",
              lineHeight: "1.8",
            }}
          >
            <li>
              <strong>Admin</strong> — Can see ALL campaigns (own + other users&apos;), manage users & roles
            </li>
            <li>
              <strong>User</strong> — Can only see their OWN campaigns, contacts & data
            </li>
          </ul>
        </div>

        {/* Users Table */}
        <div className="table-wrap">
          <table className="clean-table">
            <thead>
              <tr>
                <th>User</th>
                <th>Email</th>
                <th>Role</th>
                <th>Joined</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {users.length === 0 && !loading ? (
                <tr>
                  <td colSpan={5} className="table-empty">
                    No users found.
                  </td>
                </tr>
              ) : (
                users.map((user) => {
                  const isSelf = user.id === currentUserId;
                  return (
                    <tr key={user.id}>
                      {/* Name + Avatar */}
                      <td>
                        <div className="flex items-center gap-3">
                          <div
                            style={{
                              width: 36,
                              height: 36,
                              borderRadius: "50%",
                              display: "flex",
                              alignItems: "center",
                              justifyContent: "center",
                              fontWeight: 700,
                              fontSize: "14px",
                              color: "white",
                              background: user.isAdmin
                                ? "linear-gradient(135deg, #7c3aed, #6366f1)"
                                : "linear-gradient(135deg, #0ea5e9, #06b6d4)",
                              flexShrink: 0,
                            }}
                          >
                            {user.name.charAt(0).toUpperCase()}
                          </div>
                          <div>
                            <p
                              className="font-medium"
                              style={{ margin: 0, fontSize: "14px" }}
                            >
                              {user.name}
                              {isSelf && (
                                <span
                                  style={{
                                    marginLeft: "6px",
                                    fontSize: "10px",
                                    color: "#6366f1",
                                    background: "#ede9fe",
                                    padding: "2px 6px",
                                    borderRadius: "4px",
                                    fontWeight: 600,
                                  }}
                                >
                                  YOU
                                </span>
                              )}
                            </p>
                          </div>
                        </div>
                      </td>

                      {/* Email */}
                      <td>
                        <div className="flex items-center gap-1.5">
                          <Mail size={13} style={{ color: "#9ca3af" }} />
                          <span style={{ color: "#6b7280", fontSize: "13px" }}>
                            {user.email}
                          </span>
                        </div>
                      </td>

                      {/* Role Badge */}
                      <td>
                        {user.isAdmin ? (
                          <span
                            style={{
                              display: "inline-flex",
                              alignItems: "center",
                              gap: "5px",
                              padding: "4px 12px",
                              borderRadius: "20px",
                              fontSize: "12px",
                              fontWeight: 600,
                              background: "linear-gradient(135deg, #ede9fe, #faf5ff)",
                              color: "#7c3aed",
                              border: "1px solid #ddd6fe",
                            }}
                          >
                            <ShieldCheck size={13} /> Admin
                          </span>
                        ) : (
                          <span
                            style={{
                              display: "inline-flex",
                              alignItems: "center",
                              gap: "5px",
                              padding: "4px 12px",
                              borderRadius: "20px",
                              fontSize: "12px",
                              fontWeight: 600,
                              background: "#f0f9ff",
                              color: "#0284c7",
                              border: "1px solid #bae6fd",
                            }}
                          >
                            <User size={13} /> User
                          </span>
                        )}
                      </td>

                      {/* Joined */}
                      <td>
                        <div className="flex items-center gap-1.5">
                          <Calendar size={13} style={{ color: "#9ca3af" }} />
                          <span
                            style={{ color: "#9ca3af", fontSize: "12px" }}
                          >
                            {user.createdAt
                              ? new Date(user.createdAt).toLocaleDateString()
                              : "—"}
                          </span>
                        </div>
                      </td>

                      {/* Actions */}
                      <td>
                        <div className="flex items-center gap-2">
                          {/* Toggle Role */}
                          <button
                            type="button"
                            disabled={isSelf || updatingId === user.id}
                            onClick={() => void toggleRole(user)}
                            style={{
                              display: "flex",
                              alignItems: "center",
                              gap: "5px",
                              padding: "5px 12px",
                              borderRadius: "8px",
                              fontSize: "12px",
                              fontWeight: 500,
                              border: "1px solid #e5e7eb",
                              background: isSelf
                                ? "#f9fafb"
                                : "white",
                              color: isSelf
                                ? "#d1d5db"
                                : "#374151",
                              cursor: isSelf
                                ? "not-allowed"
                                : "pointer",
                              transition: "all 0.15s",
                            }}
                            title={
                              isSelf
                                ? "Cannot change your own role"
                                : `Switch to ${user.role === "admin" ? "User" : "Admin"}`
                            }
                          >
                            <ChevronDown size={12} />
                            {updatingId === user.id
                              ? "Updating..."
                              : user.role === "admin"
                                ? "Demote to User"
                                : "Promote to Admin"}
                          </button>

                          {/* Delete */}
                          {confirmDeleteId === user.id ? (
                            <div className="flex items-center gap-1">
                              <button
                                type="button"
                                onClick={() => void deleteUser(user)}
                                disabled={deletingId === user.id}
                                style={{
                                  padding: "5px 10px",
                                  borderRadius: "8px",
                                  fontSize: "11px",
                                  fontWeight: 600,
                                  border: "1px solid #fecaca",
                                  background: "#fef2f2",
                                  color: "#dc2626",
                                  cursor: "pointer",
                                }}
                              >
                                {deletingId === user.id
                                  ? "Deleting..."
                                  : "Confirm"}
                              </button>
                              <button
                                type="button"
                                onClick={() => setConfirmDeleteId(null)}
                                style={{
                                  padding: "5px 10px",
                                  borderRadius: "8px",
                                  fontSize: "11px",
                                  fontWeight: 500,
                                  border: "1px solid #e5e7eb",
                                  background: "white",
                                  color: "#6b7280",
                                  cursor: "pointer",
                                }}
                              >
                                Cancel
                              </button>
                            </div>
                          ) : (
                            <button
                              type="button"
                              disabled={isSelf}
                              onClick={() => setConfirmDeleteId(user.id)}
                              style={{
                                display: "flex",
                                alignItems: "center",
                                gap: "4px",
                                padding: "5px 10px",
                                borderRadius: "8px",
                                fontSize: "12px",
                                fontWeight: 500,
                                border: "1px solid #e5e7eb",
                                background: isSelf
                                  ? "#f9fafb"
                                  : "white",
                                color: isSelf
                                  ? "#d1d5db"
                                  : "#dc2626",
                                cursor: isSelf
                                  ? "not-allowed"
                                  : "pointer",
                                transition: "all 0.15s",
                              }}
                              title={
                                isSelf
                                  ? "Cannot delete yourself"
                                  : "Delete this user"
                              }
                            >
                              <Trash2 size={12} /> Delete
                            </button>
                          )}
                        </div>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
