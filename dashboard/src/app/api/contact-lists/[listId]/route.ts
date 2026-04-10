import pool from "@/lib/db";
import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/app/api/auth/[...nextauth]/route";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

interface Params {
  listId: string;
}

export async function GET(request: Request, { params }: { params: Promise<Params> }) {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { listId } = await params;
  try {
    const client = await pool.connect();
    try {
      const { rows: listRows } = await client.query(
        "SELECT list_id, name FROM contact_lists WHERE list_id = $1",
        [listId]
      );
      if (listRows.length === 0) {
        return NextResponse.json({ error: "List not found" }, { status: 404 });
      }

      const { rows: items } = await client.query(
        `SELECT company_name as "companyName", contact_url as "contactUrl"
         FROM contact_list_items WHERE list_id = $1`,
        [listId]
      );

      return NextResponse.json({ contacts: items }, { status: 200 });
    } finally {
      client.release();
    }
  } catch (error) {
    console.error(`GET /api/contact-lists/${listId} error:`, error);
    return NextResponse.json({ error: "Unable to load list details." }, { status: 500 });
  }
}

export async function PATCH(request: Request, { params }: { params: Promise<Params> }) {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { listId } = await params;
  let body: any;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON" }, { status: 400 });
  }

  const { contacts } = body;
  if (!Array.isArray(contacts)) {
    return NextResponse.json({ error: "Contacts array is required" }, { status: 400 });
  }

  try {
    const client = await pool.connect();
    try {
      const { rows: listRows } = await client.query(
        "SELECT list_id FROM contact_lists WHERE list_id = $1",
        [listId]
      );
      if (listRows.length === 0) {
        return NextResponse.json({ error: "List not found" }, { status: 404 });
      }

      const now = new Date().toISOString();
      await client.query("BEGIN");
      let idx = 0;
      for (const c of contacts) {
        const url = (c.contactUrl || "").trim();
        if (url) {
          const itemId = `${listId}-item-${Date.now()}-${idx++}-${Math.random().toString(36).substring(2, 8)}`;
          await client.query(
            `INSERT INTO contact_list_items (item_id, list_id, company_name, contact_url, created_at)
             VALUES ($1, $2, $3, $4, $5)`,
            [itemId, listId, c.companyName || "Unknown", url, now]
          );
        }
      }
      await client.query("UPDATE contact_lists SET updated_at = $1 WHERE list_id = $2", [now, listId]);
      await client.query("COMMIT");

      return NextResponse.json({ message: "List updated" }, { status: 200 });
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  } catch (error) {
    console.error(`PATCH /api/contact-lists/${listId} error:`, error);
    return NextResponse.json({ error: "Unable to update list." }, { status: 500 });
  }
}

export async function DELETE(request: Request, { params }: { params: Promise<Params> }) {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { listId } = await params;
  try {
    const client = await pool.connect();
    try {
      const { rows: listRows } = await client.query(
        "SELECT list_id FROM contact_lists WHERE list_id = $1",
        [listId]
      );
      if (listRows.length === 0) {
        return NextResponse.json({ error: "List not found" }, { status: 404 });
      }

      await client.query("BEGIN");
      await client.query("DELETE FROM contact_list_items WHERE list_id = $1", [listId]);
      await client.query("DELETE FROM contact_lists WHERE list_id = $1", [listId]);
      await client.query("COMMIT");

      return NextResponse.json({ message: "List deleted" }, { status: 200 });
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  } catch (error) {
    console.error(`DELETE /api/contact-lists/${listId} error:`, error);
    return NextResponse.json({ error: "Unable to delete list." }, { status: 500 });
  }
}
