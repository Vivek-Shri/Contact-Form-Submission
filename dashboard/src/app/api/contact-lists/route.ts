import pool from "@/lib/db";
import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/app/api/auth/[...nextauth]/route";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    const client = await pool.connect();
    try {
      const { rows } = await client.query(`
        SELECT l.list_id as id, l.name, l.created_at as "createdAt",
               COUNT(i.item_id) as "contactCount"
        FROM contact_lists l
        LEFT JOIN contact_list_items i ON l.list_id = i.list_id
        GROUP BY l.list_id, l.name, l.created_at
        ORDER BY l.created_at DESC
      `);
      return NextResponse.json({ lists: rows }, { status: 200 });
    } finally {
      client.release();
    }
  } catch (error) {
    console.error("GET /api/contact-lists error:", error);
    return NextResponse.json({ error: "Unable to load lists." }, { status: 500 });
  }
}

export async function POST(request: Request) {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  let body: any;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON" }, { status: 400 });
  }

  const { name, contacts } = body;
  if (!name?.trim()) {
    return NextResponse.json({ error: "List name is required" }, { status: 400 });
  }

  let listId = name.trim();
  const now = new Date().toISOString();

  try {
    const client = await pool.connect();
    try {
      const { rows: existingRows } = await client.query("SELECT list_id FROM contact_lists WHERE list_id = $1", [listId]);
      if (existingRows.length > 0) {
        listId = `${listId}-${Math.random().toString(36).substring(2, 6)}`;
      }

      await client.query("BEGIN");

      await client.query(
        `INSERT INTO contact_lists (list_id, name, created_at, updated_at)
         VALUES ($1, $2, $3, $4)`,
        [listId, name.trim(), now, now]
      );

      if (Array.isArray(contacts) && contacts.length > 0) {
        let idx = 0;
        for (const c of contacts) {
          const url = (c.contactUrl || "").trim();
          if (url) {
            const itemId = `${listId}-item-${idx++}-${Math.random().toString(36).substring(2, 8)}`;
            await client.query(
              `INSERT INTO contact_list_items (item_id, list_id, company_name, contact_url, created_at)
               VALUES ($1, $2, $3, $4, $5)`,
              [itemId, listId, c.companyName || "Unknown", url, now]
            );
          }
        }
      }

      await client.query("COMMIT");
      return NextResponse.json({ id: listId, name: name.trim(), createdAt: now }, { status: 201 });
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  } catch (error) {
    console.error("POST /api/contact-lists error:", error);
    return NextResponse.json({ error: "Unable to create list." }, { status: 500 });
  }
}
