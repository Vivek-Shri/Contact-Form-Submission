import pool from './db';

export interface ContactListItem {
    item_id: string;
    list_id: string;
    company_name: string;
    contact_url: string;
    created_at: string;
}

export interface ContactList {
    list_id: string;
    name: string;
    created_at: string;
    updated_at: string;
    items?: ContactListItem[];
}

export async function fetchContactLists(): Promise<ContactList[]> {
    const listsResult = await pool.query<ContactList>(
        `SELECT * FROM contact_lists ORDER BY created_at DESC`
    );
    const lists = listsResult.rows;

    for (const list of lists) {
        const itemsResult = await pool.query<ContactListItem>(
            `SELECT * FROM contact_list_items WHERE list_id = $1 ORDER BY created_at ASC`,
            [list.list_id]
        );
        list.items = itemsResult.rows;
    }

    return lists;
}

export async function createContactList(name: string): Promise<ContactList> {
    const list_id = crypto.randomUUID();
    const now = new Date().toISOString();

    const result = await pool.query<ContactList>(
        `INSERT INTO contact_lists (list_id, name, created_at, updated_at)
     VALUES ($1, $2, $3, $4)
     RETURNING *`,
        [list_id, name, now, now]
    );

    return result.rows[0];
}

export async function deleteContactList(list_id: string): Promise<void> {
    await pool.query(
        `DELETE FROM contact_lists WHERE list_id = $1`,
        [list_id]
    );
}

export async function addItemsToList(
    list_id: string,
    items: { company_name: string; contact_url: string }[]
): Promise<void> {
    for (const item of items) {
        const item_id = crypto.randomUUID();
        const now = new Date().toISOString();

        await pool.query(
            `INSERT INTO contact_list_items (item_id, list_id, company_name, contact_url, created_at)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (list_id, contact_url) DO NOTHING`,
            [item_id, list_id, item.company_name || '', item.contact_url || '', now]
        );
    }
}

export async function fetchListItems(list_id: string): Promise<ContactListItem[]> {
    const result = await pool.query<ContactListItem>(
        `SELECT * FROM contact_list_items WHERE list_id = $1 ORDER BY created_at ASC`,
        [list_id]
    );
    return result.rows;
}

export async function updateContactListName(list_id: string, name: string): Promise<void> {
    const now = new Date().toISOString();
    await pool.query(
        `UPDATE contact_lists SET name = $1, updated_at = $2 WHERE list_id = $3`,
        [name, now, list_id]
    );
}