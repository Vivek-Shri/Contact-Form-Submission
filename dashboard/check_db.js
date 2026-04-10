const { Client } = require('pg');
const client = new Client({
  connectionString: 'postgresql://postgres.rhmqhrjbknazyflmbwbv:6%3F9H%23%40Dv5W%2BVTEZ@aws-1-ap-northeast-2.pooler.supabase.com:6543/postgres'
});

async function main() {
  await client.connect();
  
  const listId = `test-${Date.now()}`;
  const now = new Date().toISOString();

  console.log('--- INSERT test ---');
  try {
    await client.query('BEGIN');
    await client.query(
      `INSERT INTO contact_lists (list_id, name, created_at, updated_at) VALUES ($1, $2, $3, $4)`,
      [listId, 'Test', now, now]
    );
    const itemId = `${listId}-item-0-abc123`;
    await client.query(
      `INSERT INTO contact_list_items (item_id, list_id, company_name, contact_url, created_at) VALUES ($1, $2, $3, $4, $5)`,
      [itemId, listId, 'TestCo', 'https://example.com', now]
    );
    await client.query('COMMIT');
    console.log('SUCCESS!');
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('FAIL:', e.message);
  }

  // Verify
  const res = await client.query(`
    SELECT l.list_id as id, l.name, COUNT(i.item_id) as "contactCount"
    FROM contact_lists l LEFT JOIN contact_list_items i ON l.list_id = i.list_id
    WHERE l.list_id = $1
    GROUP BY l.list_id, l.name
  `, [listId]);
  console.log('Verify:', res.rows);

  // Cleanup
  await client.query('DELETE FROM contact_list_items WHERE list_id = $1', [listId]);
  await client.query('DELETE FROM contact_lists WHERE list_id = $1', [listId]);
  console.log('Cleaned up.');
  await client.end();
}
main().catch(console.error);
