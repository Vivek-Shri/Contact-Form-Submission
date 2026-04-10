const { Client } = require('pg');
const client = new Client({
  connectionString: 'postgresql://postgres.rhmqhrjbknazyflmbwbv:6%3F9H%23%40Dv5W%2BVTEZ@aws-1-ap-northeast-2.pooler.supabase.com:6543/postgres'
});

async function main() {
  await client.connect();
  try {
    const insertRes = await client.query(
      'INSERT INTO public.users (email, name, hashed_password, created_at, is_admin) VALUES ($1, $2, $3, $4, $5) RETURNING id',
      ['test12345@example.com', 'test', 'hash', new Date().toISOString(), false]
    );
    console.log('SUCCESS, inserted ID:', insertRes.rows[0].id);
    await client.query('DELETE FROM public.users WHERE id = $1', [insertRes.rows[0].id]);
  } catch (err) {
    console.error('ERROR:', err.message);
  }
  await client.end();
}
main();
