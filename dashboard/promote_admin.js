const { Client } = require('pg');
const client = new Client({
  connectionString: 'postgresql://postgres.rhmqhrjbknazyflmbwbv:6%3F9H%23%40Dv5W%2BVTEZ@aws-1-ap-northeast-2.pooler.supabase.com:6543/postgres'
});

async function main() {
  await client.connect();
  
  // Add is_admin column if it doesn't exist
  try {
    await client.query(`ALTER TABLE public.users ADD COLUMN is_admin BOOLEAN DEFAULT FALSE;`);
    console.log('Added is_admin column.');
  } catch (err) {
    if (err.code === '42701') {
      console.log('is_admin column already exists.');
    } else {
      console.error('Error adding column:', err);
    }
  }

  // Make user admin
  const email = 'shrivastavvivek46@gmail.com';
  console.log('Promoting', email);
  const result = await client.query("UPDATE public.users SET is_admin = true WHERE email = $1 RETURNING *;", [email]);
  console.log('Promoted:', result.rows);

  await client.end();
}
main().catch(console.error);
