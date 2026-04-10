const { Client } = require('pg');
const client = new Client({
  connectionString: 'postgresql://postgres.rhmqhrjbknazyflmbwbv:6%3F9H%23%40Dv5W%2BVTEZ@aws-1-ap-northeast-2.pooler.supabase.com:6543/postgres'
});

async function main() {
  await client.connect();
  const res = await client.query(`
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'users' AND table_schema = 'public'
  `);
  console.log('Public Users Schema:', res.rows);
  await client.end();
}
main().catch(console.error);
