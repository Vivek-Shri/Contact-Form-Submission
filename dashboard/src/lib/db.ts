import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://postgres:6%3F9H%23%40Dv5W%2BVTEZ@db.rhmqhrjbknazyflmbwbv.supabase.co:5432/postgres',
});

export default pool;
