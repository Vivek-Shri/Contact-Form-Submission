import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://postgres:6?9H#@Dv5W+VTEZ@db.rhmqhrjbknazyflmbwbv.supabase.co:5432/postgres',
});

export default pool;
