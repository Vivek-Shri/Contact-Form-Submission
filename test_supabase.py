import psycopg2
import os

DATABASE_URL = "postgresql://postgres:6%3F9H%23%40Dv5W%2BVTEZ@db.rhmqhrjbknazyflmbwbv.supabase.co:5432/postgres"

def test_connection():
    print(f"Testing connection to: {DATABASE_URL.split('@')[-1]}")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"Success! Connected to Supabase.")
        print(f"PostgreSQL Version: {version[0]}")
        
        # Check if outreach_runs table exists
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'outreach_runs');")
        exists = cur.fetchone()[0]
        print(f"Outreach Runs Table exists: {exists}")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Failed to connect: {e}")

if __name__ == "__main__":
    test_connection()
