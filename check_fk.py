import psycopg2
conn = psycopg2.connect("postgresql://postgres:6%3F9H%23%40Dv5W%2BVTEZ@db.rhmqhrjbknazyflmbwbv.supabase.co:5432/postgres")
cur = conn.cursor()
cur.execute("ALTER TABLE campaign_contacts DROP CONSTRAINT campaign_contacts_campaign_id_fkey")
conn.commit()
print("FK constraint dropped successfully")
conn.close()
