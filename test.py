import os
from dotenv import load_dotenv
import psycopg

load_dotenv()
conn = psycopg.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()
cur.execute("SELECT version()")
print(cur.fetchone())
conn.close()