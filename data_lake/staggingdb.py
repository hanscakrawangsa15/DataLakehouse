import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2 import sql
import os

# Konfigurasi koneksi PostgreSQL
DB_NAME = 'staging'
DB_USER = 'postgres'
DB_PASS = 'chriscakra15'
DB_HOST = 'localhost'
DB_PORT = '5432'

# Cek dan buat database jika belum ada
try:
    # Coba konek ke database
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.close()
except psycopg2.OperationalError:
    # Jika database tidak ada, buat database baru
    conn = psycopg2.connect(
        dbname='postgres',  # Connect ke database default 'postgres'
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql.SQL("CREATE DATABASE {}").format(
        sql.Identifier(DB_NAME))
    )
    cur.close()
    conn.close()

# Koneksi SQLAlchemy
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Baca structured .txt
base_dir = os.path.dirname(os.path.abspath(__file__))
structured_txt_path = os.path.join(base_dir, "adventureworks", "tweets", "adventureworks_structured_150_tweets.txt")
df = pd.read_csv(structured_txt_path, sep='\t', header=None, names=[
    'tweet_id', 'tweet_text', 'timestamp', 'user_location', 'sentiment', 'matched_product'
])

# Ubah timestamp ke format datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Simpan ke PostgreSQL (tabel 'external_sentiment' di DB 'stagging')
df.to_sql("external_sentiment", con=engine, index=False, if_exists='replace')       

print("✅ Berhasil mengimpor data ke stagging.external_sentiment")
