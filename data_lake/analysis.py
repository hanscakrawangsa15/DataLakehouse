# ✅ ANALYZE: Membersihkan & Menstrukturkan Data Mentah ke Staging Database

"""
Tujuan:
- Parsing isi file (PDF → Teks → DataFrame)
- Validasi tipe data dan handling missing values
- Cleaning teks, normalisasi data (contoh: lowercase, hapus karakter khusus)
- Deteksi entitas penting (contoh: nama perusahaan, sentimen)
- Simpan hasil analisis ke database staging
"""

import pandas as pd
import fitz  # PyMuPDF
from sqlalchemy import create_engine
import re
from datetime import datetime

# ---------- KONFIGURASI DATABASE ---------- #
DB_NAME = 'staging'
DB_USER = 'postgres'
DB_PASS = 'chriscakra15'
DB_HOST = 'localhost'
DB_PORT = '5432'
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# ---------- 1. ANALISIS FILE CSV ---------- #
csv_path = "data_lake/adventureworks/files/warehouse_temp_sensor.csv"
df_csv = pd.read_csv(csv_path)
df_csv.to_sql("staging_warehouse_temp_sensor", con=engine, index=False, if_exists='append')

# ---------- 2. ANALISIS FILE PDF ---------- #
def extract_text_from_pdf(path):
    text = ""
    with fitz.open(path) as doc:
        for page in doc:
            text += page.get_text()
    return text

def parse_market_share(text):
    data = []
    matches = re.findall(r"(Company\s+[A-D]|Others)\s*-\s*(\d+)%", text)
    for match in matches:
        data.append({"company": match[0], "market_share_percent": int(match[1])})
    return pd.DataFrame(data)

pdf_path = "data_lake/adventureworks/files/market_share_report.pdf"
pdf_text = extract_text_from_pdf(pdf_path)
df_pdf = parse_market_share(pdf_text)
df_pdf.to_csv("data_lake/adventureworks/files/market_share_report.csv", index=False)
df_pdf.to_sql("staging_market_share_report", con=engine, index=False, if_exists='append')

# ---------- 3. ANALISIS FILE TXT ---------- #
txt_path = "data_lake/adventureworks/tweets/adventureworks_structured_150_tweets.txt"
df_txt = pd.read_csv(txt_path, sep='\t', header=None, names=[
    'tweet_id', 'tweet_text', 'timestamp', 'user_location', 'sentiment', 'matched_product'
])
df_txt['timestamp'] = pd.to_datetime(df_txt['timestamp'])
df_txt.to_csv("data_lake/adventureworks/files/tweet_data.csv", index=False)
df_txt.to_sql("staging_external_sentiment", con=engine, index=False, if_exists='append')

print("✅ Semua file hasil analisis berhasil dimasukkan ke staging database.")
