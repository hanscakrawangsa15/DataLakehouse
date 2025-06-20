"""
Data analysis module for processing and ingesting data into the data lake.

This module handles the extraction, transformation, and loading (ETL) of data from
various sources (CSV, PDF) into a staging database. It includes functionality to:
- Extract text from PDF files
- Parse market share data
- Process and clean data
- Load data into a PostgreSQL database
"""

import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2 
from psycopg2 import sql
import os
import fitz  # PyMuPDF
import re

def setup_database():
    """Set up the database connection and create database if it doesn't exist."""
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

    return f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def extract_text_from_pdf(path):
    """Extract text from PDF file."""
    with fitz.open(path) as doc:
        return "\n".join(page.get_text() for page in doc)

def parse_market_share(text):
    """Parse market share data from text."""
    matches = re.findall(r"(Company\s+[A-D]|Others)\s*-\s*(\d+)%", text)
    return pd.DataFrame([
        {'company': company, 'market_share_percent': int(percent)}
        for company, percent in matches
    ])

def main():
    """Main function to run the ETL process."""
    # Set up database connection
    db_url = setup_database()
    engine = create_engine(db_url)

    # Baca structured .txt
    base_dir = os.path.dirname(os.path.abspath(__file__))
    structured_txt_path = os.path.join(base_dir, "adventureworks", "tweets", "adventureworks_structured_150_tweets.txt")
    df = pd.read_csv(structured_txt_path, sep='\t', header=None, names=[
        'tweet_id', 'tweet_text', 'timestamp', 'user_location', 'sentiment', 'matched_product'
    ])

    # Ubah timestamp ke format datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Simpan ke PostgreSQL (tabel 'external_sentiment' di DB 'stagging')
    # ==================== INGEST FILE CSV ==================== #
    csv_path = os.path.join(base_dir, "adventureworks", "files", "warehouse_temp_sensor.csv")

    try:
        df_sensor = pd.read_csv(csv_path)
        df_sensor.to_sql("warehouse_temp_sensor", con=engine, index=False, if_exists='replace')
        print("✅ Berhasil mengimpor data ke stagging.warehouse_temp_sensor")
    except Exception as e:
        print(f"❌ Gagal memuat CSV: {e}")

    # ==================== INGEST FILE PDF ==================== #
    pdf_path = os.path.join(base_dir, "adventureworks", "files", "market_share_report.pdf")

    try:
        pdf_text = extract_text_from_pdf(pdf_path)
        df_pdf = parse_market_share(pdf_text)
        df_pdf.to_sql("market_share_report", con=engine, index=False, if_exists='replace')
        print("✅ Berhasil mengimpor data ke stagging.market_share_report")
    except Exception as e:
        print(f"❌ Gagal memuat PDF: {e}")

    df.to_sql("external_sentiment", con=engine, index=False, if_exists='replace')       

    print("✅ Berhasil mengimpor data ke stagging.external_sentiment")

if __name__ == "__main__":
    main()
