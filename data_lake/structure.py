# Struktur ETL Data Warehouse (Sensor + Tweet + Competitor) dengan GUI Langsung (tanpa PNG)
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import tkinter as tk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# Koneksi database
STAGGING_DB = 'postgresql://postgres:chriscakra15@localhost:5432/staging'
DWH_DB = 'postgresql://postgres:chriscakra15@localhost:5432/AdventureworksDW'
engine_stag = create_engine(STAGGING_DB)
engine_dwh = create_engine(DWH_DB)

# Function untuk memeriksa apakah tabel ada dan memiliki data
def check_table_exists_and_has_data(engine, table_name, schema='dwh'):
    try:
        df = pd.read_sql(f"SELECT * FROM {schema}.{table_name} LIMIT 1", con=engine)
        if df.empty:
            print(f"WARNING: Tabel {table_name} kosong!")
            return False
        return True
    except:
        print(f"WARNING: Tabel {table_name} tidak ditemukan!")
        return False

# Function untuk memeriksa data di staging
def check_staging_data():
    print("\nMemeriksa data di staging database...")
    tables = ['warehouse_temp_sensor', 'market_share_report', 'staging_external_sentiment']
    for table in tables:
        try:
            df = pd.read_sql(f"SELECT * FROM {table} LIMIT 1", con=engine_stag)
            if df.empty:
                print(f"WARNING: Tabel staging {table} kosong!")
            else:
                print(f"Data tersedia: Tabel staging {table} memiliki data")
        except:
            print(f"WARNING: Tabel staging {table} tidak ditemukan!")

# Function untuk memeriksa data di warehouse
def check_warehouse_data():
    print("\nMemeriksa data di warehouse...")
    tables = ['dim_sensor', 'dim_time', 'dim_tweet', 'dim_competitor', 'fact_temperature', 'fact_sentiment', 'fact_competitor_share']
    for table in tables:
        if not check_table_exists_and_has_data(engine_dwh, table):
            print(f"Tabel {table} akan dibuat...")
            if table == 'fact_competitor_share':
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS dwh.fact_competitor_share (
                    competitor_id SERIAL PRIMARY KEY,
                    company_name VARCHAR(255),
                    market_share DECIMAL(5,2),
                    time_id INT,
                    FOREIGN KEY (time_id) REFERENCES dwh.dim_time(time_id)
                );
                """
                try:
                    engine_dwh.execute(create_table_sql)
                    print(f"Tabel {table} berhasil dibuat")
                except Exception as e:
                    print(f"ERROR: Gagal membuat tabel {table}: {str(e)}")
            else:
                print(f"WARNING: Tabel {table} tidak ditemukan dan tidak dibuat secara otomatis")

# Function untuk proses ETL dan mengembalikan grafik dan wordcloud
def process_etl():
    check_staging_data()
    check_warehouse_data()

    # SENSOR FLOW
    print("Memproses data sensor...")
    df_sensor = pd.read_sql("SELECT * FROM warehouse_temp_sensor", con=engine_stag)
    df_sensor['timestamp'] = pd.to_datetime(df_sensor['timestamp'])
    df_dim_time = pd.read_sql("SELECT * FROM dwh.dim_time", con=engine_dwh)
    df_dim_time['timestamp'] = pd.to_datetime(df_dim_time['timestamp'])

    new_times = df_sensor['timestamp'].drop_duplicates()
    df_new_time = pd.DataFrame({'timestamp': new_times})
    df_new_time['year'] = df_new_time['timestamp'].dt.year
    df_new_time['month'] = df_new_time['timestamp'].dt.month
    df_new_time['day'] = df_new_time['timestamp'].dt.day
    df_new_time['hour'] = df_new_time['timestamp'].dt.hour

    existing_times = df_dim_time['timestamp'].tolist()
    df_filtered = df_new_time[~df_new_time['timestamp'].isin(existing_times)]
    if not df_filtered.empty:
        df_filtered.to_sql("dim_time", con=engine_dwh, schema='dwh', index=False, if_exists='append')

    df_dim_time = pd.read_sql("SELECT * FROM dwh.dim_time", con=engine_dwh)
    existing_sensors = pd.read_sql("SELECT DISTINCT sensor_id FROM dwh.dim_sensor", con=engine_dwh)['sensor_id'].tolist()
    df_sensor_new = df_sensor[~df_sensor['sensor_id'].isin(existing_sensors)][['sensor_id']].drop_duplicates()
    if not df_sensor_new.empty:
        df_sensor_new.to_sql("dim_sensor", con=engine_dwh, schema='dwh', index=False, if_exists='append')

    df_merged = pd.merge(df_sensor, df_dim_time[['timestamp', 'time_id']], on='timestamp', how='left')
    df_fact = df_merged[['sensor_id', 'temperature_c', 'time_id']]
    df_fact = df_fact.rename(columns={'temperature': 'temperature_c'})
    df_existing_fact = pd.read_sql("SELECT sensor_id, temperature_c, time_id FROM dwh.fact_temperature", con=engine_dwh)
    df_fact_new = pd.concat([df_fact, df_existing_fact]).drop_duplicates(keep=False)
    if not df_fact_new.empty:
        df_fact_new.to_sql("fact_temperature", con=engine_dwh, schema='dwh', index=False, if_exists='append')

    # Visualisasi suhu
    df_temp_plot = pd.read_sql("""
        SELECT t.timestamp, f.temperature_c FROM dwh.fact_temperature f
        JOIN dwh.dim_time t ON f.time_id = t.time_id
        ORDER BY t.timestamp
    """, con=engine_dwh)

    if df_temp_plot.empty:
        fig_temp, ax_temp = plt.subplots(figsize=(10, 4))
        ax_temp.text(0.5, 0.5, 'Tidak ada data suhu untuk divisualisasikan',
                     horizontalalignment='center', verticalalignment='center', transform=ax_temp.transAxes)
        ax_temp.axis('off')
    else:
        fig_temp, ax_temp = plt.subplots(figsize=(10, 4))
        ax_temp.plot(df_temp_plot['timestamp'], df_temp_plot['temperature_c'], marker='o')
        ax_temp.set_title('Perubahan Suhu Sensor dari Waktu ke Waktu')
        ax_temp.set_xlabel('Timestamp')
        ax_temp.set_ylabel('Temperature (C)')
        fig_temp.autofmt_xdate()

    # TWEET FLOW
    df_existing_tweet = pd.read_sql("SELECT tweet_id FROM dwh.dim_tweet", con=engine_dwh)
    existing_tweet_ids = set(df_existing_tweet['tweet_id'])
    
    # Get all columns from the staging table
    df_staging_tweets = pd.read_sql("SELECT * FROM staging_external_sentiment", con=engine_stag)
    
    # Map possible column names to standard names
    column_mapping = {
        'tweet_id': None,
        'author_id': None,
        'tweet_text': None
    }
    
    # Find matching columns (case insensitive)
    for col in df_staging_tweets.columns:
        col_lower = col.lower()
        if 'tweet' in col_lower and 'id' in col_lower:
            column_mapping['tweet_id'] = col
        elif 'author' in col_lower and 'id' in col_lower or 'user' in col_lower and 'id' in col_lower:
            column_mapping['author_id'] = col
        elif 'tweet' in col_lower and 'text' in col_lower or 'content' in col_lower or 'text' in col_lower:
            column_mapping['tweet_text'] = col
    
    # Check if we found all required columns
    missing_columns = [k for k, v in column_mapping.items() if v is None]
    if missing_columns:
        print(f"Warning: Could not find columns for: {', '.join(missing_columns)}")
        # Use default values for missing columns
        for col in missing_columns:
            if col == 'author_id':
                df_staging_tweets[col] = 'unknown_author'
            elif col == 'tweet_text':
                df_staging_tweets[col] = ''
    
    # Select and rename columns
    selected_columns = {v: k for k, v in column_mapping.items() if v is not None}
    df_staging_tweets = df_staging_tweets[list(selected_columns.keys())].rename(columns=selected_columns)
    
    # Ensure we have the required columns
    required_columns = ['tweet_id', 'author_id', 'tweet_text']
    for col in required_columns:
        if col not in df_staging_tweets.columns:
            df_staging_tweets[col] = ''
    
    # Filter out existing tweets and insert new ones
    new_tweets = df_staging_tweets[~df_staging_tweets['tweet_id'].isin(existing_tweet_ids)]
    if not new_tweets.empty:
        new_tweets[required_columns].to_sql(
            "dim_tweet", 
            con=engine_dwh, 
            schema='dwh', 
            index=False, 
            if_exists='append', 
            method='multi'
        )
    else:
        print("No new tweets to insert")

    # WORDCLOUD
    df_tweets = pd.read_sql("SELECT tweet_text FROM dwh.dim_tweet", con=engine_dwh)
    df_tweets['tweet_text'] = df_tweets['tweet_text'].fillna('').astype(str)
    text = ' '.join(df_tweets['tweet_text'].tolist()).strip()
    if not text:
        text = "Tidak ada tweet tersedia"
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)
    fig_wc, ax_wc = plt.subplots(figsize=(10, 4))
    ax_wc.imshow(wordcloud, interpolation='bilinear')
    ax_wc.axis('off')
    ax_wc.set_title("Wordcloud Tweet")

    return fig_temp, fig_wc

# Jalankan ETL
fig_temp, fig_wc = process_etl()

# GUI
root = tk.Tk()
root.title("Visualisasi ETL Data Warehouse")
root.geometry("1000x900")

label1 = tk.Label(root, text="Grafik Suhu Sensor", font=("Arial", 16))
label1.pack(pady=10)
canvas1 = FigureCanvasTkAgg(fig_temp, master=root)
canvas1.draw()
canvas1.get_tk_widget().pack()

label2 = tk.Label(root, text="Wordcloud Tweet", font=("Arial", 16))
label2.pack(pady=10)
canvas2 = FigureCanvasTkAgg(fig_wc, master=root)
canvas2.draw()
canvas2.get_tk_widget().pack()

root.mainloop()
