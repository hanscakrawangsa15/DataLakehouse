# Struktur ETL Data Warehouse (Sensor + Tweet + Competitor)
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import tkinter as tk
from PIL import ImageTk, Image
import os
from structure import process_etl

# Koneksi database
STAGGING_DB = 'postgresql://postgres:chriscakra15@localhost:5432/stagingdb'
DWH_DB = 'postgresql://postgres:chriscakra15@localhost:5432/adventureworksDW'
engine_stag = create_engine(STAGGING_DB)
engine_dwh = create_engine(DWH_DB)

# ... (semua definisi fungsi dan ETL tetap seperti sebelumnya)

# Jalankan ETL
process_etl()

# Generate visualizations
# Temperature plot
df_sensor = pd.read_sql("SELECT * FROM sensor_data ORDER BY timestamp", engine_dwh)
plt.figure(figsize=(15, 5))
plt.plot(df_sensor['timestamp'], df_sensor['temperature'], marker='o', linestyle='-', color='b')
plt.title('Temperature Over Time')
plt.xlabel('Timestamp')
plt.ylabel('Temperature')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('temperature_plot.png')
plt.close()

# Wordcloud
df_tweet = pd.read_sql("SELECT tweet_text FROM tweet_data", engine_dwh)
all_text = ' '.join(df_tweet['tweet_text'].astype(str))
wordcloud = WordCloud(width=800, height=400, background_color='white').generate(all_text)
plt.figure(figsize=(15, 5))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.tight_layout()
plt.savefig('wordcloud.png')
plt.close()

# Tampilkan GUI
root = tk.Tk()
root.title("Visualisasi ETL Data Warehouse")
root.geometry("1000x800")

label1 = tk.Label(root, text="Grafik Suhu Sensor", font=("Arial", 16))
label1.pack(pady=10)

if os.path.exists("temperature_plot.png"):
    img1 = Image.open("temperature_plot.png")
    img1 = img1.resize((900, 300))
    img_tk1 = ImageTk.PhotoImage(img1)
    panel1 = tk.Label(root, image=img_tk1)
    panel1.image = img_tk1
    panel1.pack()

label2 = tk.Label(root, text="Wordcloud Tweet", font=("Arial", 16))
label2.pack(pady=10)

if os.path.exists("wordcloud.png"):
    img2 = Image.open("wordcloud.png")
    img2 = img2.resize((900, 300))
    img_tk2 = ImageTk.PhotoImage(img2)
    panel2 = tk.Label(root, image=img_tk2)
    panel2.image = img_tk2
    panel2.pack()

root.mainloop()
