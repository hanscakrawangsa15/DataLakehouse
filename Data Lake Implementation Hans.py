import os
import pandas as pd
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import shutil
from PyPDF2 import PdfReader
from textblob import TextBlob
import requests

#Membuat folder staging, raw, dan processed
BASE_DIR = "data-lake"
STAGING = os.path.join(BASE_DIR, "staging")
RAW = os.path.join(BASE_DIR, "raw")
PROCESSED = os.path.join(BASE_DIR, "processed")

#Membuat folder jika belum ada
os.makedirs(STAGING, exist_ok=True)
os.makedirs(os.path.join(RAW, "sensor-data"), exist_ok=True)
os.makedirs(os.path.join(RAW, "financial-reports"), exist_ok=True)
os.makedirs(os.path.join(RAW, "social-comments"), exist_ok=True)
os.makedirs(os.path.join(PROCESSED, "sensor-parquet"), exist_ok=True)
os.makedirs(os.path.join(PROCESSED, "comments-cleaned"), exist_ok=True)

# API Functions
#Mengambil data sensor dari file parquet
def get_sensor_data_by_date(start_date, end_date):
    df = pd.read_parquet(os.path.join(PROCESSED, "sensor-parquet", "data.parquet"))
    return df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]

#Mengambil teks dari file pdf
def extract_pdf_text(file_path):
    from PyPDF2 import PdfReader
    reader = PdfReader(file_path)
    return " ".join(page.extract_text() for page in reader.pages)

#Menghitung sentimen dari file txt
def summarize_social_sentiment(txt_file):
    from textblob import TextBlob
    with open(txt_file) as f:
        comments = f.readlines()
    sentiments = [TextBlob(comment).sentiment.polarity for comment in comments]
    return sum(sentiments) / len(sentiments)

#Menghasilkan wordcloud dari file txt
def generate_wordcloud_from_txt(txt_file, output_image):
    with open(txt_file) as f:
        text = f.read()
    wordcloud = WordCloud(width=800, height=400, background_color="white").generate(text)
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.savefig(output_image)
    plt.close()

# Data Management Functions

#Memindahkan file dari folder staging ke folder raw
def move_files_to_raw():
    for file in os.listdir(STAGING):
        full_path = os.path.join(STAGING, file)
        if file.endswith(".csv"):
            shutil.move(full_path, os.path.join(RAW, "sensor-data", file))
        elif file.endswith(".pdf") or file.endswith(".docx"):
            shutil.move(full_path, os.path.join(RAW, "financial-reports", file))
        elif file.endswith(".txt"):
            shutil.move(full_path, os.path.join(RAW, "social-comments", file))

#Memindahkan file dari folder source ke folder destination
def move_files_between_folders(src_folder, dst_folder):
    os.makedirs(dst_folder, exist_ok=True)
    for file_name in os.listdir(src_folder):
        src_path = os.path.join(src_folder, file_name)
        dst_path = os.path.join(dst_folder, file_name)
        if os.path.isfile(src_path) and file_name.lower().endswith(('.csv', '.pdf', '.docx', '.txt')):
            shutil.move(src_path, dst_path)
            print(f"Moved: {src_path} -> {dst_path}")

# Data Processing Functions

#Mengubah file csv menjadi parquet
def process_sensor_csv_to_parquet(csv_file):
    df = pd.read_csv(csv_file)
    df.to_parquet(os.path.join(PROCESSED, "sensor-parquet", "data.parquet"))
    return df

#Mengambil file dari url dan menyimpan ke file

def copy_from_url(url, target_file):
    import requests
    r = requests.get(url)
    with open(target_file, "wb") as f:
        f.write(r.content)

#Memindahkan file dari folder source ke folder destination
if __name__ == "__main__":
    source_folder = "file_Adventureworks2"
    destination_folder = "file_adventureworks"
    move_files_between_folders(source_folder, destination_folder)

    # Simulasi file baru masuk staging
    shutil.copy(os.path.join(destination_folder, "warehouse_temp_jan.csv"), os.path.join(STAGING, "warehouse_temp_jan.csv"))
    shutil.copy(os.path.join(destination_folder, "IDX_Report_BikeMarket.pdf"), os.path.join(STAGING, "IDX_Report_BikeMarket.pdf"))
    shutil.copy(os.path.join(destination_folder, "Competitor_Annual_Report.docx"), os.path.join(STAGING, "Competitor_Annual_Report.docx"))
    shutil.copy(os.path.join(destination_folder, "tweets_about_adventureworks.txt"), os.path.join(STAGING, "tweets_about_adventureworks.txt"))

    move_files_to_raw()
    process_sensor_csv_to_parquet(os.path.join(RAW, "sensor-data", "warehouse_temp_jan.csv"))
    generate_wordcloud_from_txt(os.path.join(RAW, "social-comments", "tweets_about_adventureworks.txt"),
                                os.path.join(PROCESSED, "comments-cleaned", "wordcloud.png"))
