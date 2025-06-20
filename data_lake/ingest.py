# INGEST: Ambil & Simpan Data Mentah dari CSV, PDF, TXT (Pure Ingestion)

import os
import shutil
import time
import concurrent.futures
from pathlib import Path
from tqdm import tqdm
import pandas as pd
import fitz  # PyMuPDF

def copy_file(src: Path, dst: Path) -> bool:
    """Copy file with error handling and progress tracking."""
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        return True
    except Exception as e:
        print(f"Error copying {src}: {e}")
        return False

def extract_pdf_text(pdf_path: Path, txt_path: Path) -> bool:
    """Extract text from PDF and save to text file."""
    try:
        text_parts = []
        with fitz.open(pdf_path) as doc:
            for page in tqdm(doc, desc="Extracting PDF pages", unit="page"):
                text_parts.append(page.get_text())
        
        txt_path.write_text('\n'.join(text_parts), encoding='utf-8')
        return True
    except Exception as e:
        print(f"Error processing PDF {pdf_path}: {e}")
        return False

def extract_csv_text(csv_path: Path, txt_path: Path) -> bool:
    """Extract text from CSV and save to text file with progress bar."""
    try:
        print(f"\nProcessing CSV {csv_path.name}...")
        # Read CSV in chunks with progress bar
        chunk_size = 1000  # Process 1000 rows at a time
        chunks = []
        with tqdm(desc="Reading CSV chunks", unit="rows") as pbar:
            for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
                chunks.append(chunk)
                pbar.update(len(chunk))
        
        # Combine chunks
        df = pd.concat(chunks, ignore_index=True)
        
        # Convert to text
        text = df.to_csv(index=False)
        
        # Write with progress bar
        with tqdm(total=len(text), desc="Writing CSV text", unit="chars") as pbar:
            with txt_path.open('w', encoding='utf-8') as f:
                for i in range(0, len(text), 10000):  # Write in chunks of 10000 chars
                    f.write(text[i:i+10000])
                    pbar.update(10000)
        
        return True
    except Exception as e:
        print(f"Error processing CSV {csv_path}: {e}")
        return False

def extract_txt_text(txt_path: Path, txt_path_out: Path) -> bool:
    """Copy TXT file to new location with _raw suffix and progress bar."""
    try:
        print(f"\nProcessing TXT {txt_path.name}...")
        
        # Read file size
        file_size = txt_path.stat().st_size
        
        # Read and write with progress bar
        with txt_path.open('r', encoding='utf-8') as src, \
             txt_path_out.open('w', encoding='utf-8') as dst, \
             tqdm(total=file_size, desc="Copying TXT file", unit="B", unit_scale=True) as pbar:
            
            chunk_size = 1024  # 1KB chunks
            while True:
                chunk = src.read(chunk_size)
                if not chunk:
                    break
                dst.write(chunk)
                pbar.update(len(chunk))
        
        return True
    except Exception as e:
        print(f"Error processing TXT {txt_path}: {e}")
        return False

def main():
    start_time = time.time()
    print(" Starting data ingestion process...")
    
    # Get the data_lake directory path
    DATA_LAKE_DIR = Path(__file__).parent  # This file is in data_lake directory
    
    operations = [
        # (source, destination, is_pdf)
        (str(DATA_LAKE_DIR / "adventureworks" / "files" / "warehouse_temp_sensor.csv"), "data_lake/adventureworks/files/warehouse_temp_sensor.csv", False),
        (str(DATA_LAKE_DIR / "adventureworks" / "files" / "market_share_report.pdf"), "data_lake/adventureworks/files/market_share_report.pdf", True),
        (str(DATA_LAKE_DIR / "adventureworks" / "tweets" / "adventureworks_structured_150_tweets.txt"), "data_lake/adventureworks/tweets/adventureworks_structured_150_tweets.txt", False),
    ]
    
    # Process files in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for src_rel, dst_rel, is_pdf in operations:
            src = Path(src_rel)
            dst = DATA_LAKE_DIR / dst_rel
            
            # Submit copy operation
            future = executor.submit(copy_file, src, dst)
            futures.append((future, dst, is_pdf))
        
        # Process results and handle file extraction
        for future, dst, is_pdf in futures:
            if not future.result():
                continue
            
            txt_path = dst.with_stem(f"{dst.stem}_raw").with_suffix('.txt')
            if dst.suffix.lower() == '.pdf':
                extract_pdf_text(dst, txt_path)
            elif dst.suffix.lower() == '.csv':
                extract_csv_text(dst, txt_path)
            elif dst.suffix.lower() == '.txt':
                extract_txt_text(dst, txt_path)
    
    total_time = time.time() - start_time
    print(f"\n Semua file berhasil diproses dalam {total_time:.2f} detik")

if __name__ == "__main__":
    main()
