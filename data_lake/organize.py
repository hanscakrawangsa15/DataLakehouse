import os
import shutil
from pathlib import Path

def organize_files():
    # Base directory
    base_dir = Path(__file__).parent
    
    # Source files
    source_files = {
        'pdf': base_dir / 'adventureworks' / 'files' / 'market_share_report.pdf',
        'txt': base_dir / 'adventureworks' / 'tweets' / 'adventureworks_structured_150_tweets.txt',
        'csv': base_dir / 'adventureworks' / 'files' / 'warehouse_temp_sensor.csv'
    }
    
    # Destination directory
    dest_dir = base_dir / 'data_lake' / 'adventureworks' / 'organized'
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    # Organize files
    for file_type, source_path in source_files.items():
        if source_path.exists():
            dest_path = dest_dir / source_path.name
            shutil.copy2(source_path, dest_path)
            print(f"Copied {source_path} to {dest_path}")
        else:
            print(f"Warning: Source file not found: {source_path}")

if __name__ == "__main__":
    organize_files()
    print("File organization completed.")