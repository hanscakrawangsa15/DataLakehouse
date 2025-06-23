import os
import shutil

def organize_files():
    # Source directory
    source_dir = r"C:\Users\hansc\OneDrive - Institut Teknologi Sepuluh Nopember\Semester 4 SISFOR\DLH\data_lake\data_lake\adventureworks\organized"
    
    # Destination directories
    tweets_dir = r"C:\Users\hansc\OneDrive - Institut Teknologi Sepuluh Nopember\Semester 4 SISFOR\DLH\data_lake\data_lake\adventureworks\tweets"
    files_dir = r"C:\Users\hansc\OneDrive - Institut Teknologi Sepuluh Nopember\Semester 4 SISFOR\DLH\data_lake\data_lake\adventureworks\files"
    
    # Create destination directories if they don't exist
    os.makedirs(tweets_dir, exist_ok=True)
    os.makedirs(files_dir, exist_ok=True)
    
    # Get all files in the source directory
    for filename in os.listdir(source_dir):
        source_path = os.path.join(source_dir, filename)
        
        # Skip directories
        if os.path.isdir(source_path):
            continue
            
        # Move files based on extension
        if filename.lower().endswith('.txt'):
            dest_path = os.path.join(tweets_dir, filename)
            shutil.move(source_path, dest_path)
            print(f"Moved {filename} to tweets folder")
        elif filename.lower().endswith(('.pdf', '.csv')):
            dest_path = os.path.join(files_dir, filename)
            shutil.move(source_path, dest_path)
            print(f"Moved {filename} to files folder")

if __name__ == "__main__":
    organize_files()
