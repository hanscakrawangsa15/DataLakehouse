import os
import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_structured_txt(file_path: str) -> pd.DataFrame:
    """
    Load structured tweet data from txt file into DataFrame.
    
    Args:
        file_path (str): Path to the input text file
        
    Returns:
        pd.DataFrame: Loaded tweet data
    """
    try:
        df = pd.read_csv(
            file_path, 
            sep='\t', 
            header=None, 
            names=[
                'tweet_id', 
                'tweet_text', 
                'timestamp', 
                'user_location', 
                'sentiment', 
                'matched_product'
            ],
            parse_dates=['timestamp']
        )
        logger.info(f"Successfully loaded {len(df)} records from {file_path}")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading file {file_path}: {str(e)}")
        raise

def save_to_datalake(df: pd.DataFrame, output_dir: str):
    """
    Save the tweet DataFrame to multiple formats in the data lake folder.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        output_dir (str): Output directory path
    """
    try:
        # Create output directory if it doesn't exist
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Define output file paths
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"tweets_structured_{timestamp}"
        
        csv_path = output_path / f"{base_filename}.csv"
        txt_path = output_path / f"{base_filename}.txt"
        parquet_path = output_path / f"{base_filename}.parquet"

        # Save as CSV
        df.to_csv(csv_path, index=False)
        logger.info(f"Data saved to CSV: {csv_path}")

        # Save as TXT (tab-separated)
        df.to_csv(txt_path, sep='\t', index=False)
        logger.info(f"Data saved to TXT: {txt_path}")

        # Save as Parquet
        try:
            df.to_parquet(parquet_path, index=False)
            logger.info(f"Data saved to Parquet: {parquet_path}")
        except Exception as e:
            logger.warning(f"Parquet save failed (optional): {e}")

        logger.info("✅ Data saved to data lake in multiple formats.")
        
    except Exception as e:
        logger.error(f"Error saving data: {str(e)}")
        raise

def main():
    try:
        # Define paths
        base_dir = Path(__file__).parent.parent  # Go up one level from current directory
        input_file = base_dir / "data_lake" / "adventureworks" / "tweets" / "adventureworks_structured_150_tweets.txt"
        output_dir = base_dir / "data_lake" / "adventureworks" / "tweets"
        
        logger.info(f"Starting data ingestion from: {input_file}")
        
        # Load and process data
        df = load_structured_txt(input_file)
        save_to_datalake(df, output_dir)
        
        # Show data summary
        logger.info("\nData Preview:")
        print(f"Total records: {len(df)}")
        print("\nFirst 3 records:")
        print(df.head(3))
        print("\nData types:")
        print(df.dtypes)
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
