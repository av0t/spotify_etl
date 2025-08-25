# Main file for running ETL

import os
from dotenv import load_dotenv
from pathlib import Path

from extract import create_spotify_dataset
from transform import transform_dataset
from load import load_dataset
from logger_config import setup_logger

# Main execution
def main():
    # Setup logging
    logger = setup_logger()
    
    # Replace with your actual credentials in .env file 
    # Automatically load .env from project root
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)

    CLIENT_ID = os.getenv('CLIENT_ID')
    CLIENT_SECRET = os.getenv('CLIENT_SECRET')

    # optional: fail early if missing
    if not CLIENT_ID or not CLIENT_SECRET:
        logger.error("Missing CLIENT_ID or CLIENT_SECRET in environment (.env file)")
        raise RuntimeError("Missing CLIENT_ID or CLIENT_SECRET in environment (.env file)")
    
    # Configuration
    YEAR = 2024  # Change this to extract from different years
    TRACKS_PER_TERM = 200  # Number of tracks to get from each search term
    
    # EXTRACT
    # Create dataset
    df = create_spotify_dataset(CLIENT_ID, CLIENT_SECRET, year=YEAR, tracks_per_term=TRACKS_PER_TERM, logger=logger)

    # TRANSFROM
    # Carry out transformations
    df = transform_dataset(df)
    
    # Display results
    if not df.empty:
        print("\nFirst 5 rows of the dataset:")
        print(df.head())
        
        print(f"\nDataset shape: {df.shape}")
        logger.info(f"Dataset created with {len(df)} tracks")
        
        # Save to CSV with year in filename
        filename = f'csv/spotify_tracks_{YEAR}.csv'
        df.to_csv(filename, index=False)
        print(f"\nDataset saved to '{filename}'")
        logger.info(f"Dataset saved to '{filename}'")
        
    else:
        print("Failed to create dataset")
        logger.error("Failed to create dataset")

    # LOAD
    success = load_dataset(df, logger=logger)
    if success:
        print("✓ ETL process completed successfully!")
        logger.info("ETL process completed successfully!")
    
    else:
        print("✗ ETL process completed with errors in load phase")
        logger.error("ETL process completed with errors in load phase")

if __name__ == "__main__":
    main()