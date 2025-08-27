# Main file for running ETL

import os
from dotenv import load_dotenv
from pathlib import Path
import time

from extract import create_spotify_dataset
from transform import transform_dataset
from load import load_dataset, log_etl_run
from utils.logger_config import setup_logger

# Main execution
def main():
    # Setup logging
    logger = setup_logger()

    # Track ETL status and metrics
    etl_run_data = {
        'status': 'failure',  # Default to failure, update to success if all goes well
        'extract_status': 'failure',
        'transform_status': 'failure',
        'load_status': 'failure',
        'tracks_extracted': 0,
        'tracks_loaded': 0,
        'error_message': None,
        'duration_seconds': 0
    }
    
    start_time = time.time()
    
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
    YEAR = 2023  # Change this to extract from different years
    TRACKS_PER_TERM = 300  # Number of tracks to get from each search term
    
    try:
        # EXTRACT
        # Create dataset
        df = create_spotify_dataset(CLIENT_ID, CLIENT_SECRET, year=YEAR, tracks_per_term=TRACKS_PER_TERM, logger=logger)
        
        etl_run_data['tracks_extracted'] = len(df)
        etl_run_data['extract_status'] = 'success' if not df.empty else 'failure'

        # TRANSFROM
        # Carry out transformations
        df = transform_dataset(df)
        
        etl_run_data['transform_status'] = 'success'

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
        success, loaded_count = load_dataset(df, logger=logger)
        etl_run_data['load_status'] = 'success' if success else 'failure'
        etl_run_data['tracks_loaded'] = loaded_count if success else 0

        if success:
            print("✓ ETL process completed successfully!")
            logger.info("ETL process completed successfully!")
            etl_run_data['status'] = 'success'
        
        else:
            print("✗ ETL process completed with errors in load phase")
            logger.error("ETL process completed with errors in load phase")
    
    except Exception as e:
        etl_run_data['error_message'] = str(e)
        logger.error(f"ETL process failed: {e}")
    
    finally:
        # Calculate duration
        etl_run_data['duration_seconds'] = time.time() - start_time
        
        # Log the ETL run to database
        log_etl_run(etl_run_data, logger)
            


if __name__ == "__main__":
    main()