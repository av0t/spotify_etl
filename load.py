# for loading data into MySQL and powerBI

# Load phase - saving to MySQL database

import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

def load_dataset(df, logger=None):
    """
    Load pandas DataFrame to MySQL database using incremental loading
    
    Args:
        df: Transformed pandas DataFrame
        logger: Logger instance for logging
    
    Returns:
        Boolean indicating success
    """
    
    if df.empty:
        print("Cannot load empty dataset")
        if logger:
            logger.error("Cannot load empty dataset")
        return False
    
    # Load environment variables
    load_dotenv()
    
    # Rename column to avoid SQL issues
    df_copy = df.copy()
    if 'artist(s)_name' in df_copy.columns:
        df_copy.rename(columns={'artist(s)_name': 'artist_name'}, inplace=True)
    
    # Database connection parameters
    config = {
        'user': os.getenv('MYSQL_USER', 'root'),
        'password': os.getenv('MYSQL_PASSWORD'),
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'raise_on_warnings': True
    }
    
    # Get password if not in environment
    if not config['password']:
        config['password'] = input("Enter MySQL root password: ")
    
    connection = None
    cursor = None
    
    try:
        print("=== Loading to MySQL Database ===")
        if logger:
            logger.info("=== Loading to MySQL Database ===")
        
        # Connect to MySQL server
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        # Check if database exists and create if it doesn't
        try:
            cursor.execute("CREATE DATABASE spotify_db")
            print("✓ Database 'spotify_db' created")
            if logger:
                logger.info("Database 'spotify_db' created")
        except mysql.connector.Error as e:
            if e.errno == 1007:  # Database already exists
                print("✓ Database 'spotify_db' already exists")
                if logger:
                    logger.info("Database 'spotify_db' already exists")
            else:
                raise e  # Re-raise if it's a different error
            
        cursor.execute("USE spotify_db")
        print("✓ Database 'spotify_db' ready")
        if logger:
            logger.info("Database 'spotify_db' ready")
        
        # Create table with explicit error handling
        create_table_query = """
        CREATE TABLE tracks (
            id VARCHAR(255) PRIMARY KEY,
            track_name TEXT,
            artist_name TEXT,
            artist_count INT,
            release_date DATE,
            duration_min FLOAT,
            popularity INT,
            cover_image_url TEXT,
            album_type VARCHAR(50),
            total_streams BIGINT,
            danceability INT,
            tempo FLOAT
        )
        """
        try:
            cursor.execute(create_table_query)
            print("✓ Table 'tracks' created")
            if logger:
                logger.info("Table 'tracks' created")
        except mysql.connector.Error as e:
            if e.errno == 1050:  # Table already exists
                print("✓ Table 'tracks' already exists")
                if logger:
                    logger.info("Table 'tracks' already exists")
            else:
                raise e  # Re-raise if it's a different error
        
        # Get current count before insertion for comparison
        cursor.execute("SELECT COUNT(*) FROM tracks")
        count_before = cursor.fetchone()[0]
        print(f"✓ Current records in database: {count_before}")
        if logger:
            logger.info(f"Current records in database: {count_before}")
        
        # Insert data using INSERT IGNORE for incremental loading
        # Insert data using batch insert (more efficient than row by row)
        insert_query = """
        INSERT IGNORE INTO tracks 
        (id, track_name, artist_name, artist_count, release_date, duration_min, 
         popularity, cover_image_url, album_type, total_streams, danceability, tempo)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Convert DataFrame to list of tuples for batch insert
        data_tuples = [tuple(row) for row in df_copy.values]
        
        # Use executemany for better performance
        cursor.executemany(insert_query, data_tuples)
        
        connection.commit()
        
        print(f"✓ Attempted to insert {len(data_tuples)} records")
        print(f"✓ Actually inserted {cursor.rowcount} unique new records")
        if logger:
            logger.info(f"Attempted to insert {len(data_tuples)} records")
            logger.info(f"Actually inserted {cursor.rowcount} unique new records")
        
        # Quick verification
        cursor.execute("SELECT COUNT(*) FROM tracks")
        count_after = cursor.fetchone()[0]
        print(f"✓ Total records in database: {count_after}")
        if logger:
            logger.info(f"Total records in database: {count_after}")
        
        return True
        
    except Error as e:
        print(f"Error loading data to MySQL: {e}")
        if logger:
            logger.error(f"Error loading data to MySQL: {e}")
        if connection:
            connection.rollback()
        return False
        
    finally:
        # Ensure connections are properly closed
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("✓ Database connection closed")
            if logger:
                logger.info("Database connection closed")

def query_sample_data(limit=5):
    """
    Query and display sample data from the database
    
    Args:
        limit: Number of records to display
    """
    load_dotenv()
    
    config = {
        'user': os.getenv('MYSQL_USER', 'root'),
        'password': os.getenv('MYSQL_PASSWORD'),
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'database': 'spotify_db',
        'raise_on_warnings': True
    }
    
    if not config['password']:
        config['password'] = input("Enter MySQL root password: ")
    
    connection = None
    cursor = None
    
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        cursor.execute(f"""
        SELECT id, track_name, artist_name, popularity, total_streams 
        FROM tracks 
        LIMIT {limit}
        """)
        records = cursor.fetchall()
        
        print(f"\nSample data from database (first {limit} records):")
        print("-" * 80)
        for record in records:
            print(f"ID: {record[0][:10]}... | Track: {record[1]} | Artist: {record[2]} | Pop: {record[3]} | Streams: {record[4]:,}")
        
    except Error as e:
        print(f"Error querying data: {e}")
        
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

# Testing
if __name__ == "__main__":
    print("Load module ready")
    # query_sample_data(3)