# spotify_etl
End-to-end data ETL project for vizualization of spotify web API data

# Spotify ETL Project

An end-to-end ETL (Extract, Transform, Load) pipeline built with Python to work with the [Spotify Web API].  
This personal project extracts track data, transforms it into a clean format, and loads it into a database for further visualization and analysis.

---

## Features
- Extracts track metadata (artists, albums, popularity, etc.) from Spotify Web API
- Cleans and transforms raw data (converts duration, handles dates, etc.)
- Stores results into a relational database (MySQL)
- Ready for visualization (e.g., Power BI, Tableau, or Python dashboards)

---

## Requirements
- Python 3.9+  (3.11 recommended)
- Git  
- A Spotify Developer account (for API credentials)  
- MySQL installed locally

---

## Setup

1. **Clone the repo**
   ```bash
   git clone https://github.com/your-username/etl-project.git
   cd spotify_etl

2. **create virtual environemtn**

3. **Install dependencies**
    pip install -r requirements.txt

4. **Set environment variables**
    create .env and add your Spotify API and MySQL credentials
    
    CLIENT_ID=example
    CLIENT_SECRET=example
    MYSQL_HOST=example
    MYSQL_USER=example
    MYSQL_PASSWORD=example

## Usage
Run the ETL pipeling:
python etl.py

The script will:
-Extract data from Spotify
-Transform and clean it
-Load it into the configured MySQL database

## Notes
-Secrets (like .env) are ignored from version control.
-For visualization, connect your BI tool directly to the spotify_db database.