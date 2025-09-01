# ===========================
# IMPORTS
# ===========================
import pandas as pd
import os
from sqlalchemy import create_engine, inspect
import logging
import time

# ===========================
# LOGGING CONFIGURATION
# ===========================
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ===========================
# DATABASE ENGINE
# ===========================
engine = create_engine('sqlite:///inventory.db')

# ===========================
# FUNCTION: INGEST DATAFRAME INTO DATABASE
# ===========================
def ingest_db(df, table_name, engine, chunksize=50000):
    """
    Ingests a dataframe into a database table.
    Uses chunking for better performance on large files.
    """
    df.to_sql(table_name, con=engine, if_exists='replace', index=False, chunksize=chunksize)
    print(f"✅ Inserted {df.shape[0]} rows into '{table_name}' table.")

# ===========================
# FUNCTION: LOAD RAW DATA FROM CSV FILES
# ===========================
def load_raw_data():
    """
    Loads CSV files from 'data' folder and ingests them into DB.
    Large files (>100MB) are processed in chunks.
    """
    start = time.time()
    csv_files = [f for f in os.listdir('data') if f.endswith('.csv')]

    for file in csv_files:
        file_path = os.path.join('data', file)
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # Convert bytes → MB

        print(f"📂 Processing: {file} | Size: {round(file_size, 2)} MB")

        try:
            if file_size > 100:  # If file is larger than 100MB → use chunking
                logging.info(f"Large file detected: {file}, ingesting in chunks...")
                for chunk in pd.read_csv(file_path, chunksize=50000):
                    ingest_db(chunk, file[:-4], engine)
            else:
                df = pd.read_csv(file_path)
                ingest_db(df, file[:-4], engine)

            logging.info(f"{file} ingested successfully.")

        except Exception as e:
            logging.error(f"Error while ingesting {file}: {str(e)}")
            print(f"❌ Failed to ingest {file}: {e}")

    end = time.time()
    print(f"\n✅ Ingestion Completed in {round((end-start)/60, 2)} minutes!")

# ===========================
# MAIN EXECUTION
# ===========================
if __name__ == "__main__":
    load_raw_data()

    # ===========================
    # LIST ALL TABLES IN DATABASE
    # ===========================
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print("\n📌 Tables in database:", tables)
