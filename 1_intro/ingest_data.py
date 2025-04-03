import argparse as ap
from time import time
import pandas as pd
import os
from sqlalchemy import create_engine

# Execution starts here
def main(params):
        user = params.user
        password = params.password
        host = params.host
        port = params.port
        db = params.db 
        table_name = params.table_name
        url = params.url
        csv_name = "output.csv.gz"
        
        # Output to CSV 
        os.system(f"wget {url} -O {csv_name}")
        
        # Connect to the database engine
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
        engine.connect()

        # Download the CSV 
        df_chunk = pd.read_csv(csv_name, iterator=True, chunksize=100000) # Initialize an iterator to the data
        df_next_chunk = next(df_chunk) # Get the first batch of data
        df_next_chunk["tpep_pickup_datetime"] = pd.to_datetime(df_next_chunk["tpep_pickup_datetime"]) # Cleaning up pickup time
        df_next_chunk["tpep_dropoff_datetime"] = pd.to_datetime(df_next_chunk["tpep_dropoff_datetime"]) # Cleaning up dropoff time
        df_next_chunk.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace") # This command inserts the initial table with the columns into Postgres
        df_next_chunk.to_sql(name=table_name, con=engine, if_exists="append") # Store the first chunk to database
                
        # Insert all chunks one by one 
        while True:
                t_start = time()
        
                df_subset = next(df_chunk)
                
                df_subset["tpep_pickup_datetime"] = pd.to_datetime(df_subset["tpep_pickup_datetime"])
                df_subset["tpep_dropoff_datetime"] = pd.to_datetime(df_subset["tpep_dropoff_datetime"])
                
                df_subset.to_sql(name=table_name, con=engine, if_exists="append")
                
                t_end = time()
                
                print("Inserted another chunk..., took %.3f seconds" % (t_end-t_start))
                
                
if __name__ == "__main__":
        
        # Initialize the argument parser
        parser = ap.ArgumentParser(description="Ingest this data to Postgres")
        # A bunch of flags/arguments. Include user, password, host, port, database name, table name, URL of the CSV
        parser.add_argument('--user', help='user name for postgres')
        parser.add_argument('--password', help='password for postgres')
        parser.add_argument('--host', help='host for postgres')
        parser.add_argument('--port', help='port for postgres')
        parser.add_argument('--db', help='database name for postgres')
        parser.add_argument('--table_name', help='name of the table where we will write the results to')
        parser.add_argument('--url', help='URL of the CSV file')
        # Now I will parse the arguments
        args = parser.parse_args()
        
        # Pass to the main methods
        main(args)

