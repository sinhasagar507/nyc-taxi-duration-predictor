from time import time
import pandas as pd
import os
from sqlalchemy import create_engine

# Execution starts here
def ingest_callable(user, password, host, port, db, table_name, csv_file):
    
    print(table_name, csv_file)

    # Connect to the database engine
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()
    print(engine)
    
    # Download the CSV 
    # df_chunk = pd.read_csv(csv_name, iterator=True, chunksize=100000) # Initialize an iterator to the data
    # df_next_chunk = next(df_chunk) # Get the first batch of data
    # df_next_chunk["tpep_pickup_datetime"] = pd.to_datetime(df_next_chunk["tpep_pickup_datetime"]) # Cleaning up pickup time
    # df_next_chunk["tpep_dropoff_datetime"] = pd.to_datetime(df_next_chunk["tpep_dropoff_datetime"]) # Cleaning up dropoff time
    # df_next_chunk.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace") # This command inserts the initial table with the columns into Postgres
    # df_next_chunk.to_sql(name=table_name, con=engine, if_exists="append") # Store the first chunk to database
            
    # # Insert all chunks one by one 
    # while True:
    #     t_start = time()

    #     df_subset = next(df_chunk)
        
    #     df_subset["tpep_pickup_datetime"] = pd.to_datetime(df_subset["tpep_pickup_datetime"])
    #     df_subset["tpep_dropoff_datetime"] = pd.to_datetime(df_subset["tpep_dropoff_datetime"])
        
    #     df_subset.to_sql(name=table_name, con=engine, if_exists="append")
        
    #     t_end = time()
        
    #     print("Inserted another chunk..., took %.3f seconds" % (t_end-t_start))
