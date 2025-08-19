"""
Hybrid solution for PySpark with BigQuery on local machine:
- Uses direct connector for simple tables (better performance)
- Falls back to Python client for complex tables (better compatibility)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from google.cloud import bigquery
import pandas as pd
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# GCP project info
PROJECT_ID = "dtc-de-course-466501"
DATASET_ID = "dbt_production"

def create_spark_session(app_name="Local PySpark with BigQuery"):
    """Create and return a Spark session configured for BigQuery access"""
    spark = (SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    
    logger.info(f"Created Spark session: v{spark.version}")
    return spark

def try_direct_connector(spark, table_name, project_id=PROJECT_ID, dataset_id=DATASET_ID, limit=None):
    """
    Try to read a table directly using the BigQuery connector.
    Returns DataFrame if successful, None if failed.
    """
    try:
        logger.info(f"Attempting direct connector read for {project_id}.{dataset_id}.{table_name}")
        
        reader = (spark.read
                .format("bigquery")
                .option("project", project_id)
                .option("dataset", dataset_id)
                .option("table", table_name)
                .option("readDataFormat", "AVRO")
                .option("maxParallelism", "1")
        )
        
        # Load dataframe (this is when the connection happens)
        df = reader.load()
        
        if limit:
            df = df.limit(limit)
        
        # Try a simple action that doesn't process the data fully
        try:
            df.take(1)
            logger.info(f"✅ Direct connector successful: Data loaded")
            return df
        except Exception as e:
            logger.warning(f"⚠️ Table schema loaded but data access failed: {str(e)}")
            logger.warning(f"Will fallback to BigQuery client for this table")
            return None
            
    except Exception as e:
        logger.warning(f"⚠️ Direct connector failed: {str(e)}")
        return None

def query_to_spark(query, spark_session=None):
    """
    Run a BigQuery query and convert results to a Spark DataFrame.
    This is our fallback method when the direct connector doesn't work.
    """
    if spark_session is None:
        spark_session = create_spark_session()
    
    logger.info(f"Fetching data via BigQuery client")
    
    # Create BigQuery client for data fetching
    client = bigquery.Client()
    
    # Execute query and convert to pandas DataFrame
    query_job = client.query(query)
    df_pandas = query_job.to_dataframe()
    
    # If the dataframe is empty, return an empty spark dataframe
    if df_pandas.empty:
        logger.warning(f"Query returned no results")
        if spark_session is not None:
            return spark_session.createDataFrame([], schema=None)
        return None
    
    # Convert pandas DataFrame directly to Spark DataFrame
    try:
        # Use createDataFrame to convert pandas directly to Spark
        df_spark = spark_session.createDataFrame(df_pandas)
        logger.info(f"✅ BigQuery client successful: {df_spark.count()} rows loaded via pandas")
        return df_spark
    except Exception as e:
        logger.error(f"❌ Failed to convert pandas DataFrame to Spark: {str(e)}")
        return None

def load_table(spark, table_name, project_id=PROJECT_ID, dataset_id=DATASET_ID, limit=None):
    """
    Smart table loader that tries direct connector first, falls back to client if needed.
    This is the main function you should use.
    """
    # Tables known to have complex schemas that cause issues with direct connector
    complex_tables = ["fact_trips"]
    
    # Skip direct connector for known complex tables
    if table_name in complex_tables:
        logger.info(f"Table '{table_name}' known to have complex schema, using client method")
        limit_clause = f"LIMIT {limit}" if limit else ""
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}` {limit_clause}"
        return query_to_spark(query, spark)
    
    # For other tables, try direct connector first (faster, no temp files)
    df = try_direct_connector(spark, table_name, project_id, dataset_id, limit)
    
    if df is not None:
        return df
    
    # If direct connector failed, fall back to BigQuery client approach
    logger.info("Falling back to BigQuery client approach")
    
    # Build query with limit if provided
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}` {limit_clause}"
    
    return query_to_spark(query, spark)


# Example usage
if __name__ == "__main__":
    # Initialize Spark
    spark = create_spark_session()
    
    # 1. Test with simple table (should work with direct connector)
    logger.info("Testing with simple table (dim_zones)")
    df_zones = load_table(spark, "dim_zones", limit=10)
    print("\n=== Simple Table Results ===")
    df_zones.printSchema()
    df_zones.show()
    
    # 2. Test with complex table (will use fallback)
    logger.info("Testing with complex table (fact_trips)")
    df_trips = load_table(spark, "fact_trips", limit=5)
    print("\n=== Complex Table Results ===")
    df_trips.printSchema()
    df_trips.show()
    
    # 3. Example of a join operation (combining data from both tables)
    print("\n=== Running Example Join ===")
    if df_trips is not None and df_zones is not None:
        # Join trips with pickup zone info
        result = (df_trips
            .join(
                df_zones.alias("zones"), 
                df_trips["pickup_locationid"] == F.col("zones.locationid"),
                "inner"
            )
            .select(
                "tripid", 
                F.col("pickup_datetime").alias("datetime"),
                F.col("zones.borough").alias("borough"), 
                F.col("zones.zone").alias("zone"),
                "trip_distance", 
                "fare_amount"
            )
        )
        
        print("Joined results:")
        result.show()
    
    # Stop Spark session when done
    spark.stop()
