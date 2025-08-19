"""
BigQuery Connector for Local PySpark

This module provides utilities for connecting your local PySpark environment to BigQuery.
It uses a hybrid approach:
- Direct connector for simple tables (better performance)
- Python client for complex tables (better compatibility)

Usage:
    from bigquery_connector import create_spark_session, load_table
    
    # Create a Spark session
    spark = create_spark_session()
    
    # Load data from BigQuery
    df = load_table(spark, "fact_trips", limit=1000)
    
    # Process data locally with Spark
    result = df.groupBy("pickup_zone").count()
    result.show()
"""
from pyspark.sql import SparkSession
import logging
from google.cloud import bigquery

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session(app_name="Local PySpark with BigQuery", 
                        memory="4g",
                        connector_version="0.32.0"):
    """
    Create a Spark session configured for BigQuery access
    
    Args:
        app_name (str): Name of the Spark application
        memory (str): Amount of memory to allocate (e.g. "4g")
        connector_version (str): Version of the BigQuery connector
        
    Returns:
        SparkSession: Configured Spark session
    """
    spark = (SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", f"com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:{connector_version}")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", memory)
        .getOrCreate()
    )
    
    logger.info(f"Created Spark session: v{spark.version}")
    return spark

def try_direct_connector(spark, table_name, project_id, dataset_id, limit=None):
    """
    Try to read a table directly using the BigQuery connector
    
    Args:
        spark: Spark session
        table_name (str): Name of the table
        project_id (str): GCP project ID
        dataset_id (str): BigQuery dataset ID
        limit (int, optional): Limit the number of rows
        
    Returns:
        DataFrame or None: Spark DataFrame if successful, None if failed
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
        
        # Load dataframe
        df = reader.load()
        
        if limit:
            df = df.limit(limit)
        
        # Try a simple action
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
    Run a BigQuery query and convert results to a Spark DataFrame
    
    Args:
        query (str): SQL query to execute
        spark_session: Spark session (or None to create new)
        
    Returns:
        DataFrame: Spark DataFrame with query results
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

def load_table(spark, table_name, project_id=None, dataset_id=None, limit=None, columns=None):
    """
    Load a BigQuery table into a Spark DataFrame with fallback
    
    Args:
        spark: Spark session
        table_name (str): BigQuery table name
        project_id (str, optional): GCP project ID (defaults to active project)
        dataset_id (str, optional): BigQuery dataset ID (required if project_id is specified)
        limit (int, optional): Limit the number of rows
        columns (list, optional): List of columns to select
        
    Returns:
        DataFrame: Spark DataFrame with table data
    """
    # If no project provided, get from client
    if not project_id:
        client = bigquery.Client()
        project_id = client.project
        logger.info(f"Using active project: {project_id}")
    
    # Tables known to cause issues with direct connector
    complex_tables = ["fact_trips"]
    
    # Skip direct connector for known complex tables
    if table_name in complex_tables:
        logger.info(f"Table '{table_name}' known to have complex schema, using client method")
        
        # Build column selection
        column_selection = "*"
        if columns:
            column_selection = ", ".join(columns)
        
        # Build query with limit if provided
        limit_clause = f"LIMIT {limit}" if limit else ""
        query = f"SELECT {column_selection} FROM `{project_id}.{dataset_id}.{table_name}` {limit_clause}"
        
        return query_to_spark(query, spark)
    
    # For other tables, try direct connector first (faster, no temp files)
    df = try_direct_connector(spark, table_name, project_id, dataset_id, limit)
    
    if df is not None:
        # Apply column selection if provided
        if columns:
            df = df.select(*columns)
        return df
    
    # If direct connector failed, fall back to BigQuery client approach
    logger.info("Falling back to BigQuery client approach")
    
    # Build column selection
    column_selection = "*"
    if columns:
        column_selection = ", ".join(columns)
    
    # Build query with limit if provided
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"SELECT {column_selection} FROM `{project_id}.{dataset_id}.{table_name}` {limit_clause}"
    
    return query_to_spark(query, spark)

def run_query(spark, query):
    """
    Run a custom BigQuery query and load results into Spark DataFrame
    
    Args:
        spark: Spark session
        query (str): SQL query to execute
        
    Returns:
        DataFrame: Spark DataFrame with query results
    """
    return query_to_spark(query, spark)

# Example usage
if __name__ == "__main__":
    from pyspark.sql import functions as F
    
    # Initialize Spark
    spark = create_spark_session()
    
    # Your GCP project details
    PROJECT_ID = "dtc-de-course-466501"
    DATASET_ID = "dbt_production"
    
    print("\n=== Example 1: Loading simple table ===")
    df_zones = load_table(spark, "dim_zones", PROJECT_ID, DATASET_ID, limit=10)
    df_zones.printSchema()
    df_zones.show(3)
    
    print("\n=== Example 2: Loading complex table ===")
    df_trips = load_table(
        spark, 
        "fact_trips", 
        PROJECT_ID, 
        DATASET_ID, 
        limit=5,
        columns=["tripid", "pickup_zone", "dropoff_zone", "trip_distance", "fare_amount"]
    )
    df_trips.printSchema()
    df_trips.show(3)
    
    print("\n=== Example 3: Custom query ===")
    query = f"""
    SELECT pickup_zone, AVG(trip_distance) as avg_distance, COUNT(*) as trip_count
    FROM `{PROJECT_ID}.{DATASET_ID}.fact_trips`
    GROUP BY pickup_zone
    ORDER BY trip_count DESC
    LIMIT 5
    """
    df_stats = run_query(spark, query)
    df_stats.show()
    
    # Stop Spark session when done
    spark.stop()
