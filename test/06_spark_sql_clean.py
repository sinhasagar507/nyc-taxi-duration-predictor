import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def read_parquet_as_string(spark, path, rename_map):
    """
    Reads a Parquet file from the provided path,
    casts every column in the DataFrame to a string,
    and renames specified columns according to rename_map.

    Parameters:
        spark (SparkSession): The active Spark session.
        path (str): The input path for the Parquet file.
        rename_map (dict): A dictionary mapping original column names to their new names.

    Returns:
        pyspark.sql.DataFrame: The resulting DataFrame with all columns as strings.
    """
    # Read the Parquet file into a DataFrame
    df = spark.read.parquet(path)
    
    # Cast all columns to string
    df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])
    
    # Rename the specified columns
    for orig_col, new_col in rename_map.items():
        df = df.withColumnRenamed(orig_col, new_col)
    
    return df

def main():
    # Set up command-line arguments
    parser = argparse.ArgumentParser(
        description="Read taxi data with all columns as strings, fill nulls with '0', "
                    "and disable vectorized Parquet reader to avoid schema conversion issues."
    )
    parser.add_argument('--input_green', required=True, help="Input path for green taxi data")
    parser.add_argument('--input_yellow', required=True, help="Input path for yellow taxi data")
    parser.add_argument('--output', required=True, help="Output path for resulting aggregated data")
    args = parser.parse_args()
    
    input_green = args.input_green
    input_yellow = args.input_yellow
    output = args.output
    
    # Create the SparkSession and disable the vectorized Parquet reader
    spark = SparkSession.builder.appName('ReadAllColumnsAsString').getOrCreate()
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    
    # Define column renaming mappings for the green and yellow datasets
    green_rename = {
        'lpep_pickup_datetime': 'pickup_datetime',
        'lpep_dropoff_datetime': 'dropoff_datetime'
    }
    yellow_rename = {
        'tpep_pickup_datetime': 'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime'
    }
    
    # Read both datasets as strings
    df_green = read_parquet_as_string(spark, input_green, green_rename)
    df_yellow = read_parquet_as_string(spark, input_yellow, yellow_rename)
    
    # Define the common columns to be used in the aggregation (all values are strings)
    common_columns = [
        'VendorID',
        'pickup_datetime',
        'dropoff_datetime',
        'store_and_fwd_flag',
        'RatecodeID',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'fare_amount',
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'improvement_surcharge',
        'total_amount',
        'payment_type',
        'congestion_surcharge'
    ]
    
    # Select the common columns and add a new column to denote service type for each dataset
    df_green_sel = df_green.select(common_columns).withColumn('service_type', F.lit('green'))
    df_yellow_sel = df_yellow.select(common_columns).withColumn('service_type', F.lit('yellow'))
    
    # Union the two DataFrames
    df_trips_data = df_green_sel.unionAll(df_yellow_sel)
    
    # Fill all null values with "0" so that casting later will not fail
    df_trips_data = df_trips_data.fillna("0")
    
    # Register the unioned DataFrame as a temporary view for SQL queries
    df_trips_data.createOrReplaceTempView('trips_data')
    
    # Perform the aggregation using SQL. For numeric aggregations, cast string columns to DOUBLE.
    df_result = spark.sql("""
        SELECT 
            PULocationID AS revenue_zone,
            date_trunc('month', pickup_datetime) AS revenue_month, 
            service_type, 
            SUM(CAST(fare_amount AS DOUBLE)) AS revenue_monthly_fare,
            SUM(CAST(extra AS DOUBLE)) AS revenue_monthly_extra,
            SUM(CAST(mta_tax AS DOUBLE)) AS revenue_monthly_mta_tax,
            SUM(CAST(tip_amount AS DOUBLE)) AS revenue_monthly_tip_amount,
            SUM(CAST(tolls_amount AS DOUBLE)) AS revenue_monthly_tolls_amount,
            SUM(CAST(improvement_surcharge AS DOUBLE)) AS revenue_monthly_improvement_surcharge,
            SUM(CAST(total_amount AS DOUBLE)) AS revenue_monthly_total_amount,
            SUM(CAST(congestion_surcharge AS DOUBLE)) AS revenue_monthly_congestion_surcharge,
            AVG(CAST(passenger_count AS DOUBLE)) AS avg_monthly_passenger_count,
            AVG(CAST(trip_distance AS DOUBLE)) AS avg_monthly_trip_distance
        FROM trips_data
        GROUP BY PULocationID, date_trunc('month', pickup_datetime), service_type
    """)
    
    # Write the resulting aggregated DataFrame as a single Parquet file to the output path
    df_result.coalesce(1).write.parquet(output, mode='overwrite')
    
    # Stop the Spark session to free up resources
    spark.stop()

if __name__ == '__main__':
    main()