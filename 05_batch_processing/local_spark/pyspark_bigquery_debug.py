from pyspark.sql import SparkSession
import os
import sys
import traceback

# Set Java options to debug issues
os.environ["JAVA_OPTS"] = "-Dio.netty.tryReflectionSetAccessible=true"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-java-options=-Dio.netty.tryReflectionSetAccessible=true pyspark-shell"

# Make sure we're using the correct credentials
print(f"Current credentials file: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')}")

# Create Spark session with extensive configuration options
def create_spark_session(app_name="BigQuery Debug", 
                        jars_packages=None, 
                        arrow_enabled=False):
    
    builder = (SparkSession.builder
              .appName(app_name)
              .config("spark.sql.execution.arrow.pyspark.enabled", str(arrow_enabled).lower())
              .config("spark.sql.adaptive.enabled", "true")
              .config("spark.driver.memory", "2g")  # Increase memory
              .config("spark.executor.memory", "2g")
              .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
              .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
              )
    
    # Add connector packages if provided
    if jars_packages:
        builder = builder.config("spark.jars.packages", jars_packages)
    
    spark = builder.getOrCreate()
    
    # Print Spark configuration for debugging
    print("Spark Configuration:")
    print(f"Spark Version: {spark.version}")
    print(f"Hadoop Version: {spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")
    print(f"Java Version: {spark._jvm.System.getProperty('java.version')}")
    print(f"Arrow Enabled: {spark.conf.get('spark.sql.execution.arrow.pyspark.enabled')}")
    
    return spark

# Attempt to connect to BigQuery with different configurations
def try_bigquery_connection(connector_version="0.32.0"):
    """Try connecting to BigQuery with different options"""
    
    # GCP project info
    PROJECT_ID = "dtc-de-course-466501"
    DATASET_ID = "dbt_production"
    
    print(f"\n\n{'='*80}")
    print(f"Attempting BigQuery connection with connector v{connector_version}")
    print(f"{'='*80}")
    
    try:
        # Create session with specific connector version
        spark = create_spark_session(
            jars_packages=f"com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:{connector_version}",
            arrow_enabled=False  # Start with Arrow disabled
        )
        
        print("\nAttempt 1: Basic connection with AVRO")
        # Try with explicit dataset, AVRO format
        try:
            df = (spark.read
                 .format("bigquery")
                 .option("project", PROJECT_ID)
                 .option("dataset", DATASET_ID)
                 .option("table", "fact_trips")
                 .option("readDataFormat", "AVRO")
                 .option("maxParallelism", "1")
                 .load()
                 .limit(5))
            
            print("✅ Connection successful with AVRO!")
            print("Data sample:")
            df.show(3)
            return True
        except Exception as e:
            print(f"❌ Error with AVRO: {str(e)}")
            traceback.print_exc()
        
        print("\nAttempt 2: Direct SQL query")
        # Try with direct SQL query
        try:
            query = f"SELECT tripid, pickup_zone, dropoff_zone, trip_distance FROM `{PROJECT_ID}.{DATASET_ID}.fact_trips` LIMIT 5"
            df = (spark.read
                 .format("bigquery")
                 .option("query", query)
                 .option("readDataFormat", "AVRO")
                 .option("materializationDataset", DATASET_ID)
                 .load())
            
            print("✅ SQL query successful!")
            print("Data sample:")
            df.show(3)
            return True
        except Exception as e:
            print(f"❌ Error with SQL query: {str(e)}")
            traceback.print_exc()
            
        print("\nAttempt 3: Try simpler table")
        # Try a simpler table structure
        try:
            df = (spark.read
                 .format("bigquery")
                 .option("project", PROJECT_ID)
                 .option("dataset", DATASET_ID)
                 .option("table", "dim_zones")  # Try a simpler table
                 .option("readDataFormat", "AVRO")
                 .load())
            
            print("✅ Simple table connection successful!")
            print("Data sample:")
            df.show(3)
            return True
        except Exception as e:
            print(f"❌ Error with simpler table: {str(e)}")
            traceback.print_exc()
            
    except Exception as e:
        print(f"❌ Error creating Spark session: {str(e)}")
        traceback.print_exc()
    finally:
        try:
            spark.stop()
        except:
            pass
    
    return False

# Try with different connector versions
versions = ["0.32.0", "0.31.0", "0.30.0", "0.29.0", "0.28.0"]
success = False

for version in versions:
    if try_bigquery_connection(version):
        print(f"\n✅ SUCCESS with connector version {version}")
        success = True
        break

if not success:
    print("\n❌ All connection attempts failed.")
    print("Consider using the pandas-to-spark approach instead.")
