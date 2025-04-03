from diagrams import Cluster, Diagram, Edge
from diagrams.outscale.network import InternetService
from diagrams.onprem.container import Docker
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.workflow import Airflow
from diagrams.gcp.analytics import Bigquery
from diagrams.onprem.queue import Kafka
from diagrams.onprem.analytics import Dbt, Spark
from diagrams.programming.flowchart import Database


with Diagram("DB Architecture", show=False):
    data_source = InternetService("Ingest remote data") 
    with Cluster("DB Manage"):
        docker = Docker("container")
        postgres = PostgreSQL("database")
        
    # Channel the data
    data_source >> postgres
    
    with Cluster("Workflow Streaming"):
        airflow = Airflow("Work Orchestrate")
        kafka = Kafka("Realtime Streaming")
        database = Database("Database")
        
        
    # Join to cluster
    postgres >> database
    
    
    # Data Transformation
    dbt = Dbt("Database Analytics")
    database >> dbt
    
    
    with Cluster("Data Warehouse"):
        big_query = Bigquery("warehouse")
        dbt_analytics = Dbt("Analytics Engineering")
        big_query \
            - Edge(style="dotted") \
            - dbt_analytics
        spark = Spark("Batch processing")
        
        warehousing = [big_query, dbt_analytics, spark]
        
    database >> warehousing
    dbt >> warehousing