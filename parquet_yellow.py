import pyspark
from pyspark import SparkConf
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
import time
import os
import sys
import re
import io
from  pyspark.sql.types import StructType,StructField,IntegerType,TimestampType,IntegerType,DoubleType,StringType,FloatType
from pyspark.sql.functions import col,concat,lpad,round
from datetime import datetime

import statistics as stats

# Number of run for stat on query, must be > 1
NUM_RUN = 10

# Number of Size Test
NUM_SPLIT = 10


""" Size field extraction from input string """
def extract_size_in_bytes(input_string):
    # print('input: ',input_string)
    match = re.search(r'Statistics\(sizeInBytes=(.*?)\)', input_string)
    if match:
        size_in_bytes = match.group(1)
        return size_in_bytes
    else:
        return None




""" spark.sql(query) returns information to standard output.
Modification for returning it to a buffer and extracting the size field from it """



def my_explain(spark_sql_query):
    # Create an io.StringIO object to capture the output
    output_buffer = io.StringIO()

    # Redirect the standard output to the object
    original_stdout = sys.stdout
    sys.stdout = output_buffer

    # Print to the standard output
    #print("This will be captured in the output_buffer object")
    spark_sql_query.explain("cost")

    # Restore the original standard output stream
    sys.stdout = original_stdout

    # Get the captured content into the variable
    captured_output = output_buffer.getvalue()

    # Display the captured content
    #print("Captured content:", captured_output)

    return extract_size_in_bytes(captured_output)

CustomSchemaNew = StructType([
    StructField("VendorID", StringType(),True), # IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime",  TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID",StringType(),True),# DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("Payment_Type", StringType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True) ])



conf = SparkConf() \
        .setAppName("yellow taxis with Iceberg") \
        .setAll([
            ("spark.jars" ,"/opt/tdp/spark3/jars/iceberg-spark-runtime-3.2_2.12-1.3.1.jar"),
            ("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"), \
            ("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog"), \
            ("spark.sql.catalog.spark_catalog.type","hive"), \
            ("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog"), \
            ("spark.sql.catalog.local.type","hadoop"), \
            ("spark.sql.catalog.local.warehouse","hdfs:///user/tdp_user/warehouse_hadoop_iceberg"), \
            ("spark.sql.defaultCatalog","local"),
            ("spark.eventLog.enabled","true"),
            ("spark.eventLog.dir","hdfs://mycluster:8020/spark3-logs"),
#             ("spark.eventLog.dir","hdfs:///spark3-logs"),
          ])




# Create a Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()



sc = spark.sparkContext
sc.setLogLevel('ERROR')
# Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN


from pyspark.sql import SQLContext
#sqlContext = SQLContext(sc)


parquet_path = "hdfs://mycluster:8020/user/tdp_user/data/my_nyc_yellow_taxi_trip_corrected/yellow_tripdata_*.parquet"

# Load Parquet files as a DataFrame

try:
     trips_df  = spark.read.format("parquet") \
                       .load(parquet_path)



except Exception as e:
    print("parquet_path=",parquet_path)
    print("Error reading Parquet file:", str(e))
    raise SystemExit

trips_df.printSchema()


locIdConverterPath = "/user/tdp_user/data/taxis_zone.parquet"
locIdConverter_df  = spark.read.format("parquet").load(locIdConverterPath)

#locIdConverter_df = locIdConverter_df.drop(*("zone","borough"))
locIdConverter_df.createOrReplaceTempView("locIdConverter")

#locIdConverter_df.drop("longitude", "latitude").distinct().createOrReplaceTempView("taxi_zone")

locIdConverter_df.printSchema()

# Use SQL to enforce loading of modules
spark.sql("SELECT DISTINCT LocationID, zone, borough FROM locIdConverter").createOrReplaceTempView("taxi_zones")



# List of queries to execute
queries = [
#    ("Query 0: Average trip distance by vendorID", "SELECT vendorID, AVG(trip_distance) AS avg_distance FROM df GROUP BY vendorID"),
# select
    ("Query 1: Count trips", "WITH local.nyc.df AS df, SELECT COUNT(*)  FROM df"),
# group by avg   
    ("Query 2: Average trip distance by vendorID", "WITH local.nyc.df AS df, SELECT vendorID, AVG(trip_distance) AS avg_distance FROM df GROUP BY vendorID"),
    ("Query 3: Average trip duration (s) by passenger count", "WITH local.nyc.df AS df, SELECT passenger_count, AVG(UNIX_TIMESTAMP(tpep_dropoff_datetime)-UNIX_TIMESTAMP(tpep_pickup_datetime)) AS avg_duration FROM df GROUP BY passenger_count"),
    ("Query 4: Average fare amount by payment type", "WITH local.nyc.df AS df, SELECT payment_type, AVG(fare_amount) AS avg_fare FROM df GROUP BY payment_type"),
    ("Query 5: Average total amount by rate code", "SELECT RateCodeID, AVG(total_amount) AS avg_amount FROM df GROUP BY RateCodeID"),
# group by count
    ("Query 6: Total trips by pickup borough", "SELECT PULocationID, COUNT(*) AS total_trips FROM df GROUP BY PULocationID"),
    ("Query 7: Total trips by year", "SELECT YEAR(tpep_pickup_datetime), COUNT(*) AS total_trips FROM df GROUP BY YEAR(tpep_pickup_datetime)"),
    ("Query 8: Total trips by dropoff zone", "SELECT DOLocationID, COUNT(*) AS total_trips FROM df GROUP BY DOLocationID"),
# where
    ("Query 9: Total trips with passenger count greater than 4", "SELECT COUNT(*) AS total_trips FROM df WHERE passenger_count > 4"),
    ("Query 10: Total trips with tip amount greater than 10", "SELECT COUNT(*) AS total_trips FROM df WHERE tip_amount > 10"),
    ("Query 11: Total trips with rate code 1", "SELECT COUNT(*) AS total_trips FROM df WHERE RatecodeID = 1"),
# rank
    ("Query 12: Top destination origin", "SELECT DOLocationID  AS dropoff_location, PULocationID AS pickup_location, COUNT(*)/1000 AS trip_count_k,\
        AVG(df.trip_distance) AS avg_trip_distance,AVG(df.passenger_count) AS avg_passenger_count,\
        AVG(df.fare_amount) AS avg_fare_amount,DENSE_RANK() OVER(PARTITION BY DOLocationID ORDER BY COUNT(*) DESC) AS trip_count_rank \
    FROM df GROUP BY dropoff_location, pickup_location"),

    ("Query 13: Top destination", "SELECT DOLocationID  AS dropoff_location,\
        DENSE_RANK() OVER(ORDER BY COUNT(*) DESC) AS trip_count_rank, COUNT(*)/1000 AS trip_count_k\
    FROM df GROUP BY dropoff_location"),
# join   todo couper en deux faire les dense_rank d'un coté et la jointure de l'autre
    ("Query 14: Taxi trips by borough","SELECT tz.borough, COUNT(df.PULocationID) AS total_trips\
           FROM taxi_zones tz \
           JOIN  df ON tz.LocationID = df.PULocationID\
           GROUP BY tz.borough"),

   ("Query 15: complexe auto join", "SELECT tdo.dropoff_location,tdo.pickup_location,tdo.trip_count_k,tdo.avg_trip_distance,tdo.avg_passenger_count,tdo.avg_fare_amount\
    FROM top_destination td \
    INNER JOIN \
    top_destination_origin tdo\
    ON tdo.dropoff_location= td.dropoff_location\
    WHERE td.trip_count_rank IN (1, 2, 3, 4) AND tdo.trip_count_rank IN (1, 2, 3, 4, 5) ORDER BY  \
    td.trip_count_rank,tdo.trip_count_rank")

]


#queries = [queries[0]]

size = trips_df.count()

print("mesures as : ")
print("Query name ; size from plan; query (s); std ; explain (s); std; show (s); std; total (s); std\n\n")


for i in range(NUM_SPLIT):

     print(f"Using  {i+1} / {NUM_SPLIT} of the dataset for queries at {datetime.now()}")
     #current =  trips_df.limit(int(size*(i+1)/NUM_SPLIT)).cache()
     current = trips_df.sample(withReplacement=False, fraction=(i+1)/NUM_SPLIT, seed=42)
     #current.createOrReplaceTempView("df")
     #trips_df.createOrReplaceTempView("df")

     spark.sql("CREATE TABLE IF NOT EXISTS local.nyc.df (\
       VendorID string, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, passenger_count double,trip_distance double,\
       RatecodeID string, store_and_fwd_flag string, PULocationID integer, DOLocationID integer, Payment_Type string, fare_amount double,\
       extra double, mta_tax double, tip_amount double, tolls_amount double, improvement_surcharge double, total_amount double, \
       congestion_surcharge double, airport_fee double) \
       USING iceberg ;") # PARTITIONED BY (months(tpep_pickup_datetime));")

     spark.sql("TRUNCATE TABLE local.nyc.df")

     current.writeTo("local.nyc.df").append()


#     if i > 1 :
#         break # tempo debug

     # blank query :
     #if i == 0:  
     #   _ = spark.sql(queries[0][1]).collect()


     spark.sql("""WITH local.nyc.df AS df, SELECT
            DOLocationID AS dropoff_location,
            PULocationID AS pickup_location,
            COUNT(*)/1000 AS trip_count_k,
            AVG(df.trip_distance) AS avg_trip_distance,
            AVG(df.passenger_count) AS avg_passenger_count,
            AVG(df.fare_amount) AS avg_fare_amount,
            DENSE_RANK() OVER(PARTITION BY DOLocationID ORDER BY COUNT(*) DESC) AS trip_count_rank
        FROM df
        GROUP BY dropoff_location, pickup_location""").createOrReplaceTempView("top_destination_origin")
     spark.sql("""WITH local.nyc.df AS df, SELECT
        DOLocationID  AS dropoff_location,
        DENSE_RANK() OVER(ORDER BY COUNT(*) DESC) AS trip_count_rank,
        COUNT(*)/1000 AS trip_count_k
    FROM df GROUP BY dropoff_location""").createOrReplaceTempView("top_destination")

     for query_name, query_string in queries:
         print(f"\n\nRunning new test\n")

         query_duration = []
         explain_duration  = []
         show_duration = []
         total_duration = []

         output_buffer = io.StringIO()
         original_stdout = sys.stdout

         for i in range(NUM_RUN):

           start_time0 = time.time()
           result = spark.sql(query_string)
           end_time = time.time()

           query_duration.append(end_time-start_time0)

           start_time = time.time()
           print(result.explain())
           end_time = time.time()

           explain_duration.append(end_time-start_time)

           start_time = time.time()
           result.show()
           end_time = time.time()

           show_duration.append(end_time - start_time)
           total_duration.append(end_time - start_time0)

           sys.stdout = output_buffer

         sys.stdout = original_stdout
#         print("Query name ; size from plan; query (s); std ; explain (s); std; show (s); std; total (s); std")
         print(query_name,";",my_explain(result),";",stats.mean(query_duration),";",stats.stdev(query_duration),";", stats.mean(explain_duration),";", stats.stdev(explain_duration),";", stats.mean(show_duration),";", stats.stdev(show_duration),";", stats.mean(total_duration),";", stats.stdev(total_duration),";")


# Stop the Spark session
spark.stop()

