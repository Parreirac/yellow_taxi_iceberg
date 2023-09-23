import pyspark
from pyspark import SparkConf

from pyspark.sql import SparkSession
import time
import os
import sys
import re
import io
from  pyspark.sql.types import StructType,StructField,IntegerType,TimestampType,IntegerType,DoubleType,StringType,FloatType
from pyspark.sql.functions import col
import statistics as stats



CustomSchema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("passenger_count",DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", IntegerType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("trip_type", IntegerType(), True),
    StructField("congestion_surcharge", DoubleType(), True)])


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


conf = SparkConf() \
        .setAppName("Apache Iceberg with PySpark") \
        .setAll([
# from https://iceberg.apache.org/spark-quickstart/#adding-a-catalog
            ("spark.jars" ,"/opt/tdp/spark3/jars/iceberg-spark-runtime-3.2_2.12-1.3.1.jar"),
            ("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"), \
            ("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog"), \
            ("spark.sql.catalog.spark_catalog.type","hive"), \
            ("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog"), \
            ("spark.sql.catalog.local.type","hadoop"), \
            ("spark.sql.catalog.local.warehouse","hdfs:///user/tdp_user/warehouse_hadoop_iceberg"), \
            ("spark.sql.defaultCatalog","local"),
#from Romain
#            ("spark.kerberos.access.hadoopFileSystems","hdfs:///")  #,
#            ("spark.yarn.access.hadoopFileSystems","hdfs://mycluster")

        ])\


# Create a Spark session

spark = SparkSession.builder.config(conf=conf).getOrCreate()


#you should see the wholeStage codegen stage indexes in the plan.
#spark.conf.set("spark.sql.codegen.wholeStage", "true")  # TODO retester !
#spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")  

sc = spark.sparkContext
sc.setLogLevel('ERROR')
# Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN


from pyspark.sql import SQLContext
#sqlContext = SQLContext(sc)


# Path to the folder containing the Parquet files of the dataset
#parquet_path = "hdfs:///user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_*.parquet"
#parquet_path = "hdfs:/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-12.parquet"
#parquet_path = "hdfs://mycluster:8020/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-12.parquet"
parquet_path = "hdfs://mycluster:8020/user/tdp_user/data/nyc_green_taxi_trip_corrected/green_tripdata_20*-*.parquet"
#parquet_path2 = "hdfs://mycluster:8020/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-11.parquet"


# /user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-12.parquet
# Load Parquet files as a DataFrame

df = spark.createDataFrame([], CustomSchema)

df.printSchema()


df.writeTo("local.nyc.taxis_one_step").create()



try:
     trips_df1  = spark.read.format("parquet") \
                      .load(parquet_path)
#                     .option("mergeSchema","true").load(parquet_path)

#                     .schema(CustomSchema) \

     trips_df1.writeTo("local.nyc.taxis_one_step").append()


     df = spark.table("local.nyc.taxis_one_step")

     spark.sql("select * from local.nyc.taxis_one_step.history").show()




#     for col in trips_df.columns:
#           if col in ["RatecodeID","ehail_fee","trip_type","payment_type"]:
#                   trips_df = trips_df.withColumn(col, trips_df[col].cast("int"))

     #print("***",trips_df.columns)
     #trips_df = trips_df.withColumn("RatecodeID", double_cast_udf(col("RatecodeID"))) \
     #                   .withColumn("ehail_fee", double_cast_udf(col("ehail_fee"))) \
     #                   .withColumn("trip_type", double_cast_udf(col("trip_type"))) \
     #                   .withColumn("payment_type", double_cast_udf(col("payment_type")))

#.load(parquet_path)
#                     .option("mergeSchema", "true")\
#                     .schema(CustomSchema)\
# .load(parquet_path)
#,parquet_path2)
     df.createOrReplaceTempView("df")
#    sqlContext.read.parquet(parquet_path) \

except Exception as e:
    print("parquet_path=",parquet_path)
    print("Error reading Parquet file:", str(e))
    raise SystemExit


# register temp table so it can be queried with SQL
#    trips_df.registerTempTable("df")
#    trips_df.createOrReplaceTempView("df")

# List of queries to execute
#    ("Query 0: Average fare amount by payment type", "SELECT payment_type, AVG(fare_amount) AS avg_fare FROM df GROUP BY payment_type"),
    ("Query 0: Average trip distance by vendorID", "SELECT vendorID, AVG(trip_distance) AS avg_distance FROM df GROUP BY vendorID"),
#    ("Query 1: Total trips by year", "SELECT year, COUNT(*) AS total_trips FROM df GROUP BY year"),
    ("Query 1: Total trips by year", "SELECT YEAR(lpep_pickup_datetime), COUNT(*) AS total_trips FROM df GROUP BY YEAR(lpep_pickup_datetime)"),
    ("Query 2: Average fare amount by payment type", "SELECT payment_type, AVG(fare_amount) AS avg_fare FROM df GROUP BY payment_type"),
    ("Query 3: Total trips with passenger count greater than 4", "SELECT COUNT(*) AS total_trips FROM df WHERE passenger_count > 4"),
    ("Query 4: Average trip distance by vendorID", "SELECT vendorID, AVG(trip_distance) AS avg_distance FROM df GROUP BY vendorID"),
    ("Query 5: Total trips with tip amount greater than 10", "SELECT COUNT(*) AS total_trips FROM df WHERE tip_amount > 10"),
#("Query 6: Total trips by pickup borough", "SELECT pickup_borough, COUNT(*) AS total_trips FROM df GROUP BY pickup_borough"),
    ("Query 6: Total trips by pickup borough", "SELECT PULocationID, COUNT(*) AS total_trips FROM df GROUP BY PULocationID"),
#    ("Query 7: Average trip duration by passenger count", "SELECT passenger_count, AVG(trip_duration) AS avg_duration FROM df GROUP BY passenger_count"),
    ("Query 7: Average trip duration by passenger count", "SELECT passenger_count, AVG(UNIX_TIMESTAMP(lpep_dropoff_datetime)-UNIX_TIMESTAMP(lpep_pickup_datetime)) AS avg_duration FROM df GROUP BY passenger_count"),

#    ("Query 8: Total trips with rate code 1", "SELECT COUNT(*) AS total_trips FROM df WHERE rate_code = 1"),
    ("Query 8: Total trips with rate code 1", "SELECT COUNT(*) AS total_trips FROM df WHERE RatecodeID = 1"),
    ("Query 9: Average total amount by trip type", "SELECT trip_type, AVG(total_amount) AS avg_amount FROM df GROUP BY trip_type"),
#    ("Query 10: Total trips by dropoff borough", "SELECT dropoff_borough, COUNT(*) AS total_trips FROM df GROUP BY dropoff_borough")
    ("Query 10: Total trips by dropoff borough", "SELECT DOLocationID, COUNT(*) AS total_trips FROM df GROUP BY DOLocationID"),  
    ("Query 11: ", "WITH top_destination_origin AS (SELECT \
        DOLocationID  AS dropoff_location, PULocationID AS pickup_location, COUNT(*)/1000 AS trip_count_k,\
        AVG(df.trip_distance) AS avg_trip_distance,AVG(df.passenger_count) AS avg_passenger_count,\
        AVG(df.fare_amount) AS avg_fare_amount,DENSE_RANK() OVER(PARTITION BY DOLocationID ORDER BY COUNT(*) DESC) AS trip_count_rank \
    FROM df GROUP BY dropoff_location, pickup_location), \
    top_destination AS (SELECT\
        DOLocationID  AS dropoff_location,\
        DENSE_RANK() OVER(ORDER BY COUNT(*) DESC) AS trip_count_rank,\
        COUNT(*)/1000 AS trip_count_k\
    FROM df GROUP BY dropoff_location)\
    SELECT\
      tdo.dropoff_location,tdo.pickup_location,tdo.trip_count_k,tdo.avg_trip_distance,tdo.avg_passenger_count,tdo.avg_fare_amount\
    FROM top_destination td \
    INNER JOIN \
    top_destination_origin tdo\
    ON tdo.dropoff_location= td.dropoff_location\
    WHERE td.trip_count_rank IN (1, 2, 3, 4) AND tdo.trip_count_rank IN (1, 2, 3, 4, 5) ORDER BY  \
    td.trip_count_rank,tdo.trip_count_rank")
    # Add more queries here if needed
] 


# Execute queries and measure execution time
for query_name, query_string in queries:
    print(f"\n\nRunning {query_name}")

    query_duration = []
    explain_duration  = []
    show_duration = []
    total_duration = []

    output_buffer = io.StringIO()
    original_stdout = sys.stdout

    for i in range(10):

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

    print("size from plan: ",my_explain(result))

    # drop min and max value ?
    print(f"query duration : {stats.mean(query_duration)} +/- {stats.stdev(query_duration)} secondes")
    print(f"explain duration : {stats.mean(explain_duration)} +/- {stats.stdev(explain_duration)} secondes")
    print(f"show duration : {stats.mean(show_duration)} +/- {stats.stdev(show_duration)} secondes")
    print(f"total : {stats.mean(total_duration)} +/- {stats.stdev(total_duration)} secondes")




# Stop the Spark session
spark.stop()


