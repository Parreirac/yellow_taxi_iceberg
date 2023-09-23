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
import statistics as stats



CustomSchema = StructType([
    StructField("vendor_name", StringType(), True),
    StructField("Trip_Pickup_DateTime", StringType(), True),
    StructField("Trip_Dropoff_DateTime", StringType(), True),
    StructField("Passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("Start_lon", DoubleType(), True),
    StructField("Start_lat", DoubleType(), True),
    StructField("Rate_Code",DoubleType(), True),
    StructField("store_and_forward", DoubleType(), True),
    StructField("End_Lon", DoubleType(), True),
    StructField("End_Lat", DoubleType(), True),
    StructField("Payment_Type", StringType(), True),
    StructField("Fare_Amt", DoubleType(), True),
    StructField("surcharge", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("Tip_Amt", DoubleType(), True),
    StructField("Tolls_Amt", DoubleType(), True),
    StructField("Total_Amt", DoubleType(), True)])



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

# Define a UDF to cast string to double
def double_cast(value):
    return float(value)

double_cast_udf = udf(double_cast, DoubleType())


conf = SparkConf() \
        .setAppName("green taxis with parquet") \
        .setAll([
            ("spark.eventLog.enabled","true"),
#            ("spark.eventLog.dir","hdfs://mycluster:8020/spark3-logs"),
             ("spark.eventLog.dir","hdfs:///spark3-logs"),

          ])




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
parquet_path = "hdfs://mycluster:8020/user/tdp_user/data/nyc_yellow_taxi_trip/yellow_tripdata_20*-*.parquet"
#parquet_path2 = "hdfs://mycluster:8020/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-11.parquet"


# /user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-12.parquet
# Load Parquet files as a DataFrame

try:
     trips_df1  = spark.read.format("parquet") \
                       .load(parquet_path)
#                     .option("mergeSchema","true").load(parquet_path)

#                     .schema(CustomSchema) \



#     trips_df = spark.read.format("parquet").option("mergeSchema", "true").load(parquet_path)
#     trips_df = spark.read.format("parquet").load(parquet_path)

     print("cast ",parquet_path)

     trips_df = trips_df1.withColumn("vendorID",col("vendor_name")) \
                        .withColumn("lpep_pickup_datetime",col("Trip_Pickup_DateTime")) \
                        .withColumn("lpep_dropoff_datetime",col("Trip_Dropoff_DateTime"))\
                        .withColumn("RatecodeID",col("Rate_Code"))\
                        .withColumn("fare_amount",col("Fare_Amt"))\
                        .withColumn("PULocationID",concat(lpad(round(col("Start_Lon")/10),4,'0'),
                                                          lpad(round(col("Start_Lat")/10),4,'0')))\
                        .withColumn("DOLocationID",concat(lpad(round(col("End_Lon")/10),4,'0'),
                                                          lpad(round(col("End_Lat")/10),4,'0')))\


     print("fin du cast**************")


#     trips_df = trips_df1.withColumn("RatecodeID", col("RatecodeID").cast("float")) \
#                         .withColumn("ehail_fee", col("ehail_fee").cast("float")) \
#                         .withColumn("trip_type", col("trip_type").cast("float")) \
#                         .withColumn("payment_type", col("payment_type").cast("float"))

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
#     trips_df = trips_df.withColumn("RatecodeID", 


     trips_df.createOrReplaceTempView("df")
#    sqlContext.read.parquet(parquet_path) \

except Exception as e:
    print("parquet_path=",parquet_path)
    print("Error reading Parquet file:", str(e))
    raise SystemExit

trips_df.printSchema()

# register temp table so it can be queried with SQL
#    trips_df.registerTempTable("df")
#    trips_df.createOrReplaceTempView("df")

# List of queries to execute
queries = [

    ("Query 0: Average trip distance by vendorID", "SELECT vendorID, AVG(trip_distance) AS avg_distance FROM df GROUP BY vendorID"),


#group by    
    ("Query 4: Average trip distance by vendorID", "SELECT vendorID, AVG(trip_distance) AS avg_distance FROM df GROUP BY vendorID"),
    ("Query 6: Total trips by pickup borough", "SELECT PULocationID, COUNT(*) AS total_trips FROM df GROUP BY PULocationID"),
    ("Query 7: Average trip duration by passenger count", "SELECT passenger_count, AVG(UNIX_TIMESTAMP(lpep_dropoff_datetime)-UNIX_TIMESTAMP(lpep_pickup_datetime)) AS avg_duration FROM df GROUP BY passenger_count"),



#select
    ("Query 1: Total trips by year", "SELECT YEAR(lpep_pickup_datetime), COUNT(*) AS total_trips FROM df GROUP BY YEAR(lpep_pickup_datetime)"),
    ("Query 2: Average fare amount by payment type", "SELECT payment_type, AVG(fare_amount) AS avg_fare FROM df GROUP BY payment_type"),
    ("Query 9: Average total amount by trip type", "SELECT trip_type, AVG(total_amount) AS avg_amount FROM df GROUP BY trip_type"),
    ("Query 10: Total trips by dropoff borough", "SELECT DOLocationID, COUNT(*) AS total_trips FROM df GROUP BY DOLocationID"),

#where
    ("Query 3: Total trips with passenger count greater than 4", "SELECT COUNT(*) AS total_trips FROM df WHERE passenger_count > 4"),
    ("Query 5: Total trips with tip amount greater than 10", "SELECT COUNT(*) AS total_trips FROM df WHERE tip_amount > 10"),
    ("Query 8: Total trips with rate code 1", "SELECT COUNT(*) AS total_trips FROM df WHERE RatecodeID = 1"),


# todo couper en deux faire les dense_rank d'un coté et la jointure de l'autre
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

    for i in range(2):

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




A
A
A
Ai
A
A
A
Add
A
A


A
A
A
Cdd
     trips_df.createOrReplaceTempView("df")
