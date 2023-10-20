import pyspark
from pyspark import SparkConf
from pyspark.sql import SQLContext,SparkSession
import time
import sys
import io
from  pyspark.sql.types import StructType,StructField,IntegerType,FloatType 
import statistics as stats
import random

# Number of run for stat on query, must be > 1
NUM_MES = 4 

# Number of Size Test
NUM_RUN = 4 

# Number of computed rows
BASE_SIZE = 75000 

def gen_binomial(n,p):
    return sum(1 for _ in range(n) if random.random() < p)

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
          ])

# Create a Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN") 
# Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN

# List of queries to execute
queries = [
    ("Q1", "SELECT *"," WHERE low_card = 2 "),
    ("Q2", "SELECT * ","WHERE medium_card = 25 "),
    ("Q3","SELECT * ","WHERE high_card = 50 "),
    ("Q4","SELECT low_card, AVG(value)","GROUP BY low_card "),
    ("Q5","SELECT medium_card, AVG(value)"," GROUP BY medium_card "),
    ("Q6","SELECT high_card, AVG(value) ","GROUP BY high_card "),
]

res_by_queries = {}
output_buffer = io.StringIO()
original_stdout = sys.stdout

current_size = BASE_SIZE

try:
       data = [( random.normalvariate(50,10),gen_binomial(4,.5),gen_binomial(50,.5),gen_binomial(100,.5)) for i in range(int(current_size))      ]
       schema = StructType([StructField("Value", FloatType(), False),StructField("low_card", IntegerType(), False),StructField("medium_card", IntegerType(), False),StructField("high_card", IntegerType(), False)])
       base = spark.createDataFrame(data, schema)

except Exception as e:
       print("Error allocating data :", str(e))

# base.write.mode("overwrite").parquet("ds.parquet",compression="none")

df_size_bytes = current_size * (4*4)
print(base.describe().show())

print("Size in MiB", df_size_bytes/1024./1024.)

df = base

print("Values are : size (bytes); mean duration for iceberg (s); std duration for iceberg (s); mean duration default (s); std duration default (s); mean ratio; std of ratio")

for i in range(NUM_RUN): 

    df_size_bytes = df_size_bytes + df_size_bytes
    print (f"\nrun {i+1}/{NUM_RUN}")
    df = df.union(df)
    df.createOrReplaceTempView("df")
    spark.sql("DROP TABLE  IF EXISTS local.nyc.df PURGE")
    df.writeTo("local.nyc.df").using("iceberg").create()
    #"requete à blanc"
    spark.sql("SELECT * FROM local.nyc.df WHERE low_card >1")
    for q_name,q_start,q_end in queries:
        print(f"\n\nRunning new test :",q_name)

        show_duration_iceberg = []
        show_duration_base = []
        
        for _ in range(NUM_MES):
            result = spark.sql(q_start + " FROM local.nyc.df "+q_end)

            start_time = time.time()
            result.show()
            end_time = time.time()

            show_duration_iceberg.append(end_time - start_time)
            sys.stdout = output_buffer
        sys.stdout = original_stdout

        for _ in range(NUM_MES):
            result = spark.sql(q_start + " FROM df "+q_end)

            start_time = time.time()
            result.show()
            end_time = time.time()

            show_duration_base.append(end_time - start_time)
            sys.stdout = output_buffer

        sys.stdout = original_stdout

        computed_ratio = [  show_duration_base[i] / show_duration_iceberg[i] for i in range(len(show_duration_base))]

        res = f"{float(df_size_bytes)};{stats.mean(show_duration_iceberg)};{stats.stdev(show_duration_iceberg)};{stats.mean(show_duration_base)};{stats.stdev(show_duration_base)};{stats.mean(computed_ratio)}; { stats.stdev(computed_ratio)};"
        print(res)

        old_dic = res_by_queries.get(q_name, {})
        old_dic[df_size_bytes] = res
        res_by_queries[q_name] = old_dic

print("\n\n")
for key, value in res_by_queries.items():
    print("\n", key)
    for k, v in value.items():
        print(v)

# Stop the Spark session
spark.stop()



