import pyspark
from pyspark import SparkConf
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
import time
import os
import sys
import re
import io
from  pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,FloatType
#from pyspark.sql.functions import col,concat,lpad,round
from datetime import datetime
#import gc
import statistics as stats
import matplotlib.pyplot as plt
#import numpy as np
import random


# Number of run for stat on query, must be > 1
NUM_MES = 10
# Number of Size Test
NUM_RUN = 8  
# Number of computed rows
BASE_SIZE = 70000 
# Increase at each step
SAMPLING_RATIO = 1.0
# 1 hour computation for thoses values




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
#             ("spark.eventLog.dir","hdfs:///spark3-logs"),
          ])


# Create a Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()



sc = spark.sparkContext
sc.setLogLevel("WARN") # 'ERROR')
# Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN


from pyspark.sql import SQLContext
#sqlContext = SQLContext(sc)

# List of queries to execute
queries = [
    ("Q1", "SELECT *", " WHERE low_card = 2 "),
    ("Q2", "SELECT * ", "WHERE medium_card = 25 "),
    ("Q3", "SELECT * ", "WHERE high_card = 100 "),
    ("Q4", "SELECT * ", "WHERE max_card = 200 "),
    ("Q5", "SELECT low_card, AVG(value)", "GROUP BY low_card "),
    ("Q6", "SELECT medium_card, AVG(value)", " GROUP BY medium_card "),
    ("Q7", "SELECT high_card, AVG(value) ", "GROUP BY high_card "),
    ("Q8", "SELECT max_card, AVG(value) ", "GROUP BY max_card ")
]


# for debug reduce queries number
#queries = [queries[0]]


print("mesures as : ")
print("Query name ; size from plan; query (s); std ; explain (s); std; show (s); std; total (s); std\n\n")

res_by_queries = {}

output_buffer = io.StringIO()
original_stdout = sys.stdout


def gen_binomial(n,p):
    return sum(1 for _ in range(n) if random.random() < p)

try:
       data = [( random.normalvariate(50,10),random.randint(0, 4),random.randint(0,50),random.randint(0,200),random.randint(0,400)) for _ in range(BASE_SIZE)]
#      data = [(random.normalvariate(50,10),gen_binomial(4,.5),gen_binomial(50,.5),gen_binomial(200,.5)) for i in range(int(current_size))      ]
       schema = StructType([StructField("Value", FloatType(), False),
                            StructField("low_card", IntegerType(), False),
                            StructField("medium_card", IntegerType(), False),
                            StructField("high_card", IntegerType(), False), 
                            StructField("max_card", IntegerType(), False)])
    
#           StructField("low_card", IntegerType(), False),StructField("medium_card", IntegerType(), False),StructField("high_card", IntegerType(), False)])
    
       base = spark.createDataFrame(data, schema)

except Exception as e:
        print("Une erreur s'est produite lors de la génération du DataFrame :", str(e))

#base.write.mode("overwrite").parquet("ds.parquet",compression="none") # or  "uncompressed"

current_size = BASE_SIZE
base_size = current_size * (4 + 3 *4 +4)
df_size_bytes = base_size

#    print("0")
#    current_size = current_size * (i+1) * 2.
print(base.describe().show())

print("Size in MiB", df_size_bytes/1024./1024.)

df = base.alias("copy")

for i in range(NUM_RUN):
#    try :
#        gc.collect()
#    except:
#        print("*** ERROR in gc.collect() ***")   

#    df_size_bytes = df_size_bytes + df_size_bytes
    print (f"\nrun {i+1}/{NUM_RUN}")

    df = df.union(df.sample(SAMPLING_RATIO))
    df_size_bytes = df_size_bytes + SAMPLING_RATIO * df_size_bytes


    df.createOrReplaceTempView("df")
    spark.sql("DROP TABLE  IF EXISTS local.nyc.df PURGE")
    df.writeTo("local.nyc.df").using("iceberg").create()
    #"requete à blanc"
    spark.sql("SELECT * FROM local.nyc.df WHERE low_card >2")
    for q_name,q_start,q_end in queries:
        print(f"\n\nRunning new test :",q_name)

        show_duration_iceberg = []
        show_duration_base = []
#        gc.disable()
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

#        gc.enable()
        sys.stdout = original_stdout

        computed_ratio = [  show_duration_base[i] / show_duration_iceberg[i] for i in range(len(show_duration_base))]


        res = f"{float(df_size_bytes)};{stats.mean(show_duration_iceberg)};{stats.stdev(show_duration_iceberg)};{stats.mean(show_duration_base)};{stats.stdev(show_duration_base)};{stats.mean(computed_ratio)}; { stats.stdev(computed_ratio)};"

        print(res)


        old_dic = res_by_queries.get(q_name, {})
        old_dic[df_size_bytes] = res

        res_by_queries[q_name] = old_dic

# Stop the Spark session

print("\n\n")
for key, value in res_by_queries.items():
    print("\n", key)
    for k, v in value.items():
        print(v)

# Collect data


size = [key for key in res_by_queries['Q1'].keys()]
size_MiB =  [v/1024/1024.for v in size]

queries = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q8']

Q_dict = {}
STDQ_dict = {}

for query in queries:
    Q_dict[query] = []
    STDQ_dict[query] = []

    for k, v in res_by_queries[query].items():
        tab = v.split(";")
        Q_dict[query].append(float(tab[-3]))
        STDQ_dict[query].append(float(tab[-2]))

# Visualization

## Overview

fig = plt.figure()
plt.errorbar(size_MiB, Q_dict['Q1'], yerr=STDQ_dict['Q1'], label='WHERE (Q1)',  fmt='-o')
#plt.errorbar(size_GiB, Q_dict['Q2'], yerr=STDQ_dict['Q2'], label='Q2',  fmt='-o')
#plt.errorbar(size_GiB, Q_dict['Q3'], yerr=STDQ_dict['Q3'], label='Q3',  fmt='-o')
#plt.errorbar(size_GiB, Q_dict['Q4'], yerr=STDQ_dict['Q4'], label='Q4',  fmt='-o')
#plt.xscale("log")
#plt.yscale("log")
plt.xlabel("Size in MiB")
plt.errorbar(size_MiB, Q_dict['Q5'], yerr=STDQ_dict['Q5'], label='GROUP BY (Q5)',  fmt='-o')
#plt.errorbar(size_GiB, Q_dict['Q6'], yerr=STDQ_dict['Q6'], label='Q6',  fmt='-o')
#plt.errorbar(size_GiB, Q_dict['Q7'], yerr=STDQ_dict['Q7'], label='Q7',  fmt='-o')
#plt.errorbar(size_GiB, Q_dict['Q8'], yerr=STDQ_dict['Q8'], label='Q8',  fmt='-o')
plt.title('Ratio standard duration, Iceberg duration')
#plt.xlim(left=0.5) 
#plt.xlim(right=200)  
#plt.ylim(top=10)  
plt.legend(loc='lower right')

plt.show()
plt.savefig('Overview.png')



## Where queries

fig = plt.figure()
plt.errorbar(size_MiB, Q_dict['Q1'], yerr=STDQ_dict['Q1'], label='Q1',  fmt='-o')
plt.errorbar(size_MiB, Q_dict['Q2'], yerr=STDQ_dict['Q2'], label='Q2',  fmt='-o')
plt.errorbar(size_MiB, Q_dict['Q3'], yerr=STDQ_dict['Q3'], label='Q3',  fmt='-o')
plt.errorbar(size_MiB, Q_dict['Q4'], yerr=STDQ_dict['Q4'], label='Q4',  fmt='-o')
plt.xscale("log") #, **kwargs)
plt.yscale("log") #, **kwargs)
plt.title('WHERE QUERIES')
plt.xlabel("Size in MiB")
plt.legend()


plt.show()
plt.savefig('Where.png')

## GROUP BY

fig = plt.figure()
plt.errorbar(size_MiB, Q_dict['Q5'], yerr=STDQ_dict['Q5'], label='Q5',  fmt='-o')
plt.errorbar(size_MiB, Q_dict['Q6'], yerr=STDQ_dict['Q6'], label='Q6',  fmt='-o')
plt.errorbar(size_MiB, Q_dict['Q7'], yerr=STDQ_dict['Q7'], label='Q7',  fmt='-o')
plt.errorbar(size_MiB, Q_dict['Q8'], yerr=STDQ_dict['Q8'], label='Q8',  fmt='-o')
plt.xscale("log") 
plt.title('GROUP BY QUERIES')
plt.xlabel("Size in MiB")
plt.ylabel("Gain with Iceberg")
plt.legend()

plt.show()
plt.savefig('GB.png')


spark.stop()



