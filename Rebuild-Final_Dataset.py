import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import year
from pyspark.sql.functions import col, when, from_unixtime
import datetime
from orbitaal_utils import getTimeDiff

### Init pyspark session
spark = SparkSession \
    .builder \
    .config("spark.driver.memory", "100g") \
    .config("spark.executor.memory", "100g") \
    .config("spark.sql.session.timeZone", "GMT") \
    .config("spark.local.dir","/media/ssd4/tmp")\
    .config("spark.driver.maxResultSize","0")\
    .getOrCreate()
spark.sparkContext.uiWebUrl
t_0=datetime.datetime.now()

### Work Path
PATH=sys.argv[1]

### Preparing new directories
for DIR in ["NODE_TABLE", "STREAM_GRAPH"]:
    if not os.path.exists(f"{PATH}/{DIR}"):
        os.makedirs(f"{PATH}/{DIR}")
# Here we load graph stream related dataset and change the direcotry arborescence surch for each year we have 
## ./GS/year\=YYYY/edges.parquet with only one file per subdir

### reading node table and removing old ID column
table=spark.read.parquet(f"{PATH}/TEMPORAL/network_2009_2021_name/")\
    .withColumnRenamed("NEW_ID","ID")
table=table.withColumn("NEW_NAME", when(col("OLD_ID").cast("String")==col("NAME"), col("ID")).otherwise(col("NAME")))
table=table.drop("NAME")\
    .withColumnRenamed("NEW_NAME", "NAME")

### Writing node table
table.select("ID", "NAME", "FIRST_TIMESTAMP", "LAST_TIMESTAMP", "BALANCE_SATOSHI")\
    .orderBy("FIRST_TIMESTAMP")\
    .coalesce(1).write.mode("overwrite").parquet(f"{PATH}/NODE_TABLE/")

### Reading stream graph
net=spark.read.parquet(f"{PATH}/TEMPORAL/network_2009_2021.parquet").orderBy("TIMESTAMP").withColumn("year", year(from_unixtime(col("TIMESTAMP"))))

### Writing stream graph by year
for YEAR in net.select("year").distinct().collect():
    t__=datetime.datetime.now()
    print(f"Writing year {YEAR[0]}")
    net.filter(col("year")==YEAR[0]).drop("year").coalesce(1).write.mode("overwrite").parquet(f"{PATH}/STREAM_GRAPH/EDGES/year={YEAR[0]}")
    print("file wrote in", getTimeDiff(t__))
print(f"Rebuild node table and stream graph directories done in {getTimeDiff(t_0)}")

### Removing old files and directories related to stream graph
for file in os.listdir(f"{PATH}/TEMPORAL/network_2009_2021.parquet"):
    os.remove(f"{PATH}/TEMPOAL/network_2009_2021.parquet/{file}")
os.removedirs(f"{PATH}/TEMPORAL/network_2009_2021.parquet")
