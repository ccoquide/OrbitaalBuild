import sys
from pyspark.sql import SparkSession
import datetime
from orbitaal_utils import getTimeDiff, write_SNAPSHOT

### Timezone
timezone="GMT"

### Output file format 
opt="parquet"

### Init pyspark session
spark = SparkSession \
    .builder \
    .config("spark.driver.memory", "100g") \
    .config("spark.executor.memory", "100g") \
    .config("spark.sql.session.timeZone", timezone) \
    .config("spark.local.dir","/media/ssd4/tmp")\
    .config("spark.driver.maxResultSize","0")\
    .getOrCreate()
spark.sparkContext.uiWebUrl
t_0=datetime.datetime.now()

### Work Path
PATH="/media/ssd4/celestin"

fname=f"{PATH}/STREAM_GRAPH/EDGES/year*/"

### Reading stream graph as a PySpark DataFrame
net=spark.read.parquet(fname)

#Ordering by TIMESTAMP
net=net.orderBy("TIMESTAMP")

### Writing snapshots
t__0=datetime.datetime.now()
print(f"Writting SNAPSHOTS for years with time zone {timezone}\n")
write_SNAPSHOT(net, PATH, format=opt)
print("SNAPSHOTS wrote in ", getTimeDiff(t__0))
print("Total computing time is ", getTimeDiff(t_0))
