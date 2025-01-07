from pyspark.sql.functions import year, month, dayofmonth, from_unixtime, to_date
from pyspark.sql import SparkSession
import os
import sys

### Init pyspark session
spark = SparkSession \
    .builder \
    .getOrCreate()
spark.sparkContext.uiWebUrl

### Path to input data
PATH_I=sys.argv[1]
### Path to output cleaned data
PATH_O=sys.argv[2]




# Convert files from the minimal format to parquet files

def create_initial_files(json_matched,DF_raw):
    lines = spark.read.csv(json_matched,sep="\t").toDF("hash","bloc","timestamp","fee","inputs","outputs")
    lines.withColumn("date", to_date(from_unixtime("timestamp"))) \
    .withColumn("year", year("date")).withColumn("month", month("date")).withColumn("day", dayofmonth("date"))\
    .write.partitionBy("year", "month","day").save(DF_raw, mode="overwrite")

create_initial_files(PATH_I,PATH_O)