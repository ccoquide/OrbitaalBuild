from pyspark.sql import SparkSession

import sys
import pyspark.sql.functions as f

import os


### Init pyspark session
spark = SparkSession \
    .builder \
    .getOrCreate()
spark.sparkContext.uiWebUrl

### Path to input data
PATH_I=sys.argv[1]
### Path to output cleaned data
PATH_O=sys.argv[2]

PATH_DICO_output=sys.argv[3]

whole_ads = spark.read.load(PATH_I)\
    .select("trID","values_ins","values_outs")

whole_ads = whole_ads.withColumn("total_in",f.expr('AGGREGATE(values_ins, +0L, (acc, x) -> acc + x)'))
whole_ads = whole_ads.withColumn("total_out",f.expr('AGGREGATE(values_outs, +0L, (acc, x) -> acc + x)'))


whole_ads = whole_ads.withColumn("fee",f.when(f.col("total_in") == 0, 0).otherwise(f.col("total_in")-f.col("total_out")))
whole_ads=whole_ads.drop("values_ins").drop("values_outs")

whole_ads.write.save(PATH_DICO_output,mode="overwrite")


completeDF = spark.read.load(PATH_I)
fees = spark.read.load(PATH_DICO_output)
completeDF = completeDF.join(fees,on="trID",how="left")
completeDF.write.save(PATH_O,mode="overwrite")
print("writing",PATH_O)