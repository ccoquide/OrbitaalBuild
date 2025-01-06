from pyspark.sql import SparkSession

from pyspark.sql.functions import monotonically_increasing_id, coalesce, date_trunc, regexp_extract
import sys

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

PATH_DICO=sys.argv[3]

#PATH_TEMP=sys.argv[4]

#DF_detailed_with_fees=PATH_TEMP+"/DF_detailed_with_fees"



# Replace hash by trID

original = spark.read.load(PATH_I)
original_id = original.withColumn("trID", monotonically_increasing_id())

original_id.select(["hash","trID"]).write.save(PATH_DICO,mode="overwrite")

original_id.drop("hash").write.save(PATH_O,mode="overwrite")

