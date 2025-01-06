from pyspark.sql import SparkSession

import pyspark.sql.functions as f

import sys


### Init pyspark session
spark = SparkSession \
    .builder \
    .getOrCreate()
spark.sparkContext.uiWebUrl

### Path to input data
PATH_I=sys.argv[1]
### Path to output data
PATH_O=sys.argv[2]

PATH_DICOad2id=sys.argv[3]

#First, compute complete unique IDs
original = spark.read.load(PATH_I)
col1 = original.select(f.col("src").alias("ad"))
col2 = original.select(f.col("dst").alias("ad"))
# Union the two columns and remove duplicates
combined = col1.union(col2).distinct()
# remove "mining"
combined = combined.filter(f.col("ad")!="mining")

# Assign numeric IDs to the unique values
unique_ids = combined.withColumn("id", f.monotonically_increasing_id())

unique_ids.write.save(PATH_DICOad2id,mode="overwrite")




DF_by_transaction = spark.read.load(PATH_I)

ad2adID = spark.read.load(PATH_DICOad2id)
ad2adID = ad2adID.withColumnRenamed("ad","src").withColumnRenamed("id","id_src")
outputs_encoded = DF_by_transaction.join(ad2adID,on="src",how="left")

ad2adID = ad2adID.withColumnRenamed("src","dst").withColumnRenamed("id_src","id_dst")
outputs_encoded = outputs_encoded.join(ad2adID,on="dst",how="left")
outputs_encoded.write.save(PATH_O,mode="overwrite")
#outputs_encoded = outputs_encoded.select("trID","output_id","id_src","id_dst","value")
#outputs_encoded.write.save("/media/ssd4/bitcoinRemy/2022_df_parts/outputs_info/outputs_encoded",mode="overwrite")
#ad2cl = outputs