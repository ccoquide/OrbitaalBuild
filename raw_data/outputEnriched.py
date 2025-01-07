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

DICO_ad2id2cl2idenity2radical_file = sys.argv[3]


#add cluster info to the outputs

transactions = spark.read.load(PATH_I)

ad2cl2identity_all = spark.read.load(DICO_ad2id2cl2idenity2radical_file).select("id","cl","identity","identity_radical")

ad2cl2identity_all = ad2cl2identity_all.withColumnRenamed("id","id_src").withColumnRenamed("cl","src_cl").withColumnRenamed("identity","src_identity").withColumnRenamed("identity_radical","src_identity_radical")
outputs_encoded = transactions.join(ad2cl2identity_all,on="id_src",how="left")

ad2cl2identity_all = ad2cl2identity_all.withColumnRenamed("id_src","id_dst").withColumnRenamed("src_cl","dst_cl").withColumnRenamed("src_identity","dst_identity").withColumnRenamed("src_identity_radical","dst_identity_radical")
outputs_encoded = outputs_encoded.join(ad2cl2identity_all,on="id_dst",how="left")

outputs_encoded.write.save(PATH_O,mode="overwrite")