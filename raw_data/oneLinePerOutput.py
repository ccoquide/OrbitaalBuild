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


def _create_in_outs(x):
    to_return=[]
    input_ads=x["ads_ins"]
    output_ads=x["ads_outs"]
    values_outs=x["values_outs"]
    ids_outs=x["ids_outs"]
    #sum_in=10
    #sum_out=9

    if len(input_ads)==0:
        input_ad="mining"
    else:
        input_ad=input_ads[0]

        
    for i,output_ad_item in enumerate(output_ads):
        output_number=ids_outs[i]
        output_ad=output_ads[i]
        output_val=values_outs[i]
        
        to_return.append((x["trID"],output_number,input_ad,output_ad,output_val,x["nb_inputs"],x["nb_outputs"],x["timestamp"],x["year"],x["month"],x["day"],x["bloc"],x["fee"],x["total_in"],x["total_out"],x["PriceUSD"],x["HashRate"],output_val* x["PriceUSD"]/100000000))
    #if len(output_ads)>1:
    #    print(to_return)
    return(to_return)

before = spark.read.load(PATH_I)


#before = before.select("trID","ads_ins","ads_outs","values_outs","ids_outs","nb_inputs","nb_outputs","timestamp","year","month","day","bloc")
to_write = before.rdd.flatMap(_create_in_outs).toDF(["trID","output_id","src","dst","value","nb_inputs","nb_outputs","timestamp","year","month","day","bloc","fee","total_in","total_out","PriceUSD","HashRate","valueUSD"])
to_write.write.save(PATH_O,mode="overwrite")