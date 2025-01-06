import json
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StructField,StructType, LongType, FloatType

import snap
from os import path
import glob
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


def get_files_from_dir(a_dir,prefix="",suffix=""):
    if path.isfile(a_dir):
        return [a_dir]
    files = glob.glob(a_dir + '/**/'+prefix+'*'+suffix, recursive=True)
    return sorted(files)


def replace_column(df_original,df_dict,output_file,key_original,key_dict,renaming_new_col=False,replace=False,write=True):
    """
    replace column key_original in df df_original by column target_name of df_dict, renamed as new_name
    """
    if isinstance(df_original,str):
        df_original = spark.read.load(df_original)
    if isinstance(df_dict,str):
        df_dict = spark.read.load(df_dict)
        
    df_original=df_original.withColumnRenamed(key_original,"key1")
    df_dict=df_dict.withColumnRenamed(key_dict,"key2")
    joined = df_original.join(df_dict,df_original["key1"] ==  df_dict["key2"],how='left')
    if replace:
        joined = joined.drop("key1")
    else:
        joined = joined.withColumnRenamed("key1",key_original)
    joined=joined.drop("key2")
    if renaming_new_col!=False:
        joined=joined.withColumnRenamed(renaming_new_col[0],renaming_new_col[1])
    if write:
        joined.write.save(output_file,mode="overwrite")
    return joined

def _extract_in(x):
    to_return=[]
    inputs=json.loads(x.inputs)
    if len(inputs)>1:
        for i in range(len(inputs)-1):
            ads1=inputs[i][2]
            ads2=inputs[i+1][2]
            if len(ads1)==1:
                ads1=ads1[0]
            else:
                ads1=str(ads1)
            if len(ads2)==1:
                ads2=ads2[0]
            else:
                ads2=str(ads2)
            to_return.append((ads1,ads2))
        return(to_return)
    return([])

def _create_co_spending(current_dir,year="",month=""):
    #print("computing co_spending", current_dir)
    lines = spark.read.load(current_dir)
    lines.show()
    #print(current_dir)
    if year!="":
        lines = lines.filter(lines.year<=year).filter(lines.month<=month)
    inputs = lines.select("inputs")
    inputs.show()
    
    to_write = inputs.rdd.flatMap(_extract_in).toDF(["src","dst"])#.collect()#
    return(to_write)
    

###create a list of unique addresses to have int IDs
def _to_uniq_vals(row):
    return [(row.src,),(row.dst,)]


    
    
def _dfZipWithIndex (df, offset=1, colName="rowId"):
    '''
        Enumerates dataframe rows is native order, like rdd.ZipWithIndex(), but on a dataframe 
        and preserves a schema

        :param df: source dataframe
        :param offset: adjustment to zipWithIndex()'s index
        :param colName: name of the index column
    '''

    new_schema = StructType(
                    [StructField(colName,LongType(),True)]        # new added field in front
                    + df.schema.fields                            # previous schema
                )

    zipped_rdd = df.rdd.zipWithIndex()

    new_rdd = zipped_rdd.map(lambda args: ([args[1] + offset] + list(args[0])))

    return spark.createDataFrame(new_rdd, new_schema)

def _create_ad2id(co_spending):
    addresses =co_spending.rdd.flatMap(_to_uniq_vals).toDF(["ad"]).distinct()
    ad2id = _dfZipWithIndex(addresses,colName="id")       
    return ad2id


def _convert_co_spending_to_id(temp_dir,co_spending,ad2id,output_co_spending_file):
    replace_column(co_spending,
               ad2id,
               temp_dir+"/temp_co_spending",
               "src",
               "ad",
               ("id","src_id"),
               replace=True
              )
    replace_column(temp_dir+"/temp_co_spending",
               ad2id,
               output_co_spending_file,
               "dst",
               "ad",
               ("id","dst_id"),
               replace=True
              )
    

def _write_clusters(clusters,output_file,ID=True,ID2A=None):
    output_file=open(output_file, "w+")
    for i,CnCom in enumerate(clusters):
        for nodeID in CnCom:
            if ID:
                to_write=str(nodeID)
            else:
                to_write=str(ID2A[nodeID])
            output_file.write(to_write+"\t"+str(i)+"\n")
    output_file.close()
    
def _compute_with_snap(co_spend_id,id2cl_file):
    G0 = snap.LoadEdgeList(snap.TUNGraph, co_spend_id, 0,1)
    print("Number of Nodes: %d" % G0.GetNodes())
    print("Number of edges: %d" % G0.GetEdges())
    Components = G0.GetSccs()
    _write_clusters(Components,id2cl_file,True)

    
def _convert_clusters_from_id_to_addresses(ad2id,id2cl,output):
    replace_column(ad2id,
               id2cl,
               output,
               "id",
               "id",
               replace=True
              ) 
def compute_clusters(current_dir,cluster_dir,year="",month=""):
    co_spending = _create_co_spending(current_dir,year,month)
    co_spending.write.save(cluster_dir+"/co_spending",mode="overwrite")
    
    co_spending=spark.read.load(cluster_dir+"/co_spending")
    ad2id = _create_ad2id(co_spending)
    ad2id.write.save(cluster_dir+"/ad2id_cluster",mode="overwrite")
    
    _convert_co_spending_to_id(cluster_dir,cluster_dir+"/co_spending",cluster_dir+"/ad2id_cluster",cluster_dir+"/co_spending_id")
    
    co_spend_id=spark.read.load(cluster_dir+"/co_spending_id")
    co_spend_id.coalesce(1).write.format("csv").option("header", "false").option("sep"," ").save(cluster_dir+"/co_spend_id.csv",mode="overwrite")
    
    co_spend_id_file = get_files_from_dir(cluster_dir+"/co_spend_id.csv","part")[0]
    
    _compute_with_snap(co_spend_id_file,cluster_dir+"/id2cl.ssv")
    spark.read.csv(cluster_dir+"/id2cl.ssv",sep="\t").toDF("id","cl").write.save(cluster_dir+"/id2cl",mode="overwrite")                    
    _convert_clusters_from_id_to_addresses(cluster_dir+"/ad2id_cluster",cluster_dir+"/id2cl",cluster_dir+"/ad2cl")                   
                     
                
compute_clusters(PATH_I,PATH_O)#,year=2011,month=1)