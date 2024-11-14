import os
from collections import defaultdict as ddict
import numpy as np
from pyspark.sql.functions import from_unixtime, first, year, month, dayofmonth, hour, col, when, lit, row_number
from pyspark.sql import Window
import pandas as pd
import datetime
def getTimeDiff(t_0):
    """
    Calculates the time difference between the given time `t_0` and the current time.

    Parameters:
        t_0 (datetime): The starting time.

    Returns:
        str: A formatted string representing the time difference in hours, minutes, and seconds.
    """
    t_1=datetime.datetime.now()
    diff_s=(t_1-t_0).seconds
    return f"{diff_s//3600} h, {(diff_s%3600)//60} m , {(diff_s%3600)%60} s"
def CleanData(outputs_subset):
    """
    Cleans the data by performing various transformations on the given DataFrame.

    Args:
        outputs_subset (Pyspark DataFrame): The input DataFrame containing transaction data.

    Returns:
        Pyspark DataFrame: The cleaned DataFrame with the following transformations applied:
            - Setting new ID for null src_cl and dst_cl
            - Setting unique ID for all unexisting src id for mining transactions
            - Setting src_identity_radical of mining transactions to "MINING"
            - Merging all duplicated src_best_name and dst_best_name
            - Calculating the number of total transactions and the percentage of self spent and other transactions
    """
    
    #Tag mining transactions
    outputs_subset=outputs_subset.withColumn("Mining", when(col("nb_inputs")>0, False).otherwise(True))
    Ntot=outputs_subset.count()
    Nmining=outputs_subset[outputs_subset.Mining==True].count()
    print("There are ", Nmining*100.0/Ntot, " % of all transactions without inputs")
    
    #Set temporary IDs for transactions with null src_cl and dst_cl + mining node
    outputs_subset=WithNewID(outputs_subset)
    
    #Set src_identity_radical of mining transactions to MINING
    outputs_subset=outputs_subset.withColumn("src_identity_radical", when(col("Mining")==True, "MINING").otherwise(col("src_identity_radical")))
    
    #Create best name for src and dst
    outputs_subset=outputs_subset.withColumn("src_best_name", when(col("src_identity_radical").isNull(), col("src_cl")).otherwise(col("src_identity_radical")))
    outputs_subset=outputs_subset.withColumn("dst_best_name", when(col("dst_identity_radical").isNull(), col("dst_cl")).otherwise(col("dst_identity_radical")))
    
    #Merge duplicate best names
    print("Merging duplicate best names")
    outputs_subset=MergeDuplicateBestName(outputs_subset)
    print("Merging done")
    
    #Few statistics
    #l2=outputs_subset.count()
    #Ntot=l2
    #outputs_subset=outputs_subset.withColumn("loop", when(col("src_cl")==col("dst_cl"), True).otherwise(False))
    #Nself=outputs_subset[outputs_subset.loop==True].count()
    #print(f"There are {Ntot} distinct transactions")
    #print(f"{Nself*100.0/Ntot} % of self spent transaction and {100.0-Nself*100.0/Ntot} % of other transactions")
    return outputs_subset
def getFees(pydf):
    """
    Retrieves fee-related information from a PySpark DataFrame.

    Args:
        pydf (PySpark DataFrame): The input DataFrame containing fee information.

    Returns:
        PySpark DataFrame: A new DataFrame with selected columns and additional calculated columns.

    """
    selectedColumns=[
        "trID", "Mining", "src_cl", "src_best_name", "dst_cl", "dst_best_name", "value", "valueUSD", "PriceUSD","timestamp", "year", "fee", "bloc", "output_id", "nb_outputs"
            ]
    outputSelectedColumns=["src_cl", "dst_cl", "value", "valueUSD", "timestamp", "src_best_name", "dst_best_name", "year"]
    rez=pydf.select(*selectedColumns)
    
    #Get mining node ID from pydf
    rootID=getMiningID(rez)
    print("cl id for mining node is ", rootID)
    
    #Create new transactions with mining node as destination actor
    COLS=rez.columns[1:]
    rez_=rez[rez.Mining==False].groupBy("trID").agg(*[first(col).alias(col) for col in COLS])\
        .withColumn("dst_cl", lit(rootID))\
        .withColumn("dst_best_name", lit("MINING"))\
        .withColumn("value", col("fee"))\
        .withColumn("valueUSD", col("fee")*col("PriceUSD")*10**-8)\
        .withColumn("fee", lit(0))
    rez=rez.union(rez_)
    
    #Remove transactions with value=0
    rez=rez.withColumn("NON_ZERO_VALUE", when(col("value")==0, False).otherwise(True))# Tag zero value for removal
    del(rez_)
    rez=rez[rez.NON_ZERO_VALUE].orderBy("timestamp")
    
    
    return rez.select(*outputSelectedColumns)
def WithNewID(pydf):
    """
    This function replaces null values in the 'src_cl' and 'dst_cl' columns of the input DataFrame with new IDs.
    It also calculates the lowest ID value and assigns it to the 'src_cl' column for mining transactions.

    Args:
        pydf (Pyspark DataFrame): The input DataFrame containing the columns 'src_cl', 'dst_cl', 'id_src', 'id_dst', and 'Mining'.

    Returns:
        Pyspark DataFrame: The modified DataFrame with null values replaced and new IDs assigned.

    """
    #Create new temporary IDs for transaction actors whose src_cl or dst_cl is null
    rez=pydf.withColumn("src_cl", when((col("src_cl").isNull()) & (col("Mining")==False), -col("id_src")).otherwise(col("src_cl"))).withColumn("dst_cl", when(col("dst_cl").isNull(), -col("id_dst")).otherwise(col("dst_cl")))
    
    #Set ID for mining node
    min_srcid=rez.select(min("src_cl")).collect()[0][0]
    min_dstid=rez.select(min("dst_cl")).collect()[0][0]
    minning_id=int(np.array([int(min_srcid), int(min_dstid)]).min())-1
    rez=rez.withColumn("src_cl", when((col("src_cl").isNull()) & (col("Mining")==True), minning_id).otherwise(col("src_cl")))
    return rez
def MergeDuplicateBestName(pydf):
    """
    Prepare PySpark DataFrame for duplicate best names merging.

    Args:
        pydf (PySpark DataFrame): The input PySpark DataFrame.

    Returns:
        PySpark DataFrame: The modified PySpark DataFrame with merged duplicate best names.
    """
    #Get the first cl ID for each unique best name 
    src=pydf.groupBy("src_best_name").agg(first("src_cl").alias("cl_")).withColumnRenamed("src_best_name", "best_name")
    dst=pydf.groupBy("dst_best_name").agg(first("dst_cl").alias("cl_")).withColumnRenamed("dst_best_name", "best_name")
    src=src.union(dst).groupBy("best_name").agg(first("cl_").alias("cl_"))
    
    #Add replace each cl ID with the first cl ID for each unique best name
    pydf=pydf.join(src, pydf.src_best_name==src.best_name, "left")\
        .withColumnRenamed("cl_", "src_cl_")\
        .drop("best_name")
    pydf=pydf.join(src, pydf.dst_best_name==src.best_name, "left")\
        .withColumnRenamed("cl_", "dst_cl_")\
        .drop("best_name")
    del(src)
    del(dst)
    pydf=pydf.withColumn("src_cl", col("src_cl_"))\
        .drop("src_cl_")\
        .withColumn("dst_cl", col("dst_cl_"))\
        .drop("dst_cl_")
    return pydf
def getMiningID(pydf):
    """
    Returns the Mining node ID.

    Parameters:
    pydf (PySpark DataFrame): The network data.

    Returns:
    int: The mining node ID.

    """
    return pydf[pydf.Mining==True].limit(1).collect()[0]["src_cl"]
def writeNodeTable(sparksession, pydf, PATH, years=[y for y in range(2009,2022)]):
    """
    Writes a node table to a specified path after processing a PySpark DataFrame.

    Parameters:
    sparksession (SparkSession): The Spark session to use for reading and writing data.
    pydf (PySpark DataFrame): The PySpark DataFrame containing the data to process.
    PATH (str): The path where the output files will be saved.
    years (list, optional): A list of considered years, default is whole period of data.

    Returns:
    int: Returns 0 upon successful completion.

    The function performs the following steps:
    1. Logs the initial parameters and the number of rows in the input DataFrame.
    2. Creates a directory for temporary files if it does not exist.
    3. Constructs a table mapping each src_id and dst_id to the first and last timestamps they appear, and calculates the balance.
    4. Writes the constructed table to a Parquet file.
    5. Generates new IDs for the nodes and writes the updated table to a Parquet file.
    6. Cleans up temporary files and logs the time taken for each step.
    """
    ww=open("writeNodeTable_2.log", "w")
    n_row=pydf.select("src_cl").count()
    print(f"PATH = {PATH}", "\n", f"YEARS = {years}\n")
    
    #Creation of the TEMPORAL directory if it doesn't exist
    if not os.path.exists(f"{PATH}/TEMPORAL"):
        os.makedirs(f"{PATH}/TEMPORAL")
        
    #Creation of new ID for nodes (incremental)
    print("Creating new ID for nodes")
    t__0=datetime.datetime.now()
    
    # Create a DataFrame that maps each src_id and dst_id to the first timestamp it appears
    id_timestamp_df_src=pydf.select("src_cl", "src_best_name", "timestamp", "value").withColumnRenamed("src_cl","ID").withColumnRenamed("src_best_name", "name")
    id_timestamp_df_dst=pydf.select("dst_cl", "dst_best_name", "timestamp", "value").withColumnRenamed("dst_cl","ID").withColumnRenamed("dst_best_name", "name")
    id_timestamp_df_src=id_timestamp_df_src.groupBy("ID", "name").agg(min("timestamp").alias("FIRST_TIMESTAMP"), max("timestamp").alias("LAST_TIMESTAMP"), sum(col("value")*-1).alias("BALANCE"))
    id_timestamp_df_dst=id_timestamp_df_dst.groupBy("ID", "name").agg(min("timestamp").alias("FIRST_TIMESTAMP"), max("timestamp").alias("LAST_TIMESTAMP"), sum("value").alias("BALANCE"))
    id_timestamp_df_src=id_timestamp_df_src.union(id_timestamp_df_dst)
    del(id_timestamp_df_dst)
    id_timestamp_df_src=id_timestamp_df_src.groupBy("ID", "name").agg(min("FIRST_TIMESTAMP").alias("FIRST_TIMESTAMP"), max("LAST_TIMESTAMP").alias("LAST_TIMESTAMP"), sum("BALANCE").alias("BALANCE")).orderBy("FIRST_TIMESTAMP")
    print(f"Table constructed in {getTimeDiff(t__0)}")
    
    #Write the table to a Parquet file
    t__0=datetime.datetime.now()
    fname=f"{PATH}/TEMPORAL/network_{years[0]}_{years[-1]}_name"
    id_timestamp_df_src=id_timestamp_df_src.withColumnRenamed("ID", "OLD_ID")\
        .withColumnRenamed("name","NAME")\
        .withColumnRenamed("BALANCE", "BALANCE_SATOSHI")
    id_timestamp_df_src.select("OLD_ID", "NAME", "FIRST_TIMESTAMP", "LAST_TIMESTAMP", "BALANCE_SATOSHI")\
        .write.parquet(fname, mode="overwrite")
    
    #Get New Id from the table and create a temporary table as csv file
    getNewId(sparksession, fname, "/media/ssd4/celestin/TEMPORAL/tmp.csv", "/media/ssd4/celestin/TEMPORAL/tmp2.csv")
    
    #Turn csv temporary table with new IDs into parquet
    id_timestamp_df_src=sparksession.read.csv("/media/ssd4/celestin/TEMPORAL/tmp2.csv", header=True, inferSchema=True).orderBy("NEW_ID")
    print(id_timestamp_df_src.columns)
    id_timestamp_df_src.write.parquet(fname, mode="overwrite")
    
    #Remove temporary files
    os.remove(f"{PATH}/tmp2.csv")
    print("Table of node written, in ", getTimeDiff(t__0))
    del(id_timestamp_df_src)
    return 0
def getNewId(sparksession, fname, foutput, tmpfile):
    """
    Generate New IDs for the nodes.

    Parameters:
    sparksession (SparkSession): The Spark session to use for reading the parquet file.
    fname (str): The path to the input parquet file.
    foutput (str): The directory where the intermediate CSV file will be written.
    tmpfile (str): The path to the temporary file where the final CSV with the new ID column will be written.

    Returns:
    int: Always returns 0.
    """
    #Create temporary csv version of the input parquet
    sparksession.read.parquet(fname).orderBy("FIRST_TIMESTAMP").coalesce(1).write.mode("overwrite").csv(foutput, header=True)

    #get PySpark Generated FileName of the temporary csv file
    for file in os.listdir(foutput):
        if ((file.find("part")==0) & (file.endswith(".csv"))):
            fname_=f"{foutput}/{file}"
    
    #Writting table of nodes with new IDs while reading CSV file
    f=open(fname_,"r")
    ww=open(tmpfile,"w")
    t__=datetime.datetime.now()
    z=0
    for line in f:
        if z==0:
            ww.write("NEW_ID"+","+line)
        else:
            ww.write(str(z-1)+","+line)# Low ID (high) is associated to old (recent) actors
        z+=1
    f.close()
    ww.close()
    print("Done in ", getTimeDiff(t__))
    
    #Removing temporary csv and directory
    for file in os.listdir(foutput):
        fname_=f"{foutput}/{file}"
        os.remove(fname_)
    os.removedirs(foutput)
    return 0
def write_TEMPORAL(sparksession, pydf, fname, PATH, format="parquet", years=[y for y in range(2009,2022)]):
    """
    Writes a temporal network file from cleaned dataset and the node table containing new actor IDs.

    Parameters:
    sparksession (SparkSession): The Spark session to use for reading and writing data.
    pydf (ÿSpark DataFrame): The PySpark DataFrame containing the data to be written.
    fname (str): The file name of the input parquet file containing the node ID mappings.
    PATH (str): The directory path where the output file will be saved.
    format (str, optional): The format of the output file, either "parquet" or "csv". Defaults to "parquet".
    years (list, optional): The list of years to consider for the temporal network. Default is the whole period of the dataset.

    Returns:
    int: Returns 0 upon successful completion.

    Logs:
    - Logs various stages of the function execution to "writeNetData_TEMPORAL.log".
    - Logs the number of rows in the input DataFrame.
    - Logs the estimated size of the output file.
    - Logs the time taken for various stages of the function execution.

    Notes:
    - The function creates a directory named "TEMPORAL" inside the given PATH if it does not exist.
    - The function reads the node ID mappings from the input parquet file and replaces the old IDs in the input DataFrame with new IDs.
    - The function writes the modified DataFrame to the specified output format and path.
    """
    
    print(f"node_table from {fname}, output path {PATH}, output format {format}, considered years {years}")
    n_row=pydf.select("src_cl").count()
    print(f"n_row={n_row}")
    n_row_sample=32752
    size_sample=1.1#Mo
    print(f"The estimated size for the temporal net output is {n_row*size_sample/n_row_sample} Mo")
    
    #Create TEMPORAL directory if it doesn't exist
    if not os.path.exists(f"{PATH}/TEMPORAL"):
        os.makedirs(f"{PATH}/TEMPORAL")
        
    print("Loading Table of nodes")
    t__0=datetime.datetime.now()
    
    #Conversion from old IDs to New IDs
    id_timestamp_df_src=sparksession.read.parquet(fname).select("NEW_ID", "OLD_ID")
    t__0=datetime.datetime.now()
    pydf = (pydf.join(id_timestamp_df_src.withColumnRenamed("OLD_ID", "src_cl"), "src_cl", "left")\
            .withColumnRenamed("NEW_ID", "NEW_SRC_ID")\
            .join(id_timestamp_df_src.withColumnRenamed("OLD_ID", "dst_cl"), "dst_cl", "left")\
            .withColumnRenamed("NEW_ID", "NEW_DST_ID")\
            .drop("src_cl", "dst_cl")\
            .withColumnRenamed("NEW_SRC_ID", "SRC_ID")\
            .withColumnRenamed("NEW_DST_ID", "DST_ID"))
    del(id_timestamp_df_src)
    print("Change ID with new ones in the whole dataset in ", getTimeDiff(t__0))
    t__0=datetime.datetime.now()
    
    #Writting the stream graph
    pydf=pydf.select("SRC_ID", "DST_ID", "timestamp", "value", "valueUSD")\
        .withColumnRenamed("timestamp", "TIMESTAMP")\
        .withColumnRenamed("value", "VALUE_SATOSHI")\
        .withColumnRenamed("valueUSD", "VALUE_USD")
    print("Writing the temporal network file")
    fname_=f"{PATH}/TEMPORAL/network_{years[0]}_{years[-1]}"+f".{format}"
    t__0=datetime.datetime.now()
    if format=="csv":
        print(f"{format} is the format of the output file")
        pydf.write.csv(fname_, header=True, mode="overwrite")
    if format=="parquet":
        print(f"{format} is the format of the output file")
        pydf.write.parquet(fname_, mode="overwrite")    
    print(f"File wrote in {getTimeDiff(t__0)}")
    return 0
def write_SNAPSHOT(net, PATH, format="parquet"):
    """
    Writes a snapshot of the given network data to the specified path in the desired format.

    Parameters:
    net (PySpark DataFrame): The network data as a Spark DataFrame.
    PATH (str): The directory path where the snapshot will be saved (work path).
    format (str, optional): The format in which to save the snapshot. Options are "csv" or "parquet". Default is "parquet".

    Returns:
    int: Returns 0 upon successful completion.

    The function performs the following steps:
    1. Creates necessary directories if they do not exist.
    2. Adds columns for year, month, day, and hour based on the TIMESTAMP column.
    3. Groups the data by SRC_ID, DST_ID, and the specified time resolution (hour, day, month, year).
    4. Writes the grouped data to the specified path in the chosen format (CSV or Parquet).
    5. Logs the progress and time taken for each resolution.

    Note:
    - The function assumes that the input DataFrame `net` contains columns named "SRC_ID", "DST_ID", "TIMESTAMP", "VALUE_SATOSHI", and "VALUE_USD".
    """
    t__=datetime.datetime.now()
    if not os.path.exists(f"{PATH}/SNAPSHOT"):
        os.makedirs(f"{PATH}/SNAPSHOT")
        print(f"creating {PATH}/SNAPSHOT")
    if not os.path.exists(f"{PATH}/SNAPSHOT/EDGES"):
        os.makedirs(f"{PATH}/SNAPSHOT/EDGES")
        print(f"creating {PATH}/SNAPSHOT/EDGES")
    print("Writing snapshot network based on the temporal network")
    print("Create column for timestamp at different resolution : month, day and hour")
    rez=net.withColumn("year", year(from_unixtime("TIMESTAMP")))\
        .withColumn("month", month(from_unixtime("TIMESTAMP")))\
        .withColumn("day", dayofmonth(from_unixtime("TIMESTAMP")))\
        .withColumn("hour", hour(from_unixtime("TIMESTAMP")))
    print("Writing output from hour resolution to year resolution hope it wirks this time")
    z=0
    reso=["hour", "day", "month", "year"]
    t___=datetime.datetime.now()
    for r in reso:
        rez=rez.groupBy("SRC_ID", "DST_ID", *[r_ for r_ in reso[z:]]).agg(sum("VALUE_SATOSHI").alias("VALUE_SATOSHI"), sum("VALUE_USD").alias("VALUE_USD"))
        print("Writing output for resolution ", r, "in specific subdirectories and in", format, "format")
        if format=="csv":
            rez.select("SRC_ID", "DST_ID", "VALUE_SATOSHI", "VALUE_USD", *[r_ for r_ in reso[z:][::-1]])\
             .write.mode("append")\
            .partitionBy(*[r_ for r_ in reso[z:][::-1]])\
            .csv(f"{PATH}/SNAPSHOT/EDGES/", header=True)
        if format=="parquet":
            rez.select("SRC_ID", "DST_ID", "VALUE_SATOSHI", "VALUE_USD", *[r_ for r_ in reso[z:][::-1]])\
             .repartition(*[r_ for r_ in reso[z:][::-1]])\
             .write.mode("append")\
            .partitionBy(*[r_ for r_ in reso[z:][::-1]])\
            .parquet(f"{PATH}/SNAPSHOT/EDGES/")
        z+=1
    print("Done")
    return 0
