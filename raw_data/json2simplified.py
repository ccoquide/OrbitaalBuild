import json
import glob
import ntpath
from os import path
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

#Utils
def get_files_from_dir(a_dir,prefix="",suffix=""):
    if path.isfile(a_dir):
        return [a_dir]
    files = glob.glob(a_dir + '/**/'+prefix+'*'+suffix, recursive=True)
    return sorted(files)

def simplify_file(input_file,output_file):
    #create the file if it does not exist
    # Assurez-vous que le dossier existe
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Ouvrir le fichier pour écriture
    outputfile=open(output_file, "w+")    
    with open(input_file) as f:
        for ii,line in enumerate(f):
            if ii%10000==0:
                print(ii)
            #if ii==1000:
             #   break
            tr=json.loads(line)
            towrite=""
            towrite+=tr["hash"]+"\t"+str(tr["block_number"])+"\t"+str(tr["block_timestamp"])+"\t"+str(tr["fee"])+"\t"
            ins=[]
            for in_tr in tr["inputs"]:
                ins.append((in_tr["spent_transaction_hash"],in_tr["spent_output_index"]))
            towrite+=json.dumps(ins)+"\t"
            outs=[]
            for o in tr["outputs"]:
                outs.append((o["index"],o["addresses"],o["value"]))
            towrite+=json.dumps(outs)
            outputfile.write(towrite+'\n')
    outputfile.close()
            
def simplify_directory(input_dir,output_dir):
    files=get_files_from_dir(input_dir,prefix="transactions")
    for f in files:
        print("simplifying file "+f)
        f_name=ntpath.basename(f)
        simplify_file(f,output_dir+"/"+f_name+".tsv")

simplify_directory(PATH_I,PATH_O)
