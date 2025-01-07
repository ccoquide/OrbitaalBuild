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

PATH_unspent=sys.argv[3]





def get_files_from_dir(a_dir,prefix="",suffix=""):
    if path.isfile(a_dir):
        return [a_dir]
    files = glob.glob(a_dir + '/**/'+prefix+'*'+suffix, recursive=True)
    return sorted(files)


#match inputs v2
names=["hash","block","timestamp","fee","inputs","outputs"]
def match_inputs(input_dir,output_dir,save_unspent,current_unspent=None):
    """
    note that the function can export and import the current list of unspent transactions to allow updating without recomputing everything
    """
    input_files=get_files_from_dir(input_dir)
    
    unspent_outputs=dict()
    if current_unspent!=None:
        print("reloading previous file: "+current_unspent)
        unspent_outputs_files=pd.read_csv(current_unspent,sep="\t",index_col=False)
        for (_,hash_i,id_i,info_i) in unspent_outputs_files.itertuples():
            unspent_outputs[(hash_i,id_i)]=json.loads(info_i)

    for input_file in input_files:
        print(input_file,len(unspent_outputs))
        #first, memorize all outputs of the file, and add to previous unspent outputs
        with open(input_file) as f:
            for ii,line in enumerate(f):
                items=line.split("\t")
                outs=json.loads(items[-1])
                for out in outs:
                    unspent_outputs[(items[0],out[0])]=[out[1],out[2]]
                    
        output_file=output_dir+"/"+ntpath.basename(input_file)
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        print("output file: ",output_file)

        # Ouvrir le fichier pour écriture
        outputfile=open(output_file, "w+")    
        with open(input_file) as f:
            for ii,line in enumerate(f):
                items=line[:-1].split("\t")
                inputs=json.loads(items[-2])
                inputs_matched=[]
                for input_source in inputs:
                    id_source=(input_source[0],input_source[1])
                    if id_source in unspent_outputs:
                        info=unspent_outputs[id_source]
                        id_source_to_write=(input_source[0],input_source[1],info[0],info[1])
                        inputs_matched.append(id_source_to_write)
                        #input_source+=unspent_outputs[id_source]
                        del unspent_outputs[id_source]
                    else:
                        print("input not found: ",line)
                #if len(inputs_matched)>0:
                outputfile.write(items[0]+"\t"+items[1]+"\t"+items[2]+"\t"+items[3]+"\t"+json.dumps(inputs_matched)+"\t"+items[5])
                outputfile.write('\n')
        outputfile.close()
    
    os.makedirs(os.path.dirname(save_unspent), exist_ok=True)
    outputfile=open(save_unspent, "w+")
    outputfile.write("hash\tid\tinfo\n")
    for (h,i),v in unspent_outputs.items(): 
        outputfile.write(str(h)+"\t"+str(i)+"\t"+json.dumps(v)+"\n")
    outputfile.close()


match_inputs(PATH_I,PATH_O,PATH_unspent)    
    