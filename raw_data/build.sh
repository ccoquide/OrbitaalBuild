#!/bin/bash
PATH_W="data"

# Working folders
PATH_TEMP=${PATH_W}/temp
PATH_DICT=${PATH_W}/dictionary
PATH_FINAL=${PATH_W}/final

#Originale source files to use
ORIGINAL_JSON=${PATH_W}/sources/original_json
PRICE=${PATH_W}/sources/btc_prices.csv
ADDRESSBOOK=${PATH_W}/sources/walletExplorer.csv

#Temporary files

cluster_dir=${PATH_TEMP}/clusters

detailedTransactions=${PATH_TEMP}/detailedTransactions
trWithID=${PATH_TEMP}/trWithID

#Files final version
ENRICHED_TRANSACTIONS=${PATH_FINAL}/enrichedTransactions
ENRICHED_OUTPUTS=${PATH_FINAL}/enrichedOutputs


#Dictionaries to keep
tr2ID=${PATH_DICT}/tr2ID
ad2ID=${PATH_DICT}/ad2ID

tr2total2fee=${PATH_DICT}/tr2total2fee
ad2adID2cl2identity=${PATH_DICT}/ad2adID2cl2identity




### Extracting only necessay information from the original json
python3.8 json2simplified.py ${ORIGINAL_JSON} ${PATH_TEMP}/raw_simplified

# ### Adding input address information  from source transaction outputs
python3.8 matching_inputs.py ${PATH_TEMP}/raw_simplified ${PATH_TEMP}/matched ${PATH_TEMP}/unspent

# ### Creating a dataframe with the necessary information
python3.8 firstDF.py  ${PATH_TEMP}/matched ${PATH_TEMP}/as_firstDF

# ### Match addresses using co-input heuristic (clustering)
python3.8 clustering.py  ${PATH_TEMP}/as_firstDF ${cluster_dir}

### format properly all the data, extracting lists, etc.
python3.8 toDetailed.py  ${PATH_TEMP}/as_firstDF ${PATH_TEMP}/detailedTransactions

### Replace tranactions hash by unique IDs, save these IDs and create a new version of the file with those IDs instead of the hashes
python3.8 trID.py ${PATH_TEMP}/detailedTransactions ${PATH_TEMP}/trWithID ${tr2ID}

### Compute fees and total amount for each transaction
python3.8 fees.py ${PATH_TEMP}/trWithID ${PATH_TEMP}/trWithID_with_fee ${tr2total2fee}

### Create a complete enriched filed with one line per transaction, neriched with prices and fees
python3.8 EnrichPriceFee.py ${PATH_TEMP}/trWithID_with_fee ${ENRICHED_TRANSACTIONS} ${PRICE} 

### Creat a new dataframe in which one line corresponds to one output
python3.8 oneLinePerOutput.py ${ENRICHED_TRANSACTIONS} ${PATH_TEMP}/oneLinePerOutput

### Create a dictionary to replace bitcoin addresses by unique IDs. Save the dictionary and create a new version of the file with those IDs instead of the addresses
python3.8 create_ad2ID.py ${PATH_TEMP}/oneLinePerOutput ${PATH_TEMP}/perOutputWithID ${ad2ID}

### From a dictionary of addresses to user names, create a dictionary of addresses to cluster IDs to user names
python3.8 create_ad2adID2cl2identity.py ${ad2ID} ${cluster_dir}/ad2cl ${ADDRESSBOOK} ${ad2adID2cl2identity}

# ### Create a new clean version with the IDs, names, etc.
python3.8 outputEnriched.py ${PATH_TEMP}/perOutputWithID ${ENRICHED_OUTPUTS} ${PATH_DICT}/ad2adID2cl2identity

### Remove all pyspark related temporary files
for f in $( find ./ -name .*crc); do rm ${f}; done
for f in $( find ./ -name _SUCCESS); do rm ${f}; done
