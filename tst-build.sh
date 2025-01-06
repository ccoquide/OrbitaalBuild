#!/bin/bash
PATH_W="./DataSet"
PATH_1="./raw_data/enrichedOutputs"
Dir_1="Prepared_Data"
Dir_2="TEMPORAL"
Dir_3="NODE_TABLE"
Dir_4="STREAM_GRAPH"
Dir_5="SNAPSHOT"
### Cleaning transaction dataset and formatting it for temporal graph data construction
python3.8 cleanData.py ${PATH_W} ${PATH_1}

### Constructing the graph node information table
python3.8 createNodeTables.py ${PATH_W}

### Preparing temporary directories and files for stream graph and snapshot construction
python3.8 createTemporalNetwork.py ${PATH_W}/${Dir_1} ${PATH_W}

### Creation of stream graph and snapshot directories + stream graph data
python3.8 Rebuild-Final_Dataset.py ${PATH_W}

### Adding bitcoin addresses to node information table
python3.8 createNodeAddressInfo.py ${PATH_W}/${Dir_2}/network_2009_2021_name ${PATH_W}/${Dir_3} ${PATH_1}

###Removing temporary files and directories
python3.8 finalclean.py ${PATH_W}/${Dir_2}/network_2009_2021_name

### Creating snapshot for all time resolutions
python3.8 createSnapshotNetwork.py ${PATH_W}

### Reshape the dataset files and directories for simpler access in case of stream graph
python3.8 finalStreamGraphFilesRenaming.py ${PATH_W}
rm -R ${PATH_W}/${Dir_4}/; mv ${PATH_W}/STREAM_GRAPH_NEW/ ${PATH_W}/${Dir_4}/

### Reshape the dataset files and directories for simpler access in case of snapshot
python3.8 finalSnapshotFilesRenaming.py ${PATH_W}
rm -R ${PATH_W}/${Dir_5}/; mv ${PATH_W}/SNAPSHOT_NEW/ ${PATH_W}/${Dir_5}/

### Renaming user details DataFrame and suppresion of unecessary PySpark created files
mv ${PATH_W}/${Dir_3}/part-*.snappy.parquet ${PATH_W}/${Dir_3}/orbitaal-nodetable.snappy.parquet
rm ${PATH_W}/${Dir_3}/.*.crc
rm ${PATH_W}/${Dir_3}/_SUCCESS
mv ${PATH_W}/${Dir_3}/add_info/part-*.snappy.parquet ${PATH_W}/${Dir_3}/orbitaal-listaddresses.snappy.parquet
rm -R ${PATH_W}/${Dir_3}/add_info

