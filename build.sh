python3.8 cleanData.py /media/ssd4/bitcoinRemy/outputs_actors_everything /media/ssd4/celestin
python3.8 createNodeTables.py /media/ssd4/celestin
python3.8 createTemporalNetwork.py /media/ssd4/celestin/Prepared_Data /media/ssd4/celestin
python3.8 Rebuild-Final_Dataset.py /media/ssd4/celestin
python3.8 createNodeAddressInfo.py /media/ssd4/celestin/TEMPORAL/bis_2_network_2009_2021_name /media/ssd4/celestin/NODE_TABLE /media/ssd4/bitcoinRemy/2022_df_parts/address_info
python3.8 finalclean.py /media/ssd4/celestin/TEMPORAL/bis_2_network_2009_2021_name
python3.8 createSnapshotNetwork.py /media/ssd4/celestin
