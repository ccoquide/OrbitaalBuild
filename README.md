# OrbitaalBuild

This repository contains python scripts and bash scripts used in the generation of the ORBITAAL dataset [https://zenodo.org/records/12581515]. Interesting users can construct a small version of this dataset considering the period from 3rd of January 2009 to the 9th of June 2010 using the two bash scripts given in this repo `./raw_data/build.sh` and `./build.sh`.

### Operating System and Python Requirements
Linux system is required to execute bash scripts
For other OS, follow instructions given in bash scripts.

Python version: 3.8

Required package:
* pyspark: 3.5.0
* numpy: 1.24.4
* pandas: 2.0.3
* pytz: 2024.1



### Generating raw data containing all transactions' outputs information

The first step is to construct a DataFrame considering all Bitcoin transactions' outputs information from raw json. The bash builder script located at `./raw_data/build.sh` construct this DataFrame and other informative dictionary from the original json covering the 6000 first blocks (`./raw_data/data/sources/original_json`)

### Constructing all Orbitaal temporal graphs and nodes information tables

Stream graph and snashots composing the Orbitaal dataset is built from the transactions' outputs dataframe generated with the previous step. Users have to execputing the builder located at `./build.sh` to automatically construct the Orbitaal dataset based on the 6000 first blocks.


