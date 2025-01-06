<p align="left" style="margin-bottom: -200px;">
  <img src="logo.png" alt="Logo" width="200">
</p>

# OrbitaalBuild

This repository contains Python and Bash scripts used for generating the ORBITAAL dataset, available at [Zenodo](https://zenodo.org/records/12581515). Interested users can construct a smaller version of this dataset, covering the period from January 3, 2009, to June 9, 2010, using the two Bash scripts provided: `./raw_data/build.sh` and `./build.sh`.

## Operating System and Python Requirements

- **Operating System**: Linux is required to execute the Bash scripts. For other operating systems, follow the instructions provided in the Bash scripts.
- **Python Version**: 3.8
- **Required Python Packages**:
  - `pyspark`: 3.5.0
  - `numpy`: 1.24.4
  - `pandas`: 2.0.3
  - `pytz`: 2024.1

## Generating Raw Data with Transaction Outputs Information

The first step involves constructing a DataFrame that includes all Bitcoin transaction outputs from raw JSON files. The Bash script located at `./raw_data/build.sh` generates this DataFrame and additional dictionaries from the original JSON files located in `./raw_data/data/sources/original_json`. This process covers the first 6000 blocks.

All Python scripts and their associated data for this step are located in the `./raw_data` folder.

## Constructing Orbitaal Temporal Graphs and Node Information Tables

The ORBITAAL dataset, including stream graphs and snapshots, is generated from the transaction outputs DataFrame created in the previous step. To build the dataset automatically, execute the script located at `./build.sh`. This process uses the first 6000 blocks of data.

- **Python Scripts**: Located in the repository root.
- **Dataset Outputs**: The `./DataSet` folder contains the Orbitaal dataset DataFrames in `parquet` format.

---

For more details on the ORBITAAL dataset and its applications, refer to the [Zenodo record](https://zenodo.org/records/12581515).
