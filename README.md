# Reading LZ4 Compressed pandas Dataframes Saved as HDF to Parquet

This repository contains a Saturn Cloud job which can be run to convert a series lz4 compressed folders of hdf saved pandas files to parquet files and example scripts for reading these files into a Jupyter Server. The Job process involves downloading the .tar.lz4 files from s3, extracting them to /dev/shm (RAM), reading each of the individual hdf files, compiling them to a dask dataframe, and saving the resulting dataframe as parquet files on s3.

## Example Files on S3
The saturn-public-data S3 bucket contains ten tar.lz4 files of various sizes totaling aproimatly 350 GB. In total, the files contain 1,000,000 small pandas dataframes saved as hdf5 files. 

## Output Files from the Job
The output files consist of 1,000 parquet sub-files saved in S3. These files are the appended 1,000,000 small pandas files contained in the tar.lz4 files.

## Example Read Script
This script launches a Jupiter Server with Dask cluster to read the parquet files using Dask, select a subset, and send the resulting dataframe to the main Jupyter Server instance. The smaller files could then be saved for use or used in the Jupiter Server for analysis/training.

## Things to Keep in Mind
The resource required for the Job requires approximatly 2.5 times the file size in RAM in order to extract and save the files. This means that for the examples here (~350 GB of files), you must use a x116xlarge resource (978 GB RAM). 

The Job process takes approximatly 2 hours for the example files. Most of this time is spent with the download and upload of the files, which is limited by the bandwith provided by AWS between instances and S3.

The resulting parquet files are approximatly 350 GB in size. They could be smaller if a different compression algorithm is used (e.g., gzip), but there is a tradeoff with speed of read and write.

The example script takes approximatly 10 minutes to download the resulting parquet files. This is due to each individual Dask worker accessing the files at the bandwidth limits from instance to S3. More worker instances could result in faster download times, but there are diminishing returns.