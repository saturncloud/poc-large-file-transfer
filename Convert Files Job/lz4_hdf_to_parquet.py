""" This script takes lz4 compressed folders of pandas dataframes saved as hdf files saved
to s3, extracts them to a ram disk (/dev/shm), reads the hdf file to a central dataframe, 
and saves this dataframe to a specified s3 folder as parquet files."""

import os
import re
import shutil
import subprocess
from multiprocessing import Pool

import boto3
import botocore

import pandas as pd

import dask.dataframe as dd


"""Globals"""
s3 = boto3.resource("s3")
bucket = s3.Bucket(os.environ["AWS_DATA_BUCKET"])


def download_and_extract_lz4():
    """
    Function to download and extract the lz4 folders to ram disk. Uses the subproces
    module with tar and lz4 to extract. As files are extracted, the downloaded files are
    deleted to save ram space
    """

    files = []
    for object_summary in bucket.objects.filter(Prefix=os.environ["AWS_DATA_FOLDER"]):
        files.append(object_summary.key)

    filter_pattern = re.compile(r".*\.tar\.lz4")
    files = [f for f in files if filter_pattern.match(f)]

    if not os.path.exists("/dev/shm/data"):
        os.mkdir("/dev/shm/data")

    for file in files:
        try:
            print(f"Downloading {os.path.basename(file)}.")
            bucket.download_file(file, f"/dev/shm/{os.path.basename(file)}")
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print("The object does not exist.")
            else:
                raise

        file = os.path.basename(file)
        print(f"Extracting {file}.")
        ps = subprocess.Popen(["lz4", "-cd", f"/dev/shm/{file}"], stdout=subprocess.PIPE)
        system = subprocess.check_output(["tar", "xvf", "-", "-C", "/dev/shm/data"], stdin=ps.stdout)
        ps.wait()

        if os.path.exists(f"/dev/shm/{file}"):
            os.remove(f"/dev/shm/{file}")


def read_hdf_file(filename):
    """
    Simple function to use with multiprocessing
    """
    return pd.read_hdf(filename)


def read_hdf_and_save_as_parquet():
    """
    Takes the data files saved in the ram disk and reads them to a central dataframe
    using multiprocessing. This files is then converted to a dask dataframe to allow for
    smaller partitioned parquet files to be saved. These files are simply parititioned by
    row number, but could be partitioned by column (for instance for time series data.)
    """
    data_files = []
    for folder in os.listdir("/dev/shm/data"):
        for file in os.listdir(f"/dev/shm/data/{folder}"):
            data_files.append(f"/dev/shm/data/{folder}/{file}")

    print("Reading hdfs.")
    with Pool() as pool:
        df_list = pool.map(read_hdf_file, data_files)

    if os.path.exists("/dev/shm/data"):
        shutil.rmtree("/dev/shm/data")

    df = pd.concat(df_list, ignore_index=True)

    df.columns = df.columns.astype(str)

    print("Converting to dask dataframe.")
    ddf = dd.from_pandas(df, chunksize=200000)
    del df
    print("Saving to parquet.")
    ddf.to_parquet(os.environ["AWS_SAVE_URI"], overwrite=True)


def main():
    download_and_extract_lz4()
    read_hdf_and_save_as_parquet()


if __name__ == "__main__":
    main()
