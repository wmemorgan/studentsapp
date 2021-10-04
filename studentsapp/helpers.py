#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Wilfred Morgan
# version ='1.0'
# license = 'MIT'
# ---------------------------------------------------------------------------
"""Files utilities to manage input and output data."""

from os import remove
from os.path import isfile, getsize, exists
import sys
import json
import dask.dataframe as dd


def transform_input_files(
    students_file, teachers_files, sep=",", columns=None
):
    """Import student csv and teacher parquet files and return dataframes"""
    students_df = dd.read_csv(students_file, sep=sep)
    teachers_df = dd.read_parquet(teachers_files, columns=columns)

    return (students_df, teachers_df)


def output_json_file(record, output_path):
    """Create or append record to a json file"""
    if not isfile(output_path) or getsize(output_path) == 0:
        with open(output_path, "w") as file_output:
            file_output.write(json.dumps([record]))
    else:
        with open(output_path, "r+") as json_file:
            data = json.load(json_file)

            data.append(record)
            with open(output_path, "w") as file_output:
                file_output.write(json.dumps(data))


def verify_file_delete(output_path):
    """Verify file deletion"""
    if exists(output_path):
        print(
            f"The {output_path} file exists. Do you want to overwrite the file? (Y/n):"
        )
        user_response = input().lower()
        if user_response == "y":
            remove(output_path)
            print("Creating new file...")
        else:
            print("Program exit.")
            sys.exit()
    else:
        print("Creating new file...")
