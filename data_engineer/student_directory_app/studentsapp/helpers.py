from os.path import isfile, getsize
import dask.dataframe as dd
import json


def transform_input_files(students_file, teachers_files, columns=None):
    """Convert source files to dataframes"""
    students_df = dd.read_csv(students_file, sep="_").head(5)
    teachers_df = dd.read_parquet(teachers_files, columns=columns)

    return (students_df, teachers_df)


def create_json_file(record, output_path):
    """Create or append record to a json file"""

    if not isfile(output_path) or getsize(output_path) == 0:
        with open(output_path, "w") as f:
            f.write(json.dumps([record]))
    else:
        with open(output_path, "r+") as json_file:
            data = json.load(json_file)

            data.append(record)
            with open(output_path, "w") as f:
                f.write(json.dumps(data))
