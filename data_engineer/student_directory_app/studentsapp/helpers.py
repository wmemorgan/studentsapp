from os import remove
from os.path import isfile, getsize, exists
import dask.dataframe as dd
import json


def transform_input_files(
    students_file, teachers_files, sep=",", columns=None
):
    students_df = dd.read_csv(students_file, sep=sep)
    teachers_df = dd.read_parquet(teachers_files, columns=columns)

    return (students_df, teachers_df)


def output_json_file(record, output_path):
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


def clean_up_file(output_path):
    if exists:
        remove(output_path)
