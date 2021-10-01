from os.path import isfile, getsize
import json


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
