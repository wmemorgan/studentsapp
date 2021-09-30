from os.path import isfile, getsize
import pandas as pd
import json

teachers = pd.read_parquet("teachers.parquet", engine="auto")
students = pd.read_csv("students.csv", "_", nrows=5)


for index, student in students.iterrows():
    teacher = teachers.loc[teachers["cid"] == student["cid"]]
    student = {
        "firstName": student["fname"],
        "lastName": student["lname"],
        "classId": student["cid"],
        "teacher": {
            "firstName": teacher["fname"].values[0],
            "lastName": teacher["lname"].values[0],
        },
    }

    if not isfile("data.json") or getsize("data.json") == 0:
        with open("data.json", "w") as f:
            f.write(json.dumps([student]))
    else:
        with open("data.json", "r+") as json_file:
            data = json.load(json_file)

            data.append(student)
            with open("data.json", "w") as f:
                f.write(json.dumps(data))
