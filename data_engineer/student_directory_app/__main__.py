from os.path import isfile, getsize
import dask.dataframe as dd
import json

teachers = dd.read_parquet("teachers.parquet", columns=["fname", "lname", "cid"])
students = dd.read_csv("students.csv", sep="_")


for student in students.iterrows():
    student = student[1]
    teacher = teachers[teachers["cid"] == student["cid"]].compute()

    student_record = {
        "firstName": student["fname"],
        "lastName": student["lname"],
        "classId": student["cid"],
        "teacher": {
            "firstName": teacher["fname"].values[0],
            "lastName": teacher["lname"].values[0],
        },
    }

    print(student_record)

    if not isfile("data.json") or getsize("data.json") == 0:
        with open("data.json", "w") as f:
            f.write(json.dumps([student_record]))
    else:
        with open("data.json", "r+") as json_file:
            data = json.load(json_file)

            data.append(student_record)
            with open("data.json", "w") as f:
                f.write(json.dumps(data))
