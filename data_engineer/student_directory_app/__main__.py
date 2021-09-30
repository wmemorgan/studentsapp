import pandas as pd

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

    print(student)
