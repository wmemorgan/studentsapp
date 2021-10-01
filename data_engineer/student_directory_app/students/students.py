import dask.dataframe as dd

teachers_data = "data/teachers.parquet"
students_data = "data/students.csv"
output_path = "data/students.json"
teachers = dd.read_parquet(teachers_data, columns=["fname", "lname", "cid"])
students = dd.read_csv(students_data, sep="_").head(5)


def create_student_directory(students, teachers):
    """Generates student directory"""

    for student in students.iterrows():
        student = student[1]
        teacher = teachers[teachers["cid"] == student["cid"]].compute()

        print(create_student_record(student, teacher))


def create_student_record(student, teacher):
    """Create student information record"""

    student_record = {
        "firstName": student["fname"],
        "lastName": student["lname"],
        "classId": student["cid"],
        "teacher": {
            "firstName": teacher["fname"].values[0],
            "lastName": teacher["lname"].values[0],
        },
    }

    return student_record


create_student_directory(students, teachers)
