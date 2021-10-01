def create_student_directory(students, teachers):
    """Generates student directory"""

    for student in students.iterrows():
        student = student[1]
        teacher = teachers[teachers["cid"] == student["cid"]].compute()

        print(_create_student_record(student, teacher))


def _create_student_record(student, teacher):
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
