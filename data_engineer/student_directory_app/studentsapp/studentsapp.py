from .helpers import create_json_file


def create_student_directory(students, teachers, output_path):
    """Generates student directory"""

    for student in students.iterrows():
        student = student[1]
        teacher = teachers[teachers["cid"] == student["cid"]].compute()

        create_json_file(add_student_record(student, teacher), output_path)


def add_student_record(student, teacher):
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
