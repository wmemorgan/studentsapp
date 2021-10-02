from studentsapp.studentsapp import create_student_directory
from studentsapp.helpers import transform_input_files


def main():
    students_df, teachers_df = transform_input_files(
        "data/students.csv",
        "data/teachers.parquet",
        sep="_",
        columns=["fname", "lname", "cid"],
    )

    create_student_directory(students_df, teachers_df, "data/students.json")


if __name__ == "__main__":
    main()
