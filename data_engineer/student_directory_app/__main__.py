import dask.dataframe as dd
from studentsapp.studentsapp import create_student_directory

teachers_data = "data/teachers.parquet"
students_data = "data/students.csv"
teachers_df = dd.read_parquet(teachers_data, columns=["fname", "lname", "cid"])
students_df = dd.read_csv(students_data, sep="_").head(5)
output_path = "data/students.json"

create_student_directory(students_df, teachers_df, output_path)
