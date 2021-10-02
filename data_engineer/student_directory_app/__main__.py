#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Wilfred Morgan
# version ='1.0'
# license = 'MIT'
# ---------------------------------------------------------------------------
"""Generates student directory data.

This package creates a student directory which contains student contact 
information and their class enrollment.

"""

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
