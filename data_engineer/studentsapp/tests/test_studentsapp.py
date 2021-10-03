#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Wilfred Morgan
# version ='1.0'
# license = 'MIT'
# ---------------------------------------------------------------------------
"""
StudentsApp test suite.

"""

import os
import json
import dask.dataframe as dd
from studentsapp import studentsapp


def test_select_matching_record():
    """
    Verify the function assigns the correct teacher
    to the student.
    """
    students_df = dd.read_csv(
        "tests/data/students.csv", sep="_", sample_rows=1
    )
    teachers_df = dd.read_parquet(
        "tests/data/teachers.parquet", ["fname", "lname", "cid"]
    )
    student = list(students_df.iterrows())[0][1]

    actual = studentsapp.select_matching_record(teachers_df, student, "cid")

    assert actual["fname"].values[0] == "Jessa"
    assert actual["lname"].values[0] == "Gibbs"
    assert actual["cid"].values[0] == "08-2046381"


def test_create_student_record():
    """Verify the function creates a valid student record."""
    students_df = dd.read_csv(
        "tests/data/students.csv", sep="_", sample_rows=1
    )
    teachers_df = dd.read_parquet(
        "tests/data/teachers.parquet", ["fname", "lname", "cid"]
    )
    student = list(students_df.iterrows())[0][1]
    teacher = teachers_df[teachers_df["cid"] == student["cid"]].compute()

    actual = studentsapp.create_student_record(student, teacher)

    assert actual["firstName"] == "Dniren"
    assert actual["teacher"]["lastName"] == "Gibbs"


def test_create_student_directory():
    """Test the entire student directory creation workflow."""
    output_path = "tests/data/students.json"
    if os.path.exists(output_path):
        os.remove(output_path)

    students_df = dd.read_csv("tests/data/students.csv", sep="_").head(5)
    teachers_df = dd.read_parquet(
        "tests/data/teachers.parquet", ["fname", "lname", "cid"]
    )

    studentsapp.create_student_directory(students_df, teachers_df, output_path)

    assert os.path.exists(output_path)
    with open(output_path) as json_file:
        actual_file = json.load(json_file)
        assert len(actual_file) == 5

    os.remove(output_path)
