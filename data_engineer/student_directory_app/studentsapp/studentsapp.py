#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Wilfred Morgan
# version ='1.0'
# license = 'MIT'
# ---------------------------------------------------------------------------
"""Generates a student directory to JSON.

This module reads parses student and teacher source files
to generate a JSON file.

"""

from .helpers import output_json_file, clean_up_file


def create_student_directory(students_df, teachers_df, output_path):
    """Create student directory data in json file format"""
    clean_up_file(output_path)  # Prevent appending duplicate records

    students_df.apply(
        lambda student: add_student_json_record(
            student, teachers_df, output_path
        ),
        axis=1,
    )


def add_student_json_record(student, teachers_df, output_path):
    """Append JSON file with student record"""
    teacher = select_matching_record(teachers_df, student, "cid")
    student_record = create_student_record(student, teacher)

    output_json_file(student_record, output_path)


def create_student_record(student, teacher):
    student_record = {
        "id": student["id"],
        "firstName": student["fname"],
        "lastName": student["lname"],
        "email": student["email"],
        "ssn": student["ssn"],
        "address": student["address"],
        "classId": student["cid"],
        "teacher": {
            "firstName": teacher["fname"].values[0],
            "lastName": teacher["lname"].values[0],
        },
    }

    return student_record


def select_matching_record(df, linked_row, matching_col):
    """Return dataframe row based on matching column name"""
    return df[df[matching_col] == linked_row[matching_col]].compute()
