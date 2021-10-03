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

from .helpers import output_json_file, verify_file_delete, delete_file


def create_student_directory(students_df, teachers_df, output_path):
    """Create student directory data in json file format"""
    verify_file_delete(output_path)  # Prevent appending duplicate records

    for student in students_df.iterrows():
        try:
            add_student_json_record(student[1], teachers_df, output_path)
        except ValueError:
            print("Error adding student record.")

    print(f"Student directory created at: {output_path}")


def add_student_json_record(student, teachers_df, output_path):
    """Append JSON file with student record"""
    teacher = select_matching_record(teachers_df, student, "cid")
    student_record = create_student_record(student, teacher)

    output_json_file(student_record, output_path)


def create_student_record(student, teacher):
    """Create and return a student record object"""
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


def select_matching_record(dataframe, linked_row, matching_col):
    """Return dataframe row based on matching column name"""
    return dataframe[
        dataframe[matching_col] == linked_row[matching_col]
    ].compute()
