import os
from unittest.mock import patch

import pytest
import psycopg2
import csv

from main import copy_files_into_raw, HOST, PORT, DATABASE, USER, PASSWORD

TEST_TABLE_NAME = "nonexistent_table"


@patch("main.TABLE_SCHEMAS", {f"{TEST_TABLE_NAME}": "(id INT, name TEXT)"})
def test_copy_files_into_raw_error():
    # Mock data for testing
    file_path = f"{TEST_TABLE_NAME}.csv"
    conn = psycopg2.connect(host=HOST, port=PORT, database=DATABASE, user=USER, password=PASSWORD)
    cur = conn.cursor()
    try:
        # Create a test CSV file
        with open(file_path, "w") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name"])
            writer.writerow([1, "John Doe"])
            writer.writerow([2, "Jane Smith"])

        copy_files_into_raw(TEST_TABLE_NAME, file_path, conn)
        cur.execute(f"SELECT COUNT(*) FROM {TEST_TABLE_NAME}_raw")
        count_table_rows = cur.fetchone()[0]
        assert count_table_rows == 2

    finally:
        # Close the connection and clean up the test file
        cur.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_NAME}_raw;")
        conn.close()
        if file_path:
            os.remove(file_path)

# Run the test function
test_copy_files_into_raw_error()
