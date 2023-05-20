import os
import psycopg2
import csv

TABLE_SCHEMAS = {'users': '(rn INTEGER, created_at TIMESTAMP, company_id INTEGER, status VARCHAR(50), demo_user VARCHAR(50))',
                   'booking': '(user_id INTEGER, booking_id INTEGER, created_at VARCHAR(50), status VARCHAR(50), checkin_status VARCHAR(50), booking_start_time VARCHAR(50), booking_end_time VARCHAR(50), is_demo VARCHAR(50))',
                   'company': '(company_id INTEGER, status VARCHAR(50), created_at TIMESTAMP,company_name VARCHAR(50))'}
BOOKING_FILES = {'users': 'users.csv', 'booking': 'booking.csv', 'company': 'company.csv'}
QUERIES_LIST = ['first_query', 'second_query', 'third_query']

# Access environment variables
USER = os.getenv("POSTGRES_USER")
PASSWORD = os.getenv("POSTGRES_PASSWORD")
DATABASE = os.getenv("POSTGRES_DB")
HOST = os.getenv("POSTGRES_HOST")
PORT = os.getenv("POSTGRES_PORT")


def copy_files_into_raw(table_name: str, file_path: str, conn):
    try:
        with open(file_path, "r") as f:
            reader = csv.reader(f)
            next(reader) # skip header row
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}_raw;")
                cur.execute(f"""CREATE TABLE {table_name}_raw {TABLE_SCHEMAS[table_name]};""")
                # Copy the data from the CSV file into the PostgreSQL table
                cur.copy_from(f, f'{table_name}_raw', sep=",", null="")
    except (Exception, psycopg2.Error) as error:
        return print("Error:", error)

    finally:
        conn.commit()
    return print(f"Table {table_name}_raw successfully created and loaded")


def perform_quality_checks(file_path: str, table_name: str, conn):
    try:
        cursor = conn.cursor()
        with open(file_path, "r") as f:
            reader = csv.reader(f)
            count_file_rows = len(list(reader)) - 1

        # Performing quality checks
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}_raw")
        count_table_rows = cursor.fetchone()[0]
        if count_file_rows != count_table_rows:
            print(f"Quantity of rows in file {table_name} are not equal with {table_name}_raw table: "
                  f"{count_file_rows} != {count_table_rows}")
            return False

    except (Exception, psycopg2.Error) as error:
        print("Error retrieving count:", error)

    finally:
        conn.commit()

    return print(f'Checks successfully passed')


def run_queries(sql_file: str, conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {sql_file};")
            cursor.execute(open(f"{sql_file}.sql", "r").read())

    except (Exception, psycopg2.Error) as error:
        print("Error retrieving count:", error)

    finally:
        conn.commit()
    return print(f"SQL file {sql_file} successfully executed")


def run_initial_script():
    conn = psycopg2.connect(host=HOST, port=PORT, database=DATABASE, user=USER, password=PASSWORD)
    conn.autocommit = True
    for table_name, file_path in BOOKING_FILES.items():
        # copy from files into raw tables
        copy_files_into_raw(table_name, file_path, conn)

        # lightweight etl on raw files
        perform_quality_checks(file_path, table_name, conn)

        # lightweight etl on raw files
        run_queries(table_name, conn)

    #running test queries
    for q_name in QUERIES_LIST:
        run_queries(q_name, conn)
    conn.close()
    return print(f"Initial script successfully executed")


if __name__ == '__main__':
    run_initial_script()
