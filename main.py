import argparse
import time
import csv
from typing import Tuple
import psycopg2

TABLE_NAME = "reviews"
DATABASE_NAME = "imdb_review_kaggle"

# Connect to an existing database
conn = psycopg2.connect(f"dbname={DATABASE_NAME} user=postgres")


def load_csv_data() -> list[Tuple]:
    """
    Load CSV data.
    """
    reviews: list[Tuple] = []
    with open("imdb_master.csv", mode="r", encoding="ISO-8859-1") as csv_file:
        rows = csv.DictReader(csv_file)
        for row in rows:
            reviews.append(
                (
                    row[""],
                    row["type"],
                    row["review"],
                    row["label"],
                    row["file"],
                )
            )
    return reviews


def update_sql_query() -> None:
    """
    Execute a SQL statement to update all records.
    """
    cur = conn.cursor()
    sql = f"UPDATE {TABLE_NAME} SET type = 'test2'"
    start_time = time.time()
    cur.execute(sql)
    conn.commit()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
    cur.close()


def update_sql_query_in_batches() -> None:
    """
    Update all records using fetchmany.
    """
    cur = conn.cursor()
    start_time = time.time()

    batch_size = 100
    offset = 0

    # Fetch and process results in batches
    while True:
        cur.execute(
            f"SELECT id, type FROM {TABLE_NAME} ORDER BY id ASC LIMIT %s OFFSET %s",
            (batch_size, offset),
        )
        rows = cur.fetchall()

        if not rows:
            # No more rows, break out of loop.
            end_time = time.time()
            break

        for row in rows:
            # Process each row here.
            id, _ = row
            cur.execute(f"UPDATE {TABLE_NAME} SET type = 'test1' WHERE id = %s", (id,))

        conn.commit()
        offset += batch_size

    elapsed_time = end_time - start_time
    # Elapsed time: 159.76 seconds
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
    cur.close()


def delete_all_records() -> None:
    """
    Delete all existing records from the specified table.
    """
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {TABLE_NAME}")
    conn.commit()
    cur.close()


def insert_csv_data_to_table(imdb_reviews: list[Tuple]) -> None:
    """
    Inserts data from a CSV file to a specified table.
    """
    cur = conn.cursor()
    cur.execute(
        f"SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = '{TABLE_NAME}');"
    )
    (does_table_exists,) = cur.fetchone()

    if not does_table_exists:
        cur.execute(
            f"CREATE TABLE {TABLE_NAME} (id serial PRIMARY KEY, row_number integer, type varchar, review varchar, label varchar, file varchar);"
        )
        conn.commit()

    # Check if the table is empty
    cur.execute(f"SELECT COUNT(row_number) FROM {TABLE_NAME}")
    (has_records,) = cur.fetchone()

    if has_records:
        print("Data was populated, exiting!")
        return None

    # execute the statement with the data
    sql = f"INSERT INTO {TABLE_NAME} (row_number, type, review, label, file) VALUES (%s, %s, %s, %s, %s)"
    cur.executemany(sql, imdb_reviews)

    # commit the transaction
    conn.commit()

    # close the cursor
    cur.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Something here...")
    parser.add_argument(
        "--clean_table",
        action="store_true",
        help="Delete existing records from the TABLE_NAME.",
    )
    args = parser.parse_args()

    # Get CSV data
    csv_data = load_csv_data()

    if args.clean_table:
        # Delete existing records
        delete_all_records()

    # Insert CSV data to table
    insert_csv_data_to_table(csv_data)

    # Update records
    update_sql_query_in_batches()

    # Close the connection
    conn.close()
