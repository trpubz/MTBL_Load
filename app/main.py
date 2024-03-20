import psycopg2
import pandas as pd
import json
from sqlalchemy import types

from app.src.mtbl_globals import DIR_TRANSFORM


def load_json_to_postgres(directory, table_name, db_name="mtbl_pre_szn", host="localhost", port=5432):
    # Connect to PostgreSQL
    conn = psycopg2.connect(host=host, database=db_name)
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    conn.close()

    # Connect to the newly created (or existing) database
    conn = psycopg2.connect(host=host, database=db_name)
    cursor = conn.cursor()

    # Load JSON files and create DataFrames
    dfs = []
    # open the local directory and load each file
    for file in DIR_TRANSFORM:
        with open(file) as f:
            data = json.load(f)
        df = pd.json_normalize(data)
        dfs.append(df)

    # Create a table in PostgreSQL
    cursor = conn.cursor()

    # Define a mapping between pandas data types and PostgreSQL data types
    dtype_mapping = {
        'object': types.Text,
        'int64': types.BigInteger,
        'float64': types.Float,
        'bool': types.Boolean,
        # Add more mappings as needed
    }

    # Generate the CREATE TABLE query from the DataFrame
    table_name = "your_table"
    columns = []
    for col in df.columns:
        pandas_dtype = df[col].dtypes
        postgres_dtype = dtype_mapping.get(str(pandas_dtype), types.Text)
        columns.append(f"{col} {postgres_dtype.python_type.__name__}")

    columns = ", ".join(columns)
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"

    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into the table
    for df in dfs:
        for _, row in df.iterrows():
            insert_query = f"INSERT INTO {table_name} VALUES ({','.join(['%s'] * len(row))})"
            cursor.execute(insert_query, tuple(row))
        conn.commit()

    # Close the connection
    cursor.close()
    conn.close()


if __name__ == "__main__":
    load_json_to_postgres(DIR_TRANSFORM, "your_table")
