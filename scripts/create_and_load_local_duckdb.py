import argparse
import os
import duckdb


def create_local_duckdb_and_load_data(args):
    local_db_path = args.local_db_path
    data_root_dir = args.data_root_dir
    debug = args.debug

    with duckdb.connect(local_db_path) as con:
        schema_folders = os.listdir(data_root_dir)
        for schema in schema_folders:
            schema_folder_path = os.path.join(data_root_dir, schema)
            if not os.path.isdir(schema_folder_path):
                f"Unknown schema folder '{schema_folder_path}' or is not a directory, skipping"
            else:
                for table_file_name in os.listdir(schema_folder_path):
                    table_path = os.path.join(schema_folder_path, table_file_name)
                    table, _  = os.path.splitext(os.path.basename(table_path))
                    if not os.path.isfile(table_path) or not table_path.endswith('.parquet'):
                        print(f"Unknown table file '{table_path}' or is not a parquet file, skipping")
                    else:
                        print(f"Creating {schema}.{table} - table file path {table_path}")
                        con.sql(f"""
                        CREATE SCHEMA IF NOT EXISTS {schema};
                        CREATE TABLE {schema}.{table} AS
                        SELECT * FROM '{table_path}'
                        ;
                        """)


parser = argparse.ArgumentParser(description="Creates a local DuckDB database if one at the path doesn't exist and loads data from a set of parquet files generated with a script.")
parser.add_argument("local_db_path", nargs='?', default=os.getcwd() + "/local.db", help="Path to your local database file or where you want to create it")
parser.add_argument("data_root_dir", nargs='?', help="Path to the root directory containing the data files to load into the DuckDB database, in parquet format", default="/Users/bradybates/Desktop/personal_projects/LOCAL_BASEBALL_COMPUTER_DATASET/data/parquet_exports")
parser.add_argument('-d', '--debug', default=None, action='store_true', help='Enable debug mode')
args = parser.parse_args()

create_local_duckdb_and_load_data(args)
