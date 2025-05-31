import argparse
import duckdb
import os
import time

DUCKDB_FILE_NAME = "bc_remote.db"
FILE_PATH = f"/Users/bradybates/Desktop/personal_projects/LOCAL_BASEBALL_COMPUTER_DATASET/data"
DUCKDB_FILE_PATH = f"{FILE_PATH}/{DUCKDB_FILE_NAME}"
OUTPUT_ROOT = f"{FILE_PATH}/parquet_exports"

with duckdb.connect(DUCKDB_FILE_PATH) as con:
    start = time.time()
    skipped_count = 0
    created_count = 0
    target_schemas = con.execute("""
    SELECT DISTINCT schema_name
    FROM information_schema.schemata
    WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
    ;
    """).fetchdf()['schema_name'].tolist()

    for schema in target_schemas:
        print(f"SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema = '{schema}';")
        table_names = con.execute(f"""
        SELECT DISTINCT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
def export_remote_bc_files_to_parquet(args):
    bc_remote_db_path = args.bc_remote_db_path
    with duckdb.connect(bc_remote_db_path) as con:
        output_root = args.output_root
        debug = args.debug

        start = time.time()
        skipped_count = 0
        created_count = 0

        target_schemas = con.execute("""
        SELECT DISTINCT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
        ;
        """).fetchdf()['table_name'].tolist()
        
        for table in table_names:
            schema_table = f"{schema}.{table}"
            output_dir = os.path.join(OUTPUT_ROOT, schema)
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, schema_table) + ".parquet"

            if os.path.exists(f"{output_path}"):
                skipped_count+=1
                print(f"{output_path} exists - skipping")
            else:
                added_count+=1
                con.execute(f"""
    print(f"Finished retrieving files in {time_delta} seconds. Created: {created_count} - Skipped: {skipped_count}")
        """).fetchdf()['schema_name'].tolist()

        for schema in target_schemas:
            table_names = con.execute(f"""
            SELECT DISTINCT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            """).fetchdf()['table_name'].tolist()

            for table in table_names:
                output_path = os.path.join(output_root, schema)
                output_file = output_path + f"{table}.parquet"
                debug and print(f"{schema}.{table} file already exists - skipping") #\nPath: {output_file}")

                if os.path.exists(output_path):
                    skipped_count+=1
                else:
                    created_count+=1
                    os.makedirs(output_path, exist_ok=True)
                    con.execute(f"""
                    COPY (SELECT * FROM {schema}.{table})
                    TO '{output_path}' (FORMAT PARQUET)
                    ;
                    """)
                    print(f"Exporting {schema}.{table} to {output_path}")

        end = time.time()
        time_delta = end - start


parser = argparse.ArgumentParser(description="Exports all existing schemas and tables from baseball.computer's 'bc_remote.db' to Parquet files.")
parser.add_argument("bc_remote_db_path", nargs='?', default=None, help="Path to your 'bc_remote.db' database file from the baseball.computer repo")
parser.add_argument("output_root", help="Top level directory to save parquet files")
parser.add_argument('-d', '--debug', default=None, action='store_true', help='Enable debug mode')
args = parser.parse_args()

export_remote_bc_files_to_parquet(args)

