import argparse
import duckdb
import os
import pathlib
import time

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
        """).fetchdf()['schema_name'].tolist()

        for schema in target_schemas:
            table_names = con.execute(f"""
            SELECT DISTINCT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            ;
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
        print(
            f"\n |- Finished retrieving files in {time_delta} seconds" +
            f"\n |- Files created: {created_count}" +
            f"\n |- Files skipped: {skipped_count}"
        )


def find_bc_remote_db(debug: bool):
    filename = "bc_remote.db"
    cwd = pathlib.Path.cwd()
    search_folders = cwd.rglob(filename)
    for path in search_folders:
        debug and print(f"search_path: {path}")
        if path.exists():
            return str(path)
        return None

parser = argparse.ArgumentParser(description="Exports all existing schemas and tables from baseball.computer's 'bc_remote.db' to Parquet files.")
parser.add_argument("bc_remote_db_path", nargs='?', default=None, help="Path to your 'bc_remote.db' database file from the baseball.computer repo")
parser.add_argument("output_root", help="Top level directory to save parquet files")
parser.add_argument('-d', '--debug', default=None, action='store_true', help='Enable debug mode')
args = parser.parse_args()

if args.bc_remote_db_path is None:
    found_path = find_bc_remote_db(args.debug)
    if found_path is None:
        print("Error: bc_remote.db not found in current or parent directories. Please specify in argument.")
        exit(1)
    else:
        print(f"Found bc_remote.db at {found_path}")
        args.bc_remote_db_path = found_path

export_remote_bc_files_to_parquet(args)

