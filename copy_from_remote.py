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
                COPY (SELECT * FROM {schema_table})
                TO '{output_path}' (FORMAT PARQUET)
                ;
                """)
                print(f"Exporting {schema_table} to {output_path}")
            

    end = time.time()
    time_delta = end - start
    print(f"Finished retrieving files in {time_delta} seconds. Created: {created_count} - Skipped: {skipped_count}")

