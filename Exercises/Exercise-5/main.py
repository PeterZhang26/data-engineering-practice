import psycopg2
from pathlib import Path
import csv

def create_tables(cur):
    """
    Reads schema.sql and executes the CREATE TABLE statements
    """
    with open("schema.sql", "r") as f:
        sql_commands = f.read()
    cur.execute(sql_commands) # Execute Data Definition Language (DDL) commands
    print("✅ Tables created successfully")

def insert_data_from_csv(cur, conn, csv_file, table_name):
    """
    Inserts data from a CSV file into the specified Postgres table.
    Uses COPPY FROM STDIN for fast bulk loading.
    """
    print(f"📥 Inserting data from {csv_file} into {table_name}...")

    with open(csv_file, "r") as f:
        next(f) # Skip the header row
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)

    conn.commit()
    print(f"✅ Data inserted into {table_name}.")
        

def main():
    """
    1. Read all CSV files into 'data/'.
    2. Connect to the Postgres database.
    3. Execute schema.sql to create tables.
    4. Insert data from CSVs into corresponding tables.
    """

    # Database connection parameters
    host = "postgres" # This parameter works inside Docker
    database = "postgres"
    user = "postgres"
    pas = "postgres"

    # Step 1: Connect to the PostgresSQL database
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    # your code here
    cur = conn.cursor()
    print("✅ Connected to PostgreSQL database")

    # Step 2: Execute schema.sql to create tables
    create_tables(cur)

    # Step 3: Process all CSV files in 'data/'
    csv_files = list(Path("data/").glob("*.csv"))
    print(f"📂 Found {len(csv_files)} CSV files: {[file.name for file in csv_files]}")

    # Map CSV filenames to table names
    table_mapping = {
        "accounts.csv": "accounts",
        "products.csv": "products",
        "transactions.csv": "transactions",
    }

    for csv_file in csv_files:
        table_name = table_mapping.get(csv_file.name)
        if table_name:
            insert_data_from_csv(cur, conn, csv_file, table_name)
        else:
            print(f"❌ No table mapping found for {csv_file.name}, skipping.")

    # Step 4: Close the connection
    cur.close()
    conn.close()
    print("✅ Connection to PostgreSQL closed.")


if __name__ == "__main__":
    main()
