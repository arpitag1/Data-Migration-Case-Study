import pandas as pd
import psycopg2
from psycopg2 import sql

# === Configuration ===
CSV_FILE_PATH = "data/clothing.csv"        # Path to your CSV file
TABLE_NAME = "clothing"           # Name of the table to create
DB_CONFIG = {
    "host": "localhost",
    "port": "5433",
    "dbname": "postgres",
    "user": "postgres",
    "password": <your_password>>
}

# === Step 1: Read CSV into DataFrame ===
df = pd.read_csv(CSV_FILE_PATH)
print(f"CSV loaded: {len(df)} rows, {len(df.columns)} columns.")

# === Step 2: Connect to PostgreSQL ===
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# === Step 3: Create table dynamically ===
# Generate SQL column definitions
columns = []
for col in df.columns:
    columns.append(sql.Identifier(col))

column_defs = [sql.SQL("{} TEXT").format(col) for col in columns]

create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({});").format(
    sql.Identifier(TABLE_NAME),
    sql.SQL(", ").join(column_defs)
)

cur.execute(create_table_query)
conn.commit()
print(f"Table '{TABLE_NAME}' created (if not exists).")

# === Step 4: Insert data into table ===
insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
    sql.Identifier(TABLE_NAME),
    sql.SQL(", ").join(columns),
    sql.SQL(", ").join(sql.Placeholder() * len(columns))
)

for row in df.itertuples(index=False, name=None):
    cur.execute(insert_query, row)

conn.commit()
print(f"Inserted {len(df)} rows into '{TABLE_NAME}'.")

# === Step 5: Cleanup ===
cur.close()
conn.close()
print("Done.")
