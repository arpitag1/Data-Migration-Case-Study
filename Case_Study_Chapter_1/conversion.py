import psycopg2
from pymongo import MongoClient
import json

# PostgreSQL connection
pg_conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password=<your_password>,
    host="localhost",
    port="5433"
)
pg_cursor = pg_conn.cursor()

# MongoDB connection
mongo_client =  MongoClient("mongodb://localhost:27017")
mongo_db = mongo_client["local"]
collection_name = "clothing"

# Query data from PostgreSQL
pg_cursor.execute("SELECT * FROM clothing;")
columns = [desc[0] for desc in pg_cursor.description]
rows = pg_cursor.fetchall()

# Transform data (convert tuples → dicts)
data = [dict(zip(columns, row)) for row in rows]

# Create MongoDB collection automatically if it doesn’t exist
if collection_name not in mongo_db.list_collection_names():
    mongo_db.create_collection(collection_name)

# Insert or update data
mongo_collection = mongo_db[collection_name]
mongo_collection.delete_many({})  # optional, clears old data
mongo_collection.insert_many(data)

print(f"✅ Migrated {len(data)} records from PostgreSQL to MongoDB.")

# Close connections
pg_cursor.close()
pg_conn.close()
mongo_client.close()
