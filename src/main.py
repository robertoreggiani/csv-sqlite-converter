import csv
import sqlite3
import pandas as pd
import os

csv_path = "results.csv"
db_path = os.path.join( "database", "results.db")

os.makedirs(os.path.dirname(db_path), exist_ok=True)

rows = []
with open(csv_path, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        rows.append(row)
print(f"Read {len(rows)} rows from CSV file")
df = pd.DataFrame(rows)

df.columns = df.columns.str.strip()
print("Cleaned columns:", df.columns.tolist())

for col in ["target_q", "prevalence", "power", "speed", "q", "pump_power"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

conn = sqlite3.connect(db_path)
df.to_sql("results", conn, if_exists="replace", index=False)
print(f"Successfully imported {len(df)} rows into {db_path}")

# Create indexes on selected columns
columns_to_index = ["target_q", "prevalence", "power", "speed", "q", "pump_power"]
cursor = conn.cursor()
for col in columns_to_index:
    if col in df.columns:
        index_name = f"idx_results_{col}"
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON results ({col});")
        print(f"Index created for column: {col}")

conn.commit()
conn.close()
print("All indexes created and database closed.")
