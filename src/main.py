import csv
import json
import sqlite3
import pandas as pd
import os

csv_path = "results.csv"
range_csv_path = "range_results.csv"

df = pd.read_csv(csv_path)
df.columns = df.columns.str.strip()


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

columns_to_remove = ["subset", "solver_time"]
for col in columns_to_remove:
    if col in df.columns:
        df = df.drop(columns=[col])
        print(f"Removed column: {col}")

print("Final columns after cleanup:", df.columns.tolist())

df.to_csv(csv_path, index=False)
print(f"Updated CSV file saved: {csv_path}")

for col in ["pump_combination", "flow", "prevalence", "power", "target_k"]:
    if col in df.columns and col != "pump_combination":
        df[col] = pd.to_numeric(df[col], errors="coerce")

conn = sqlite3.connect(db_path)
df.to_sql("results", conn, if_exists="replace", index=False)
print(f"Successfully imported {len(df)} rows into {db_path}")

columns_to_index = ["pump_combination", "target_k", "prevalence", "power", "flow"]
cursor = conn.cursor()
for col in columns_to_index:
    if col in df.columns:
        index_name = f"idx_results_{col}"
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON results ({col});")
        print(f"Index created for column: {col}")

conn.commit()

if os.path.exists(range_csv_path):
    print(f"\nProcessing {range_csv_path}...")

    range_df = pd.read_csv(range_csv_path)
    range_df.columns = range_df.columns.str.strip()

    print(f"Read {len(range_df)} rows from {range_csv_path}")
    range_rows = []

    for idx, row in range_df.iterrows():
        combo_str = row.get('combo', '{}')
        try:
            combo_dict = json.loads(combo_str)
            for active_inverters, k_range in combo_dict.items():
                range_rows.append({
                    'active_inverters': int(active_inverters),
                    'min_k': k_range.get('min_target_k'),
                    'max_k': k_range.get('max_target_k')
                })
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error parsing row {idx}: {e}")
            continue

    if range_rows:
        range_results_df = pd.DataFrame(range_rows)
        range_results_df['active_inverters'] = pd.to_numeric(range_results_df['active_inverters'], errors='coerce')
        range_results_df['min_k'] = pd.to_numeric(range_results_df['min_k'], errors='coerce')
        range_results_df['max_k'] = pd.to_numeric(range_results_df['max_k'], errors='coerce')
        range_results_df.to_sql("range_results", conn, if_exists="replace", index=False)
        print(f"Successfully imported {len(range_results_df)} rows into range_results table")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_range_results_active_inverters ON range_results (active_inverters);")
        print("Index created for column: active_inverters")
        conn.commit()
    else:
        print("No valid data found in range_results.csv")
else:
    print(f"\n{range_csv_path} not found, skipping range_results table creation")

conn.close()
print("\nAll indexes created and database closed.")
