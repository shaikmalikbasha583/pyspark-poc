import time
import os
import sqlite3

import pandas as pd

source_file_path = os.path.join(os.getcwd(), "data", "survey-bands-full.csv")

print(source_file_path)
conn = sqlite3.connect("database1.sqlite3")

df = pd.DataFrame()
try:
    with open(source_file_path, mode="r+", encoding="utf=8") as f:
        df = pd.read_csv(f, encoding="utf-8", sep=",")
except Exception as e:
    print(e)

print(df.head())
if len(df) == 0:
    print("No data")
    exit(400)

start = time.perf_counter()
print("Inserting...")
df.to_sql("bands", con=conn, if_exists="replace", index=False, method=None)
time_taken = time.perf_counter() - start
print(f"Done in {round(time_taken, 2)} second(s)")
