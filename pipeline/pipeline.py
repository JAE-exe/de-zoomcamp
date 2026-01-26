import sys

import pandas as pd

print("Pipeline script is running.")
print("arguments", sys.argv)

if len(sys.argv) < 2:
    print("ERROR: Month argument missing")
    print("Usage: python pipeline.py <month>")
    sys.exit(1)

pd.DataFrame()
month = int(sys.argv[1])
print(f"Processing data for month: {month}")

import pandas as pd

df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")