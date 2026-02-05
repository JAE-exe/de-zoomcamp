import sys
import pandas as pd

if len(sys.argv) < 2:
    print("Usage: python pipeline.py <day>")
    sys.exit(1)

day = sys.argv[1]

df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

df.to_parquet(f"output_day_{day}.parquet")
