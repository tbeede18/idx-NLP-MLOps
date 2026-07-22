import pandas as pd

# Load the file
df = pd.read_parquet('/Users/23tylerb/Downloads/part-00000-57115f18-d271-4c13-b751-f4d2b825063b-c000.snappy.parquet')

total_rows = len(df)
print(f"--- THE TRUTH ---")
print(f"Total Rows: {total_rows}")

# 2. Check for True Nulls (NaN / None)
true_nulls = df['DaysOnMarket'].isnull().sum()
print(f"True Nulls (NaN): {true_nulls}")

# 3. Check for Empty Strings
empty_strings = (df['DaysOnMarket'] == "").sum()
print(f"Empty Strings (''): {empty_strings}")

# 4. Check for spaces
blank_spaces = (df['DaysOnMarket'] == " ").sum()
print(f"Blank Spaces (' '): {blank_spaces}")

# 5. The value_counts sum check
total_counted = df['DaysOnMarket'].value_counts().sum()
print(f"\nDoes value_counts total ({total_counted}) equal Total Rows ({total_rows})?")
print(total_counted == total_rows)