import csv
import json
import os

os.makedirs("reviews_json", exist_ok=True)

with open("seeds/reviews_raw.csv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        row["rating"] = int(row["rating"])
        with open(f"reviews_json/review_{row['id']}.json", "w", encoding="utf-8") as out:
            json.dump(row, out, indent=2)

print("Done. Check the 'reviews_json' folder.")