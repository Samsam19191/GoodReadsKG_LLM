import csv

with open("processed_data/cleaned_ratings.csv", "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    seen_ratings = set()
    for row in reader:
        if row["Rating"] not in seen_ratings:
            seen_ratings.add(row["Rating"])
    print(seen_ratings)
