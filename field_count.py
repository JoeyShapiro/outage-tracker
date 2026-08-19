import json
from collections import Counter

with open("outages.json") as f:
    data = json.load(f)

outages = data["outageList"]

counts = Counter()
for outage in outages:
    counts.update(outage.keys())

print(f"total outages: {len(outages)}")
for key, count in counts.most_common():
    print(f"{key}: {count}")
