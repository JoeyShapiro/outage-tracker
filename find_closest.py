#!/usr/bin/env python3
"""Find the outage locations closest to a given lat/lng."""
import argparse
import json

import numpy as np

EARTH_RADIUS_MI = 3958.8


def haversine(lat1, lng1, lat2, lng2):
    lat1_r, lng1_r = np.radians(lat1), np.radians(lng1)
    lat2_r, lng2_r = np.radians(lat2), np.radians(lng2)

    dlat = lat2_r - lat1_r
    dlng = lng2_r - lng1_r

    a = np.sin(dlat / 2) ** 2 + np.cos(lat1_r) * np.cos(lat2_r) * np.sin(dlng / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
    return EARTH_RADIUS_MI * c


def main(path, target_lat, target_lng, top_n):
    with open(path) as f:
        data = json.load(f)

    outages = data["outageList"]
    lat = np.array([o["lat"] for o in outages])
    lng = np.array([o["lng"] for o in outages])

    total_affected = sum(o["affected"] for o in outages)
    print(f"Total outages: {len(outages)}")
    print(f"Total affected: {total_affected}\n")

    dist = haversine(target_lat, target_lng, lat, lng)
    order = np.argsort(dist)[:top_n]

    print(f"Top {top_n} outage locations closest to ({target_lat}, {target_lng}):\n")
    for rank, i in enumerate(order, 1):
        o = outages[i]
        print(f"{rank}. {dist[i]:.3f} mi - {o['city']} ({o['lat']}, {o['lng']}) zip {o['zip']}")
        print(f"   status: {o['status']}, cause: {o['cause']}, affected: {o['affected']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lat", type=float, help="Target latitude")
    parser.add_argument("lng", type=float, help="Target longitude")
    parser.add_argument("-n", "--top-n", type=int, default=5, help="Number of results (default 5)")
    parser.add_argument("-f", "--file", default="outages.json", help="Path to outages.json")
    args = parser.parse_args()

    main(args.file, args.lat, args.lng, args.top_n)
