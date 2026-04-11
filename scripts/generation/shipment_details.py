"""
generate_shipment_details.py
----------------------------
Generates wms_shipment_details.csv — one row per shipment leg.

Grain:
  Direct shipment:   1 row  (origin ON_ -> destination DN_)
  Indirect shipment: 2 rows sharing the same shipment_id
    Leg 1: origin ON_ -> destination CH_
    Leg 2: origin CH_ -> destination DN_

Columns:
  shipment_id, truck_id, container_id, order_date,
  origin_node_id, destination_node_id, sort_code

Container type rules (unified compliance model):
  Correct type per destination:
    consolidation_hub  -> MTL (hub processing standard)
    MTL-capable DN     -> MTL
    BOX-only DN        -> BOX

  Correctness rate per lane (from lanes.py):
    Direct lanes  : origin tier  (40-80%)
    Leg1 lanes    : origin tier  (40-80%, same origin same tier)
    Leg2 lanes    : hub tier     (85-95%)

  Container ID format:
    Origin-created : {TYPE}_{truck_id}_{index:03d}
    Hub-created    : {TYPE}_{hub_id}_{date}_{sequence:05d}

  Where TYPE is BOX or MTL depending on compliance draw.

Inbound containers at hub != outbound containers at hub.
Hub re-containerizes — outbound containers get new IDs.

Memory strategy: three streaming passes, quality injection on final pass.
  Pass 1 — leg1 trucks
  Pass 2 — leg2 trucks (reuses shipment_ids from pool)
  Pass 3 — direct trucks

Data quality issues injected:
  - NULL sort_code in ~1% of rows
  - order_date after departure_datetime in ~0.5% of rows
  - Duplicate shipment row in ~0.3% of rows
  - Wrong container ID format in ~0.3% of rows

Output: data/wms_shipment_details.csv
"""

import csv
import json
import os
import random
import sys
from collections import defaultdict
from datetime import datetime, timedelta

random.seed(42)

# ── PATH SETUP ───────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(__file__)
DATA_DIR   = os.path.join(SCRIPT_DIR, "..", "..", "data")
sys.path.insert(0, SCRIPT_DIR)
from lanes import build_network

# ── HELPERS ──────────────────────────────────────────────────────────────────

def classify_lane_type(origin_id, dest_id):
    if origin_id.startswith("ON_") and dest_id.startswith("DN_"):
        return "direct"
    elif origin_id.startswith("ON_") and dest_id.startswith("CH_"):
        return "indirect_leg1"
    elif origin_id.startswith("CH_") and dest_id.startswith("DN_"):
        return "indirect_leg2"
    return "unknown"


def distribute_packages(package_count, container_count):
    if container_count >= package_count:
        return [(i, 1) for i in range(package_count)]
    base      = package_count // container_count
    remainder = package_count % container_count
    counts    = [base] * container_count
    for i in random.sample(range(container_count), remainder):
        counts[i] += 1
    return [(i, c) for i, c in enumerate(counts)]


def make_shipment_id(counter):
    return f"SHP_{counter:09d}"


def correct_container_type(dest_id, capability_map):
    """
    Returns the correct container type for a given destination.
      consolidation_hub -> MTL
      MTL-capable DN    -> MTL
      BOX-only DN       -> BOX
    """
    if dest_id.startswith("CH_"):
        return "MTL"
    cap = capability_map.get(dest_id, "MTL")
    return "MTL" if cap in ("MTL", "ALL") else "BOX"


def draw_container_type(dest_id, lane_key, capability_map, lane_container_rates):
    """
    Draws container type for a single container on this lane.
    Uses lane correctness rate to decide correct vs wrong type.
    """
    correct = correct_container_type(dest_id, capability_map)
    wrong   = "BOX" if correct == "MTL" else "MTL"
    rate    = lane_container_rates.get(lane_key, 0.75)
    return correct if random.random() < rate else wrong


def make_origin_container_id(container_type, truck_id, container_index):
    return f"{container_type}_{truck_id}_{container_index:03d}"


def make_hub_container_id(container_type, hub_id, date_str, sequence):
    date_compact = date_str.replace("-", "")
    return f"{container_type}_{hub_id}_{date_compact}_{sequence:05d}"


# ── BUILD HUB->DN MAP FROM LEG2 TRUCKS ───────────────────────────────────────

def build_hub_dn_map(transport_path):
    hub_to_dns = defaultdict(set)
    with open(transport_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            o, d = row["origin_node_id"], row["destination_node_id"]
            if o.startswith("CH_") and d.startswith("DN_"):
                hub_to_dns[o].add(d)
    return {k: list(v) for k, v in hub_to_dns.items()}


# ── PASS 1: LEG1 ─────────────────────────────────────────────────────────────

def process_leg1(transport_path, hub_to_dns, capability_map,
                 lane_container_rates, output_writer, pool_path, counter_start):
    counter   = counter_start
    pool      = defaultdict(list)
    row_count = 0

    with open(transport_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for truck in reader:
            origin_id = truck["origin_node_id"]
            dest_id   = truck["destination_node_id"]
            if classify_lane_type(origin_id, dest_id) != "indirect_leg1":
                continue

            hub_id        = dest_id
            available_dns = hub_to_dns.get(hub_id, [])
            if not available_dns:
                continue

            try:
                pkg_count = int(truck["package_count"])
                ctr_count = int(truck["container_count"])
            except (ValueError, TypeError):
                continue

            truck_id     = truck["truck_id"]
            dep_date     = datetime.strptime(
                truck["departure_datetime"], "%Y-%m-%d %H:%M:%S"
            ).date()
            order_date   = dep_date - timedelta(days=random.randint(1, 5))
            lane_key     = (origin_id, hub_id)
            distribution = distribute_packages(pkg_count, ctr_count)

            for ctr_idx, count in distribution:
                ctr_type     = draw_container_type(
                    hub_id, lane_key, capability_map, lane_container_rates
                )
                container_id = make_origin_container_id(ctr_type, truck_id, ctr_idx)

                for _ in range(count):
                    shipment_id = make_shipment_id(counter)
                    counter    += 1
                    sort_code   = random.choice(available_dns)

                    output_writer.writerow({
                        "shipment_id":         shipment_id,
                        "truck_id":            truck_id,
                        "container_id":        container_id,
                        "order_date":          str(order_date),
                        "origin_node_id":      origin_id,
                        "destination_node_id": hub_id,
                        "sort_code":           sort_code,
                    })
                    row_count += 1
                    pool[(hub_id, sort_code)].append(shipment_id)

    pool_serialisable = {f"{k[0]}||{k[1]}": v for k, v in pool.items()}
    with open(pool_path, "w") as f:
        json.dump(pool_serialisable, f)

    return counter, row_count


# ── PASS 2: LEG2 ─────────────────────────────────────────────────────────────

def process_leg2(transport_path, pool_path, capability_map,
                 lane_container_rates, output_writer, counter_start):
    counter   = counter_start
    row_count = 0

    with open(pool_path) as f:
        pool_raw = json.load(f)
    pool = {tuple(k.split("||")): v for k, v in pool_raw.items()}

    hub_sequence = defaultdict(int)

    with open(transport_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for truck in reader:
            origin_id = truck["origin_node_id"]
            dest_id   = truck["destination_node_id"]
            if classify_lane_type(origin_id, dest_id) != "indirect_leg2":
                continue

            try:
                pkg_count = int(truck["package_count"])
                ctr_count = int(truck["container_count"])
            except (ValueError, TypeError):
                continue

            hub_id        = origin_id
            dn_id         = dest_id
            truck_id      = truck["truck_id"]
            dep_str       = truck["departure_datetime"]
            dep_date      = datetime.strptime(dep_str, "%Y-%m-%d %H:%M:%S").date()
            order_date    = dep_date - timedelta(days=random.randint(1, 7))
            date_str      = str(dep_date)
            lane_key      = (hub_id, dn_id)
            available_ids = pool.get((hub_id, dn_id), [])
            distribution  = distribute_packages(pkg_count, ctr_count)

            for ctr_idx, count in distribution:
                ctr_type = draw_container_type(
                    dn_id, lane_key, capability_map, lane_container_rates
                )
                hub_seq_key  = (hub_id, date_str)
                hub_sequence[hub_seq_key] += 1
                container_id = make_hub_container_id(
                    ctr_type, hub_id, date_str, hub_sequence[hub_seq_key]
                )

                for _ in range(count):
                    if available_ids:
                        shipment_id = available_ids.pop(0)
                    else:
                        shipment_id = make_shipment_id(counter)
                        counter    += 1

                    output_writer.writerow({
                        "shipment_id":         shipment_id,
                        "truck_id":            truck_id,
                        "container_id":        container_id,
                        "order_date":          str(order_date),
                        "origin_node_id":      hub_id,
                        "destination_node_id": dn_id,
                        "sort_code":           dn_id,
                    })
                    row_count += 1

    return counter, row_count


# ── PASS 3: DIRECT ───────────────────────────────────────────────────────────

def process_direct(transport_path, capability_map,
                   lane_container_rates, output_writer, counter_start):
    counter   = counter_start
    row_count = 0

    with open(transport_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for truck in reader:
            origin_id = truck["origin_node_id"]
            dest_id   = truck["destination_node_id"]
            if classify_lane_type(origin_id, dest_id) != "direct":
                continue

            try:
                pkg_count = int(truck["package_count"])
                ctr_count = int(truck["container_count"])
            except (ValueError, TypeError):
                continue

            truck_id     = truck["truck_id"]
            dep_date     = datetime.strptime(
                truck["departure_datetime"], "%Y-%m-%d %H:%M:%S"
            ).date()
            order_date   = dep_date - timedelta(days=random.randint(1, 5))
            lane_key     = (origin_id, dest_id)
            distribution = distribute_packages(pkg_count, ctr_count)

            for ctr_idx, count in distribution:
                ctr_type     = draw_container_type(
                    dest_id, lane_key, capability_map, lane_container_rates
                )
                container_id = make_origin_container_id(ctr_type, truck_id, ctr_idx)

                for _ in range(count):
                    shipment_id = make_shipment_id(counter)
                    counter    += 1

                    output_writer.writerow({
                        "shipment_id":         shipment_id,
                        "truck_id":            truck_id,
                        "container_id":        container_id,
                        "order_date":          str(order_date),
                        "origin_node_id":      origin_id,
                        "destination_node_id": dest_id,
                        "sort_code":           dest_id,
                    })
                    row_count += 1

    return counter, row_count


# ── QUALITY INJECTION (streaming) ────────────────────────────────────────────

def inject_quality_issues_streaming(input_path, output_path,
                                     total_rows, transport_path):
    null_sort_count  = max(1, round(total_rows * 0.010))
    late_order_count = max(1, round(total_rows * 0.005))
    dup_count        = max(1, round(total_rows * 0.003))
    fmt_count        = max(1, round(total_rows * 0.003))

    truck_departure = {}
    with open(transport_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            truck_departure[row["truck_id"]] = row["departure_datetime"]

    all_indices    = list(range(total_rows))
    null_sort_idx  = set(random.sample(all_indices, null_sort_count))
    remaining      = [i for i in all_indices if i not in null_sort_idx]
    late_order_idx = set(random.sample(remaining, late_order_count))
    remaining      = [i for i in remaining if i not in late_order_idx]
    fmt_idx        = set(random.sample(remaining, fmt_count))
    remaining      = [i for i in remaining if i not in fmt_idx]
    dup_idx        = set(random.sample(remaining, dup_count))

    fieldnames = [
        "shipment_id", "truck_id", "container_id", "order_date",
        "origin_node_id", "destination_node_id", "sort_code",
    ]
    duplicates_buffer = []

    with open(input_path, newline="", encoding="utf-8") as fin, \
         open(output_path, "w", newline="", encoding="utf-8") as fout:

        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        for idx, row in enumerate(reader):
            r = row.copy()

            if idx in null_sort_idx:
                r["sort_code"] = ""

            if idx in late_order_idx:
                dep_str = truck_departure.get(r["truck_id"])
                if dep_str:
                    dep_date = datetime.strptime(
                        dep_str, "%Y-%m-%d %H:%M:%S"
                    ).date()
                    r["order_date"] = str(
                        dep_date + timedelta(days=random.randint(1, 3))
                    )

            if idx in fmt_idx:
                r["container_id"] = r["container_id"].replace(
                    "MTL_", "M-"
                ).replace("BOX_", "B-")

            writer.writerow(r)

            if idx in dup_idx:
                duplicates_buffer.append(r.copy())

        for dup in duplicates_buffer:
            writer.writerow(dup)

    return total_rows + len(duplicates_buffer)


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    transport_path = os.path.join(DATA_DIR, "tms_transportation.csv")
    temp_path      = os.path.join(DATA_DIR, "_shipment_details_temp.csv")
    pool_path      = os.path.join(DATA_DIR, "_leg1_pool.json")
    output_path    = os.path.join(DATA_DIR, "wms_shipment_details.csv")

    fieldnames = [
        "shipment_id", "truck_id", "container_id", "order_date",
        "origin_node_id", "destination_node_id", "sort_code",
    ]

    print("Building network connectivity...")
    network              = build_network()
    capability_map       = network["capability_map"]
    lane_container_rates = network["lane_container_rates"]

    print("Building hub->DN map from leg2 trucks...")
    hub_to_dns = build_hub_dn_map(transport_path)
    print(f"  Hubs with active leg2 lanes: {len(hub_to_dns)}")

    total_rows = 0
    counter    = 1

    with open(temp_path, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        print("Pass 1 — generating leg1 shipments...")
        counter, leg1_count = process_leg1(
            transport_path, hub_to_dns, capability_map,
            lane_container_rates, writer, pool_path, counter
        )
        total_rows += leg1_count
        print(f"  Leg1 rows written: {leg1_count:,}")

        print("Pass 2 — generating leg2 shipments...")
        counter, leg2_count = process_leg2(
            transport_path, pool_path, capability_map,
            lane_container_rates, writer, counter
        )
        total_rows += leg2_count
        print(f"  Leg2 rows written: {leg2_count:,}")

        print("Pass 3 — generating direct shipments...")
        counter, direct_count = process_direct(
            transport_path, capability_map,
            lane_container_rates, writer, counter
        )
        total_rows += direct_count
        print(f"  Direct rows written: {direct_count:,}")

    print(f"\nTotal clean rows: {total_rows:,}")

    print("Injecting data quality issues (streaming)...")
    final_count = inject_quality_issues_streaming(
        temp_path, output_path, total_rows, transport_path
    )

    os.remove(temp_path)
    os.remove(pool_path)

    print()
    print(f"Shipment details generation complete:")
    print(f"  Clean rows:     {total_rows:,}")
    print(f"  Rows written:   {final_count:,} (includes duplicates)")
    print(f"  Output:         {output_path}")
    print()
    print(f"Row breakdown (clean data):")
    print(f"  Leg1 (ON->CH): {leg1_count:,}  ({leg1_count/total_rows*100:.1f}%)")
    print(f"  Leg2 (CH->DN): {leg2_count:,}  ({leg2_count/total_rows*100:.1f}%)")
    print(f"  Direct(ON->DN):{direct_count:,}  ({direct_count/total_rows*100:.1f}%)")
    print()
    print("Quality issues injected:")
    print(f"  NULL sort_code:          ~{max(1, round(total_rows*0.010)):,} rows")
    print(f"  order_date after depart: ~{max(1, round(total_rows*0.005)):,} rows")
    print(f"  Duplicate rows:          ~{max(1, round(total_rows*0.003)):,} rows")
    print(f"  Wrong container format:  ~{max(1, round(total_rows*0.003)):,} rows")


if __name__ == "__main__":
    main()