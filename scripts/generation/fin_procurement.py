"""
generate_fin_procurement.py
---------------------------
Generates fin_procurement.csv — procurement cost reference table.

Columns:
  item, cost_per_unit_eur, reusable, description

Two rows:
  - BOX_CONTAINER: single-use cardboard box + pallet, €15.00
  - MTL_CONTAINER: reusable metallic container, €0.25 amortised cost per use

No quality issues injected — reference table with two rows.

Output: data/fin_procurement.csv
"""

import csv
import os

PROCUREMENT_ITEMS = [
    {
        "item": "BOX_CONTAINER",
        "cost_per_unit_eur": 15.00,
        "reusable": False,
        "description": "Single-use cardboard box and pallet. Disposed after delivery."
    },
    {
        "item": "MTL_CONTAINER",
        "cost_per_unit_eur": 0.25,
        "reusable": True,
        "description": "Reusable metallic container. Amortised cost per use over 500 cycles."
    }
]


def main():
    output_dir  = os.path.join(os.path.dirname(__file__), "..", "..", "data")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "fin_procurement.csv")

    fieldnames = ["item", "cost_per_unit_eur", "reusable", "description"]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(PROCUREMENT_ITEMS)

    print(f"Rows written:   {len(PROCUREMENT_ITEMS)}")
    print(f"Output:         {output_path}")
    for item in PROCUREMENT_ITEMS:
        print(f"  {item['item']}: €{item['cost_per_unit_eur']} reusable={item['reusable']}")


if __name__ == "__main__":
    main()