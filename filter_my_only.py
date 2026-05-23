#!/usr/bin/env python3
"""Залишити в all_1.yml лише offer id=\"my_*\" (разова очистка / локальний запуск)."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from lxml import etree

YML = Path("all_1.yml")


def main() -> None:
    if not YML.exists():
        print(f"❌ {YML} не знайдено", file=sys.stderr)
        sys.exit(1)
    parser = etree.XMLParser(remove_blank_text=False, huge_tree=True)
    tree = etree.parse(str(YML), parser)
    root = tree.getroot()
    shop = root.find("shop")
    if shop is None:
        print("❌ Немає <shop>", file=sys.stderr)
        sys.exit(1)
    offers_parent = shop.find("offers")
    if offers_parent is None:
        print("❌ Немає <offers>", file=sys.stderr)
        sys.exit(1)
    all_offers = list(offers_parent.findall("offer"))
    kept = [o for o in all_offers if (o.get("id") or "").startswith("my_")]
    removed = len(all_offers) - len(kept)
    for o in all_offers:
        if o not in kept:
            offers_parent.remove(o)
    used_cat_ids: set[str] = set()
    for o in kept:
        cid = o.findtext("categoryId")
        if cid:
            used_cat_ids.add(cid.strip())
    cats_parent = shop.find("categories")
    if cats_parent is not None:
        for cat in list(cats_parent.findall("category")):
            if (cat.get("id") or "") not in used_cat_ids:
                cats_parent.remove(cat)
    root.set("date", datetime.now().strftime("%Y-%m-%d %H:%M"))
    tree.write(
        str(YML),
        encoding="UTF-8",
        xml_declaration=True,
        pretty_print=True,
    )
    print(f"✅ Залишено {len(kept)} my_, видалено {removed} інших")
    if len(kept) < 1:
        print("❌ Немає my_ товарів у файлі", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
