# -*- coding: utf-8 -*-
# Генератор YML з гарантованим додаванням всіх categoryId у секцію <categories>
# Залежності: pandas, openpyxl/xlrd, aiohttp, lxml

from __future__ import annotations

import os
import asyncio
import hashlib
import re
from datetime import datetime
from io import BytesIO
from typing import Dict, Iterable, List, Optional, Set, Tuple

import aiohttp
from aiohttp import ClientError
import pandas as pd
from lxml import etree

FEEDS_FILE: str = "feeds.txt"
EXCEL_FILE: str = "prom_categories.xlsx"
MAX_FILE_SIZE_MB: int = 95
MAX_FILE_SIZE_BYTES: int = MAX_FILE_SIZE_MB * 1024 * 1024

HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
}

# ---------------- Excel ----------------

def _read_excel_auto(file_path: str) -> pd.DataFrame:
    ext = os.path.splitext(file_path)[1].lower()
    last_err: Optional[Exception] = None
    if ext == ".xlsx":
        try:
            return pd.read_excel(file_path, engine="openpyxl")
        except Exception as e:
            last_err = e
    elif ext == ".xls":
        try:
            import xlrd  # noqa: F401
            return pd.read_excel(file_path, engine="xlrd")
        except Exception as e:
            last_err = e
    for engine in ("openpyxl", "xlrd"):
        try:
            if engine == "xlrd":
                import xlrd  # noqa: F401
            return pd.read_excel(file_path, engine=engine)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Не вдалося прочитати Excel '{file_path}': {last_err}")

def _norm(text: Optional[str]) -> str:
    if not text:
        return ""
    t = str(text).strip().lower()
    return re.sub(r"\s+", " ", t)

def load_excel_categories(file_path: str) -> Tuple[
    Dict[str, Dict[str, Optional[str]]],  # portal_id -> {name, parentId}
    Dict[str, str]                         # normalized_name -> portal_id
]:
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)
    df = _read_excel_auto(file_path)

    tree: Dict[str, Dict[str, Optional[str]]] = {}
    name_index: Dict[str, str] = {}

    for _, row in df.iterrows():
        cid = str(row.get("Идентификатор_подраздела", "")).strip()
        if not cid:
            continue
        level4 = row.get("Категория4")
        level3 = row.get("Категория3")
        level2 = row.get("Категория2")
        level1 = row.get("Категория1")
        name_source = level4 or level3 or level2 or level1 or ""
        name = str(name_source).strip() or "Невідома категорія"

        tree[cid] = {"name": name, "parentId": None}

        # індексуємо всі рівні назв
        for candidate in (level1, level2, level3, level4, name):
            nn = _norm(candidate)
            if nn and nn not in name_index:
                name_index[nn] = cid

    return tree, name_index

# --------------- Feeds: urls ---------------

def load_urls(feeds_file: str = FEEDS_FILE) -> List[str]:
    if not os.path.exists(feeds_file):
        print(f"❌ Файл {feeds_file} не знайдено")
        return []
    with open(feeds_file, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip().startswith("http")]

# --------------- XML sanitize ---------------

def sanitize_text(text: Optional[str]) -> str:
    if not text:
        return ""
    text = re.sub(r'&(?![a-zA-Z]+;|#\d+;)', "&amp;", text)
    return text.replace("<", "&lt;").replace(">", "&gt;")

def sanitize_offer(elem: etree._Element) -> etree._Element:
    for child in elem.iter():
        if child.text:
            child.text = sanitize_text(child.text)
        if child.tail:
            child.tail = sanitize_text(child.tail)
    return elem

# --------------- Feed categories parse ---------------

FeedCat = Dict[str, Dict[str, Optional[str]]]  # id -> {name, parentId}

def parse_feed_categories(xml_bytes: bytes) -> FeedCat:
    cats: FeedCat = {}
    try:
        root = etree.fromstring(xml_bytes)
        nodes = root.xpath(".//categories/category")
        for node in nodes:
            cid = (node.get("id") or "").strip()
            if not cid:
                continue
            parent_id = node.get("parentId")
            parent_id = parent_id.strip() if parent_id else None
            name = (node.text or "").strip()
            cats[cid] = {"name": name, "parentId": parent_id}
    except Exception:
        pass
    return cats

# --------------- Offer iteration with mapping ---------------

def resolve_category(
    original_id: str,
    feed_cats: FeedCat,
    excel_name_index: Dict[str, str],
    excel_tree: Dict[str, Dict[str, Optional[str]]],
) -> Tuple[str, bool]:
    """Повертає (mapped_id, is_excel)."""
    # 1) Якщо вже відповідає Excel id
    if original_id in excel_tree:
        return original_id, True

    # 2) Якщо відома назва у фіді — шукаємо її в Excel за normalized name
    name = feed_cats.get(original_id, {}).get("name", "")
    nn = _norm(name)
    if nn and nn in excel_name_index:
        return excel_name_index[nn], True

    # 3) Fallback: залишаємо feed-id
    return original_id, False

def iter_offers(
    xml_bytes: bytes,
    feed_prefix: str,
    used_category_ids: Set[str],
    excel_tree: Dict[str, Dict[str, Optional[str]]],
    excel_name_index: Dict[str, str],
    global_feed_categories: FeedCat,
) -> Iterable[str]:
    local_feed_cats = parse_feed_categories(xml_bytes)
    # мерджимо локальні у глобальні
    for k, v in local_feed_cats.items():
        if k not in global_feed_categories:
            global_feed_categories[k] = v

    try:
        context = etree.iterparse(BytesIO(xml_bytes), tag="offer", recover=True)
        for _, elem in context:
            elem = sanitize_offer(elem)

            offer_id = (elem.get("id") or "").strip()
            vendor_code = elem.findtext("vendorCode")

            base = (vendor_code or offer_id or hashlib.md5(etree.tostring(elem)).hexdigest()).strip()
            unique_code = f"{feed_prefix}_{base}" if feed_prefix else base
            elem.set("id", unique_code)

            vc_elem = elem.find("vendorCode")
            if vc_elem is not None:
                vc_elem.text = unique_code
            else:
                new_vc = etree.Element("vendorCode")
                new_vc.text = unique_code
                elem.insert(0, new_vc)

            url_elem = elem.find("url")
            if url_elem is not None and url_elem.text:
                clean_url = url_elem.text.strip().split("?")[0]
                url_elem.text = f"{clean_url}?id={unique_code}"

            cat_elem = elem.find("categoryId")
            if cat_elem is not None and cat_elem.text:
                orig = cat_elem.text.strip()
                mapped_id, is_excel = resolve_category(orig, global_feed_categories, excel_name_index, excel_tree)
                cat_elem.text = mapped_id
                used_category_ids.add(mapped_id)

            yield etree.tostring(elem, encoding="utf-8").decode("utf-8")
            elem.clear()
    except etree.XMLSyntaxError as e:
        print(f"❌ Помилка синтаксису XML: {e}")
    except Exception as e:
        print(f"❌ Помилка парсингу XML: {e}")

# --------------- HTTP ---------------

async def fetch_offers_from_url(
    session: aiohttp.ClientSession,
    url: str,
    feed_index: int,
    used_category_ids: Set[str],
    excel_tree: Dict[str, Dict[str, Optional[str]]],
    excel_name_index: Dict[str, str],
    global_feed_categories: FeedCat,
) -> List[str]:
    try:
        timeout = aiohttp.ClientTimeout(total=180)
        async with session.get(url, headers=HEADERS, timeout=timeout) as response:
            if response.status != 200:
                print(f"❌ {url} — HTTP {response.status}")
                return []
            content = await response.read()
            offers = list(
                iter_offers(
                    content,
                    f"f{feed_index}",
                    used_category_ids,
                    excel_tree,
                    excel_name_index,
                    global_feed_categories,
                )
            )
            print(f"✅ {url} — {len(offers)} товарів")
            return offers
    except (ClientError, asyncio.TimeoutError) as e:
        print(f"❌ {url}: {e}")
        return []
    except Exception as e:
        print(f"❌ {url}: {e}")
        return []

async def fetch_all_offers(
    urls: List[str],
    used_category_ids: Set[str],
    excel_tree: Dict[str, Dict[str, Optional[str]]],
    excel_name_index: Dict[str, str],
    global_feed_categories: FeedCat,
) -> List[str]:
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_offers_from_url(
                session, url, i + 1, used_category_ids, excel_tree, excel_name_index, global_feed_categories
            )
            for i, url in enumerate(urls)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return [offer for sublist in results for offer in sublist]

# --------------- Categories emission ---------------

def build_categories_for_output(
    used_ids: Iterable[str],
    excel_tree: Dict[str, Dict[str, Optional[str]]],
    global_feed_categories: FeedCat,
) -> Dict[str, Dict[str, Optional[str]]]:
    """ГАРАНТОВАНО додаємо ВСІ використані categoryId."""
    out: Dict[str, Dict[str, Optional[str]]] = {}
    
    for cid in set(used_ids):
        if cid in excel_tree:
            # з Excel
            out[cid] = {"name": excel_tree[cid]["name"], "parentId": None}
        elif cid in global_feed_categories:
            # з фіду
            node = global_feed_categories[cid]
            out[cid] = {"name": node.get("name") or f"Категорія {cid}", "parentId": node.get("parentId")}
        else:
            # дефолт для невідомих
            out[cid] = {"name": f"Категорія {cid}", "parentId": None}
    
    return out

def generate_categories_block(categories: Dict[str, Dict[str, Optional[str]]]) -> str:
    lines: List[str] = []
    for cid in sorted(categories.keys(), key=lambda x: (categories[x].get("parentId") or "", x)):
        name = categories[cid].get("name") or "Категорія"
        parent = categories[cid].get("parentId")
        attrs = f'id="{cid}"' + (f' parentId="{parent}"' if parent else "")
        lines.append(f"<category {attrs}>{name}</category>")
    return "<categories>\n" + "\n".join(lines) + "\n</categories>\n"

# --------------- Save YML ---------------

def save_split_yml(
    offers: Iterable[str],
    used_category_ids: Set[str],
    excel_tree: Dict[str, Dict[str, Optional[str]]],
    global_feed_categories: FeedCat,
    prefix: str = "all",
) -> None:
    categories_dict = build_categories_for_output(used_category_ids, excel_tree, global_feed_categories)

    header = '<?xml version="1.0" encoding="UTF-8"?>\n'
    header += f'<yml_catalog date="{datetime.now().strftime("%Y-%m-%d %H:%M")}">\n'
    header += "<shop>\n"
    header += "<name>MyShop</name>\n"
    header += "<company>My Company</company>\n"
    header += "<url>https://myshop.example.com</url>\n"
    header += generate_categories_block(categories_dict)
    header += "<offers>\n"

    footer = "</offers>\n</shop>\n</yml_catalog>\n"

    file_index = 1
    current_parts: List[str] = [header]
    current_size = len(header.encode("utf-8"))
    offers_in_file = 0

    for offer in offers:
        offer_line = offer + "\n"
        offer_bytes = offer_line.encode("utf-8")
        if current_size + len(offer_bytes) + len(footer.encode("utf-8")) > MAX_FILE_SIZE_BYTES:
            current_parts.append(footer)
            filename = f"{prefix}_{file_index}.yml"
            with open(filename, "wb") as f:
                f.write("".join(current_parts).encode("utf-8"))
            print(f"✅ Збережено: {filename} ({offers_in_file} товарів)")

            file_index += 1
            current_parts = [header, offer_line]
            current_size = len(header.encode("utf-8")) + len(offer_bytes)
            offers_in_file = 1
        else:
            current_parts.append(offer_line)
            current_size += len(offer_bytes)
            offers_in_file += 1

    if offers_in_file > 0:
        current_parts.append(footer)
        filename = f"{prefix}_{file_index}.yml"
        with open(filename, "wb") as f:
            f.write("".join(current_parts).encode("utf-8"))
        print(f"✅ Збережено: {filename} ({offers_in_file} товарів)")

# --------------- Entry ---------------

def main() -> None:
    print("🚀 Стартуємо генерацію YML...\n")

    urls = load_urls(FEEDS_FILE)
    print(f"🔗 Знайдено {len(urls)} посилань у {FEEDS_FILE}\n")
    if not urls:
        print("⚠️ Немає посилань для обробки. Завершення.")
        return

    try:
        excel_tree, excel_name_index = load_excel_categories(EXCEL_FILE)
        print(f"📁 Завантажено {len(excel_tree)} категорій з Excel\n")
    except FileNotFoundError:
        print(f"❌ Файл Excel не знайдено: {EXCEL_FILE}")
        return
    except RuntimeError as e:
        print(f"❌ Помилка при завантаженні Excel: {e}")
        return

    used_category_ids: Set[str] = set()
    global_feed_categories: FeedCat = {}

    try:
        async def _run() -> List[str]:
            async with aiohttp.ClientSession() as session:
                tasks = [
                    fetch_offers_from_url(
                        session, url, i + 1, used_category_ids, excel_tree, excel_name_index, global_feed_categories
                    )
                    for i, url in enumerate(urls)
                ]
                results = await asyncio.gather(*tasks, return_exceptions=False)
                return [offer for sublist in results for offer in sublist]

        all_offers = asyncio.run(_run())
    except Exception as e:
        print(f"❌ Помилка при завантаженні товарів: {e}")
        return

    print("\n📊 Підсумок:")
    print(f"🔹 Всього фідів: {len(urls)}")
    print(f"📦 Загальна кількість товарів: {len(all_offers)}")
    print(f"📁 Унікальних категорій (в offers): {len(used_category_ids)}")

    save_split_yml(all_offers, used_category_ids, excel_tree, global_feed_categories, prefix="all")
    print("\n✅ Всі файли згенеровані та готові до імпорту!")

if __name__ == "__main__":
    main()
