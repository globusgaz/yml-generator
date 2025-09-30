# -*- coding: utf-8 -*-
# Генератор YML з багатьох фідів + карта категорій з Excel
# Залежності: aiohttp, lxml, pandas, openpyxl

from __future__ import annotations

import os
import asyncio
import hashlib
import re
from datetime import datetime
from io import BytesIO
from typing import Dict, Iterable, List, Optional, Set

import aiohttp
from aiohttp import ClientError
import pandas as pd
from lxml import etree


# --- Константи конфігурації ---
FEEDS_FILE: str = "feeds.txt"              # список URL YML/фідів (по одному в рядку)
EXCEL_FILE: str = "prom_categories.xlsx"   # Excel з категоріями (очікувані колонки нижче)
MAX_FILE_SIZE_MB: int = 95                 # ліміт на розмір вихідного YML
MAX_FILE_SIZE_BYTES: int = MAX_FILE_SIZE_MB * 1024 * 1024

HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
}


# --- Допоміжні функції для категорій ---
def load_category_tree_from_excel(file_path: str) -> Dict[str, Dict[str, Optional[str]]]:
    """
    Завантажити дерево категорій з Excel (.xlsx). Очікувані колонки:
    - 'Идентификатор_подраздела' (ідентифікатор категорії)
    - 'Категория1'..'Категория4' (назви на різних рівнях)
    - 'Адрес_подраздела' (URL категорії)
    """
    try:
        df = pd.read_excel(file_path, engine="openpyxl")
    except FileNotFoundError:
        raise
    except Exception as e:
        raise RuntimeError(f"Не вдалося прочитати Excel '{file_path}': {e}") from e

    tree: Dict[str, Dict[str, Optional[str]]] = {}
    for _, row in df.iterrows():
        cid = str(row.get("Идентификатор_подраздела", "")).strip()
        if not cid:
            continue
        name_source = (
            row.get("Категория4")
            or row.get("Категория3")
            or row.get("Категория2")
            or row.get("Категория1")
            or ""
        )
        name = str(name_source).strip() or "Невідома категорія"
        portal_url = str(row.get("Адрес_подраздела", "")).strip()
        tree[cid] = {
            "name": name,
            "parentId": None,
            "portal_id": cid,
            "portal_url": portal_url,
        }
    return tree


def generate_categories_block(used_ids: Iterable[str], category_tree: Dict[str, Dict[str, Optional[str]]]) -> str:
    categories: List[str] = []
    for cid in sorted(set(used_ids)):
        cat = category_tree.get(cid)
        if not cat:
            cat = {"name": "Невідома категорія", "parentId": None, "portal_id": cid, "portal_url": ""}
            category_tree[cid] = cat

        attribs = [f'id="{cid}"']
        if cat.get("parentId"):
            attribs.append(f'parentId="{cat["parentId"]}"')
        if cat.get("portal_id"):
            attribs.append(f'portal_id="{cat["portal_id"]}"')
        elif cat.get("portal_url"):
            attribs.append(f'portal_url="{cat["portal_url"]}"')

        categories.append(f'<category {" ".join(attribs)}>{cat["name"]}</category>')
    return "<categories>\n" + "\n".join(categories) + "\n</categories>\n"


# --- Обробка вхідних даних ---
def load_urls(feeds_file: str = FEEDS_FILE) -> List[str]:
    if not os.path.exists(feeds_file):
        print(f"❌ Файл {feeds_file} не знайдено")
        return []
    with open(feeds_file, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip().startswith("http")]


# --- Санітизація XML ---
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


# --- Парсинг пропозицій з XML ---
def iter_offers(
    xml_bytes: bytes,
    feed_prefix: str,
    used_category_ids: Set[str],
    category_tree: Dict[str, Dict[str, Optional[str]]],
) -> Iterable[str]:
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
                original_id = cat_elem.text.strip()
                if original_id in category_tree:
                    cat_elem.text = original_id
                else:
                    category_tree[original_id] = {
                        "name": "Невідома категорія",
                        "parentId": None,
                        "portal_id": original_id,
                        "portal_url": "",
                    }
                used_category_ids.add(original_id)

            yield etree.tostring(elem, encoding="utf-8").decode("utf-8")
            elem.clear()
    except etree.XMLSyntaxError as e:
        print(f"❌ Помилка синтаксису XML: {e}")
    except Exception as e:
        print(f"❌ Помилка парсингу XML: {e}")


# --- HTTP завантаження фідів ---
async def fetch_offers_from_url(
    session: aiohttp.ClientSession,
    url: str,
    feed_index: int,
    used_category_ids: Set[str],
    category_tree: Dict[str, Dict[str, Optional[str]]],
) -> List[str]:
    try:
        timeout = aiohttp.ClientTimeout(total=180)
        async with session.get(url, headers=HEADERS, timeout=timeout) as response:
            if response.status != 200:
                print(f"❌ {url} — HTTP {response.status}")
                return []
            content = await response.read()
            offers = list(iter_offers(content, f"f{feed_index}", used_category_ids, category_tree))
            print(f"✅ {url} — {len(offers)} товарів")
            return offers
    except (ClientError, asyncio.TimeoutError) as e:
        print(f"❌ {url}: {e}")
        return []
    except Exception as e:
        print(f"❌ {url}: {e}")
        return []


async def fetch_all_offers(urls: List[str], used_category_ids: Set[str], category_tree: Dict[str, Dict[str, Optional[str]]]) -> List[str]:
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_offers_from_url(session, url, i + 1, used_category_ids, category_tree)
            for i, url in enumerate(urls)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return [offer for sublist in results for offer in sublist]


# --- Запис вихідних YML, розбиття за розміром ---
def save_split_yml(
    offers: Iterable[str],
    used_category_ids: Set[str],
    category_tree: Dict[str, Dict[str, Optional[str]]],
    prefix: str = "all",
) -> None:
    header = '<?xml version="1.0" encoding="UTF-8"?>\n'
    header += f'<yml_catalog date="{datetime.now().strftime("%Y-%m-%d %H:%M")}">\n'
    header += "<shop>\n"
    header += "<name>MyShop</name>\n"
    header += "<company>My Company</company>\n"
    header += "<url>https://myshop.example.com</url>\n"
    header += generate_categories_block(used_category_ids, category_tree)
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


# --- Точка входу ---
def main() -> None:
    print("🚀 Стартуємо генерацію YML...\n")

    urls = load_urls(FEEDS_FILE)
    print(f"🔗 Знайдено {len(urls)} посилань у {FEEDS_FILE}\n")
    if not urls:
        print("⚠️ Немає посилань для обробки. Завершення.")
        return

    try:
        category_tree = load_category_tree_from_excel(EXCEL_FILE)
        print(f"📁 Завантажено {len(category_tree)} категорій з Excel\n")
    except FileNotFoundError:
        print(f"❌ Файл Excel не знайдено: {EXCEL_FILE}")
        return
    except RuntimeError as e:
        print(f"❌ Помилка при завантаженні Excel: {e}")
        return

    used_category_ids: Set[str] = set()
    try:
        all_offers = asyncio.run(fetch_all_offers(urls, used_category_ids, category_tree))
    except Exception as e:
        print(f"❌ Помилка при завантаженні товарів: {e}")
        return

    print("\n📊 Підсумок:")
    print(f"🔹 Всього фідів: {len(urls)}")
    print(f"📦 Загальна кількість товарів: {len(all_offers)}")
    print(f"📁 Унікальних категорій: {len(used_category_ids)}")

    save_split_yml(all_offers, used_category_ids, category_tree, prefix="all")
    print("\n✅ Всі файли згенеровані та готові до імпорту!")


if __name__ == "__main__":
    main()
