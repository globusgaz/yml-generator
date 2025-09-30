# -*- coding: utf-8 -*-
# Генератор YML з мапінгом категорій з Excel на основі назв категорій із фідів
# Підтримка .xlsx/.xls, авто-визначення engine. Залежності: pandas, openpyxl, xlrd, aiohttp, lxml

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


# --- Константи конфігурації ---
FEEDS_FILE: str = "feeds.txt"
EXCEL_FILE: str = "prom_categories.xlsx"   # справжній .xlsx або .xls
MAX_FILE_SIZE_MB: int = 95
MAX_FILE_SIZE_BYTES: int = MAX_FILE_SIZE_MB * 1024 * 1024

HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
}


# --- Excel: авто-визначення та індексація ---
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


def normalize_name(name: Optional[str]) -> str:
    if not name:
        return ""
    text = str(name).strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def load_category_tree_from_excel(file_path: str) -> Tuple[
    Dict[str, Dict[str, Optional[str]]],  # category_tree: id -> {name,parentId,portal_id,portal_url}
    Dict[str, str]                        # excel_name_index: normalized_name -> portal_id
]:
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    df = _read_excel_auto(file_path)

    category_tree: Dict[str, Dict[str, Optional[str]]] = {}
    excel_name_index: Dict[str, str] = {}

    for _, row in df.iterrows():
        cid = str(row.get("Идентификатор_подраздела", "")).strip()
        if not cid:
            continue
        level4 = row.get("Категория4")
        level3 = row.get("Категория3")
        level2 = row.get("Категория2")
        level1 = row.get("Категория1")

        # Використовуємо найглибшу доступну назву як “офіційну”
        name_source = level4 or level3 or level2 or level1 or ""
        name = str(name_source).strip() or "Невідома категорія"
        portal_url = str(row.get("Адрес_подраздела", "") or "").strip()

        category_tree[cid] = {
            "name": name,
            "parentId": None,
            "portal_id": cid,
            "portal_url": portal_url,
        }

        norm = normalize_name(name)
        if norm and norm not in excel_name_index:
            excel_name_index[norm] = cid

        # Додатково індексуємо всі рівні, якщо заповнені
        for candidate in (level1, level2, level3, level4):
            nn = normalize_name(candidate)
            if nn and nn not in excel_name_index:
                excel_name_index[nn] = cid

    return category_tree, excel_name_index


# --- Завантаження URL фідів ---
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


# --- Парсинг секції категорій з фіду ---
def extract_feed_categories(xml_bytes: bytes) -> Dict[str, str]:
    """
    Повертає мапу feed_category_id -> feed_category_name, якщо у фіді є <categories>.
    """
    mapping: Dict[str, str] = {}
    try:
        root = etree.fromstring(xml_bytes)
        nodes = root.xpath(".//categories/category")
        for node in nodes:
            cid = (node.get("id") or "").strip()
            name = (node.text or "").strip()
            if cid and name:
                mapping[cid] = name
    except Exception:
        # тихо ідемо далі — не всі фіди мають секцію categories
        pass
    return mapping


def resolve_category_id(
    original_id: str,
    feed_cat_map: Dict[str, str],
    excel_name_index: Dict[str, str],
    category_tree: Dict[str, Dict[str, Optional[str]]],
) -> str:
    """
    Повертає portal_id з Excel. Алгоритм:
    1) Якщо original_id вже є як portal_id у Excel — повертаємо його.
    2) Якщо у фіді є назва категорії для original_id, пробуємо знайти її у excel_name_index.
    3) Інакше — залишаємо original_id, але додамо “Невідома категорія”.
    """
    if original_id in category_tree:
        return original_id

    feed_name = feed_cat_map.get(original_id, "")
    norm = normalize_name(feed_name)
    if norm and norm in excel_name_index:
        return excel_name_index[norm]

    return original_id  # далі викличе створення “Невідома категорія”


# --- Парсинг пропозицій з підстановкою коректного portal_id ---
def iter_offers(
    xml_bytes: bytes,
    feed_prefix: str,
    used_category_ids: Set[str],
    category_tree: Dict[str, Dict[str, Optional[str]]],
    excel_name_index: Dict[str, str],
) -> Iterable[str]:
    feed_cat_map = extract_feed_categories(xml_bytes)

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
                mapped_id = resolve_category_id(original_id, feed_cat_map, excel_name_index, category_tree)

                if mapped_id in category_tree:
                    cat_elem.text = mapped_id
                else:
                    # створюємо “Невідома категорія” під mapped_id (який дорівнює original_id, якщо мапінг не знайшли)
                    category_tree[mapped_id] = {
                        "name": "Невідома категорія",
                        "parentId": None,
                        "portal_id": mapped_id,
                        "portal_url": "",
                    }
                    cat_elem.text = mapped_id

                used_category_ids.add(cat_elem.text.strip())

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
    excel_name_index: Dict[str, str],
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
                    category_tree,
                    excel_name_index,
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
    category_tree: Dict[str, Dict[str, Optional[str]]],
    excel_name_index: Dict[str, str],
) -> List[str]:
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_offers_from_url(session, url, i + 1, used_category_ids, category_tree, excel_name_index)
            for i, url in enumerate(urls)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return [offer for sublist in results for offer in sublist]


# --- Запис вихідних YML, розбиття за розміром ---
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
        category_tree, excel_name_index = load_category_tree_from_excel(EXCEL_FILE)
        print(f"📁 Завантажено {len(category_tree)} категорій з Excel\n")
    except FileNotFoundError:
        print(f"❌ Файл Excel не знайдено: {EXCEL_FILE}")
        return
    except RuntimeError as e:
        print(f"❌ Помилка при завантаженні Excel: {e}")
        return

    used_category_ids: Set[str] = set()
    try:
        all_offers = asyncio.run(fetch_all_offers(urls, used_category_ids, category_tree, excel_name_index))
    except Exception as e:
        print(f"❌ Помилка при завантаженні товарів: {e}")
        return

    print("\n📊 Підсумок:")
    print(f"🔹 Всього фідів: {len(urls)}")
    print(f"📦 Загальна кількість товарів: {len(all_offers)}")
    print(f"📁 Унікальних категорій (після мапінгу): {len(used_category_ids)}")

    save_split_yml(all_offers, used_category_ids, category_tree, prefix="all")
    print("\n✅ Всі файли згенеровані та готові до імпорту!")


if __name__ == "__main__":
    main()
