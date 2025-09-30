import os
import aiohttp
import asyncio
from lxml import etree
from datetime import datetime
from io import BytesIO
import hashlib
import re
import pandas as pd

FEEDS_FILE = "feeds.txt"
EXCEL_FILE = "prom_categories.xlsx"
MAX_FILE_SIZE_MB = 95
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
}

def load_category_tree_from_excel(file_path):
    df = pd.read_excel(file_path, engine="xlrd")
    tree = {}
    for _, row in df.iterrows():
        cid = str(row["Идентификатор_подраздела"]).strip()
        name = (
            str(row.get("Категория4") or row.get("Категория3") or row.get("Категория2") or row.get("Категория1"))
        ).strip()
        tree[cid] = {
            "name": name or "Невідома категорія",
            "parentId": None,
            "portal_id": cid,
            "portal_url": str(row["Адрес_подраздела"]).strip()
        }
    return tree

def generate_categories_block(used_ids, category_tree):
    categories = []
    for cid in sorted(used_ids):
        cat = category_tree.get(cid)
        if not cat:
            cat = {
                "name": "Невідома категорія",
                "parentId": None,
                "portal_id": cid,
                "portal_url": ""
            }
            category_tree[cid] = cat
        attribs = f'id="{cid}"'
        if cat.get("parentId"):
            attribs += f' parentId="{cat["parentId"]}"'
        if cat.get("portal_id"):
            attribs += f' portal_id="{cat["portal_id"]}"'
        elif cat.get("portal_url"):
            attribs += f' portal_url="{cat["portal_url"]}"'
        categories.append(f'<category {attribs}>{cat["name"]}</category>')
    return "<categories>\n" + "\n".join(categories) + "\n</categories>\n"

def load_urls():
    if not os.path.exists(FEEDS_FILE):
        print(f"❌ Файл {FEEDS_FILE} не знайдено")
        return []
    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip().startswith("http")]

def sanitize_text(text):
    if not text:
        return ""
    text = re.sub(r'&(?![a-zA-Z]+;|#\d+;)', '&amp;', text)
    text = text.replace('<', '&lt;').replace('>', '&gt;')
    return text

def sanitize_offer(elem):
    for child in elem.iter():
        if child.text:
            child.text = sanitize_text(child.text)
        if child.tail:
            child.tail = sanitize_text(child.tail)
    return elem

def iter_offers(xml_bytes, feed_prefix, used_category_ids, category_tree):
    try:
        context = etree.iterparse(BytesIO(xml_bytes), tag="offer", recover=True)
        for _, elem in context:
            elem = sanitize_offer(elem)

            offer_id = elem.get("id", "").strip()
            vendor_code = elem.findtext("vendorCode")

            unique_code = vendor_code.strip() if vendor_code else offer_id or hashlib.md5(etree.tostring(elem)).hexdigest()
            unique_code = unique_code
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
                        "portal_url": ""
                    }
                used_category_ids.add(original_id)

            yield etree.tostring(elem, encoding="utf-8").decode("utf-8")
            elem.clear()
    except Exception as e:
        print(f"❌ Помилка парсингу XML: {e}")

async def fetch_offers_from_url(session, url, feed_index, used_category_ids, category_tree):
    try:
        async with session.get(url, headers=HEADERS, timeout=120) as response:
            if response.status != 200:
                print(f"❌ {url} — HTTP {response.status}")
                return []
            content = await response.read()
            offers = list(iter_offers(content, f"{feed_index}", used_category_ids, category_tree))
            print(f"✅ {url} — {len(offers)} товарів")
            return offers
    except Exception as e:
        print(f"❌ {url}: {e}")
        return []

async def fetch_all_offers(urls, used_category_ids, category_tree):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_offers_from_url(session, url, i+1, used_category_ids, category_tree) for i, url in enumerate(urls)]
        results = await asyncio.gather(*tasks)
        all_offers = [offer for sublist in results for offer in sublist]
        return all_offers

def save_split_yml(offers, used_category_ids, category_tree, prefix="all"):
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
    current_parts = [header]
    current_size = len(header.encode("utf-8"))
    offers_in_file = 0

    for offer in offers:
        offer_bytes = (offer + "\n").encode("utf-8")
        if current_size + len(offer_bytes) + len(footer.encode("utf-8")) > MAX_FILE_SIZE_BYTES:
            current_parts.append(footer)
            filename = f"{prefix}_{file_index}.yml"
            with open(filename, "wb") as f:
                f.write("".join(current_parts).encode("utf-8"))
            print(f"✅ Збережено: {filename} ({offers_in_file} товарів)")

            file_index += 1
            current_parts = [header, offer + "\n"]
            current_size = len(header.encode("utf-8")) + len(offer_bytes)
            offers_in_file = 1
        else:
            current_parts.append(offer + "\n")
            current_size += len(offer_bytes)
            offers_in_file += 1

    if offers_in_file > 0:
        current_parts.append(footer)
        filename = f"{prefix}_{file_index}.yml"
        with open(filename, "wb") as f:
            f.write("".join(current_parts).encode("utf-8"))
        print(f"✅ Збережено: {filename} ({offers_in_file} товарів)")

def main():
    urls = load_urls()
    print(f"\n🔗 Знайдено {len(urls)} посилань у {FEEDS_FILE}\n")
    if not urls:
        return

    used_category_ids = set()
    category_tree = load_category_tree_from_excel(EXCEL_FILE)
    all_offers = asyncio.run(fetch_all_offers(urls, used_category_ids, category_tree))

    print("\n📊 Підсумок:")
    print(f"🔹 Всього фідів: {len(urls)}")
    print(f"📦 Загальна кількість товар
