import os
import aiohttp
import asyncio
from lxml import etree
from datetime import datetime
from io import BytesIO
import hashlib
import re
import logging
import csv

FEEDS_FILE = "feeds.txt"
MAX_FILE_SIZE_MB = 95
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
}
PREV_YML = "prev_feed.yml"  # для порівняння
CHANGES_CSV = "changes.csv"

# -------------------- Логування --------------------
logging.basicConfig(
    filename="logs.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -------------------- Завантаження URL --------------------
def load_urls():
    if not os.path.exists(FEEDS_FILE):
        logging.error(f"Файл {FEEDS_FILE} не знайдено")
        return []
    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip().startswith("http")]

# -------------------- Санітайз тексту --------------------
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

# -------------------- Нормалізація ID --------------------
def normalize_id(feed_prefix, vendor_code, offer_id):
    base_id = vendor_code or offer_id or ""
    base_id = base_id.lower()
    base_id = re.sub(r'[^a-z0-9_\-]', '_', base_id)
    return f"{feed_prefix}_{base_id}"

# -------------------- Потоковий парсинг --------------------
def iter_offers(xml_bytes, feed_prefix):
    seen_ids = set()
    try:
        context = etree.iterparse(BytesIO(xml_bytes), tag="offer", recover=True)
        for _, elem in context:
            elem = sanitize_offer(elem)

            vendor_code = elem.findtext("vendorCode")
            offer_id = elem.get("id", "").strip()

            unique_id = normalize_id(feed_prefix, vendor_code, offer_id)

            if unique_id in seen_ids:
                logging.warning(f"Пропущено дублікат ID: {unique_id}")
                elem.clear()
                continue
            seen_ids.add(unique_id)

            elem.set("id", unique_id)

            # Фільтрація порожніх товарів
            name = elem.findtext("name")
            price = elem.findtext("price")
            if not name or not price:
                logging.warning(f"Пропущено товар без name/price: {unique_id}")
                elem.clear()
                continue

            yield unique_id, elem
            elem.clear()
    except Exception as e:
        logging.error(f"Помилка парсингу XML: {e}")

# -------------------- Асинхронне завантаження --------------------
async def fetch_offers_from_url(session, url, feed_index):
    try:
        async with session.get(url, headers=HEADERS, timeout=120) as response:
            if response.status != 200:
                logging.error(f"{url} — HTTP {response.status}")
                return []
            content = await response.read()
            feed_prefix = f"f{feed_index}"
            offers = list(iter_offers(content, feed_prefix))
            logging.info(f"{url} — {len(offers)} товарів")
            return offers
    except Exception as e:
        logging.error(f"{url}: {e}")
        return []

async def fetch_all_offers(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_offers_from_url(session, url, i+1) for i, url in enumerate(urls)]
        results = await asyncio.gather(*tasks)
        all_offers = [offer for sublist in results for offer in sublist]
        return all_offers, results

# -------------------- Збереження у кілька файлів --------------------
def save_split_yml(offers):
    header = '<?xml version="1.0" encoding="UTF-8"?>\n'
    header += f'<yml_catalog date="{datetime.now().strftime("%Y-%m-%d %H:%M")}">\n'
    header += "<shop>\n"
    header += "<name>MyShop</name>\n"
    header += "<company>My Company</company>\n"
    header += "<url>https://myshop.example.com</url>\n"

    # Збираємо унікальні категорії
    categories = {}
    for _, elem in offers:
        cat_elem = elem.find("categoryId")
        if cat_elem is not None and cat_elem.text:
            categories[cat_elem.text] = cat_elem.text
    cat_xml = "".join(f'<category id="{cid}">{cid}</category>' for cid in categories)
    header += f"<categories>{cat_xml}</categories>\n"

    header += "<offers>\n"
    footer = "</offers>\n</shop>\n</yml_catalog>\n"

    file_index = 1
    current_parts = [header]
    current_size = len(header.encode("utf-8"))

    for _, elem in offers:
        offer_xml = etree.tostring(elem, encoding="utf-8").decode("utf-8")
        offer_bytes = (offer_xml + "\n").encode("utf-8")
        if current_size + len(offer_bytes) + len(footer.encode("utf-8")) > MAX_FILE_SIZE_BYTES:
            current_parts.append(footer)
            xml_bytes = "".join(current_parts).encode("utf-8")

            filename = f"all_{file_index}.yml"
            new_hash = hashlib.md5(xml_bytes).hexdigest()
            old_hash = file_hash(filename)

            if new_hash != old_hash:
                with open(filename, "wb") as f:
                    f.write(xml_bytes)
                logging.info(f"Збережено: {filename} ({len(current_parts) - 2} товарів)")
            else:
                logging.info(f"Без змін: {filename}")

            file_index += 1
            current_parts = [header, offer_xml + "\n"]
            current_size = len(header.encode("utf-8")) + len(offer_bytes)
        else:
            current_parts.append(offer_xml + "\n")
            current_size += len(offer_bytes)

    if len(current_parts) > 1:
        current_parts.append(footer)
        xml_bytes = "".join(current_parts).encode("utf-8")
        filename = f"all_{file_index}.yml"
        new_hash = hashlib.md5(xml_bytes).hexdigest()
        old_hash = file_hash(filename)

        if new_hash != old_hash:
            with open(filename, "wb") as f:
                f.write(xml_bytes)
            logging.info(f"Збережено: {filename} ({len(current_parts) - 2} товарів)")
        else:
            logging.info(f"Без змін: {filename}")

# -------------------- Хеш файлу --------------------
def file_hash(path):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

# -------------------- Порівняння з попереднім YML --------------------
def load_previous_offers(path):
    if not os.path.exists(path):
        return {}
    tree = etree.parse(path)
    offers = {}
    for elem in tree.iterfind(".//offer"):
        oid = elem.get("id")
        price = elem.findtext("price") or ""
        quantity = elem.findtext("quantity") or ""
        offers[oid] = {"price": price, "quantity": quantity}
    return offers

def compare_and_log_changes(offers):
    prev = load_previous_offers(PREV_YML)
    changes = []

    for oid, elem in offers:
        name = elem.findtext("name")
        price = elem.findtext("price") or ""
        quantity = elem.findtext("quantity") or ""
        old_price = prev.get(oid, {}).get("price", "NEW")
        old_quantity = prev.get(oid, {}).get("quantity", "NEW")

        if old_price != price or old_quantity != quantity:
            changes.append([oid, name, old_price, price, old_quantity, quantity])

    if changes:
        with open(CHANGES_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "old_price", "new_price", "old_quantity", "new_quantity"])
            writer.writerows(changes)
        logging.info(f"Зміни записані у {CHANGES_CSV}")
    else:
        logging.info("Змін у цінах/залишках не виявлено")

# -------------------- MAIN --------------------
def main():
    urls = load_urls()
    logging.info(f"Знайдено {len(urls)} посилань у {FEEDS_FILE}")

    if not urls:
        return

    all_offers, _ = asyncio.run(fetch_all_offers(urls))
    compare_and_log_changes(all_offers)
    save_split_yml(all_offers)

if __name__ == "__main__":
    main()
