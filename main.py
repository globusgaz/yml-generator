import os
import aiohttp
import asyncio
from lxml import etree
from datetime import datetime
from io import BytesIO
import hashlib
import re

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

# -------------------- Завантаження URL --------------------
def load_urls():
    if not os.path.exists(FEEDS_FILE):
        print(f"❌ Файл {FEEDS_FILE} не знайдено")
        return []
    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip().startswith("http")]

# -------------------- Санітайз тексту --------------------
def escape_text(text):
    if not text:
        return text
    # Замінюємо небезпечні символи
    text = re.sub(r'&(?![a-zA-Z]+;|#\d+;)', '&amp;', text)
    text = text.replace('"', "'")  # лапки
    text = text.replace('<', '&lt;').replace('>', '&gt;')
    return text.strip()

def sanitize_offer(elem):
    for child in elem.iter():
        if child.text:
            child.text = escape_text(child.text)
        if child.tail:
            child.tail = escape_text(child.tail)
    return elem

# -------------------- Потоковий парсинг --------------------
def iter_offers(xml_bytes):
    try:
        context = etree.iterparse(BytesIO(xml_bytes), tag="offer", recover=True)
        for _, elem in context:
            yield sanitize_offer(elem)
            elem.clear()
    except Exception as e:
        print(f"❌ Помилка парсингу XML: {e}")

# -------------------- Асинхронне завантаження --------------------
async def fetch_offers_from_url(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=120) as response:
            if response.status != 200:
                print(f"❌ {url} — HTTP {response.status}")
                return {}
            content = await response.read()
            offers_dict = {}
            for offer in iter_offers(content):
                sku = offer.get("id") or offer.findtext("vendorCode")
                if not sku:
                    continue
                offers_dict[sku] = etree.tostring(offer, encoding="utf-8").decode("utf-8")
            print(f"✅ {url} — {len(offers_dict)} унікальних товарів")
            return offers_dict
    except Exception as e:
        print(f"❌ {url}: {e}")
        return {}

async def fetch_all_offers(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_offers_from_url(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        merged = {}
        for offers in results:
            merged.update(offers)  # останній постачальник перезаписує дубль
        return merged, results

# -------------------- Хеш файлу --------------------
def file_hash(path):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

# -------------------- Збереження у кілька файлів --------------------
def save_split_yml(offers_dict):
    offers = list(offers_dict.values())

    header = '<?xml version="1.0" encoding="UTF-8"?>\n'
    header += f'<yml_catalog date="{datetime.now().strftime("%Y-%m-%d %H:%M")}">\n'
    header += "<shop>\n"
    header += "<name>MyShop</name>\n"
    header += "<company>My Company</company>\n"
    header += "<url>https://myshop.example.com</url>\n"
    header += '<categories><category id="1">Загальна категорія</category></categories>\n'
    header += "<offers>\n"

    footer = "</offers>\n</shop>\n</yml_catalog>\n"

    file_index = 1
    current_parts = [header]
    current_size = len(header.encode("utf-8"))
    current_count = 0

    for offer in offers:
        offer_bytes = (offer + "\n").encode("utf-8")

        if current_size + len(offer_bytes) + len(footer.encode("utf-8")) > MAX_FILE_SIZE_BYTES:
            current_parts.append(footer)
            xml_bytes = "".join(current_parts).encode("utf-8")

            filename = f"all_{file_index}.yml"
            new_hash = hashlib.md5(xml_bytes).hexdigest()
            old_hash = file_hash(filename)

            if new_hash != old_hash:
                with open(filename, "wb") as f:
                    f.write(xml_bytes)
                print(f"✅ Збережено: {filename} ({current_count} товарів)")
            else:
                print(f"⚠️ Без змін: {filename}")

            file_index += 1
            current_parts = [header, offer + "\n"]
            current_size = len(header.encode("utf-8")) + len(offer_bytes)
            current_count = 1
        else:
            current_parts.append(offer + "\n")
            current_size += len(offer_bytes)
            current_count += 1

    if len(current_parts) > 1:
        current_parts.append(footer)
        xml_bytes = "".join(current_parts).encode("utf-8")

        filename = f"all_{file_index}.yml"
        new_hash = hashlib.md5(xml_bytes).hexdigest()
        old_hash = file_hash(filename)

        if new_hash != old_hash:
            with open(filename, "wb") as f:
                f.write(xml_bytes)
            print(f"✅ Збережено: {filename} ({current_count} товарів)")
        else:
            print(f"⚠️ Без змін: {filename}")

# -------------------- MAIN --------------------
def main():
    urls = load_urls()
    print(f"\n🔗 Знайдено {len(urls)} посилань у {FEEDS_FILE}\n")

    if not urls:
        return

    all_offers_dict, results = asyncio.run(fetch_all_offers(urls))

    successful_feeds = sum(1 for r in results if r)
    failed_feeds = len(urls) - successful_feeds

    print("\n📊 Підсумок:")
    print(f"🔹 Всього фідів: {len(urls)}")
    print(f"✅ Успішно оброблено: {successful_feeds}")
    print(f"❌ З помилками: {failed_feeds}")
    print(f"📦 Загальна кількість унікальних товарів: {len(all_offers_dict)}")

    save_split_yml(all_offers_dict)

if __name__ == "__main__":
    main()
