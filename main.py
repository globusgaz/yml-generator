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
    urls = []
    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and line.startswith("http"):
                # Очікуємо формат: url|постачальник
                parts = line.split("|")
                url = parts[0].strip()
                supplier = parts[1].strip() if len(parts) > 1 else f"f{len(urls)+1}"
                urls.append((url, supplier))
    return urls

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

# -------------------- Потоковий парсинг --------------------
def iter_offers(xml_bytes, supplier_prefix):
    try:
        context = etree.iterparse(BytesIO(xml_bytes), tag="offer", recover=True)
        for _, elem in context:
            elem = sanitize_offer(elem)

            # Унікальний ID
            offer_id = elem.get("id", "").strip()
            vendor_code = elem.findtext("vendorCode")
            if vendor_code and vendor_code.strip():
                unique_id = f"{supplier_prefix}_{vendor_code.strip()}"
            else:
                unique_id = f"{supplier_prefix}_{offer_id or hashlib.md5(etree.tostring(elem)).hexdigest()}"

            elem.set("id", unique_id)

            yield etree.tostring(elem, encoding="utf-8").decode("utf-8")
            elem.clear()
    except Exception as e:
        print(f"❌ Помилка парсингу XML: {e}")

# -------------------- Асинхронне завантаження --------------------
async def fetch_offers_from_url(session, url, supplier_prefix):
    try:
        async with session.get(url, headers=HEADERS, timeout=120) as response:
            if response.status != 200:
                print(f"❌ {url} — HTTP {response.status}")
                return []
            content = await response.read()
            offers = list(iter_offers(content, supplier_prefix))
            print(f"✅ {url} ({supplier_prefix}) — {len(offers)} товарів")
            return offers
    except Exception as e:
        print(f"❌ {url}: {e}")
        return []

async def fetch_all_offers(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_offers_from_url(session, url, supplier) for url, supplier in urls]
        results = await asyncio.gather(*tasks)
        all_offers = [offer for sublist in results for offer in sublist]
        return all_offers, results

# -------------------- Збереження у кілька файлів --------------------
def save_split_yml(offers, prefix="all"):
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

# -------------------- MAIN --------------------
def main():
    urls = load_urls()
    print(f"\n🔗 Знайдено {len(urls)} посилань у {FEEDS_FILE}\n")
    if not urls:
        return

    all_offers, results = asyncio.run(fetch_all_offers(urls))

    successful_feeds = sum(1 for r in results if r)
    failed_feeds = len(urls) - successful_feeds

    print("\n📊 Підсумок:")
    print(f"🔹 Всього фідів: {len(urls)}")
    print(f"✅ Успішно оброблено: {successful_feeds}")
    print(f"❌ З помилками: {failed_feeds}")
    print(f"📦 Загальна кількість товарів: {len(all_offers)}")

    save_split_yml(all_offers, prefix="all")
    print("\n✅ Всі файли згенеровані та готові до пушу!")

if __name__ == "__main__":
    main()
