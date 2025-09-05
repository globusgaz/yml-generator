import os
import aiohttp
import asyncio
from lxml import etree
from datetime import datetime
import hashlib
from io import BytesIO

FEEDS_FILE = "feeds.txt"
MAX_FILE_SIZE_MB = 100
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

# -------------------- Потоковий парсинг --------------------
def iter_offers(xml_bytes):
    try:
        context = etree.iterparse(BytesIO(xml_bytes), tag="offer", recover=True)
        for _, elem in context:
            yield etree.tostring(elem, encoding="utf-8").decode("utf-8")
            elem.clear()
    except Exception as e:
        print(f"❌ Помилка парсингу XML: {e}")

# -------------------- Асинхронне завантаження --------------------
async def fetch_offers_from_url(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=60) as response:
            if response.status != 200:
                print(f"❌ {url} — HTTP {response.status}")
                return []
            content = await response.read()
            offers = list(iter_offers(content))
            print(f"✅ {url} — {len(offers)} товарів")
            return offers
    except Exception as e:
        print(f"❌ {url}: {e}")
        return []

async def fetch_all_offers(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_offers_from_url(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return [offer for sublist in results for offer in sublist], results

# -------------------- Формування YML --------------------
def build_yml_string(offers_str_list):
    parts = []
    parts.append('<?xml version="1.0" encoding="UTF-8"?>')
    parts.append(f'<yml_catalog date="{datetime.now().strftime("%Y-%m-%d %H:%M")}">')
    parts.append("<shop>")
    parts.append("<name>MyShop</name>")
    parts.append("<company>My Company</company>")
    parts.append("<url>https://myshop.example.com</url>")
    parts.append('<categories><category id="1">Загальна категорія</category></categories>')
    parts.append("<offers>")
    parts.extend(offers_str_list)  # вже готові рядки XML
    parts.append("</offers>")
    parts.append("</shop>")
    parts.append("</yml_catalog>")
    return "\n".join(parts).encode("utf-8")

# -------------------- Хеш файлу --------------------
def file_hash(path):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

# -------------------- Збереження YML --------------------
def save_file(offers, filename):
    xml_bytes = build_yml_string(offers)
    new_hash = hashlib.md5(xml_bytes).hexdigest()
    old_hash = file_hash(filename)

    if new_hash != old_hash:
        with open(filename, "wb") as f:
            f.write(xml_bytes)
        print(f"✅ Збережено: {filename} ({len(offers)} товарів)")
    else:
        print(f"⚠️ Без змін: {filename}")

def save_yml_by_size(offers):
    file_index = 1
    current_offers = []

    for offer in offers:
        current_offers.append(offer)
        xml_bytes = build_yml_string(current_offers)

        if len(xml_bytes) > MAX_FILE_SIZE_BYTES:
            current_offers.pop()
            save_file(current_offers, f"output_{file_index}.yml")
            file_index += 1
            current_offers = [offer]

    if current_offers:
        save_file(current_offers, f"output_{file_index}.yml")

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

    save_yml_by_size(all_offers)

if __name__ == "__main__":
    main()
