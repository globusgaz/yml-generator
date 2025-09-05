import os
import aiohttp
import asyncio
from lxml import etree
from datetime import datetime
from io import BytesIO
import hashlib
import time
import glob

MAX_FILE_SIZE_MB = 100
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
FEEDS_FILE = "feeds.txt"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
}

def cleanup_old_files():
    for f in glob.glob("output_*.yml"):
        os.remove(f)
    print("🧹 Видалено старі YML-файли")

def load_urls():
    if not os.path.exists(FEEDS_FILE):
        print(f"❌ Файл {FEEDS_FILE} не знайдено")
        return []
    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip().startswith("http")]

def build_yml(offers):
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<yml_catalog date="{datetime.now().strftime("%Y-%m-%d %H:%M")}">',
        "<shop>",
        "<name>MyShop</name>",
        "<company>My Company</company>",
        "<url>https://myshop.example.com</url>",
        '<categories><category id="1">Загальна категорія</category></categories>',
        "<offers>",
        *offers,
        "</offers>",
        "</shop>",
        "</yml_catalog>"
    ]
    return "\n".join(parts).encode("utf-8")

def save_file(offers, index):
    xml_bytes = build_yml(offers)
    filename = f"output_{index}.yml"
    with open(filename, "wb") as f:
        f.write(xml_bytes)
    print(f"✅ Збережено: {filename} ({len(offers)} товарів)")

async def fetch_and_split(session, url, start_index):
    try:
        async with session.get(url, headers=HEADERS, timeout=120) as response:
            if response.status != 200:
                print(f"❌ {url} — HTTP {response.status}")
                return 0, 0

            content = await response.read()
            offers = []
            file_index = start_index
            count = 0

            for _, elem in etree.iterparse(BytesIO(content), tag="offer", recover=True):
                offer_str = etree.tostring(elem, encoding="utf-8").decode("utf-8")
                offers.append(offer_str)
                count += 1
                elem.clear()

                if len(build_yml(offers)) > MAX_FILE_SIZE_BYTES:
                    offers.pop()
                    save_file(offers, file_index)
                    file_index += 1
                    offers = [offer_str]

            if offers:
                save_file(offers, file_index)
                file_index += 1

            print(f"✅ {url} — {count} товарів")
            return count, file_index - start_index
    except Exception as e:
        print(f"❌ {url}: {e}")
        return 0, 0

async def process_feeds(urls):
    async with aiohttp.ClientSession() as session:
        tasks = []
        index = 1
        for url in urls:
            tasks.append(fetch_and_split(session, url, index))
        results = await asyncio.gather(*tasks)
        total_items = sum(r[0] for r in results)
        total_files = sum(r[1] for r in results)
        return total_items, total_files, len([r for r in results if r[0] == 0])

def main():
    start = time.time()
    cleanup_old_files()
    urls = load_urls()
    print(f"\n🔗 Знайдено {len(urls)} посилань у {FEEDS_FILE}\n")
    if not urls:
        return

    total_items, total_files, failed = asyncio.run(process_feeds(urls))

    print("\n📊 Підсумок:")
    print(f"🔹 Всього фідів: {len(urls)}")
    print(f"✅ Успішно оброблено: {len(urls) - failed}")
    print(f"❌ З помилками: {failed}")
    print(f"📦 Загальна кількість товарів: {total_items}")
    print(f"📁 Створено YML-файлів: {total_files}")
    print(f"⏱️ Час виконання: {round(time.time() - start, 2)} сек")

if __name__ == "__main__":
    main()
