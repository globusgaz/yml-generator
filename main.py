import os
import aiohttp
import asyncio
from lxml import etree
from datetime import datetime
from io import BytesIO
import hashlib

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

# -------------------- Потоковий парсинг --------------------
def iter_offers(xml_bytes):
    try:
        context = etree.iterparse(BytesIO(xml_bytes), tag="offer", recover=True)
        for _, elem in context:
            # Перетворюємо всі текстові поля на CDATA
            for e in elem.iter():
                if e.text:
                    e.text = etree.CDATA(e.text)
                if e.tail:
                    e.tail = etree.CDATA(e.tail)
            yield etree.tostring(elem, encoding="utf-8").decode("utf-8")
            elem.clear()
    except Exception as e:
        print(f"❌ Помилка парсингу XML: {e}")

# -------------------- Асинхронне завантаження --------------------
async def fetch_offers_from_url(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=120) as response:
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
        all_offers = [offer for sublist in results for offer in sublist]
        return all_offers, results

# -------------------- Хеш файлу --------------------
def file_hash(path):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

# -------------------- Збереження у кілька файлів --------------------
def save_split_yml(offers):
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

    for offer in offers:
        offer_bytes = (offer + "\n").encode("utf-8")

        if current_size + len(offer_bytes) + len(footer.encode("utf-8")) > MAX_FILE_SIZE_BYTES:
            # зберігаємо файл
            current_parts.append(footer)
            xml_bytes = "".join(current_parts).encode("utf-8")

            filename = f"all_{file_index}.yml"
            new_hash = hashlib.md5(xml_bytes).hexdigest()
            old_hash = file_hash(filename)

            if new_hash != old_hash:
                with open(filename, "wb") as f:
                    f.write(xml_bytes)
                print(f"✅ Збережено: {filename} ({len(current_parts) - 2} товарів)")
            else:
                print(f"⚠️ Без змін: {filename}")

            # новий файл
            file_index += 1
            current_parts = [header, offer + "\n"]
            current_size = len(header.encode("utf-8")) + len(offer_bytes)
        else:
            current_parts.append(offer + "\n")
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
            print(f"✅ Збережено: {filename} ({len(current_parts) - 2} товарів)")
        else:
            print(f"⚠️ Без змін: {filename}")

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

    save_split_yml(all_offers)

if __name__ == "__main__":
    main()
