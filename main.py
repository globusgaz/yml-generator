import os
import aiohttp
import asyncio
from lxml import etree
from datetime import datetime
import hashlib
import tempfile

FEEDS_FILE = "feeds.txt"
MAX_FILE_SIZE_MB = 100
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; YML-Generator/1.0)"}


# -------------------- Завантаження URL --------------------
def load_urls():
    if not os.path.exists(FEEDS_FILE):
        print(f"❌ Файл {FEEDS_FILE} не знайдено")
        return []
    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip().startswith("http")]


# -------------------- Потоковий парсинг --------------------
def iter_offers_from_stream(stream):
    context = etree.iterparse(stream, tag="offer", recover=True)
    for _, elem in context:
        yield etree.tostring(elem, encoding="utf-8")
        elem.clear()


# -------------------- Асинхронне завантаження --------------------
async def fetch_and_parse(session, url):
    offers = []
    try:
        async with session.get(url, headers=HEADERS, timeout=180) as resp:
            if resp.status != 200:
                print(f"❌ {url} — HTTP {resp.status}")
                return []

            # зберігаємо тимчасовий файл, щоб віддати iterparse
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                while True:
                    chunk = await resp.content.read(65536)
                    if not chunk:
                        break
                    tmp.write(chunk)
                tmp_path = tmp.name

            with open(tmp_path, "rb") as f:
                for offer in iter_offers_from_stream(f):
                    offers.append(offer)

            os.remove(tmp_path)
            print(f"✅ {url} — {len(offers)} товарів")
            return offers

    except Exception as e:
        print(f"❌ {url}: {e}")
        return []


async def fetch_all_offers(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_and_parse(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return [offer for sublist in results for offer in sublist], results


# -------------------- Хеш --------------------
def file_hash(path):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


# -------------------- Збереження потокове --------------------
def save_yml_stream(offers):
    file_index = 1
    size = 0
    outfile = None

    def start_file(idx):
        path = f"output_{idx}.yml"
        f = open(path, "wb")
        f.write(f'<?xml version="1.0" encoding="UTF-8"?>\n'.encode())
        f.write(f'<yml_catalog date="{datetime.now().strftime("%Y-%m-%d %H:%M")}">\n'.encode())
        f.write(b"<shop>\n")
        f.write(b"<name>MyShop</name>\n")
        f.write(b"<company>My Company</company>\n")
        f.write(b"<url>https://myshop.example.com</url>\n")
        f.write(b'<categories><category id="1">Загальна категорія</category></categories>\n')
        f.write(b"<offers>\n")
        return f, path

    def end_file(f):
        f.write(b"</offers>\n</shop>\n</yml_catalog>\n")
        f.close()

    outfile, path = start_file(file_index)

    for offer in offers:
        encoded = offer + b"\n"
        if size + len(encoded) > MAX_FILE_SIZE_BYTES:
            end_file(outfile)
            print(f"✅ Збережено: {path}")
            file_index += 1
            size = 0
            outfile, path = start_file(file_index)
        outfile.write(encoded)
        size += len(encoded)

    if outfile:
        end_file(outfile)
        print(f"✅ Збережено: {path}")


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

    save_yml_stream(all_offers)


if __name__ == "__main__":
    main()
