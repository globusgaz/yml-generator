import os
import aiohttp
import asyncio
from lxml import etree
from datetime import datetime

FEEDS_FILE = "feeds.txt"
OUTPUT_FILE = "all.yml"
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

# -------------------- Асинхронне завантаження --------------------
async def fetch_and_write_offers(session, url, f):
    try:
        async with session.get(url, headers=HEADERS, timeout=120) as response:
            if response.status != 200:
                print(f"❌ {url} — HTTP {response.status}")
                return 0

            offers = 0
            context = etree.iterparse(await response.content.readany(), tag="offer", recover=True)
            # lxml не підтримує напряму asyncio stream → читаємо chunks
            async for chunk in response.content.iter_chunked(1024 * 1024):
                for _, elem in etree.iterparse(
                    io.BytesIO(chunk), tag="offer", recover=True
                ):
                    f.write(etree.tostring(elem, encoding="utf-8").decode("utf-8"))
                    offers += 1
                    elem.clear()

            print(f"✅ {url} — {offers} товарів")
            return offers
    except Exception as e:
        print(f"❌ {url}: {e}")
        return 0

async def process_feeds(urls, f):
    total_offers = 0
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_and_write_offers(session, url, f) for url in urls]
        results = await asyncio.gather(*tasks)
        total_offers = sum(results)
    return total_offers, len([r for r in results if r > 0]), len([r for r in results if r == 0])

# -------------------- MAIN --------------------
def main():
    urls = load_urls()
    print(f"\n🔗 Знайдено {len(urls)} посилань у {FEEDS_FILE}\n")
    if not urls:
        return

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        # Заголовок
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write(f'<yml_catalog date="{datetime.now().strftime("%Y-%m-%d %H:%M")}">\n')
        f.write("<shop>\n")
        f.write("<name>MyShop</name>\n")
        f.write("<company>My Company</company>\n")
        f.write("<url>https://myshop.example.com</url>\n")
        f.write('<categories><category id="1">Загальна категорія</category></categories>\n')
        f.write("<offers>\n")

        total_offers, ok, failed = asyncio.run(process_feeds(urls, f))

        # Закриваємо структуру
        f.write("</offers>\n</shop>\n</yml_catalog>\n")

    print("\n📊 Підсумок:")
    print(f"🔹 Всього фідів: {len(urls)}")
    print(f"✅ Успішно оброблено: {ok}")
    print(f"❌ З помилками: {failed}")
    print(f"📦 Загальна кількість товарів: {total_offers}")
    print(f"📄 Файл збережено: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
