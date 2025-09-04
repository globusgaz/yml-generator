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


# -------------------- ЗАВАНТАЖЕННЯ URL --------------------
def load_urls():
    if not os.path.exists(FEEDS_FILE):
        print(f"❌ Файл {FEEDS_FILE} не знайдено")
        return []
    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip().startswith("http")]


# -------------------- ПОТОКОВИЙ ПАРСИНГ --------------------
def iter_offers(xml_bytes):
    try:
        context = etree.iterparse(BytesIO(xml_bytes), tag="offer", recover=True)
        for _, elem in context:
            # серіалізуємо оффер у байти й парсимо знову,
            # щоб він був повним і незалежним від оригінального дерева
            offer_copy = etree.fromstring(etree.tostring(elem))
            yield offer_copy
            elem.clear()  # чистимо оригінал для економії пам'яті
    except Exception as e:
        print(f"❌ Помилка парсингу XML: {e}")


# -------------------- АСИНХРОННЕ ЗАВАНТАЖЕННЯ --------------------
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


# -------------------- ФОРМУВАННЯ YML --------------------
def build_prom_yml(offers):
    yml_catalog = etree.Element(
        "yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M")
    )
    shop = etree.SubElement(yml_catalog, "shop")

    etree.SubElement(shop, "name").text = "MyShop"
    etree.SubElement(shop, "company").text = "My Company"
    etree.SubElement(shop, "url").text = "https://myshop.example.com"

    categories_el = etree.SubElement(shop, "categories")
    etree.SubElement(categories_el, "category", id="1").text = "Загальна категорія"

    offers_el = etree.SubElement(shop, "offers")
    for offer in offers:
        offers_el.append(offer)

    return etree.ElementTree(yml_catalog)


# -------------------- ХЕШ ФАЙЛУ --------------------
def file_hash(path):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


# -------------------- ЗБЕРЕЖЕННЯ YML --------------------
def save_file(offers, filename):
    tree = build_prom_yml(offers)
    xml_bytes = etree.tostring(tree, encoding="utf-8", xml_declaration=True)

    new_hash = hashlib.md5(xml_bytes).hexdigest()
    old_hash = file_hash(filename)

    if new_hash != old_hash:
        tree.write(filename, encoding="utf-8", xml_declaration=True)
        print(f"✅ Збережено: {filename} ({len(offers)} товарів)")
    else:
        print(f"⚠️ Без змін: {filename}")


def save_yml_by_size(offers):
    file_index = 1
    current_offers = []

    for offer in offers:
        current_offers.append(offer)
        tree = build_prom_yml(current_offers)
        xml_bytes = etree.tostring(tree, encoding="utf-8", xml_declaration=True)

        if len(xml_bytes) > MAX_FILE_SIZE_BYTES:
            # зберігаємо без останнього оффера
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
