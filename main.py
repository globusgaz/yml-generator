import os
import requests
from lxml import etree
from datetime import datetime

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

def load_urls():
    if not os.path.exists(FEEDS_FILE):
        print(f"❌ Файл {FEEDS_FILE} не знайдено")
        return []
    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip().startswith("http")]

def fetch_offers_from_url(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        tree = etree.fromstring(response.content)
        offers = tree.findall(".//offer")
        print(f"✅ Завантажено: {url} — знайдено {len(offers)} товарів")
        return offers
    except Exception as e:
        print(f"❌ Помилка завантаження {url}: {e}")
        return []

def build_prom_yml(offers):
    yml_catalog = etree.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
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

def save_yml_by_size(offers):
    file_index = 1
    current_offers = []

    for offer in offers:
        current_offers.append(offer)
        tree = build_prom_yml(current_offers)
        xml_bytes = etree.tostring(tree, encoding="utf-8", xml_declaration=True, pretty_print=True)

        if len(xml_bytes) >= MAX_FILE_SIZE_BYTES:
            current_offers.pop()  # Забираємо останній, бо він переповнив
            tree = build_prom_yml(current_offers)
            filename = f"output_{file_index}.yml"
            tree.write(filename, encoding="utf-8", xml_declaration=True, pretty_print=True)
            print(f"✅ Збережено: {filename} ({len(current_offers)} товарів)")

            file_index += 1
            current_offers = [offer]  # Починаємо новий файл з останнього

    if current_offers:
        tree = build_prom_yml(current_offers)
        filename = f"output_{file_index}.yml"
        tree.write(filename, encoding="utf-8", xml_declaration=True, pretty_print=True)
        print(f"✅ Збережено: {filename} ({len(current_offers)} товарів)")

def main():
    urls = load_urls()
    print(f"\n🔗 Знайдено {len(urls)} посилань у {FEEDS_FILE}\n")

    all_offers = []
    successful_feeds = 0
    failed_feeds = 0

    for url in urls:
        offers = fetch_offers_from_url(url)
        if offers:
            successful_feeds += 1
            all_offers.extend(offers)
        else:
            failed_feeds += 1

    print("\n📊 Підсумок:")
    print(f"🔹 Всього фідів: {len(urls)}")
    print(f"✅ Успішно оброблено: {successful_feeds}")
    print(f"❌ З помилками: {failed_feeds}")
    print(f"📦 Загальна кількість товарів: {len(all_offers)}")

    save_yml_by_size(all_offers)

if __name__ == "__main__":
    main()
