import os
import requests
from lxml import etree
from datetime import datetime

FEEDS_FILE = "feeds.txt"
OUTPUT_FILE = "output.yml"  # Це XML-файл, просто з розширенням .yml

def load_urls():
    if not os.path.exists(FEEDS_FILE):
        print(f"❌ Файл {FEEDS_FILE} не знайдено")
        return []
    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip().startswith("http")]

def fetch_offers_from_url(url):
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        tree = etree.fromstring(response.content)
        return tree.findall(".//offer")
    except Exception as e:
        print(f"❌ Помилка завантаження {url}: {e}")
        return []

def build_prom_yml(offers):
    yml_catalog = etree.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = etree.SubElement(yml_catalog, "shop")

    # Базова інформація про магазин
    etree.SubElement(shop, "name").text = "MyShop"
    etree.SubElement(shop, "company").text = "My Company"
    etree.SubElement(shop, "url").text = "https://myshop.example.com"

    # Категорії (можна розширити)
    categories_el = etree.SubElement(shop, "categories")
    etree.SubElement(categories_el, "category", id="1").text = "Загальна категорія"

    # Пропозиції
    offers_el = etree.SubElement(shop, "offers")
    for offer in offers:
        offers_el.append(offer)

    return etree.ElementTree(yml_catalog)

def main():
    urls = load_urls()
    print(f"🔗 Знайдено {len(urls)} посилань")
    all_offers = []
    for url in urls:
        offers = fetch_offers_from_url(url)
        print(f"📦 {url}: {len(offers)} товарів")
        all_offers.extend(offers)

    tree = build_prom_yml(all_offers)
    tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True, pretty_print=True)
    print(f"✅ Збережено: {OUTPUT_FILE} ({len(all_offers)} товарів)")

if __name__ == "__main__":
    main()
