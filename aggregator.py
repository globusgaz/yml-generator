import os
from lxml import etree
from datetime import datetime

def collect_offers():
    offers = []
    for fname in sorted(os.listdir(".")):
        if fname.startswith("output_") and fname.endswith(".yml"):
            try:
                tree = etree.parse(fname)
                file_offers = tree.findall(".//offer")
                offers.extend(file_offers)
                print(f"📥 {fname} — {len(file_offers)} товарів")
            except Exception as e:
                print(f"❌ Помилка читання {fname}: {e}")
    return offers

def build_all_yml(offers):
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

def main():
    offers = collect_offers()
    print(f"📦 Всього зібрано: {len(offers)} товарів")

    tree = build_all_yml(offers)
    tree.write("all.yml", encoding="utf-8", xml_declaration=True, pretty_print=True)
    print("✅ Збережено: all.yml")

if __name__ == "__main__":
    main()
