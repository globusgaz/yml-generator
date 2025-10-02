#!/usr/bin/env python3
"""
Повний робочий yml.generator з виправленням дублікатів
"""

import os
import asyncio
import hashlib
import re
from datetime import datetime
from io import BytesIO
from typing import Dict, Iterable, List, Optional, Set, Tuple

import aiohttp
from aiohttp import ClientError
import pandas as pd
from lxml import etree

# Конфігурація
FEEDS_FILE = "feeds.txt"
OUTPUT_DIR = "output"
BATCH_SIZE = 1000
MAX_CONCURRENT = 5
TIMEOUT = 30

# Заголовки для HTTP запитів
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

def sanitize_text(text: str) -> str:
    """Очищає текст від небажаних символів"""
    if not text:
        return ""
    
    # Видаляємо контрольні символи
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
    
    # Нормалізуємо пробіли
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def sanitize_offer(elem: etree._Element) -> etree._Element:
    """Санітизація з видаленням дублікатів тегів available"""
    for child in elem.iter():
        if child.text:
            child.text = sanitize_text(child.text)
        if child.tail:
            child.tail = sanitize_text(child.tail)
    
    # ВИПРАВЛЕННЯ: Видаляємо дублікати тегів available
    available_tags = elem.findall("available")
    if len(available_tags) > 1:
        # Залишаємо тільки останній тег available
        last_available = available_tags[-1]
        
        # Видаляємо всі теги available
        for tag in available_tags:
            elem.remove(tag)
        
        # Додаємо тільки останній
        elem.append(last_available)
    
    return elem

def load_feeds() -> List[str]:
    """Завантажує список URL фідів"""
    if not os.path.exists(FEEDS_FILE):
        print(f"❌ Файл {FEEDS_FILE} не знайдено")
        return []
    
    with open(FEEDS_FILE, 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip()]
    
    print(f"📋 Завантажено {len(urls)} URL фідів")
    return urls

async def fetch_feed(session: aiohttp.ClientSession, url: str) -> Tuple[bool, bytes]:
    """Завантажує XML фід"""
    try:
        print(f"🔄 Завантажую: {url}")
        
        # Перевіряємо чи потрібна авторизація
        auth = None
        if "api.dropshipping.ua" in url:
            # Додаємо Basic Auth для dropshipping API
            auth = aiohttp.BasicAuth("your_username", "your_password")
        
        async with session.get(url, headers=HEADERS, auth=auth, timeout=TIMEOUT) as response:
            if response.status == 200:
                content = await response.read()
                print(f"✅ Завантажено: {len(content)} байт")
                return True, content
            else:
                print(f"❌ HTTP {response.status}: {url}")
                return False, b""
                
    except Exception as e:
        print(f"❌ Помилка завантаження {url}: {e}")
        return False, b""

def parse_xml_content(content: bytes) -> Tuple[List[Dict[str, any]], Dict[str, any]]:
    """Парсить XML контент і повертає список товарів та категорії"""
    try:
        # Парсимо XML
        root = etree.fromstring(content)
        
        # Знаходимо категорії
        categories = {}
        category_elements = root.findall(".//category")
        for cat in category_elements:
            cat_id = cat.get("id")
            cat_name = cat.text
            if cat_id and cat_name:
                categories[cat_id] = sanitize_text(cat_name)
        
        # Знаходимо всі товари
        offers = root.findall(".//offer")
        print(f"📦 Знайдено {len(offers)} товарів, {len(categories)} категорій")
        
        products = []
        for offer in offers:
            try:
                # Очищаємо товар від дублікатів
                offer = sanitize_offer(offer)
                
                # Витягуємо дані
                product_id = offer.get("id")
                if not product_id:
                    continue
                
                # Додаємо префікс до ID
                if not product_id.startswith(("f3_", "f4_", "f7_")):
                    # Визначаємо префікс на основі URL або інших критеріїв
                    if "dropshipping" in str(offer):
                        product_id = f"f3_{product_id}"
                    elif "api" in str(offer):
                        product_id = f"f4_{product_id}"
                    else:
                        product_id = f"f7_{product_id}"
                
                # Основна інформація
                name_elem = offer.find("name")
                name = sanitize_text(name_elem.text) if name_elem is not None and name_elem.text else ""
                
                # Ціна
                price_elem = offer.find("price")
                price = None
                if price_elem is not None and price_elem.text:
                    try:
                        price = float(price_elem.text.strip().replace(",", "."))
                    except (ValueError, AttributeError):
                        pass
                
                # Наявність
                available = offer.get("available", "true")
                presence = available.lower() in ("true", "1", "yes", "available", "in_stock")
                
                # Кількість
                quantity_elem = offer.find("quantity")
                quantity = 1 if presence else 0
                if quantity_elem is not None and quantity_elem.text:
                    try:
                        quantity = int(float(quantity_elem.text.strip()))
                    except (ValueError, AttributeError):
                        pass
                
                # Категорія
                category_id = offer.get("categoryId")
                category_name = categories.get(category_id, "Без категорії") if category_id else "Без категорії"
                
                # Опис
                description_elem = offer.find("description")
                description = sanitize_text(description_elem.text) if description_elem is not None and description_elem.text else ""
                
                # URL товару
                url_elem = offer.find("url")
                url = sanitize_text(url_elem.text) if url_elem is not None and url_elem.text else ""
                
                # Зображення
                picture_elem = offer.find("picture")
                picture = sanitize_text(picture_elem.text) if picture_elem is not None and picture_elem.text else ""
                
                # Валюта
                currency_elem = offer.find("currencyId")
                currency = sanitize_text(currency_elem.text) if currency_elem is not None and currency_elem.text else "UAH"
                
                product = {
                    "id": product_id,
                    "name": name,
                    "price": price,
                    "presence": presence,
                    "quantity": quantity,
                    "category_id": category_id,
                    "category_name": category_name,
                    "description": description,
                    "url": url,
                    "picture": picture,
                    "currency": currency
                }
                
                products.append(product)
                
            except Exception as e:
                print(f"⚠️ Помилка парсингу товару: {e}")
                continue
        
        return products, categories
        
    except Exception as e:
        print(f"❌ Помилка парсингу XML: {e}")
        return [], {}

def create_yml_file(products: List[Dict], categories: Dict, filename: str) -> bool:
    """Створює YML файл з правильною структурою"""
    try:
        # Створюємо XML структуру
        root = etree.Element("yml_catalog")
        root.set("date", datetime.now().strftime("%Y-%m-%d %H:%M"))
        
        shop = etree.SubElement(root, "shop")
        
        # Додаємо інформацію про магазин
        name = etree.SubElement(shop, "name")
        name.text = "API-Prom.ua Store"
        
        company = etree.SubElement(shop, "company")
        company.text = "API-Prom.ua"
        
        url_elem = etree.SubElement(shop, "url")
        url_elem.text = "https://prom.ua"
        
        # Додаємо категорії
        categories_elem = etree.SubElement(shop, "categories")
        for cat_id, cat_name in categories.items():
            category = etree.SubElement(categories_elem, "category")
            category.set("id", cat_id)
            category.text = cat_name
        
        # Додаємо товари
        offers = etree.SubElement(shop, "offers")
        
        for product in products:
            offer = etree.SubElement(offers, "offer")
            offer.set("id", str(product["id"]))
            offer.set("available", "true" if product["presence"] else "false")
            
            # Назва товару
            if product["name"]:
                name_elem = etree.SubElement(offer, "name")
                name_elem.text = product["name"]
            
            # Ціна
            if product["price"] is not None:
                price_elem = etree.SubElement(offer, "price")
                price_elem.text = str(product["price"])
            
            # Валюта
            currency_elem = etree.SubElement(offer, "currencyId")
            currency_elem.text = product["currency"]
            
            # Кількість
            quantity_elem = etree.SubElement(offer, "quantity")
            quantity_elem.text = str(product["quantity"])
            
            # Категорія
            if product["category_id"]:
                category_elem = etree.SubElement(offer, "categoryId")
                category_elem.text = str(product["category_id"])
            
            # Опис
            if product["description"]:
                description_elem = etree.SubElement(offer, "description")
                description_elem.text = product["description"]
            
            # URL товару
            if product["url"]:
                url_elem = etree.SubElement(offer, "url")
                url_elem.text = product["url"]
            
            # Зображення
            if product["picture"]:
                picture_elem = etree.SubElement(offer, "picture")
                picture_elem.text = product["picture"]
        
        # Зберігаємо файл
        tree = etree.ElementTree(root)
        tree.write(filename, encoding="utf-8", xml_declaration=True, pretty_print=True)
        
        print(f"✅ Створено YML файл: {filename} ({len(products)} товарів, {len(categories)} категорій)")
        return True
        
    except Exception as e:
        print(f"❌ Помилка створення YML файлу: {e}")
        return False

async def process_feeds():
    """Основна функція обробки фідів"""
    print("🚀 Запуск yml.generator з виправленням дублікатів")
    
    # Створюємо директорію для виводу
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Завантажуємо URL фідів
    feed_urls = load_feeds()
    if not feed_urls:
        return
    
    # Створюємо HTTP сесію
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        all_products = []
        all_categories = {}
        
        # Обробляємо фіди по черзі
        for i, url in enumerate(feed_urls, 1):
            print(f"\n📡 Обробка фіду {i}/{len(feed_urls)}: {url}")
            
            success, content = await fetch_feed(session, url)
            if not success:
                continue
            
            products, categories = parse_xml_content(content)
            all_products.extend(products)
            all_categories.update(categories)
            
            print(f"📊 Загалом товарів: {len(all_products)}, категорій: {len(all_categories)}")
        
        # Розділяємо на 3 файли
        total_products = len(all_products)
        print(f"\n📈 Загалом оброблено: {total_products} товарів, {len(all_categories)} категорій")
        
        if total_products == 0:
            print("❌ Немає товарів для обробки")
            return
        
        # Створюємо 3 YML файли
        products_per_file = total_products // 3
        
        for i in range(3):
            start_idx = i * products_per_file
            if i == 2:  # Останній файл отримує всі залишкові товари
                end_idx = total_products
            else:
                end_idx = (i + 1) * products_per_file
            
            batch_products = all_products[start_idx:end_idx]
            
            filename = os.path.join(OUTPUT_DIR, f"all_{i + 1}.yml")
            create_yml_file(batch_products, all_categories, filename)
        
        print(f"\n🎉 Генерація завершена! Створено 3 YML файли")
        print(f"📁 Файли збережено в: {OUTPUT_DIR}/")
        
        # Завантажуємо файли в GitHub
        await upload_to_github()

async def upload_to_github():
    """Завантажує YML файли в GitHub репозиторій"""
    try:
        print("\n🚀 Завантажую файли в GitHub...")
        
        # Перевіряємо чи є git репозиторій
        if not os.path.exists(".git"):
            print("❌ Не знайдено git репозиторій")
            return
        
        # Додаємо файли до git
        for i in range(1, 4):
            filename = os.path.join(OUTPUT_DIR, f"all_{i}.yml")
            if os.path.exists(filename):
                # Копіюємо файл в корінь репозиторію
                import shutil
                shutil.copy2(filename, f"all_{i}.yml")
                print(f"📋 Скопійовано: all_{i}.yml")
        
        # Git команди
        import subprocess
        
        # Скасовуємо rebase якщо активний
        try:
            subprocess.run(["git", "rebase", "--abort"], check=False)
        except:
            pass
        
        subprocess.run(["git", "add", "all_1.yml", "all_2.yml", "all_3.yml"], check=True)
        subprocess.run(["git", "commit", "-m", f"Update YML files - {datetime.now().strftime('%Y-%m-%d %H:%M')}"], check=True)
        subprocess.run(["git", "push", "origin", "main"], check=True)
        
        print("✅ Файли успішно завантажено в GitHub!")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Помилка git: {e}")
        print("💡 Спробуйте налаштувати git config:")
        print("   git config user.name 'Your Name'")
        print("   git config user.email 'your@email.com'")
    except Exception as e:
        print(f"❌ Помилка завантаження в GitHub: {e}")

if __name__ == "__main__":
    asyncio.run(process_feeds())
