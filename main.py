#!/usr/bin/env python3
"""
FEEDS_GENERATOR - Генератор фідів для Prom.ua
Обробляє 7 фідів → 1-4 YML файли з контролем розміру 95MB
"""

import os
import asyncio
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import aiohttp
import pandas as pd
from lxml import etree

# Конфігурація
FEEDS_FILE = "feeds.txt"
MAX_FILE_SIZE_MB = 80  # Зменшено для GitHub (ліміт 100MB)
MAX_FILES = 4
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
    
    # Видаляємо дублікати тегів available
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

def load_prom_categories() -> Dict[str, str]:
    """Завантажує категорії з prom_categories.xlsx"""
    try:
        if os.path.exists("prom_categories.xlsx"):
            print("📁 Файл prom_categories.xlsx знайдено")
            
            # Спробуємо різні двигуни для Excel
            try:
                df = pd.read_excel("prom_categories.xlsx", engine='openpyxl')
                print("✅ Excel файл прочитано через openpyxl")
            except Exception as e1:
                try:
                    df = pd.read_excel("prom_categories.xlsx", engine='xlrd')
                    print("✅ Excel файл прочитано через xlrd")
                except Exception as e2:
                    print(f"❌ Не вдалося прочитати Excel файл")
                    print(f"openpyxl помилка: {e1}")
                    print(f"xlrd помилка: {e2}")
                    return {}
            
            categories = {}
            
            print(f"📊 Доступні колонки: {list(df.columns)}")
            print(f"📊 Розмір файлу: {df.shape[0]} рядків, {df.shape[1]} колонок")
            
            # Використовуємо колонку F (Идентификатор_подраздела) як ID та колонку C (Категория3) як назву
            for _, row in df.iterrows():
                # ID з колонки F (Идентификатор_подраздела)
                category_id = None
                category_name = None
                
                # Знаходимо ID в колонці F (остання колонка з ID)
                if len(df.columns) >= 6:  # Перевіряємо що є колонка F
                    id_col = df.columns[5]  # Колонка F (індекс 5)
                    if pd.notna(row.get(id_col)):
                        try:
                            category_id = str(int(row[id_col]))
                        except (ValueError, TypeError):
                            continue
                
                # Знаходимо назву в колонці C (Категория3)
                if len(df.columns) >= 3:  # Перевіряємо що є колонка C
                    name_col = df.columns[2]  # Колонка C (індекс 2)
                    if pd.notna(row.get(name_col)):
                        category_name = str(row[name_col]).strip()
                
                if category_id and category_name:
                    categories[category_id] = category_name
                    print(f"✅ Додано категорію: {category_id} -> {category_name}")
            
            print(f"📋 Завантажено {len(categories)} категорій з prom_categories.xlsx")
            if len(categories) == 0:
                print("⚠️ Категорії не знайдено в Excel файлі")
                print(f"Доступні колонки: {list(df.columns)}")
            return categories
        else:
            print("⚠️ Файл prom_categories.xlsx не знайдено, використовуємо категорії з XML")
            return {}
    except Exception as e:
        print(f"❌ Помилка завантаження prom_categories.xlsx: {e}")
        print(f"Деталі помилки: {str(e)}")
        return {}

def parse_xml_content(content: bytes, prom_categories: Dict[str, str], feed_index: int) -> Tuple[List[Dict], Dict[str, str]]:
    """Парсить XML контент і повертає список товарів та категорії"""
    try:
        # Парсимо XML
        root = etree.fromstring(content)
        
        # Використовуємо категорії з prom_categories.xlsx або з XML
        categories = prom_categories.copy()
        if not categories:
            # Знаходимо категорії з XML як fallback
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
        skipped_no_id = 0
        skipped_no_name = 0
        skipped_no_price = 0
        
        for offer in offers:
            try:
                # Очищаємо товар від дублікатів
                offer = sanitize_offer(offer)
                
                # Витягуємо дані
                product_id = offer.get("id")
                if not product_id:
                    skipped_no_id += 1
                    continue
                
                # Основна інформація
                name_elem = offer.find("name")
                name = sanitize_text(name_elem.text) if name_elem is not None and name_elem.text else ""
                
                if not name:
                    skipped_no_name += 1
                    continue
                
                # Ціна
                price_elem = offer.find("price")
                price = None
                if price_elem is not None and price_elem.text:
                    try:
                        price = float(price_elem.text.strip().replace(",", "."))
                    except (ValueError, AttributeError):
                        pass
                
                if price is None or price <= 0:
                    skipped_no_price += 1
                    continue
                
                # Додаємо унікальний префікс на основі номера фіду
                prefix = f"f{feed_index}_"
                if not product_id.startswith(prefix):
                    product_id = f"{prefix}{product_id}"
                
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
        
        print(f"\n📊 Статистика фільтрації фіду {feed_index}:")
        print(f"✅ Оброблено товарів: {len(products)}")
        print(f"❌ Пропущено без ID: {skipped_no_id}")
        print(f"❌ Пропущено без назви: {skipped_no_name}")
        print(f"❌ Пропущено без ціни: {skipped_no_price}")
        print(f"📦 Загалом товарів у фіді: {len(offers)}")
        
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
        
        # Перевіряємо розмір файлу
        file_size = os.path.getsize(filename)
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"✅ Створено YML файл: {filename} ({len(products)} товарів, {len(categories)} категорій)")
        print(f"📏 Розмір файлу: {file_size_mb:.1f} MB")
        
        if file_size_mb > MAX_FILE_SIZE_MB:
            print(f"⚠️ УВАГА: Файл {filename} перевищує {MAX_FILE_SIZE_MB}MB ({file_size_mb:.1f}MB)!")
        elif file_size_mb > 90:
            print(f"⚠️ УВАГА: Файл {filename} наближається до ліміту GitHub 100MB ({file_size_mb:.1f}MB)!")
        
        return True
        
    except Exception as e:
        print(f"❌ Помилка створення YML файлу: {e}")
        return False

def estimate_product_size(product: Dict) -> int:
    """Оцінює розмір товару в байтах"""
    size = 0
    
    # Базовий розмір XML структури (збільшено для точності)
    size += 500  # <offer> теги та структура
    
    # Розмір полів
    for field in ["id", "name", "price", "currency", "quantity", "category_id", "description", "url", "picture"]:
        if product.get(field):
            size += len(str(product[field]).encode('utf-8'))
    
    return size

def distribute_products(products: List[Dict], categories: Dict) -> List[List[Dict]]:
    """Розподіляє товари на файли з контролем розміру 95MB"""
    max_size_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
    
    file_batches = []
    current_batch = []
    current_size = 0
    
    for product in products:
        product_size = estimate_product_size(product)
        
        # Якщо додавання товару перевищить ліміт, створюємо новий файл
        if current_size + product_size > max_size_bytes and current_batch:
            file_batches.append(current_batch)
            current_batch = [product]
            current_size = product_size
        else:
            current_batch.append(product)
            current_size += product_size
        
        # Обмежуємо кількість файлів
        if len(file_batches) >= MAX_FILES - 1:  # -1 для останнього файлу
            # Додаємо всі залишкові товари в останній файл
            current_batch.extend(products[len([item for batch in file_batches for item in batch]) + len(current_batch):])
            break
    
    # Додаємо останній файл
    if current_batch:
        file_batches.append(current_batch)
    
    print(f"📊 Розподіл товарів: {len(file_batches)} файлів")
    for i, batch in enumerate(file_batches, 1):
        estimated_size = sum(estimate_product_size(p) for p in batch) / (1024 * 1024)
        print(f"   Файл {i}: {len(batch)} товарів (~{estimated_size:.1f}MB)")
    
    return file_batches

async def process_feeds():
    """Основна функція обробки фідів"""
    print("🚀 Запуск feeds_generator з правильними префіксами та контролем розміру")
    
    # Завантажуємо категорії з prom_categories.xlsx
    prom_categories = load_prom_categories()
    
    # Завантажуємо URL фідів
    feed_urls = load_feeds()
    if not feed_urls:
        return
    
    # Створюємо HTTP сесію
    connector = aiohttp.TCPConnector(limit=5)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        all_products = []
        all_categories = {}
        
        # Обробляємо фіди по черзі з правильними префіксами
        for i, url in enumerate(feed_urls, 1):
            print(f"\n📡 Обробка фіду {i}/{len(feed_urls)}: {url}")
            
            success, content = await fetch_feed(session, url)
            if not success:
                continue
            
            products, categories = parse_xml_content(content, prom_categories, i)
            all_products.extend(products)
            all_categories.update(categories)
            
            print(f"📊 Загалом товарів: {len(all_products)}, категорій: {len(all_categories)}")
        
        # Підраховуємо статистику
        total_products = len(all_products)
        available_products = sum(1 for p in all_products if p.get("presence", False))
        unavailable_products = total_products - available_products
        
        print(f"\n📈 Загалом оброблено: {total_products} товарів, {len(all_categories)} категорій")
        print(f"✅ Доступних товарів: {available_products} ({available_products/total_products*100:.1f}%)")
        print(f"❌ Відсутніх товарів: {unavailable_products} ({unavailable_products/total_products*100:.1f}%)")
        
        if total_products == 0:
            print("❌ Немає товарів для обробки")
            return
        
        # Розподіляємо товари на файли з контролем розміру
        file_batches = distribute_products(all_products, all_categories)
        
        # Створюємо YML файли
        for i, batch_products in enumerate(file_batches, 1):
            filename = f"all_{i}.yml"
            create_yml_file(batch_products, all_categories, filename)
        
        print(f"\n🎉 Генерація завершена! Створено {len(file_batches)} YML файлів")
        print(f"📁 Файли збережено в корінь репозиторію")

if __name__ == "__main__":
    asyncio.run(process_feeds())

