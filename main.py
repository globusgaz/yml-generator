#!/usr/bin/env python3
"""
FEEDS_GENERATOR - Генератор фідів для Prom.ua
Обробляє 7 фідів → 1-4 YML файли з контролем розміру 95MB
"""

import os
import asyncio
import re
import json
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import aiohttp
import pandas as pd
from lxml import etree

# Конфігурація
FEEDS_FILE = "feeds.txt"
PREFIX_MAP_FILE = "prefix_map.json"
STATE_FILE = "product_state.json"
MAX_FILE_SIZE_MB = 80  # Зменшено для GitHub (ліміт 100MB)
MAX_FILES = 4
TIMEOUT = 30

# Google Sheets для ваших товарів
MY_PRODUCTS_SHEET_URL = os.getenv("MY_PRODUCTS_SHEET_URL", "")

# Архівація зниклих товарів
ENABLE_ARCHIVE = os.getenv("ENABLE_ARCHIVE", "true").lower() == "true"
ARCHIVE_AFTER_HOURS = int(os.getenv("ARCHIVE_AFTER_HOURS", "1"))  # швидка реакція
MAX_ARCHIVE_PER_RUN = int(os.getenv("MAX_ARCHIVE_PER_RUN", "500"))

# Енрічмент та пороги якості
ENRICH_STRICT = os.getenv("ENRICH_STRICT", "true").lower() == "true"
COMPLETENESS_THRESHOLD = int(os.getenv("COMPLETENESS_THRESHOLD", "80"))  # 0-100
DROP_NEW_BELOW_THRESHOLD = os.getenv("DROP_NEW_BELOW_THRESHOLD", "true").lower() == "true"
ARCHIVE_EXISTING_BELOW_THRESHOLD = os.getenv("ARCHIVE_EXISTING_BELOW_THRESHOLD", "true").lower() == "true"

# Заголовки для HTTP запитів
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

GSHEET_ENV_VAR = "PROM_CATEGORIES_SHEET_URL"
PRODUCTS_CONTROL_SHEET_URL = os.getenv("PRODUCTS_CONTROL_SHEET_URL", "")

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

def gsheet_to_csv_url(sheet_url: str) -> str:
    """Конвертує Google Sheets URL в CSV export URL"""
    if '/edit' in sheet_url:
        base = sheet_url.split('/edit')[0]
        return f"{base}/export?format=csv"
    return sheet_url


def load_my_products() -> List[Dict]:
    """
    Завантажує ваші власні товари з Google Sheets (експорт Prom.ua).
    Повертає список товарів у форматі для YML.
    """
    if not MY_PRODUCTS_SHEET_URL:
        print("ℹ️ Власні товари через Google Sheets не налаштовано")
        return []
    
    try:
        csv_url = gsheet_to_csv_url(MY_PRODUCTS_SHEET_URL)
        print(f"📦 Завантажую ваші власні товари: {csv_url}")
        df = pd.read_csv(csv_url)
        
        
        products: List[Dict] = []
        loaded = 0
        skipped = 0
        skipped_reasons = {"no_id": 0, "no_name": 0, "no_price": 0, "parse_error": 0}
        
        # Мапінг колонок з експорту Prom.ua
        for idx, row in df.iterrows():
            try:
                # Основні поля (за номерами колонок з експорту Prom.ua)
                product_id = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None  # Колонка 0: Код_товару
                name = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None  # Колонка 2: Назва_позиції_укр
                description = str(row.iloc[6]).strip() if pd.notna(row.iloc[6]) else ""  # Колонка 6: Опис_укр
                
                # Категорія з експорту
                category_id = str(row.iloc[26]).strip() if pd.notna(row.iloc[26]) and str(row.iloc[26]).strip() else "0"  # Колонка 26: Ідентифікатор_підрозділу
                category_name = str(row.iloc[18]).strip() if pd.notna(row.iloc[18]) else ""  # Колонка 18: Назва_групи
                
                # Парсинг ціни (колонка 8: Ціна)
                try:
                    price_str = str(row.iloc[8]).strip().replace(',', '.') if pd.notna(row.iloc[8]) else None
                    price = float(price_str) if price_str and price_str != '' and price_str.lower() != 'nan' else None
                except (ValueError, AttributeError):
                    price = None
                
                currency = str(row.iloc[9]).strip() if pd.notna(row.iloc[9]) else "UAH"  # Колонка 9: Валюта
                image_url = str(row.iloc[14]).strip() if pd.notna(row.iloc[14]) else None  # Колонка 14: Посилання_зображення
                presence_str = str(row.iloc[15]).strip().lower() if pd.notna(row.iloc[15]) else ""  # Колонка 15: Наявність
                
                # Парсинг кількості (колонка 16: Кількість)
                try:
                    quantity = int(float(str(row.iloc[16]).strip())) if pd.notna(row.iloc[16]) and str(row.iloc[16]).strip() else 0
                except (ValueError, AttributeError):
                    quantity = 0
                
                
                # Перевірка обов'язкових полів
                if not product_id:
                    skipped += 1
                    skipped_reasons["no_id"] += 1
                    continue
                if not name:
                    skipped += 1
                    skipped_reasons["no_name"] += 1
                    continue
                if not price:
                    skipped += 1
                    skipped_reasons["no_price"] += 1
                    continue
                
                # Визначення наявності
                presence = presence_str in ['в наявності', 'наявний', 'available', '+']
                
                # Формуємо товар
                product = {
                    "id": f"my_{product_id}",  # Префікс my_ для ваших товарів
                    "name": name,
                    "price": price,
                    "currency": currency.upper(),
                    "description": description if description else name,
                    "presence": presence,
                    "quantity": quantity if presence else 0,
                    "pictures": [image_url] if image_url else [],
                    "category_id": category_id,  # З експорту Prom.ua
                    "category_name": category_name,  # Назва категорії з експорту
                    "vendor": "My Store",
                    "vendor_code": product_id,
                    "url": f"https://prom.ua/p{product_id}",
                    "params": {},
                    "is_my_product": True  # Маркер для пропуску фільтра якості
                }
                
                products.append(product)
                loaded += 1
                
            except Exception as e:
                skipped += 1
                skipped_reasons["parse_error"] += 1
                if idx < 3:
                    print(f"❌ Помилка парсингу товару #{idx+1}: {e}")
                continue
        
        print(f"\n✅ Завантажено власних товарів: {loaded}")
        print(f"⚠️ Пропущено: {skipped}")
        if skipped > 0:
            print(f"   Причини пропуску:")
            print(f"   • Немає ID: {skipped_reasons['no_id']}")
            print(f"   • Немає назви: {skipped_reasons['no_name']}")
            print(f"   • Немає ціни: {skipped_reasons['no_price']}")
            print(f"   • Помилка парсингу: {skipped_reasons['parse_error']}")
        available_count = sum(1 for p in products if p.get("presence", False))
        print(f"📊 В наявності: {available_count}/{loaded}")
        
        return products
    except Exception as e:
        print(f"❌ Помилка завантаження власних товарів: {e}")
        import traceback
        traceback.print_exc()
        return []


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

def gsheet_to_csv_url(sheet_url: str) -> str:
    """Перетворює URL Google Sheets на CSV export URL."""
    if "export?format=csv" in sheet_url:
        return sheet_url
    if "/edit" in sheet_url:
        base = sheet_url.split("/edit")[0]
        return f"{base}/export?format=csv"
    # Фолбек: повертаємо як є
    return sheet_url


def is_good_category_name(name: str) -> bool:
    """Перевіряє чи назва категорії зрозуміла і не є fallback."""
    if not name or len(name.strip()) < 3:
        return False
    
    # Відкидаємо fallback назви
    bad_patterns = [
        r"^Категорія \d+$",  # "Категорія 123"
        r"^Category \d+$",    # "Category 123"  
        r"^Cat \d+$",         # "Cat 123"
        r"^\d+$",             # Тільки цифри
        r"^[A-Z]{2,}\d{3,}$", # "ABC123"
    ]
    
    for pattern in bad_patterns:
        if re.match(pattern, name.strip(), re.IGNORECASE):
            return False
    
    return True


def load_products_control_rules() -> Dict[str, str]:
    """
    Завантажує правила контролю товарів з Google Sheets.
    Формат: product_id | action (show/hide/unavailable)
    Повертає: {product_id: action}
    """
    if not PRODUCTS_CONTROL_SHEET_URL:
        print("ℹ️ Контроль товарів через Google Sheets не налаштовано")
        return {}
    
    try:
        csv_url = gsheet_to_csv_url(PRODUCTS_CONTROL_SHEET_URL)
        print(f"📋 Завантажую правила контролю товарів: {csv_url}")
        df = pd.read_csv(csv_url)
        
        rules: Dict[str, str] = {}
        loaded = 0
        
        # Очікуємо колонки: product_id (A), action (B)
        for _, row in df.iterrows():
            product_id = None
            action = None
            
            try:
                # Колонка A = product_id
                if 0 in row and pd.notna(row[0]):
                    product_id = str(row[0]).strip()
                # Колонка B = action
                if 1 in row and pd.notna(row[1]):
                    action = str(row[1]).strip().lower()
            except Exception:
                try:
                    if pd.notna(row.iloc[0]):
                        product_id = str(row.iloc[0]).strip()
                    if pd.notna(row.iloc[1]):
                        action = str(row.iloc[1]).strip().lower()
                except Exception:
                    continue
            
            if product_id and action in ['show', 'hide', 'unavailable']:
                rules[product_id] = action
                loaded += 1
        
        print(f"✅ Завантажено правил контролю: {loaded} товарів")
        print(f"   • show: {sum(1 for a in rules.values() if a == 'show')}")
        print(f"   • hide: {sum(1 for a in rules.values() if a == 'hide')}")
        print(f"   • unavailable: {sum(1 for a in rules.values() if a == 'unavailable')}")
        
        return rules
    except Exception as e:
        print(f"⚠️ Помилка завантаження правил контролю товарів: {e}")
        return {}


def load_prom_categories() -> Dict[str, str]:
    """Завантажує категорії з Google Sheets (пріоритет), потім з prom_categories.xlsx, інакше пусто."""
    # 1) Google Sheets через CSV (якщо задано змінну оточення)
    gsheet_url = os.getenv(GSHEET_ENV_VAR)
    if gsheet_url:
        try:
            csv_url = gsheet_to_csv_url(gsheet_url)
            print(f"📁 Завантажую категорії з Google Sheets: {csv_url}")
            df = pd.read_csv(csv_url)
            categories: Dict[str, str] = {}
            loaded_count = 0
            error_count = 0
            bad_names = 0

            # Очікуємо, що колонка A = ID, колонка C = Назва (позиційно)
            for _, row in df.iterrows():
                category_id = None
                category_name = None
                try:
                    # колонки за позицією: 0 -> A, 2 -> C
                    if 0 in row and pd.notna(row[0]):
                        category_id = str(int(row[0]))
                    if 2 in row and pd.notna(row[2]):
                        category_name = str(row[2]).strip()
                except Exception:
                    # fallback через .iloc
                    try:
                        if pd.notna(row.iloc[0]):
                            category_id = str(int(row.iloc[0]))
                        if pd.notna(row.iloc[2]):
                            category_name = str(row.iloc[2]).strip()
                    except Exception:
                        error_count += 1
                        continue
                
                if category_id and category_name:
                    if is_good_category_name(category_name):
                        categories[category_id] = category_name
                        loaded_count += 1
                    else:
                        bad_names += 1
                        # Не додаємо погані категорії взагалі
                else:
                    error_count += 1
            
            print(f"📋 Категорії (Google Sheets): {loaded_count} завантажено, {error_count} помилок, {bad_names} поганих назв з {df.shape[0]} рядків")
            if loaded_count:
                return categories
            else:
                print("⚠️ Порожній набір з Google Sheets, пробуємо Excel")
        except Exception as e:
            print(f"⚠️ Не вдалося прочитати Google Sheets: {e}")
            print("Пробуємо Excel...")

    # 2) Excel
    try:
        if os.path.exists("prom_categories.xlsx"):
            print("📁 Файл prom_categories.xlsx знайдено")
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
            loaded_count = 0
            error_count = 0
            bad_names = 0
            
            # Діагностика структури Excel
            print(f"📊 Excel структура: {len(df.columns)} колонок")
            for i, col in enumerate(df.columns):
                print(f"  Колонка {i}: '{col}'")
            
            for _, row in df.iterrows():
                category_id = None
                category_name = None
                if len(df.columns) >= 6:
                    id_col = df.columns[5]  # Колонка F
                    if pd.notna(row.get(id_col)):
                        try:
                            category_id = str(int(row[id_col]))
                        except (ValueError, TypeError):
                            error_count += 1
                            continue
                if len(df.columns) >= 3:
                    name_col = df.columns[2]  # Колонка C
                    if pd.notna(row.get(name_col)):
                        category_name = str(row[name_col]).strip()
                
                if category_id and category_name:
                    if is_good_category_name(category_name):
                        categories[category_id] = category_name
                        loaded_count += 1
                    else:
                        bad_names += 1
                        # Не додаємо погані категорії взагалі
            
            print(f"📋 Категорії (Excel): {loaded_count} завантажено, {error_count} помилок, {bad_names} поганих назв з {df.shape[0]} рядків")
            if loaded_count == 0:
                print("⚠️ Категорії не знайдено - використовуємо XML як fallback")
            return categories
        else:
            print("⚠️ Файл prom_categories.xlsx не знайдено, використовуємо категорії з XML")
            return {}
    except Exception as e:
        print(f"❌ Помилка завантаження prom_categories.xlsx: {e}")
        print(f"Деталі помилки: {str(e)}")
        return {}

def load_prefix_map(feed_urls: List[str]) -> Dict[str, str]:
    """Завантажує/оновлює стабільну мапу URL→префікс (f1_, f2_, ...)."""
    existing_map: Dict[str, str] = {}
    # Завантажуємо існуючу мапу, якщо є
    if os.path.exists(PREFIX_MAP_FILE):
        try:
            with open(PREFIX_MAP_FILE, "r", encoding="utf-8") as f:
                existing_map = json.load(f)
        except Exception:
            existing_map = {}
    # Залоговані префікси, що вже зайняті
    used_prefix_numbers = set()
    for prefix in existing_map.values():
        if prefix.startswith("f") and prefix.endswith("_"):
            try:
                used_prefix_numbers.add(int(prefix[1:-1]))
            except ValueError:
                continue
    # Призначаємо префікси для нових URL
    next_num = 1
    for url in feed_urls:
        if url not in existing_map:
            # Шукаємо найменший вільний номер
            while next_num in used_prefix_numbers:
                next_num += 1
            existing_map[url] = f"f{next_num}_"
            used_prefix_numbers.add(next_num)
            next_num += 1
    # Зберігаємо оновлену мапу
    try:
        with open(PREFIX_MAP_FILE, "w", encoding="utf-8") as f:
            json.dump(existing_map, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    return existing_map

def parse_xml_content(content: bytes, prom_categories: Dict[str, str], feed_prefix: str) -> Tuple[List[Dict], Dict[str, str]]:
    """Парсить XML контент і повертає список товарів та категорії"""
    try:
        # Парсимо XML
        root = etree.fromstring(content)
        
        # Використовуємо категорії з prom_categories.xlsx + доповнюємо з XML
        categories = prom_categories.copy()
        
        # Доповнюємо категоріями з XML (якщо їх немає в Excel)
        category_elements = root.findall(".//category")
        for cat in category_elements:
            cat_id = cat.get("id")
            cat_name = cat.text
            if cat_id and cat_name and cat_id not in categories:  # Додаємо тільки відсутні
                sanitized_name = sanitize_text(cat_name)
                # Додаємо тільки категорії з нормальними назвами
                if is_good_category_name(sanitized_name):
                    categories[cat_id] = sanitized_name
        
        # Знаходимо всі товари
        offers = root.findall(".//offer")
        print(f"📦 Фід {feed_prefix}: {len(offers)} товарів, {len(categories)} категорій")
        
        products = []
        seen_products = {}  # Для відстеження дублікатів: {product_id: name}
        skipped_no_id = 0
        skipped_no_name = 0
        skipped_no_price = 0
        skipped_services = 0
        skipped_bad_category = 0
        skipped_duplicates = 0
        skipped_different_products = 0  # Різні товари з однаковим ID
        category_corrections = 0  # Лічильник виправлень категорій
        
        for offer in offers:
            try:
                # Очищаємо товар від дублікатів
                offer = sanitize_offer(offer)
                
                # Витягуємо дані
                product_id = offer.get("id")
                if not product_id:
                    skipped_no_id += 1
                    continue
                
                # ОПТИМІЗАЦІЯ: Перевіряємо наявність ОДРАЗУ (щоб не обробляти відсутні товари)
                available = offer.get("available", "true")
                presence = available.lower() in ("true", "1", "yes", "available", "in_stock")
                
                # Перевіряємо кількість
                quantity_elem = offer.find("quantity")
                quantity = 1 if presence else 0
                if quantity_elem is not None and quantity_elem.text:
                    try:
                        quantity = int(float(quantity_elem.text.strip()))
                    except (ValueError, AttributeError):
                        pass
                
                # Якщо товару немає в наявності - пропускаємо (не має сенсу обробляти)
                if not presence or quantity <= 0:
                    continue
                
                # Ціна (перевіряємо рано, бо багато товарів без ціни)
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
                
                # Основна інформація
                name_elem = offer.find("name")
                name = sanitize_text(name_elem.text) if name_elem is not None and name_elem.text else ""
                
                if not name:
                    skipped_no_name += 1
                    continue
                
                # Фільтрація послуг (невигідно для дропшипінгу)
                service_keywords = [
                    'услуга', 'услуги', 'сервис', 'обслуживание', 'обслуговування',
                    'ремонт', 'настройка', 'настроювання', 'установка', 'встановлення',
                    'доставка', 'доставка', 'монтаж', 'консультация', 'консультація',
                    'диагностика', 'діагностика', 'замена', 'заміна', 'прошивка',
                    'обновление', 'оновлення', 'подключение', 'підключення', 'настройка',
                    'гарантия', 'гарантія', 'гарантийное', 'гарантійне', 'поддержка',
                    'підтримка', 'техподдержка', 'техпідтримка', 'сервисное', 'сервісне',
                    'техническое', 'технічне', 'обслуживание', 'обслуговування',
                    'абонемент', 'абонемент', 'подписка', 'підписка', 'аренда', 'оренда'
                ]
                
                # Перевіряємо назву на ключові слова послуг
                name_lower = name.lower()
                if any(keyword in name_lower for keyword in service_keywords):
                    skipped_services += 1
                    continue
                
                # Артикул з XML фіду (використовуємо для формування унікального ID)
                vendor_code_elem = offer.find("vendorCode")
                original_vendor_code = None
                if vendor_code_elem is not None and vendor_code_elem.text:
                    original_vendor_code = sanitize_text(vendor_code_elem.text)  # Оригінальний артикул (20323)
                
                # Формуємо унікальний ID: префікс_originalID_vendorCode (f6_729470_20323)
                if not product_id.startswith(feed_prefix):
                    if original_vendor_code:
                        product_id = f"{feed_prefix}{product_id}_{original_vendor_code}"  # f6_729470_20323
                    else:
                        product_id = f"{feed_prefix}{product_id}"  # f6_729470
                
                # vendorCode має бути унікальним - використовуємо product_id
                vendor_code = product_id  # f6_729470_20323 - завжди унікальний
                
                # Перевіряємо на дублікат (якщо постачальник надав той самий товар двічі)
                if product_id in seen_products:
                    previous_name = seen_products[product_id]
                    # Порівнюємо назви (нормалізовано: без пробілів, lowercase)
                    name_normalized = name.lower().replace(" ", "").replace("-", "")
                    prev_name_normalized = previous_name.lower().replace(" ", "").replace("-", "")
                    
                    if name_normalized == prev_name_normalized:
                        # Це справжній дублікат (той самий товар)
                        skipped_duplicates += 1
                        continue
                    else:
                        # Це РІЗНІ товари з однаковим ID! Додаємо суфікс до ID
                        product_id = f"{product_id}_v2"
                        vendor_code = product_id
                        skipped_different_products += 1
                        print(f"⚠️ УВАГА: Різні товари з однаковим ID! '{previous_name}' vs '{name}' → додано суфікс _v2")
                
                # Зберігаємо product_id та назву для наступних перевірок
                seen_products[product_id] = name
                
                # Категорія - спочатку беремо з XML, потім намагаємось виправити через prom_categories
                category_elem = offer.find("categoryId")
                xml_category_id = sanitize_text(category_elem.text) if category_elem is not None and category_elem.text else "0"
                xml_category_name = categories.get(xml_category_id, f"Категорія {xml_category_id}")
                
                # Спробуємо знайти краще відповідність в prom_categories за назвою товару
                better_cat_id, better_cat_name = find_category_by_keywords(name, prom_categories)
                
                if better_cat_id and better_cat_name:
                    # Знайшли кращу категорію за ключовими словами
                    category_id = better_cat_id
                    category_name = better_cat_name
                    category_corrections += 1
                    # Додаємо виправлену категорію до списку
                    if category_id not in categories:
                        categories[category_id] = category_name
                else:
                    # Використовуємо категорію з XML
                    category_id = xml_category_id
                    category_name = xml_category_name
                
                # Виробник
                vendor_elem = offer.find("vendor")
                vendor = sanitize_text(vendor_elem.text) if vendor_elem is not None and vendor_elem.text else "API-Prom.ua"
                
                # Опис
                description_elem = offer.find("description")
                description = sanitize_text(description_elem.text) if description_elem is not None and description_elem.text else ""
                
                # URL товару
                url_elem = offer.find("url")
                url = sanitize_text(url_elem.text) if url_elem is not None and url_elem.text else ""
                
                # Зображення (може бути кілька, максимум 10)
                # Відфільтровуємо проблемні фото (великі файли з 24.ecomm.plus)
                pictures = []
                for picture_elem in offer.findall("picture"):
                    if picture_elem is not None and picture_elem.text:
                        picture_url = sanitize_text(picture_elem.text)
                        
                        # Пропускаємо проблемні URL
                        if '24.ecomm.plus:8080/TrampOpt/' in picture_url:
                            continue  # Великі файли >10MB
                        
                        # Пропускаємо битіURL (обрізані або неповні)
                        if picture_url.endswith('...') or '...' in picture_url:
                            continue
                        
                        # Перевіряємо базову валідність URL
                        if not picture_url.startswith(('http://', 'https://')):
                            continue
                        
                        # Виправляємо розширення (Prom.ua підтримує тільки .jpg, .png, .gif)
                        if picture_url.upper().endswith('.JPEG'):
                            picture_url = picture_url[:-5] + '.jpg'  # .jpeg → .jpg
                        elif picture_url.upper().endswith('.PNG'):
                            picture_url = picture_url[:-4] + '.png'
                        elif picture_url.upper().endswith('.JPG'):
                            picture_url = picture_url[:-4] + '.jpg'
                        
                        pictures.append(picture_url)
                        
                        # Обмежуємо до 10 фото
                        if len(pictures) >= 10:
                            break
                
                # Якщо немає зображень, додаємо порожній список
                if not pictures:
                    pictures = []
                
                # Валюта
                currency_elem = offer.find("currencyId")
                currency = sanitize_text(currency_elem.text) if currency_elem is not None and currency_elem.text else "UAH"
                
                # Параметри товару
                params = {}
                for param_elem in offer.findall("param"):
                    param_name = param_elem.get("name", "")
                    param_value = sanitize_text(param_elem.text) if param_elem.text else ""
                    if param_name and param_value:
                        params[param_name] = param_value
                
                # СТРОГИЙ ФІЛЬТР: пропускаємо товари з поганими категоріями
                if not is_good_category_name(category_name):
                    skipped_bad_category += 1
                    continue  # Відсіюємо товари типу "Категорія 95118362"
                
                product = {
                    "id": product_id,
                    "name": name,
                    "price": price,
                    "presence": presence,
                    "quantity": quantity,
                    "category_id": category_id,
                    "category_name": category_name,
                    "vendor": vendor,
                    "vendor_code": vendor_code,
                    "description": description,
                    "url": url,
                    "pictures": pictures,
                    "currency": currency,
                    "params": params
                }
                
                products.append(product)
                
            except Exception as e:
                print(f"⚠️ Помилка парсингу товару: {e}")
                continue
        
        # Коротка статистика фіду
        total_skipped = skipped_no_id + skipped_no_name + skipped_no_price + skipped_services + skipped_bad_category + skipped_duplicates
        print(f"📊 Фід {feed_prefix}: {len(products)} оброблено, {total_skipped} пропущено ({skipped_services} послуг, {skipped_bad_category} погані категорії, {skipped_duplicates} дублікатів)")
        if skipped_different_products > 0:
            print(f"⚠️ Різні товари з однаковим ID (додано суфікс _v2): {skipped_different_products}")
        if category_corrections > 0:
            print(f"✅ Виправлено категорій за ключовими словами: {category_corrections}")
        
        # Детальна діагностика фільтрів вимкнена для зменшення логів
        # Розкоментуйте якщо потрібна детальна інформація
        # if len(products) > 0:
        #     sample_products = products[:5]
        #     print(f"🔍 Діагностика фільтрів для {feed_prefix}:")
        #     for i, p in enumerate(sample_products, 1):
        #         name = p.get("name", "")[:50]
        #         print(f"  {i}. '{name}...' | ціна={p.get('price')} | наявність={p.get('presence')}")
        
        return products, categories
        
    except Exception as e:
        print(f"❌ Помилка парсингу XML: {e}")
        return [], {}

def normalize_categories(categories: Dict[str, str]) -> tuple:
    """Нормалізує категорії - об'єднує різні ID з однаковою назвою в один ID."""
    # Фільтруємо категорії з дуже великими ID (ймовірно, неправильні)
    MAX_VALID_CATEGORY_ID = 10000000  # 10 мільйонів
    filtered_categories = {}
    for cat_id, cat_name in categories.items():
        try:
            if int(cat_id) < MAX_VALID_CATEGORY_ID:
                filtered_categories[cat_id] = cat_name
        except (ValueError, TypeError):
            # Якщо ID не число - пропускаємо
            pass
    
    # Створюємо мапу назва -> найменший ID
    name_to_id = {}
    for cat_id, cat_name in filtered_categories.items():
        if cat_name in name_to_id:
            # Використовуємо найменший ID
            if int(cat_id) < int(name_to_id[cat_name]):
                name_to_id[cat_name] = cat_id
        else:
            name_to_id[cat_name] = cat_id
    
    # Створюємо нормалізовану мапу ID -> ID
    id_mapping = {}
    for cat_id, cat_name in filtered_categories.items():
        normalized_id = name_to_id.get(cat_name)
        if normalized_id:
            id_mapping[cat_id] = normalized_id
    
    # Створюємо нормалізовану мапу категорій
    normalized_categories = {}
    for cat_name, normalized_id in name_to_id.items():
        normalized_categories[normalized_id] = cat_name
    
    bad_count = len(categories) - len(filtered_categories)
    print(f"🔄 Нормалізація категорій: {len(categories)} → {len(filtered_categories)} валідних → {len(normalized_categories)} унікальних (відфільтровано {bad_count} з великими ID)")
    return normalized_categories, id_mapping

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
        
        # Додаємо тільки категорії, які використовуються в товарах
        categories_elem = etree.SubElement(shop, "categories")
        used_categories = set()
        
        # Збираємо ID категорій, які використовуються в товарах
        for product in products:
            if product.get("category_id"):
                used_categories.add(product["category_id"])
        
        # Додаємо тільки використовувані категорії з нормальними назвами
        for cat_id in used_categories:
            if cat_id in categories:
                cat_name = categories[cat_id]
                # Перевіряємо, чи назва категорії нормальна
                if is_good_category_name(cat_name):
                    category = etree.SubElement(categories_elem, "category")
                    category.set("id", cat_id)
                    category.text = cat_name
                # Якщо назва погана - просто пропускаємо цю категорію
            # Якщо категорія не знайдена в Excel - також пропускаємо
        
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
            
            # Виробник (обов'язково для Prom.ua)
            vendor_text = product.get("vendor", "").strip()
            if not vendor_text or vendor_text.lower() in ['невідомий', 'unknown', '']:
                vendor_text = "Виробник"  # Fallback для Prom.ua
            vendor_elem = etree.SubElement(offer, "vendor")
            vendor_elem.text = vendor_text
            
            # Артикул (обов'язково для Prom.ua)
            vendor_code = product.get("vendor_code", "").strip()
            if not vendor_code:
                vendor_code = str(product["id"])  # Використовуємо ID як артикул
            vendor_code_elem = etree.SubElement(offer, "vendorCode")
            vendor_code_elem.text = vendor_code
            
            # Опис
            if product["description"]:
                description_elem = etree.SubElement(offer, "description")
                description_elem.text = product["description"]
            
            # URL товару
            if product["url"]:
                url_elem = etree.SubElement(offer, "url")
                url_elem.text = product["url"]
            
            # Зображення (максимум 10 для Prom.ua)
            pictures = product.get("pictures", [])[:10]  # Обмежуємо до 10 фото
            for picture in pictures:
                if picture:
                    # Виправляємо розширення файлів (Prom.ua підтримує тільки .jpg, .png, .gif)
                    picture_url = picture
                    # .JPEG → .jpg, .PNG → .png, .JPG → .jpg
                    if picture_url.upper().endswith('.JPEG'):
                        picture_url = picture_url[:-5] + '.jpg'  # .jpeg → .jpg
                    elif picture_url.upper().endswith('.PNG'):
                        picture_url = picture_url[:-4] + '.png'
                    elif picture_url.upper().endswith('.JPG'):
                        picture_url = picture_url[:-4] + '.jpg'
                    
                    picture_elem = etree.SubElement(offer, "picture")
                    picture_elem.text = picture_url
            
            # Параметри товару (обмеження Prom.ua: назва ≤255, значення ≤255)
            for param_name, param_value in product["params"].items():
                if param_name and param_value:
                    # Обрізаємо до 255 символів
                    param_name_str = str(param_name)[:255]
                    param_value_str = str(param_value)[:255]
                    
                    param_elem = etree.SubElement(offer, "param")
                    param_elem.set("name", param_name_str)
                    param_elem.text = param_value_str
        
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
    """Оцінює розмір товару в байтах з запасом безпеки"""
    size = 0
    
    # Базовий розмір XML структури + запас на XML escape символи та форматування
    size += 1000  # <offer> теги, атрибути, відступи, escape символи
    
    # Розмір основних полів
    for field in ["id", "name", "price", "currency", "quantity", "category_id", "description", "url", "vendor", "vendor_code"]:
        if product.get(field):
            size += len(str(product[field]).encode('utf-8'))
    
    # Розмір зображень
    for picture in product.get("pictures", []):
        if picture:
            size += len(picture.encode('utf-8'))
    
    # Розмір параметрів (кожен <param> додає теги)
    for param_name, param_value in product.get("params", {}).items():
        if param_name and param_value:
            size += len(param_name.encode('utf-8')) + len(param_value.encode('utf-8'))
            size += 50  # теги <param name="...">...</param>
    
    # Додаємо 15% запасу на XML escape (&lt; &gt; &amp; тощо) та форматування
    size = int(size * 1.15)
    
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

def load_state() -> Dict[str, Dict]:
    """Завантажує стан товарів з STATE_FILE."""
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
            return {}
    except Exception:
        return {}


def save_state(state: Dict[str, Dict]) -> None:
    """Зберігає стан товарів у STATE_FILE."""
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception:
        pass


def update_state_with_products(state: Dict[str, Dict], products: List[Dict]) -> None:
    """Оновлює last_seen та кеш полів для активних товарів."""
    now_iso = datetime.utcnow().isoformat()
    for p in products:
        pid = p.get("id")
        if not pid:
            continue
        cached = state.get(pid, {})
        cached.update({
            "last_seen": now_iso,
            "name": p.get("name"),
            "price": p.get("price"),
            "currency": p.get("currency"),
            "quantity": p.get("quantity"),
            "category_id": p.get("category_id"),
            "description": p.get("description"),
            "url": p.get("url"),
            "pictures": p.get("pictures", []),
            "vendor": p.get("vendor"),
            "vendor_code": p.get("vendor_code"),
            "params": p.get("params", {}),
        })
        state[pid] = cached


def build_archive_offers(state: Dict[str, Dict], active_ids: set) -> List[Dict]:
    """Формує список оферів для товарів, що зникли понад ARCHIVE_AFTER_HOURS."""
    if not ENABLE_ARCHIVE:
        return []
    archive_offers: List[Dict] = []
    now = datetime.utcnow()
    threshold = now.timestamp() - ARCHIVE_AFTER_HOURS * 3600
    for pid, cached in state.items():
        if pid in active_ids:
            continue
        last_seen = cached.get("last_seen")
        if not last_seen:
            continue
        try:
            ts = datetime.fromisoformat(last_seen).timestamp()
        except Exception:
            continue
        if ts <= threshold:
            archive_offers.append({
                "id": pid,
                "name": cached.get("name", pid),
                "price": cached.get("price", 0),
                "presence": False,
                "quantity": 0,
                "category_id": cached.get("category_id"),
                "category_name": "Без категорії",
                "vendor": cached.get("vendor", "API-Prom.ua"),
                "vendor_code": cached.get("vendor_code", pid),
                "description": cached.get("description", ""),
                "url": cached.get("url", ""),
                "pictures": cached.get("pictures", []),
                "currency": cached.get("currency", "UAH"),
                "params": cached.get("params", {}),
            })
            if len(archive_offers) >= MAX_ARCHIVE_PER_RUN:
                break
    return archive_offers

def normalize_word(word: str) -> str:
    """
    Нормалізує слово для кращого порівняння (стеммінг).
    Приводить до спільного кореня укр/рос варіанти.
    """
    if len(word) < 3:
        return word
    
    normalized = word.lower()
    
    # Укр/Рос варіанти → спільний корінь
    transliterations = {
        'і': 'и', 'ї': 'и', 'є': 'е', 'ґ': 'г',
    }
    
    for uk, ru in transliterations.items():
        normalized = normalized.replace(uk, ru)
    
    # Видаляємо закінчення (від найдовших до найкоротших)
    # Множина, відмінки, закінчення
    endings = ['ами', 'ями', 'ах', 'ях', 'ів', 'ей', 'ою', 'єю', 'ем', 'ом', 'им', 'ий', 'ій', 'ого', 'ому', 'ім', 'ка', 'ок', 'ик', 'ік', 'ач', 'і', 'и', 'а', 'я', 'у', 'ю']
    
    for ending in endings:
        if len(normalized) > 4 and normalized.endswith(ending):
            normalized = normalized[:-len(ending)]
            break
    
    return normalized


# Глобальний кеш для нормалізованих категорій (щоб не нормалізувати кожен раз)
_normalized_categories_cache = {}

def find_category_by_keywords(product_name: str, prom_categories: Dict[str, str]) -> tuple:
    """
    Шукає відповідну категорію за ключовими словами в назві товару.
    Повертає (category_id, category_name) або (None, None) якщо не знайдено.
    """
    global _normalized_categories_cache
    
    if not product_name or not prom_categories:
        return None, None
    
    # Ініціалізуємо кеш категорій один раз
    if not _normalized_categories_cache:
        for cat_id, cat_name in prom_categories.items():
            if not cat_name or len(cat_name.strip()) < 3:
                continue
            cat_name_lower = cat_name.lower()
            cat_words_normalized = []
            for cat_word in cat_name_lower.split():
                cat_word = cat_word.strip('.,;:!?()[]{}\"\'')
                if len(cat_word) >= 3:
                    cat_words_normalized.append(normalize_word(cat_word))
            _normalized_categories_cache[cat_id] = (cat_name, cat_name_lower, cat_words_normalized)
    
    # Нормалізуємо назву товару
    product_name_lower = product_name.lower()
    
    # Стоп-слова, які ігноруємо (не несуть смислу для категоризації)
    stop_words = {
        'для', 'на', 'з', 'та', 'і', 'в', 'по', 'від', 'до', 'під', 'над', 'або', 'при',
        'api-prom.ua', 'valeso', 'шт', 'мм', 'см', 'м', 'кг', 'г', 'л', 'мл', 'шт.',
        'комплект', 'набір', 'new', 'pro', 'max', 'mini', 'plus', 'sale', 'код',
        'black', 'white', 'red', 'blue', 'green', 'чорний', 'білий', 'червоний',
        'із', 'зі', 'без', 'про', 'через', 'після', 'перед', 'між'
    }
    
    # Whitelist: явні відповідності (товар → категорія)
    # Ключ - слово в назві товару, значення - категорія з prom_categories
    whitelist_keywords = {
        'молоток': ['Ручний інструмент', 'Інструменти'],
        'відверт': ['Ручний інструмент', 'Інструменти'],
        'дриль': ['Електроінструменти', 'Інструменти'],
        'перфоратор': ['Електроінструменти', 'Інструменти'],
        'шуруповерт': ['Електроінструменти', 'Інструменти'],
        'болгарк': ['Електроінструменти', 'Інструменти'],
        'міксер': ['Дрібна побутова техніка', 'Техніка для кухні', 'Кухонна техніка'],
        'блендер': ['Дрібна побутова техніка', 'Техніка для кухні', 'Кухонна техніка'],
        'кавоварк': ['Дрібна побутова техніка', 'Техніка для кухні', 'Кухонна техніка'],
        'чайник': ['Дрібна побутова техніка', 'Техніка для кухні', 'Кухонна техніка'],
        'лампа': ['Освітлення', 'Світильники', 'Лампи', 'Люстри'],
        'світильник': ['Освітлення', 'Світильники', 'Лампи', 'Люстри'],
        'люстр': ['Освітлення', 'Світільники', 'Лампи', 'Люстри'],
        'сумка': ['Сумки', 'Аксесуари', 'Одяг', 'Жіночі аксесуари', 'Чоловічі аксесуари'],
        'рюкзак': ['Сумки', 'Аксесуари', 'Одяг', 'Спортивні товари'],
        'ножиц': ['Ручний інструмент', 'Інструменти', 'Канцелярія'],
        'пасатиж': ['Ручний інструмент', 'Інструменти'],
        'тепловізор': ['Вимірювальні прилади', 'Інструменти', 'Обладнання'],
    }
    
    # Правила виключення: якщо в назві товару є ключ, не можна в категорії зі значень
    exclude_rules = {
        'іграшк': ['бджільництв', 'інструмент', 'будівельн', 'сантехнік', 'паливо', 'харчов', 'снек'],
        'дитяч': ['інструмент', 'будівельн', 'сантехнік', 'паливо', 'харчов', 'снек'],
        'самоклеюч': ['автокрісл', 'електронік', 'комп\'ютер', 'паливо'],
        'самоклеящ': ['автокрісл', 'електронік', 'комп\'ютер', 'паливо'],
        'панел': ['автокрісл', 'електронік', 'комп\'ютер', 'паливо', 'снек', 'харчов'],
        '3d': ['автокрісл', 'паливо', 'снек', 'харчов'],
        'декоратив': ['паливо', 'снек', 'харчов', 'інструмент'],
        'рейк': ['снек', 'харчов', 'паливо'],
        'сумк': ['тепловізор', 'прилад', 'обладнання', 'паливо', 'харчов', 'снек'],
        'ножиц': ['паливо', 'харчов', 'снек', 'продукт', 'їжа'],
        'міксер': ['комп\'ютер', 'оргтехніка', 'електронік', 'паливо'],
        'лампа': ['харчов', 'снек', 'продукт', 'їжа', 'паливо'],
        'світильник': ['харчов', 'снек', 'продукт', 'їжа', 'паливо'],
        'інструмент': ['харчов', 'снек', 'продукт', 'їжа', 'одяг', 'взуття'],
        'техніка': ['харчов', 'снек', 'паливо', 'одяг', 'взуття'],
    }
    
    # Витягуємо значущі слова з назви товару та нормалізуємо їх
    product_words = []
    product_words_normalized = []
    for word in product_name_lower.split():
        # Прибираємо розділові знаки
        word = word.strip('.,;:!?()[]{}\"\'')
        # Перевіряємо довжину та чи не є стоп-словом
        if len(word) >= 3 and word not in stop_words:
            product_words.append(word)
            product_words_normalized.append(normalize_word(word))
    
    if not product_words:
        return None, None
    
    # ЕТАП 1: Перевіряємо whitelist (явні відповідності)
    for keyword, target_categories in whitelist_keywords.items():
        if keyword in product_name_lower:
            # Шукаємо чи є така категорія в prom_categories
            for cat_id, cat_name in prom_categories.items():
                cat_name_lower = cat_name.lower()
                for target_cat in target_categories:
                    if target_cat.lower() in cat_name_lower:
                        # Знайшли точну відповідність з whitelist!
                        return cat_id, cat_name
            # Якщо категорію з whitelist не знайдено в prom_categories, продовжуємо звичайний пошук
            break
    
    # ЕТАП 2: Шукаємо категорію з найбільшою кількістю збігів слів (використовуємо кеш)
    best_match = None
    best_match_score = 0
    
    for cat_id, (cat_name, cat_name_lower, cat_words_normalized) in _normalized_categories_cache.items():
        # Перевіряємо правила виключення
        skip_category = False
        for exclude_key, exclude_cats in exclude_rules.items():
            if exclude_key in product_name_lower:
                for exclude_cat in exclude_cats:
                    if exclude_cat in cat_name_lower:
                        skip_category = True
                        break
            if skip_category:
                break
        
        if skip_category:
            continue
        
        match_score = 0
        
        # Порівнюємо нормалізовані слова (для укр/рос варіантів)
        for i, norm_word in enumerate(product_words_normalized):
            if norm_word in cat_words_normalized:
                # Довші слова дають більший вес (більш специфічні)
                original_word = product_words[i]
                weight = len(original_word) / 8.0
                match_score += weight
        
        # Якщо знайшли кращий збіг
        if match_score > best_match_score and match_score > 0:
            best_match_score = match_score
            best_match = (cat_id, cat_name)
    
    # Повертаємо результат тільки якщо є хороший збіг (мінімальний score)
    # MIN_SCORE = мінімум 2 слова по 6 символів або 1 слово довжиною 12+ символів
    MIN_SCORE = 1.5  # Підвищено для зменшення помилкових збігів
    if best_match and best_match_score >= MIN_SCORE:
        return best_match
    else:
        return None, None


def normalize_param_name(name: str) -> str:
    if not name:
        return ""
    name = sanitize_text(name)
    mapping = {
        "Тип цоколю": "Тип цоколю",
        "Матеріал": "Матеріал",
        "Матеріал каркаса": "Матеріал каркаса",
        "Матеріал плафона": "Матеріал плафона",
        "Колір": "Колір",
        "Потужність": "Потужність",
        "Розмір": "Розмір",
        "Довжина, мм": "Довжина, мм",
        "Висота, мм": "Висота, мм",
        "Ширина, мм": "Ширина, мм",
        "Кількість джерел світла": "Кількість джерел світла",
    }
    return mapping.get(name, name)


def build_clean_title(category_name: str, vendor: str, vendor_code: str, name: str) -> str:
    base = sanitize_text(name)
    # Прибрати службові коди типу SP000..., комбінації великих літер+цифр без слів
    base = re.sub(r"\b[A-ZА-ЯІЇЄ]{2,}\d{3,}\b", " ", base)
    base = re.sub(r"\bSP\d{6,}\b", " ", base, flags=re.IGNORECASE)
    base = re.sub(r"\s+", " ", base).strip()
    parts = []
    
    # НЕ додаємо category_name до назви товару - воно часто неправильне з XML фідів
    # if category_name and not base.lower().startswith(category_name.lower()):
    #     parts.append(category_name)
    
    if vendor:
        parts.append(vendor)
    # Модель/артикул як короткий ідентифікатор
    model = vendor_code if vendor_code and len(vendor_code) <= 20 else ""
    title_core = base if base else ""
    # Формуємо
    if title_core:
        parts.append(title_core)
    elif model:
        parts.append(model)
    title = " ".join(parts)
    title = re.sub(r"\s+", " ", title).strip()
    # Обмеження довжини
    if len(title) > 95:
        title = title[:95].rstrip()
    return title


def build_bullets_from_params(params: Dict[str, str]) -> List[str]:
    bullets: List[str] = []
    allow = {
        "Матеріал", "Матеріал каркаса", "Матеріал плафона", "Колір",
        "Тип цоколю", "Кількість джерел світла", "Потужність",
        "Розмір", "Довжина, мм", "Висота, мм", "Ширина, мм",
    }
    for k, v in params.items():
        k2 = normalize_param_name(k)
        if k2 in allow and v:
            bullets.append(f"{k2}: {v}")
            if len(bullets) >= 6:
                break
    return bullets


def render_description_html(name: str, bullets: List[str], params: Dict[str, str]) -> str:
    safe_name = sanitize_text(name)
    html_parts = []
    if safe_name:
        html_parts.append(f"<p><b>{safe_name}</b></p>")
    if bullets:
        html_parts.append("<ul>" + "".join([f"<li>{sanitize_text(b)}</li>" for b in bullets]) + "</ul>")
    # Легка секція характеристик (параметри дублювати не обов'язково — вони окремо передаються як <param>)
    return " ".join(html_parts)


def score_completeness(product: Dict) -> int:
    """
    Строгий scoring для Prom.ua:
    - Назва >20 символів: 20 балів
    - Ціна: 15 балів
    - Валюта: 5 балів
    - Категорія зрозуміла: 15 балів
    - Мінімум 1 валідне фото: 20 балів
    - Опис >50 символів: 15 балів
    - Хоча б 1 параметр: 10 балів
    """
    score = 0
    
    # Назва (мінімум 20 символів)
    name = product.get("name", "")
    if len(name) >= 20:
        score += 20
    elif len(name) >= 10:
        score += 10
    
    # Ціна та валюта
    if product.get("price"): 
        score += 15
    if product.get("currency"): 
        score += 5
    
    # Категорія з бонусом за зрозумілість назви
    category_id = product.get("category_id")
    category_name = product.get("category_name", "")
    if category_id:
        if is_good_category_name(category_name):
            score += 15  # Зрозуміла назва категорії
        else:
            score += 5   # Є категорія, але незрозуміла назва
    
    # Фото (СТРОГО: мінімум 1 непорожнє фото)
    pics = product.get("pictures", [])
    valid_pics = [p for p in pics if p and len(p) > 10]  # URL має бути >10 символів
    if len(valid_pics) >= 1:
        score += 20
    
    # Опис (мінімум 50 символів для змістовності)
    desc = product.get("description", "")
    if len(desc) >= 100:
        score += 15
    elif len(desc) >= 50:
        score += 10
    
    # Параметри (хоча б 1 параметр)
    params = product.get("params", {})
    if params and len(params) > 0:
        score += 10
    
    return min(score, 100)


def enrich_product(product: Dict, categories: Dict[str, str]) -> Dict:
    if not ENRICH_STRICT:
        return product
    category_name = categories.get(product.get("category_id", ""), "")
    # Назва
    product["name"] = build_clean_title(category_name, product.get("vendor", ""), product.get("vendor_code", ""), product.get("name", ""))
    # Буллети
    bullets = build_bullets_from_params(product.get("params", {}))
    # Опис
    if not product.get("description"):
        product["description"] = render_description_html(product.get("name", ""), bullets, product.get("params", {}))
    # Перерахунок скора
    product["completeness_score"] = score_completeness(product)
    return product

async def process_feeds():
    """Основна функція обробки фідів"""
    print("🚀 Запуск feeds_generator з правильними префіксами та контролем розміру")
    
    # Завантажуємо категорії з prom_categories.xlsx
    prom_categories = load_prom_categories()
    
    # Завантажуємо ваші власні товари з Google Sheets
    my_products = load_my_products()
    
    # Завантажуємо URL фідів
    feed_urls = load_feeds()
    if not feed_urls:
        return
    
    # Завантажуємо/оновлюємо стабільну мапу URL→префікс
    url_prefix_map = load_prefix_map(feed_urls)
    
    # Створюємо HTTP сесію
    connector = aiohttp.TCPConnector(limit=5)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        all_products = []
        all_categories = {}
        
        # Обробляємо фіди по черзі з СТАБІЛЬНИМИ префіксами
        for i, url in enumerate(feed_urls, 1):
            print(f"\n📡 Обробка фіду {i}/{len(feed_urls)}: {url}")
            prefix = url_prefix_map.get(url, f"f{i}_")
            
            success, content = await fetch_feed(session, url)
            if not success:
                continue
            
            products, categories = parse_xml_content(content, prom_categories, prefix)
            # Енрічмент і скоринг для кожного продукту
            enriched = []
            for p in products:
                p2 = enrich_product(p, prom_categories)
                enriched.append(p2)
            all_products.extend(enriched)
            # Додаємо категорії з XML фіду постачальника
            all_categories.update(categories)
        
        # Фільтрація товарів з поганими категоріями
        filtered_products = []
        bad_category_count = 0
        for p in all_products:
            category_name = p.get("category_name", "")
            if is_good_category_name(category_name) or not category_name:
                filtered_products.append(p)
            else:
                bad_category_count += 1
        
        if bad_category_count > 0:
            print(f"🚫 Відфільтровано товарів з поганими категоріями: {bad_category_count}")
        
        all_products = filtered_products
        
        # Фільтрація/архівація за порогом заповненості (попередній підрахунок)
        total_scored = len(all_products)
        above_threshold = 0
        below_threshold = 0
        for p in all_products:
            score = p.get("completeness_score", score_completeness(p))
            if score >= COMPLETENESS_THRESHOLD:
                above_threshold += 1
            else:
                below_threshold += 1
        print(f"\n🧮 Якість наповнення: {total_scored} товарів; ≥{COMPLETENESS_THRESHOLD}: {above_threshold}, <{COMPLETENESS_THRESHOLD}: {below_threshold}")
        
        # Діагностика completeness_score для зразків
        if total_scored > 0:
            sample_for_analysis = all_products[:3]  # Перші 3 товари
            print(f"🔍 Діагностика completeness_score:")
            for i, p in enumerate(sample_for_analysis, 1):
                score = p.get("completeness_score", score_completeness(p))
                name = p.get("name", "")[:40]
                has_name = bool(p.get("name"))
                has_price = bool(p.get("price"))
                has_currency = bool(p.get("currency"))
                has_category = bool(p.get("category_id"))
                has_pics = bool(p.get("pictures") and any(p.get("pictures", [])))
                has_desc = bool(p.get("description"))
                has_params = bool(p.get("params"))
                print(f"  {i}. '{name}...' | score={score} | name={has_name} price={has_price} currency={has_currency} cat={has_category} pics={has_pics} desc={has_desc} params={has_params}")
        # ДОДАЄМО ВАШІ ВЛАСНІ ТОВАРИ (ДО ФІЛЬТРАЦІЇ)
        if my_products:
            print(f"\n📦 Додаю ваші власні товари: {len(my_products)} шт (пропускають фільтр якості)")
            all_products.extend(my_products)
            
            # Додаємо категорії з ваших товарів до загального словника
            added_categories = 0
            for p in my_products:
                cat_id = p.get("category_id")
                cat_name = p.get("category_name")
                if cat_id and cat_name and cat_id != "0":
                    all_categories[cat_id] = cat_name
                    added_categories += 1
            if added_categories > 0:
                print(f"   ➕ Додано категорій з моїх товарів: {added_categories}")
        
        # Тимчасово залишимо всі; остаточне рішення нижче після завантаження state
        filtered_products: List[Dict] = list(all_products)
        all_products = filtered_products
        
        # Завантажуємо стан та оновлюємо last_seen по активних товарах
        state = load_state()
        update_state_with_products(state, all_products)
        
        # Визначаємо нові/існуючі та коригуємо за порогом
        final_products: List[Dict] = []
        dropped_new_low = 0
        archived_existing_low = 0
        kept_ok = 0
        avg_score_acc = 0.0
        counted = 0
        for p in all_products:
            # ВАШІ ТОВАРИ ЗАВЖДИ ПРОХОДЯТЬ
            if p.get("is_my_product"):
                final_products.append(p)
                kept_ok += 1
                score = p.get("completeness_score", score_completeness(p))
                if isinstance(score, (int, float)):
                    avg_score_acc += float(score)
                    counted += 1
                continue
            
            # Фільтруємо товари постачальників
            score = p.get("completeness_score", score_completeness(p))
            if isinstance(score, (int, float)):
                avg_score_acc += float(score)
                counted += 1
            pid = p.get("id")
            is_existing = pid in state and state[pid].get("last_seen") is not None
            
            # Якщо score вище порогу - завжди додаємо
            if score >= COMPLETENESS_THRESHOLD:
                final_products.append(p)
                kept_ok += 1
            else:
                # Якщо score нижче порогу - перевіряємо прапорці
                if not is_existing and DROP_NEW_BELOW_THRESHOLD:
                    # Новий товар з низькою якістю - пропускаємо
                    dropped_new_low += 1
                    continue
                elif is_existing and ARCHIVE_EXISTING_BELOW_THRESHOLD:
                    # Існуючий товар з низькою якістю - архівуємо
                    p_arch = dict(p)
                    p_arch["presence"] = False
                    p_arch["quantity"] = 0
                    final_products.append(p_arch)
                    archived_existing_low += 1
                else:
                    # Прапорці вимкнені - додаємо товар як є
                    final_products.append(p)
                    kept_ok += 1
        all_products = final_products
        avg_score = (avg_score_acc / counted) if counted else 0.0
        print(
            f"📉 Фільтр якості: залишено OK={kept_ok}, відсіяно нових={dropped_new_low}, заархівовано існуючих={archived_existing_low}; середня повнота={avg_score:.1f}"
        )
        
        # Побудова архівних оферів для зниклих позицій
        active_ids = {p["id"] for p in all_products}
        archive_offers = build_archive_offers(state, active_ids)
        if archive_offers:
            print(f"🗄️ Додано до архіву (available=false): {len(archive_offers)} позицій")
            all_products.extend(archive_offers)
        
        
        # Статистика по вашим товарам
        my_products_in_final = [p for p in final_products if p.get("is_my_product")]
        if my_products_in_final:
            my_available = sum(1 for p in my_products_in_final if p.get("presence"))
            my_total_score = sum(p.get("completeness_score", 0) for p in my_products_in_final)
            my_avg_score = my_total_score / len(my_products_in_final) if my_products_in_final else 0
            print(f"\n📊 Ваші товари у фіналі:")
            print(f"   • Всього: {len(my_products_in_final)}")
            print(f"   • В наявності: {my_available}")
            print(f"   • Середній score: {my_avg_score:.1f}")
        
        # Зберігаємо оновлений стан
        save_state(state)
        
        # Підраховуємо статистику ДО фільтрації
        total_before = len(all_products)
        available_before = sum(1 for p in all_products if p.get("presence", False))
        unavailable_before = total_before - available_before
        
        print(f"\n📈 Всього товарів: {total_before} ({available_before} доступно, {unavailable_before} відсутніх)")
        
        # Завантажуємо правила контролю товарів з Google Sheets
        control_rules = load_products_control_rules()
        
        # Застосовуємо правила контролю
        if control_rules:
            hidden_count = 0
            made_unavailable = 0
            forced_show = 0
            
            filtered_products = []
            for p in all_products:
                pid = p.get("id")
                action = control_rules.get(pid)
                
                if action == "hide":
                    # Видаляємо товар з файлу (Prom.ua приховає автоматично)
                    hidden_count += 1
                    continue
                elif action == "unavailable":
                    # Позначаємо як недоступний
                    p["presence"] = False
                    p["quantity"] = 0
                    made_unavailable += 1
                    filtered_products.append(p)
                elif action == "show":
                    # Примусово показуємо (навіть якщо був відсутній)
                    if not p.get("presence", False):
                        p["presence"] = True
                        forced_show += 1
                    filtered_products.append(p)
                else:
                    # Немає правила - залишаємо як є
                    filtered_products.append(p)
            
            all_products = filtered_products
            print(f"📋 Застосовано правила контролю:")
            print(f"   • Приховано: {hidden_count}")
            print(f"   • Зроблено недоступними: {made_unavailable}")
            print(f"   • Примусово показано: {forced_show}")
        
        # ФІЛЬТР: Залишаємо тільки товари в наявності
        all_products = [p for p in all_products if p.get("presence", False)]
        
        total_products = len(all_products)
        print(f"✅ Після фільтрації (тільки в наявності): {total_products} товарів")
        print(f"📊 Відфільтровано відсутніх: {unavailable_before} товарів")
        
        if total_products == 0:
            print("❌ Немає товарів для обробки")
            return
        
        # Розподіляємо товари на файли з контролем розміру
        file_batches = distribute_products(all_products, all_categories)
        
        # Нормалізуємо категорії (об'єднуємо дублікати з однаковими назвами)
        normalized_categories, id_mapping = normalize_categories(all_categories)
        
        # Оновлюємо category_id у товарах відповідно до нормалізованих категорій
        # Товари з невалідними категоріями отримують дефолтну категорію "0"
        if "0" not in normalized_categories:
            normalized_categories["0"] = "Без категорії"
        
        reassigned_count = 0
        for p in all_products:
            old_cat_id = p.get("category_id")
            if old_cat_id and old_cat_id in id_mapping:
                p["category_id"] = id_mapping[old_cat_id]
            elif old_cat_id and old_cat_id not in normalized_categories:
                # Категорія була відфільтрована - призначаємо дефолтну
                p["category_id"] = "0"
                reassigned_count += 1
        
        if reassigned_count > 0:
            print(f"⚠️ Перенаправлено {reassigned_count} товарів до категорії 'Без категорії' (невалідні категорії)")
        
        # Створюємо YML файли (статичні імена для стабільних посилань у Prom.ua)
        for i, batch_products in enumerate(file_batches, 1):
            filename = f"all_{i}.yml"
            create_yml_file(batch_products, normalized_categories, filename)
        
        print(f"🎉 Створено {len(file_batches)} YML файлів в корінь репозиторію")

if __name__ == "__main__":
    asyncio.run(process_feeds())

