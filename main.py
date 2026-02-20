#!/usr/bin/env python3
"""
FEEDS_GENERATOR - Генератор фідів для Prom.ua
Обробляє 7 фідів → 1-4 YML файли з контролем розміру 95MB
Фільтрує тільки послуги — жорсткі фільтри товарів вимкнено.
"""

import os
import asyncio
import re
import json
import hashlib
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
MY_PRODUCTS_SHEET_URL = os.environ.get("MY_PRODUCTS_SHEET_URL", "")

# Архівація зниклих товарів
ENABLE_ARCHIVE = os.environ.get("ENABLE_ARCHIVE", "true").lower() == "true"
ARCHIVE_AFTER_HOURS = int(os.environ.get("ARCHIVE_AFTER_HOURS", "1"))
MAX_ARCHIVE_PER_RUN = int(os.environ.get("MAX_ARCHIVE_PER_RUN", "500"))

# Енрічмент — без жорстких порогів якості
ENRICH_STRICT = os.environ.get("ENRICH_STRICT", "true").lower() == "true"
# Фільтр якості ВИМКНЕНО — пропускаємо всі товари крім послуг
COMPLETENESS_THRESHOLD = 0
DROP_NEW_BELOW_THRESHOLD = False
ARCHIVE_EXISTING_BELOW_THRESHOLD = False

# Дедуплікація: виключаємо дублі (зберігаємо перший), можна вимкнути через ENABLE_DEDUP=false
ENABLE_DEDUP = os.environ.get("ENABLE_DEDUP", "true").lower() == "true"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

GSHEET_ENV_VAR = "PROM_CATEGORIES_SHEET_URL"
PRODUCTS_CONTROL_SHEET_URL = os.environ.get("PRODUCTS_CONTROL_SHEET_URL", "")


def sanitize_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def sanitize_offer(elem: etree._Element) -> etree._Element:
    for child in elem.iter():
        if child.text:
            child.text = sanitize_text(child.text)
        if child.tail:
            child.tail = sanitize_text(child.tail)
    available_tags = elem.findall("available")
    if len(available_tags) > 1:
        last_available = available_tags[-1]
        for tag in available_tags:
            elem.remove(tag)
        elem.append(last_available)
    return elem


def gsheet_to_csv_url(sheet_url: str) -> str:
    if '/edit' in sheet_url:
        base = sheet_url.split('/edit')[0]
        return f"{base}/export?format=csv"
    return sheet_url


def load_my_products() -> List[Dict]:
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
        for idx, row in df.iterrows():
            try:
                product_id = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
                name = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None
                description = str(row.iloc[6]).strip() if pd.notna(row.iloc[6]) else ""
                category_id = str(row.iloc[26]).strip() if pd.notna(row.iloc[26]) and str(row.iloc[26]).strip() else "0"
                category_name = str(row.iloc[18]).strip() if pd.notna(row.iloc[18]) else ""
                try:
                    price_str = str(row.iloc[8]).strip().replace(',', '.') if pd.notna(row.iloc[8]) else None
                    price = float(price_str) if price_str and price_str != '' and price_str.lower() != 'nan' else None
                except (ValueError, AttributeError):
                    price = None
                currency = str(row.iloc[9]).strip() if pd.notna(row.iloc[9]) else "UAH"
                image_url = str(row.iloc[14]).strip() if pd.notna(row.iloc[14]) else None
                presence_str = str(row.iloc[15]).strip().lower() if pd.notna(row.iloc[15]) else ""
                try:
                    quantity = int(float(str(row.iloc[16]).strip())) if pd.notna(row.iloc[16]) and str(row.iloc[16]).strip() else 0
                except (ValueError, AttributeError):
                    quantity = 0
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
                presence = presence_str in ['в наявності', 'наявний', 'available', '+']
                product = {
                    "id": f"my_{product_id}",
                    "name": name,
                    "price": price,
                    "currency": currency.upper(),
                    "description": description if description else name,
                    "presence": presence,
                    "quantity": quantity if presence else 0,
                    "pictures": [image_url] if image_url else [],
                    "category_id": category_id,
                    "category_name": category_name,
                    "vendor": "My Store",
                    "vendor_code": product_id,
                    "url": f"https://prom.ua/p{product_id}",
                    "params": {},
                    "is_my_product": True
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
        if skipped > 0:
            print(f"⚠️ Пропущено: {skipped}")
        return products
    except Exception as e:
        print(f"❌ Помилка завантаження власних товарів: {e}")
        import traceback
        traceback.print_exc()
        return []


def load_feeds() -> List[str]:
    if not os.path.exists(FEEDS_FILE):
        print(f"❌ Файл {FEEDS_FILE} не знайдено")
        return []
    with open(FEEDS_FILE, 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip()]
    print(f"📋 Завантажено {len(urls)} URL фідів")
    return urls


async def fetch_feed(session: aiohttp.ClientSession, url: str) -> Tuple[bool, bytes]:
    try:
        print(f"🔄 Завантажую: {url}")
        auth = None
        if "api.dropshipping.ua" in url:
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


def is_good_category_name(name: str) -> bool:
    """М'яка перевірка — приймаємо майже всі категорії."""
    if not name or len(name.strip()) < 2:
        return False
    return True


def load_products_control_rules() -> Dict[str, str]:
    if not PRODUCTS_CONTROL_SHEET_URL:
        return {}
    try:
        csv_url = gsheet_to_csv_url(PRODUCTS_CONTROL_SHEET_URL)
        df = pd.read_csv(csv_url)
        rules: Dict[str, str] = {}
        for _, row in df.iterrows():
            try:
                product_id = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
                action = str(row.iloc[1]).strip().lower() if pd.notna(row.iloc[1]) else None
                if product_id and action in ['show', 'hide', 'unavailable']:
                    rules[product_id] = action
            except Exception:
                continue
        return rules
    except Exception:
        return {}


def load_prom_categories() -> Dict[str, str]:
    gsheet_url = os.environ.get(GSHEET_ENV_VAR)
    if gsheet_url:
        try:
            csv_url = gsheet_to_csv_url(gsheet_url)
            df = pd.read_csv(csv_url)
            categories: Dict[str, str] = {}
            for _, row in df.iterrows():
                try:
                    category_id = str(int(row.iloc[0])) if pd.notna(row.iloc[0]) else None
                    category_name = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None
                    if category_id and category_name:
                        categories[category_id] = category_name
                except Exception:
                    continue
            if categories:
                return categories
        except Exception:
            pass
    try:
        if os.path.exists("prom_categories.xlsx"):
            df = pd.read_excel("prom_categories.xlsx", engine='openpyxl')
            categories = {}
            for _, row in df.iterrows():
                try:
                    if len(df.columns) >= 6 and pd.notna(row.get(df.columns[5])):
                        category_id = str(int(row[df.columns[5]]))
                        category_name = str(row[df.columns[2]]).strip() if len(df.columns) >= 3 and pd.notna(row.get(df.columns[2])) else ""
                        if category_id and category_name:
                            categories[category_id] = category_name
                except Exception:
                    continue
            return categories
    except Exception:
        pass
    return {}


def load_prefix_map(feed_urls: List[str]) -> Dict[str, str]:
    existing_map: Dict[str, str] = {}
    if os.path.exists(PREFIX_MAP_FILE):
        try:
            with open(PREFIX_MAP_FILE, "r", encoding="utf-8") as f:
                existing_map = json.load(f)
        except Exception:
            pass
    used_prefix_numbers = set()
    for prefix in existing_map.values():
        if prefix.startswith("f") and prefix.endswith("_"):
            try:
                used_prefix_numbers.add(int(prefix[1:-1]))
            except ValueError:
                pass
    next_num = 1
    for url in feed_urls:
        if url not in existing_map:
            while next_num in used_prefix_numbers:
                next_num += 1
            existing_map[url] = f"f{next_num}_"
            used_prefix_numbers.add(next_num)
            next_num += 1
    try:
        with open(PREFIX_MAP_FILE, "w", encoding="utf-8") as f:
            json.dump(existing_map, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    return existing_map


# Ключові слова послуг — єдиний фільтр товарів
SERVICE_KEYWORDS = [
    'услуга', 'услуги', 'сервис', 'обслуживание', 'обслуговування',
    'ремонт', 'настройка', 'настроювання', 'установка', 'встановлення',
    'доставка', 'монтаж', 'консультация', 'консультація',
    'диагностика', 'діагностика', 'замена', 'заміна', 'прошивка',
    'обновление', 'оновлення', 'подключение', 'підключення',
    'гарантия', 'гарантія', 'гарантийное', 'гарантійне',
    'поддержка', 'підтримка', 'техподдержка', 'техпідтримка',
    'сервисное', 'сервісне', 'техническое', 'технічне',
    'абонемент', 'подписка', 'підписка', 'аренда', 'оренда'
]


def parse_xml_content(content: bytes, prom_categories: Dict[str, str], feed_prefix: str) -> Tuple[List[Dict], Dict[str, str]]:
    try:
        root = etree.fromstring(content)
        categories = prom_categories.copy()
        category_elements = root.findall(".//category")
        for cat in category_elements:
            cat_id = cat.get("id")
            cat_name = cat.text
            if cat_id and cat_name:
                sanitized_name = sanitize_text(cat_name)
                if sanitized_name:
                    categories[cat_id] = sanitized_name
        offers = root.findall(".//offer")
        print(f"📦 Фід {feed_prefix}: {len(offers)} товарів, {len(categories)} категорій")
        products = []
        seen_products = {}
        skipped_services = 0
        for offer in offers:
            try:
                offer = sanitize_offer(offer)
                product_id = offer.get("id")
                if not product_id:
                    continue
                available = offer.get("available", "true")
                presence = available.lower() in ("true", "1", "yes", "available", "in_stock")
                quantity_elem = offer.find("quantity")
                quantity = 1 if presence else 0
                if quantity_elem is not None and quantity_elem.text:
                    try:
                        quantity = int(float(quantity_elem.text.strip()))
                    except (ValueError, AttributeError):
                        pass
                if not presence or quantity <= 0:
                    continue
                price_elem = offer.find("price")
                price = None
                if price_elem is not None and price_elem.text:
                    try:
                        price = float(price_elem.text.strip().replace(",", "."))
                    except (ValueError, AttributeError):
                        pass
                if price is None or price <= 0:
                    continue
                name_elem = offer.find("name")
                name = sanitize_text(name_elem.text) if name_elem is not None and name_elem.text else ""
                if not name:
                    continue
                # ЄДИНИЙ ФІЛЬТР: тільки послуги
                name_lower = name.lower()
                if any(kw in name_lower for kw in SERVICE_KEYWORDS):
                    skipped_services += 1
                    continue
                vendor_code_elem = offer.find("vendorCode")
                original_vendor_code = sanitize_text(vendor_code_elem.text) if vendor_code_elem is not None and vendor_code_elem.text else None
                if not product_id.startswith(feed_prefix):
                    product_id = f"{feed_prefix}{product_id}_{original_vendor_code}" if original_vendor_code else f"{feed_prefix}{product_id}"
                if len(product_id) > 25:
                    hash_part = hashlib.md5(product_id.encode()).hexdigest()[:18]
                    vendor_code = f"{feed_prefix}{hash_part}"
                else:
                    vendor_code = product_id
                if product_id in seen_products:
                    prev = seen_products[product_id]
                    if name.lower().replace(" ", "") == prev.lower().replace(" ", ""):
                        continue
                    product_id = f"{product_id}_v2"
                    vendor_code = hashlib.md5(product_id.encode()).hexdigest()[:18] if len(product_id) > 25 else product_id
                seen_products[product_id] = name
                category_elem = offer.find("categoryId")
                xml_category_id = sanitize_text(category_elem.text) if category_elem is not None and category_elem.text else "0"
                category_name = categories.get(xml_category_id, f"Категорія {xml_category_id}" if xml_category_id != "0" else "Без категорії")
                vendor_elem = offer.find("vendor")
                vendor = sanitize_text(vendor_elem.text) if vendor_elem is not None and vendor_elem.text else "API-Prom.ua"
                description_elem = offer.find("description")
                description = sanitize_text(description_elem.text) if description_elem is not None and description_elem.text else ""
                url_elem = offer.find("url")
                url = sanitize_text(url_elem.text) if url_elem is not None and url_elem.text else ""
                pictures = []
                for picture_elem in offer.findall("picture"):
                    if picture_elem is not None and picture_elem.text:
                        pic = sanitize_text(picture_elem.text)
                        if pic and pic.startswith(('http://', 'https://')) and '24.ecomm.plus:8080/TrampOpt/' not in pic and '...' not in pic:
                            pictures.append(pic)
                            if len(pictures) >= 10:
                                break
                currency_elem = offer.find("currencyId")
                currency = sanitize_text(currency_elem.text) if currency_elem is not None and currency_elem.text else "UAH"
                params = {}
                for param_elem in offer.findall("param"):
                    pn = param_elem.get("name", "")
                    pv = sanitize_text(param_elem.text) if param_elem.text else ""
                    if pn and pv:
                        params[pn] = pv
                product = {
                    "id": product_id,
                    "name": name,
                    "price": price,
                    "presence": presence,
                    "quantity": quantity,
                    "category_id": xml_category_id,
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
                continue
        print(f"📊 Фід {feed_prefix}: {len(products)} товарів (пропущено {skipped_services} послуг)")
        return products, categories
    except Exception as e:
        print(f"❌ Помилка парсингу XML: {e}")
        return [], {}


def _extract_base_id(product_id: str) -> str:
    """Витягує базовий ідентифікатор без префікса (f1_, f2_, my_)."""
    if not product_id:
        return ""
    parts = product_id.split("_", 1)
    return parts[1] if len(parts) > 1 else product_id


def _normalize_name(name: str) -> str:
    """Нормалізує назву для порівняння."""
    if not name:
        return ""
    return re.sub(r"\s+", " ", name.lower().strip())


def deduplicate_products(products: List[Dict]) -> Tuple[List[Dict], int]:
    """
    Виключає дублікати — зберігає перший товар, решту відкидає.
    Ключ: (base_id, name_normalized). Товари my_ не дедуплікуються.
    Повертає (список без дублів, кількість виключених).
    """
    if not ENABLE_DEDUP:
        return products, 0
    seen: Dict[tuple, int] = {}
    result = []
    excluded = 0
    for i, p in enumerate(products):
        if p.get("is_my_product"):
            result.append(p)
            continue
        pid = p.get("id", "")
        name = p.get("name", "")
        base_id = _extract_base_id(pid)
        name_norm = _normalize_name(name)
        key = (base_id, name_norm)
        if key in seen:
            excluded += 1
            continue
        seen[key] = i
        result.append(p)
    return result, excluded


def normalize_categories(categories: Dict[str, str]) -> tuple:
    filtered = {k: v for k, v in categories.items() if k and v}
    name_to_id = {}
    for cat_id, cat_name in filtered.items():
        if cat_name not in name_to_id:
            name_to_id[cat_name] = cat_id
    id_mapping = {cid: name_to_id.get(cname, cid) for cid, cname in filtered.items()}
    normalized = {name_to_id[n]: n for n in name_to_id}
    return normalized, id_mapping


def create_yml_file(products: List[Dict], categories: Dict, filename: str) -> bool:
    try:
        root = etree.Element("yml_catalog")
        root.set("date", datetime.now().strftime("%Y-%m-%d %H:%M"))
        shop = etree.SubElement(root, "shop")
        etree.SubElement(shop, "name").text = "API-Prom.ua Store"
        etree.SubElement(shop, "company").text = "API-Prom.ua"
        etree.SubElement(shop, "url").text = "https://prom.ua"
        categories_elem = etree.SubElement(shop, "categories")
        used_categories = {p.get("category_id") for p in products if p.get("category_id") and p.get("category_id") != "0"}
        for cat_id in used_categories:
            if cat_id in categories:
                cat = etree.SubElement(categories_elem, "category")
                cat.set("id", cat_id)
                cat.text = categories[cat_id]
        offers = etree.SubElement(shop, "offers")
        for product in products:
            offer = etree.SubElement(offers, "offer")
            offer.set("id", str(product["id"]))
            offer.set("available", "true" if product["presence"] else "false")
            etree.SubElement(offer, "name").text = product["name"]
            etree.SubElement(offer, "price").text = str(product["price"])
            etree.SubElement(offer, "currencyId").text = product["currency"]
            etree.SubElement(offer, "quantity").text = str(product["quantity"])
            if product.get("category_id") and product["category_id"] != "0":
                etree.SubElement(offer, "categoryId").text = str(product["category_id"])
            vendor_text = product.get("vendor", "").strip() or "Виробник"
            etree.SubElement(offer, "vendor").text = vendor_text
            etree.SubElement(offer, "vendorCode").text = product.get("vendor_code", str(product["id"]))
            if product.get("description"):
                etree.SubElement(offer, "description").text = product["description"]
            if product.get("url"):
                etree.SubElement(offer, "url").text = product["url"]
            for pic in product.get("pictures", [])[:10]:
                if pic:
                    etree.SubElement(offer, "picture").text = pic
            for pn, pv in product.get("params", {}).items():
                if pn and pv:
                    pe = etree.SubElement(offer, "param")
                    pe.set("name", str(pn)[:255])
                    pe.text = str(pv)[:255]
        tree = etree.ElementTree(root)
        tree.write(filename, encoding="utf-8", xml_declaration=True, pretty_print=True)
        print(f"✅ Створено: {filename} ({len(products)} товарів)")
        return True
    except Exception as e:
        print(f"❌ Помилка: {e}")
        return False


def estimate_product_size(product: Dict) -> int:
    size = 1000
    for f in ["id", "name", "price", "description", "url"]:
        if product.get(f):
            size += len(str(product[f]).encode('utf-8'))
    for pic in product.get("pictures", []):
        size += len(str(pic).encode('utf-8'))
    return int(size * 1.15)


def distribute_products(products: List[Dict], categories: Dict) -> List[List[Dict]]:
    max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
    batches = []
    current = []
    size = 0
    for p in products:
        s = estimate_product_size(p)
        if size + s > max_bytes and current:
            batches.append(current)
            current = [p]
            size = s
        else:
            current.append(p)
            size += s
        if len(batches) >= MAX_FILES - 1:
            break
    if current:
        batches.append(current)
    return batches


def load_state() -> Dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state: Dict) -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception:
        pass


def update_state_with_products(state: Dict, products: List[Dict]) -> None:
    now = datetime.utcnow().isoformat()
    for p in products:
        pid = p.get("id")
        if pid:
            state[pid] = state.get(pid, {})
            state[pid]["last_seen"] = now


def build_archive_offers(state: Dict, active_ids: set) -> List[Dict]:
    if not ENABLE_ARCHIVE:
        return []
    archive = []
    threshold = datetime.utcnow().timestamp() - ARCHIVE_AFTER_HOURS * 3600
    for pid, cached in state.items():
        if pid in active_ids:
            continue
        ls = cached.get("last_seen")
        if not ls:
            continue
        try:
            if datetime.fromisoformat(ls).timestamp() <= threshold:
                archive.append({
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
                if len(archive) >= MAX_ARCHIVE_PER_RUN:
                    break
        except Exception:
            pass
    return archive


def score_completeness(product: Dict) -> int:
    return 100  # Всі проходять


def enrich_product(product: Dict, categories: Dict) -> Dict:
    return product


async def process_feeds():
    print("🚀 Запуск feeds_generator (фільтр тільки послуги)")
    prom_categories = load_prom_categories()
    my_products = load_my_products()
    feed_urls = load_feeds()
    if not feed_urls:
        return
    url_prefix_map = load_prefix_map(feed_urls)
    connector = aiohttp.TCPConnector(limit=5)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        all_products = []
        all_categories = {}
        for i, url in enumerate(feed_urls, 1):
            prefix = url_prefix_map.get(url, f"f{i}_")
            success, content = await fetch_feed(session, url)
            if not success:
                continue
            products, categories = parse_xml_content(content, prom_categories, prefix)
            for p in products:
                all_products.append(enrich_product(p, prom_categories))
            all_categories.update(categories)
        if my_products:
            all_products.extend(my_products)
            for p in my_products:
                cid, cname = p.get("category_id"), p.get("category_name")
                if cid and cname and cid != "0":
                    all_categories[cid] = cname
        all_products, dedup_excluded = deduplicate_products(all_products)
        if dedup_excluded > 0:
            print(f"🔄 Дедуплікація: виключено {dedup_excluded} дублів, залишено {len(all_products)} унікальних")
        state = load_state()
        update_state_with_products(state, all_products)
        active_ids = {p["id"] for p in all_products}
        archive = build_archive_offers(state, active_ids)
        if archive:
            all_products.extend(archive)
        control_rules = load_products_control_rules()
        if control_rules:
            filtered = []
            for p in all_products:
                action = control_rules.get(p.get("id"))
                if action == "hide":
                    continue
                if action == "unavailable":
                    p["presence"] = False
                    p["quantity"] = 0
                elif action == "show":
                    p["presence"] = True
                filtered.append(p)
            all_products = filtered
        all_products = [p for p in all_products if p.get("presence", False)]
        if not all_products:
            print("❌ Немає товарів")
            return
        save_state(state)
        batches = distribute_products(all_products, all_categories)
        norm_cats, id_mapping = normalize_categories(all_categories)
        if "0" not in norm_cats:
            norm_cats["0"] = "Без категорії"
        for p in all_products:
            oid = p.get("category_id")
            p["category_id"] = id_mapping.get(oid, oid) if oid in norm_cats else "0"
        for i, batch in enumerate(batches, 1):
            create_yml_file(batch, norm_cats, f"all_{i}.yml")
        print(f"🎉 Створено {len(batches)} YML файлів, {len(all_products)} товарів")


if __name__ == "__main__":
    asyncio.run(process_feeds())
