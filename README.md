# yml-generator

Генерує YML для [Prom.ua](https://prom.ua) з фідів у `feeds.txt` → `all_1.yml` … `all_4.yml` у репозиторії.

## Поточний фід

- `feeds.txt` — URL постачальника (зараз Lugi: `feed.lugi.com.ua`)
- Префікс ID у YML: `f11_` (див. `prefix_map.json`)

## Публічний YML для Prom

Після успішного GitHub Actions:

`https://raw.githubusercontent.com/globusgaz/yml-generator/main/all_1.yml`

У кабінеті Prom: **Імпорт → за посиланням** — вказати цей URL (або синхронізація через `prom-sync-api`).

## Google Sheets (власні товари)

Секрет репозиторію: **`MY_PRODUCTS_SHEET_URL`** — посилання на таблицю (перегляд для всіх з посиланням).

Експорт: `.../edit#gid=0` → `.../export?format=csv&gid=0` (скрипт робить це сам).

Очікувані **колонки за індексом** (з нуля):

| Колонка | Поле |
|--------|------|
| A (0) | ID товару |
| C (2) | Назва |
| I (8) | Ціна |
| J (9) | Валюта |
| O (14) | Фото URL |
| P (15) | Наявність (`в наявності`, `+`, `available`…) |
| Q (16) | Кількість |
| S (18) | Назва категорії |
| G (6) | Опис |
| AA (26) | ID категорії Prom |

Рядки **без ID, назви або ціни** пропускаються. Якщо в колонці «наявність» інший текст — товар **виключається з YML** (не потрапляє в `all_1.yml`).

У GitHub Actions товари з таблиці **підтягуються** (id `my_…`). Це **не та сама помилка**, що з Lugi: Lugi часто не завантажується в CI; таблиця — окремий канал.

Оновлення на Prom: після зміни таблиці дочекайтесь workflow (щогодини) + синхронізація `prom-sync-api`.

## Якщо товари не з’являються

1. Відкрийте `all_1.yml` на GitHub — має бути **тисячі** `<offer>`, є рядки з `f11_`. Якщо лише ~17 шт. з `my_` — фід Lugi **не завантажився** в CI (див. Actions → лог «Завантажено … байт»).
2. Запустіть workflow вручну: **Actions → Generate Prom Feed → Run workflow**.
3. Переконайтесь, що `prom-sync-api` отримав `repository_dispatch` (крок workflow «Trigger prom-sync-api»).
4. Категорії з фіду Lugi — свої ID; для відображення на Prom потрібне зіставлення з категоріями магазину (`PROM_CATEGORIES_SHEET_URL` або логіка в `prom-sync-api`).

## Локально

```bash
pip install -r requirements.txt
python main.py
```

Змінні: `TIMEOUT` (сек., за замовч. 300), `FEED_FETCH_RETRIES`, `MY_PRODUCTS_SHEET_URL`, `PROM_CATEGORIES_SHEET_URL`.
