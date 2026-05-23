# yml-generator

Генерує YML для [Prom.ua](https://prom.ua) з фідів у `feeds.txt` → `all_1.yml` … `all_4.yml` у репозиторії.

## Поточний режим

- **`ONLY_MY_PRODUCTS=true`** у GitHub Actions — у `all_1.yml` лише товари з Google Sheets (`my_…`)
- `feeds.txt` порожній (постачальницькі фіди вимкнено)

## Публічний YML для Prom

Після успішного GitHub Actions:

`https://raw.githubusercontent.com/globusgaz/yml-generator/main/all_1.yml`

У кабінеті Prom: **Імпорт → за посиланням** — вказати цей URL (або синхронізація через `prom-sync-api`).

## Тільки ваші товари в посиланні (без Lugi / feeds.txt)

Посилання для Prom лишається тим самим:

`https://raw.githubusercontent.com/globusgaz/yml-generator/main/all_1.yml`

Щоб у файлі були **лише** позиції з Google Sheets (`id="my_…"`), без тисяч товарів постачальника:

1. У GitHub Secrets залишити **`MY_PRODUCTS_SHEET_URL`** (таблиця з вашими товарами).
2. У workflow (або локально) увімкнути **`ONLY_MY_PRODUCTS=true`**.
3. `feeds.txt` можна **очистити** — у цьому режимі він не використовується.
4. Перегенерувати YML (`python main.py` або Actions) і оновити імпорт на Prom.

Локально:

```bash
export MY_PRODUCTS_SHEET_URL='https://docs.google.com/spreadsheets/d/.../edit#gid=0'
export ONLY_MY_PRODUCTS=true
python main.py
```

У `all_1.yml` мають залишитися лише рядки `<offer id="my_…">`. Старі `f11_` зникнуть після наступного імпорту на Prom (залежить від налаштувань синхронізації).

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
