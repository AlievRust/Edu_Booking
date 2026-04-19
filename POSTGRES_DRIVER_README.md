# PostgresDriver — документация

Синхронный драйвер для работы с PostgreSQL на базе `psycopg2`. Предоставляет
контекстные менеджеры, CRUD-методы и утилиты для повседневной работы с БД.

Параметры подключения читаются из файла `.env` через `dotenv`.

---

## Установка зависимостей

```bash
pip install psycopg2-binary dotenv
```

---

## Настройка подключения

### 1. Скопируйте и заполните `.env`

В корне проекта создайте файл `.env` по шаблону `.env.example`:

```env
# .env
DB_NAME=my_database
DB_USER=postgres
DB_PASSWORD=secret
DB_HOST=localhost
DB_PORT=5432
```

### Переменные окружения

| Переменная      | Обязательная | По умолчанию   | Описание                |
|-----------------|:------------:|----------------|-------------------------|
| `DB_NAME`       | **да**       | —              | Имя базы данных         |
| `DB_USER`       | **да**       | —              | Пользователь PostgreSQL |
| `DB_PASSWORD`   | нет          | `""` (пусто)   | Пароль                  |
| `DB_HOST`       | нет          | `localhost`    | Адрес сервера           |
| `DB_PORT`       | нет          | `5432`         | Порт сервера            |

> ⚠️ Файл `.env` добавьте в `.gitignore`, чтобы не коммитить пароли.

### 2. Используйте драйвер

```python
from postgres_driver import PostgresDriver

# Параметры берутся из .env автоматически
db = PostgresDriver()
db.connect()
# ...
db.disconnect()
```

---

## Управление соединением

### Ручное подключение / отключение

```python
db = PostgresDriver()
db.connect()

# ... работа с БД ...

db.disconnect()

# Проверить статус
print(db.is_connected)  # True / False
```

### Указание пути к `.env`

По умолчанию драйвер ищет файл `.env` в текущей рабочей директории.
Можно указать другой путь:

```python
db = PostgresDriver(env_path="/path/to/project/.env")
```

### Контекстный менеджер (рекомендуемый способ)

Соединение автоматически открывается при входе и закрывается при выходе.
При ошибке внутри блока транзакция откатывается.

```python
with PostgresDriver() as db:
    db.insert("users", {"name": "Alice"})
    # При выходе из блока — disconnect()
```

### Явное управление транзакцией

Не закрывает соединение — только управляет `COMMIT` / `ROLLBACK`.

```python
with PostgresDriver() as db:
    with db.transaction():
        db.insert("accounts", {"user_id": 1, "balance": 100})
        db.update("accounts", {"balance": 50}, conditions={"user_id": 1})
        # Если произойдёт ошибка — оба запроса будут откатаны
```

---

## CRUD-операции

### INSERT — вставка

**Одна строка:**

```python
# Без возврата данных
db.insert("users", {"name": "Alice", "email": "alice@example.com"})

# С возвратом созданной строки
row = db.insert("users", {"name": "Bob"}, returning="*")
# → {'id': 2, 'name': 'Bob', 'email': None}

# Вернуть только id
row = db.insert("users", {"name": "Charlie"}, returning="id")
# → {'id': 3}
```

| Параметр     | Тип          | Описание |
|--------------|--------------|----------|
| `table`      | `str`        | Имя таблицы |
| `data`       | `dict`       | `{столбец: значение}` |
| `returning`  | `str` или `None` | Выражение `RETURNING` (`"id"`, `"*"`) |
| `commit`     | `bool`       | Автокоммит (по умолчанию `True`) |

**Массовая вставка:**

```python
rows = [
    {"name": "Alice", "email": "a@example.com"},
    {"name": "Bob",   "email": "b@example.com"},
    {"name": "Carol", "email": "c@example.com"},
]
inserted = db.insert_many("users", rows)
# → 3  (количество вставленных строк)
```

> Все словари должны иметь **одинаковый** набор ключей.

---

### SELECT — чтение

**Выборка с фильтрацией:**

```python
# Все строки
users = db.select("users")
# → [{'id': 1, 'name': 'Alice'}, ...]

# Определённые столбцы + условие
users = db.select("users", columns=["id", "name"], conditions={"active": True})

# С сортировкой и лимитом
users = db.select("users", order_by="created_at DESC", limit=10)

# С пагинацией
page = db.select("users", limit=10, offset=20)
```

| Параметр      | Тип              | Описание |
|---------------|------------------|----------|
| `table`       | `str`            | Имя таблицы |
| `columns`     | `list[str]`      | Список столбцов (по умолчанию `["*"]`) |
| `conditions`  | `dict` или `None` | Условия `WHERE` (`{столбец: значение}`) |
| `order_by`    | `str` или `None` | `ORDER BY` (например `"name ASC"`) |
| `limit`       | `int` или `None` | Ограничение строк |
| `offset`      | `int` или `None` | Смещение (пагинация) |

> **Значение `None` в conditions** генерирует `IS NULL`:
> ```python
> db.select("users", conditions={"email": None})
> # WHERE email IS NULL
> ```

**Одна строка:**

```python
user = db.select_one("users", conditions={"id": 1})
# → {'id': 1, 'name': 'Alice', ...} или None
```

---

### UPDATE — обновление

```python
# Обновить по условию
db.update("users", {"name": "Bob"}, conditions={"id": 1})

# С возвратом обновлённой строки
row = db.update("users", {"name": "Bob"}, conditions={"id": 1}, returning="*")
# → {'id': 1, 'name': 'Bob', 'email': 'alice@example.com'}
```

| Параметр      | Тип          | Описание |
|---------------|--------------|----------|
| `table`       | `str`        | Имя таблицы |
| `data`        | `dict`       | Новые значения `{столбец: значение}` |
| `conditions`  | `dict`       | Условия `WHERE` (если не указаны — обновятся **все** строки!) |
| `returning`   | `str`        | Выражение `RETURNING` |
| `commit`      | `bool`       | Автокоммит |

---

### DELETE — удаление

```python
# Удалить по условию
db.delete("users", conditions={"id": 1})

# С возвратом удалённой строки
row = db.delete("users", conditions={"id": 1}, returning="*")
# → {'id': 1, 'name': 'Bob', 'email': 'bob@example.com'}
```

| Параметр      | Тип          | Описание |
|---------------|--------------|----------|
| `table`       | `str`        | Имя таблицы |
| `conditions`  | `dict`       | Условия `WHERE` |
| `returning`   | `str`        | Выражение `RETURNING` |
| `commit`      | `bool`       | Автокоммит |

> ⚠️ **Вызов `delete` без `conditions` удалит ВСЕ строки таблицы!**

---

## Произвольные SQL-запросы

### execute — выполнение запроса

```python
cursor = db.execute("SELECT version()")
print(cursor.fetchone())

# С параметрами (защита от SQL-инъекций)
cursor = db.execute("SELECT * FROM users WHERE name = %s", ("Alice",))
```

### fetchone — одна строка

```python
row = db.fetchone("SELECT * FROM users WHERE id = %s", (1,))
# → {'id': 1, 'name': 'Alice'} или None
```

### fetchall — все строки

```python
rows = db.fetchall("SELECT * FROM users WHERE active = %s", (True,))
# → [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
```

### fetchvalue — одно значение

```python
total = db.fetchvalue("SELECT COUNT(*) FROM users")
# → 42

max_id = db.fetchvalue("SELECT MAX(id) FROM users")
# → 42
```

---

## Утилиты

### count — количество строк

```python
total = db.count("users")
# → 42

active = db.count("users", conditions={"active": True})
# → 30
```

### exists — проверка существования

```python
if db.exists("users", {"email": "alice@example.com"}):
    print("Пользователь уже существует")
```

### table_exists — существует ли таблица

```python
if not db.table_exists("users"):
    print("Таблица users не найдена")
```

### table_columns — список столбцов

```python
cols = db.table_columns("users")
# → ['id', 'name', 'email', 'active', 'created_at']
```

### truncate — очистка таблицы

```python
# Обычная очистка
db.truncate("logs")

# С каскадом (для таблиц с внешними ключами)
db.truncate("users", cascade=True)
```

### copy_expert — массовый импорт / экспорт

```python
# Импорт из CSV
db.copy_expert(
    "COPY users(name, email) FROM STDIN WITH CSV HEADER",
    "import.csv"
)

# Экспорт в файловый объект
with open("export.csv", "w", encoding="utf-8") as f:
    db.copy_expert("COPY users TO STDOUT WITH CSV HEADER", f)
```

---

## Использование во внешних проектах

1. Скопируйте файл `postgres_driver.py` в свой проект.
2. Создайте файл `.env` по шаблону `.env.example`.
3. Подключайте драйвер:

```python
from postgres_driver import PostgresDriver

# Читает .env из текущей директории
with PostgresDriver() as db:
    users = db.select("users")
    print(users)
```

Если `.env` находится в другом месте:

```python
db = PostgresDriver(env_path="../secrets/.env")
```

---

## Безопасность

- Все методы используют **параметризованные запросы** (`%s`), что защищает от SQL-инъекций.
- Параметры никогда не интерполируются в строку SQL напрямую.
- При ошибке транзакция автоматически откатывается.
- Пароли хранятся **только** в файле `.env`, который не попадает в Git.

---

## Полная таблица методов

| Метод | Возвращает | Описание |
|-------|-----------|----------|
| `connect()` | `None` | Установить соединение |
| `disconnect()` | `None` | Закрыть соединение |
| `is_connected` | `bool` | Статус соединения (свойство) |
| `transaction()` | `contextmanager` | Явное управление транзакцией |
| `execute(query, params)` | `cursor` | Произвольный SQL-запрос |
| `fetchone(query, params)` | `dict \| None` | Одна строка |
| `fetchall(query, params)` | `list[dict]` | Все строки |
| `fetchvalue(query, params)` | `Any` | Одно скалярное значение |
| `insert(table, data)` | `dict \| None` | Вставить строку |
| `insert_many(table, rows)` | `int` | Массовая вставка |
| `select(table, ...)` | `list[dict]` | Выборка строк |
| `select_one(table, ...)` | `dict \| None` | Одна строка |
| `update(table, data, ...)` | `dict \| None` | Обновить строки |
| `delete(table, conditions, ...)` | `dict \| None` | Удалить строки |
| `count(table, conditions)` | `int` | Количество строк |
| `exists(table, conditions)` | `bool` | Проверка существования |
| `table_exists(table)` | `bool` | Существует ли таблица |
| `table_columns(table)` | `list[str]` | Список столбцов |
| `truncate(table, cascade)` | `None` | Очистить таблицу |
| `copy_expert(sql, file)` | `None` | Массовый импорт / экспорт |
