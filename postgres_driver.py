"""
Модуль-драйвер для работы с базой данных PostgreSQL.

Предоставляет синхронный класс PostgresDriver с контекстным менеджером
и полным набором CRUD-методов. Подходит для использования как в текущем
проекте, так и во внешних модулях.

Параметры подключения читаются из файла .env (dotenv).
Ожидаемые переменные:
    DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT

Зависимости: psycopg2-binary, dotenv
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Optional, Sequence

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Ключи переменных окружения, используемых для подключения
_ENV_KEYS = ("DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT")


class PostgresDriver:
    """Драйвер подключения к PostgreSQL с набором CRUD-операций.

    Параметры подключения загружаются из файла ``.env`` автоматически.
    Если файл не найден — используется переменная окружения ``DATABASE_URL``
    или отдельные переменные ``DB_*``, уже заданные в системе.

    Параметры конструктора
    ----------------------
    env_path : str или Path, optional
        Путь к файлу ``.env``. По умолчанию ``".env"`` в текущей рабочей
        директории. Если файл не существует — молча пропускается (полагается
        на уже заданные переменные окружения).
    autocommit : bool
        Если ``True``, каждый запрос коммитится автоматически.
        По умолчанию ``False`` — транзакция управляется вручную (или
        автоматически внутри методов драйвера).
    """

    def __init__(
        self,
        env_path: Optional[str | Path] = None,
        autocommit: bool = False,
    ) -> None:
        # Загружаем .env
        path = Path(env_path) if env_path else Path(".env")
        load_dotenv(dotenv_path=path)

        # Собираем параметры подключения из окружения
        self._db_params: dict[str, Any] = {
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD", ""),
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
        }

        # Проверяем обязательные параметры
        missing = [k for k in ("dbname", "user") if not self._db_params[k]]
        if missing:
            raise ValueError(
                f"Не заданы обязательные переменные окружения: "
                f"{', '.join('DB_' + k.upper() if k != 'dbname' else 'DB_NAME' for k in missing)}. "
                f"Проверьте файл .env ({path.resolve()})."
            )

        self._autocommit = autocommit
        self._connection: Optional[psycopg2.extensions.connection] = None

    # ------------------------------------------------------------------ #
    #  Подключение / отключение
    # ------------------------------------------------------------------ #

    def connect(self) -> None:
        """Установить соединение с базой данных.

        Параметры подключения берутся из переменных окружения (файл ``.env``).
        При повторном вызове закрывает предыдущее соединение.
        """
        if self._connection is not None and not self._connection.closed:
            self.disconnect()

        self._connection = psycopg2.connect(**self._db_params)
        self._connection.autocommit = self._autocommit
        logger.info(
            "Соединение с PostgreSQL установлено (%s@%s:%s/%s).",
            self._db_params["user"],
            self._db_params["host"],
            self._db_params["port"],
            self._db_params["dbname"],
        )

    def disconnect(self) -> None:
        """Закрыть соединение с базой данных.

        Безопасно закрывает текущее подключение. Если соединение уже закрыто
        или не было создано — ничего не делает.
        """
        if self._connection is not None and not self._connection.closed:
            self._connection.close()
            logger.info("Соединение с PostgreSQL закрыто.")
        self._connection = None

    @property
    def is_connected(self) -> bool:
        """Вернуть ``True``, если соединение открыто и живое."""
        return self._connection is not None and not self._connection.closed

    # ------------------------------------------------------------------ #
    #  Контекстный менеджер
    # ------------------------------------------------------------------ #

    def __enter__(self) -> "PostgresDriver":
        """Войти в контекст: открыть соединение и вернуть драйвер."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Выйти из контекста: откатить незавершённую транзакцию (при ошибке)
        и закрыть соединение."""
        if self._connection is not None and not self._connection.closed:
            if exc_type is not None:
                self._connection.rollback()
                logger.warning("Транзакция откатана из-за ошибки: %s", exc_val)
            self.disconnect()

    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        """Контекстный менеджер для явного управления транзакцией.

        Автоматически делает ``COMMIT`` при успешном выходе и ``ROLLBACK``
        при возникновении исключения. Соединение при этом **не закрывается**.

        Пример
        -------
        >>> with driver.transaction():
        ...     driver.insert("users", {"name": "Alice"})
        ...     driver.update("users", {"name": "Bob"}, conditions={"id": 1})
        """
        if self._connection is None or self._connection.closed:
            raise RuntimeError("Соединение с БД не установлено. Вызовите connect().")
        try:
            yield
            self._connection.commit()
            logger.debug("Транзакция успешно закоммичена.")
        except Exception:
            self._connection.rollback()
            logger.warning("Транзакция откатана.", exc_info=True)
            raise

    # ------------------------------------------------------------------ #
    #  Внутренние утилиты
    # ------------------------------------------------------------------ #

    def _ensure_connection(self) -> psycopg2.extensions.connection:
        """Проверить, что соединение живо, и вернуть его объект.

        Raises
        ------
        RuntimeError
            Если соединение не было установлено.
        """
        if self._connection is None or self._connection.closed:
            raise RuntimeError("Соединение с БД не установлено. Вызовите connect().")
        return self._connection

    @staticmethod
    def _build_where(
        conditions: dict[str, Any],
    ) -> tuple[str, tuple[Any, ...]]:
        """Построить ``WHERE``-часть SQL-запроса из словаря условий.

        Параметры
        ---------
        conditions : dict
            Словарь вида ``{"column": value}``. Значение ``None`` превращается
            в ``IS NULL``.

        Возвращает
        ----------
        tuple[str, tuple]
            Кортеж ``(where_clause, params)``.
        """
        clauses: list[str] = []
        params: list[Any] = []

        for column, value in conditions.items():
            if value is None:
                clauses.append(f"{column} IS NULL")
            else:
                clauses.append(f"{column} = %s")
                params.append(value)

        where_sql = " AND ".join(clauses) if clauses else "TRUE"
        return where_sql, tuple(params)

    @staticmethod
    def _build_set(
        data: dict[str, Any],
    ) -> tuple[str, tuple[Any, ...]]:
        """Построить ``SET``-часть SQL-запроса из словаря данных.

        Возвращает
        ----------
        tuple[str, tuple]
            Кортеж ``(set_clause, params)``.
        """
        set_parts = [f"{col} = %s" for col in data.keys()]
        return ", ".join(set_parts), tuple(data.values())

    def _execute(
        self,
        query: str,
        params: Optional[tuple[Any, ...]] = None,
        commit: bool = True,
    ) -> psycopg2.extensions.cursor:
        """Выполнить произвольный SQL-запрос.

        Параметры
        ---------
        query : str
            SQL-запрос с плейсхолдерами ``%s``.
        params : tuple, optional
            Параметры для подстановки.
        commit : bool
            Сделать ``COMMIT`` после выполнения (по умолчанию ``True``).

        Возвращает
        ----------
        psycopg2.extensions.cursor
            Курсор с результатом выполнения.
        """
        conn = self._ensure_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            if commit and not self._autocommit:
                conn.commit()
            logger.debug("Выполнен запрос: %s | params: %s", query, params)
            return cursor
        except Exception:
            conn.rollback()
            cursor.close()
            raise

    def _execute_returning(
        self,
        query: str,
        params: Optional[tuple[Any, ...]] = None,
        commit: bool = True,
    ) -> Optional[dict[str, Any]]:
        """Выполнить INSERT/UPDATE/DELETE … RETURNING и вернуть первую строку.

        Возвращает ``None``, если затронутых строк нет.
        """
        conn = self._ensure_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(query, params)
            row = cursor.fetchone()
            if commit and not self._autocommit:
                conn.commit()
            cursor.close()
            return dict(row) if row else None
        except Exception:
            conn.rollback()
            cursor.close()
            raise

    # ------------------------------------------------------------------ #
    #  Выполнение произвольных запросов
    # ------------------------------------------------------------------ #

    def execute(
        self,
        query: str,
        params: Optional[tuple[Any, ...]] = None,
        commit: bool = True,
    ) -> psycopg2.extensions.cursor:
        """Выполнить произвольный SQL-запрос и вернуть курсор.

        Параметры
        ---------
        query : str
            SQL-запрос. Плейсхолдеры обозначаются ``%s``.
        params : tuple, optional
            Значения для подстановки в плейсхолдеры.
        commit : bool
            Автоматически коммитить после выполнения (по умолчанию ``True``).

        Возвращает
        -------
        psycopg2.extensions.cursor

        Пример
        -------
        >>> cur = driver.execute("SELECT version()")
        >>> cur.fetchone()
        """
        return self._execute(query, params, commit=commit)

    def fetchone(
        self,
        query: str,
        params: Optional[tuple[Any, ...]] = None,
    ) -> Optional[dict[str, Any]]:
        """Выполнить SELECT и вернуть одну строку в виде словаря.

        Параметры
        ----------
        query : str
            SQL SELECT-запрос.
        params : tuple, optional
            Параметры запроса.

        Возвращает
        -------
        dict or None
            Строка результата, или ``None``, если данных нет.

        Пример
        -------
        >>> driver.fetchone("SELECT * FROM users WHERE id = %s", (1,))
        {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'}
        """
        conn = self._ensure_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(query, params)
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            cursor.close()

    def fetchall(
        self,
        query: str,
        params: Optional[tuple[Any, ...]] = None,
    ) -> list[dict[str, Any]]:
        """Выполнить SELECT и вернуть все строки в виде списка словарей.

        Параметры
        ----------
        query : str
            SQL SELECT-запрос.
        params : tuple, optional
            Параметры запроса.

        Возвращает
        -------
        list[dict]
            Список строк результата. Пустой список, если данных нет.

        Пример
        -------
        >>> driver.fetchall("SELECT * FROM users LIMIT %s", (10,))
        [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
        """
        conn = self._ensure_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def fetchvalue(
        self,
        query: str,
        params: Optional[tuple[Any, ...]] = None,
    ) -> Any:
        """Выполнить запрос и вернуть одно скалярное значение (первый столбец первой строки).

        Удобно для ``COUNT(*)``, ``MAX(...)`` и т.п.

        Возвращает ``None``, если результат пуст.
        """
        conn = self._ensure_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            cursor.close()

    # ------------------------------------------------------------------ #
    #  CRUD: INSERT
    # ------------------------------------------------------------------ #

    def insert(
        self,
        table: str,
        data: dict[str, Any],
        returning: Optional[str] = None,
        commit: bool = True,
    ) -> Optional[dict[str, Any]]:
        """Вставить одну строку в таблицу.

        Параметры
        ---------
        table : str
            Имя таблицы.
        data : dict
            Словарь ``{столбец: значение}``.
        returning : str, optional
            Выражение для ``RETURNING`` (например, ``"id"`` или ``"*"``).
            Если ``None`` — ``RETURNING`` не добавляется.
        commit : bool
            Автоматически коммитить (по умолчанию ``True``).

        Возвращает
        -------
        dict or None
            Данные из ``RETURNING``, если указано, иначе ``None``.

        Пример
        -------
        >>> driver.insert("users", {"name": "Alice", "email": "a@b.com"}, returning="*")
        {'id': 1, 'name': 'Alice', 'email': 'a@b.com'}
        """
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        if returning:
            query += f" RETURNING {returning}"
            return self._execute_returning(query, tuple(data.values()), commit=commit)

        self._execute(query, tuple(data.values()), commit=commit)
        return None

    def insert_many(
        self,
        table: str,
        rows: Sequence[dict[str, Any]],
        commit: bool = True,
    ) -> int:
        """Массовая вставка нескольких строк в таблицу.

        Все словари должны иметь одинаковый набор ключей.

        Параметры
        ---------
        table : str
            Имя таблицы.
        rows : sequence of dict
            Список словарей ``{столбец: значение}``.
        commit : bool
            Автоматически коммитить.

        Возвращает
        -------
        int
            Количество вставленных строк.
        """
        if not rows:
            return 0

        columns = list(rows[0].keys())
        columns_sql = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders})"

        conn = self._ensure_connection()
        cursor = conn.cursor()
        try:
            params_list = [tuple(row[col] for col in columns) for row in rows]
            cursor.executemany(query, params_list)
            rowcount = cursor.rowcount
            if commit and not self._autocommit:
                conn.commit()
            cursor.close()
            logger.info("Вставлено %d строк в таблицу '%s'.", rowcount, table)
            return rowcount
        except Exception:
            conn.rollback()
            cursor.close()
            raise

    # ------------------------------------------------------------------ #
    #  CRUD: SELECT
    # ------------------------------------------------------------------ #

    def select(
        self,
        table: str,
        columns: Optional[list[str]] = None,
        conditions: Optional[dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Выбрать строки из таблицы с необязательной фильтрацией.

        Параметры
        ---------
        table : str
            Имя таблицы.
        columns : list[str], optional
            Список столбцов. По умолчанию ``["*"]``.
        conditions : dict, optional
            Условия ``WHERE`` вида ``{столбец: значение}``.
            Значение ``None`` интерпретируется как ``IS NULL``.
        order_by : str, optional
            Выражение ``ORDER BY`` (например, ``"created_at DESC"``).
        limit : int, optional
            Ограничение количества строк.
        offset : int, optional
            Смещение (для пагинации).

        Возвращает
        -------
        list[dict]

        Пример
        -------
        >>> driver.select("users", ["id", "name"], conditions={"active": True}, limit=5)
        [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
        """
        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM {table}"

        params: tuple[Any, ...] = ()
        if conditions:
            where_sql, params = self._build_where(conditions)
            query += f" WHERE {where_sql}"

        if order_by:
            query += f" ORDER BY {order_by}"
        if limit is not None:
            query += f" LIMIT {int(limit)}"
        if offset is not None:
            query += f" OFFSET {int(offset)}"

        return self.fetchall(query, params)

    def select_one(
        self,
        table: str,
        columns: Optional[list[str]] = None,
        conditions: Optional[dict[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        """Выбрать одну строку из таблицы.

        Эквивалентно ``select(..., limit=1)[0]``, но возвращает ``None``,
        если строк нет.

        Параметры — те же, что у :meth:`select`, кроме ``limit``/``offset``.
        """
        rows = self.select(table, columns=columns, conditions=conditions, limit=1)
        return rows[0] if rows else None

    # ------------------------------------------------------------------ #
    #  CRUD: UPDATE
    # ------------------------------------------------------------------ #

    def update(
        self,
        table: str,
        data: dict[str, Any],
        conditions: Optional[dict[str, Any]] = None,
        returning: Optional[str] = None,
        commit: bool = True,
    ) -> Optional[dict[str, Any]]:
        """Обновить строки в таблице.

        Параметры
        ---------
        table : str
            Имя таблицы.
        data : dict
            Новые значения ``{столбец: значение}``.
        conditions : dict, optional
            Условия ``WHERE``. Если не указаны — обновятся **все** строки.
        returning : str, optional
            Выражение ``RETURNING``.
        commit : bool
            Автоматически коммитить.

        Возвращает
        -------
        dict or None
            Данные из ``RETURNING``, если указано.

        Пример
        -------
        >>> driver.update("users", {"name": "Bob"}, conditions={"id": 1}, returning="*")
        {'id': 1, 'name': 'Bob', 'email': 'alice@example.com'}
        """
        set_sql, set_params = self._build_set(data)
        query = f"UPDATE {table} SET {set_sql}"
        all_params = list(set_params)

        if conditions:
            where_sql, where_params = self._build_where(conditions)
            query += f" WHERE {where_sql}"
            all_params.extend(where_params)

        if returning:
            query += f" RETURNING {returning}"
            return self._execute_returning(query, tuple(all_params), commit=commit)

        self._execute(query, tuple(all_params), commit=commit)
        return None

    # ------------------------------------------------------------------ #
    #  CRUD: DELETE
    # ------------------------------------------------------------------ #

    def delete(
        self,
        table: str,
        conditions: Optional[dict[str, Any]] = None,
        returning: Optional[str] = None,
        commit: bool = True,
    ) -> Optional[dict[str, Any]]:
        """Удалить строки из таблицы.

        Параметры
        ---------
        table : str
            Имя таблицы.
        conditions : dict, optional
            Условия ``WHERE``. Если не указаны — удалятся **все** строки.
        returning : str, optional
            Выражение ``RETURNING``.
        commit : bool
            Автоматически коммитить.

        Возвращает
        -------
        dict or None
            Данные из ``RETURNING``, если указано.

        Предупреждение
        --------------
        Вызов без ``conditions`` удалит **все** строки таблицы!

        Пример
        -------
        >>> driver.delete("users", conditions={"id": 1}, returning="*")
        {'id': 1, 'name': 'Bob', 'email': 'bob@example.com'}
        """
        query = f"DELETE FROM {table}"
        params: tuple[Any, ...] = ()

        if conditions:
            where_sql, params = self._build_where(conditions)
            query += f" WHERE {where_sql}"

        if returning:
            query += f" RETURNING {returning}"
            return self._execute_returning(query, params, commit=commit)

        self._execute(query, params, commit=commit)
        return None

    # ------------------------------------------------------------------ #
    #  Утилиты
    # ------------------------------------------------------------------ #

    def count(
        self,
        table: str,
        conditions: Optional[dict[str, Any]] = None,
    ) -> int:
        """Вернуть количество строк в таблице (с необязательной фильтрацией).

        Параметры
        ---------
        table : str
            Имя таблицы.
        conditions : dict, optional
            Условия ``WHERE``.

        Возвращает
        -------
        int
        """
        query = f"SELECT COUNT(*) FROM {table}"
        params: tuple[Any, ...] = ()
        if conditions:
            where_sql, params = self._build_where(conditions)
            query += f" WHERE {where_sql}"
        result = self.fetchvalue(query, params)
        return result if result is not None else 0

    def exists(
        self,
        table: str,
        conditions: dict[str, Any],
    ) -> bool:
        """Проверить существование строки по условиям.

        Параметры
        ---------
        table : str
        conditions : dict

        Возвращает
        -------
        bool
        """
        query = f"SELECT EXISTS(SELECT 1 FROM {table}"
        where_sql, params = self._build_where(conditions)
        query += f" WHERE {where_sql}) LIMIT 1"
        return bool(self.fetchvalue(query, params))

    def table_columns(self, table: str) -> list[str]:
        """Вернуть список имён столбцов таблицы.

        Параметры
        ---------
        table : str
            Имя таблицы.

        Возвращает
        -------
        list[str]
        """
        query = (
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s ORDER BY ordinal_position"
        )
        rows = self.fetchall(query, (table,))
        return [row["column_name"] for row in rows]

    def table_exists(self, table: str) -> bool:
        """Проверить, существует ли таблица в текущей схеме.

        Возвращает
        -------
        bool
        """
        query = (
            "SELECT EXISTS("
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name = %s"
            ")"
        )
        return bool(self.fetchvalue(query, (table,)))

    def truncate(self, table: str, cascade: bool = False, commit: bool = True) -> None:
        """Очистить таблицу (TRUNCATE).

        Параметры
        ---------
        table : str
            Имя таблицы.
        cascade : bool
            Добавить ``CASCADE`` для таблиц с внешними ключами.
        commit : bool
            Автоматически коммитить.
        """
        cascade_sql = " CASCADE" if cascade else ""
        self._execute(f"TRUNCATE TABLE {table}{cascade_sql}", commit=commit)

    def copy_expert(
        self,
        sql: str,
        file_or_path: Any,
    ) -> None:
        """Обёртка вокруг ``psycopg2.copy_expert`` для массового импорта/экспорта.

        Параметры
        ---------
        sql : str
            SQL-выражение ``COPY ... FROM STDIN`` или ``COPY ... TO STDOUT``.
        file_or_path
            Файловый объект или путь к файлу.
        """
        conn = self._ensure_connection()
        cursor = conn.cursor()
        try:
            if isinstance(file_or_path, str):
                with open(file_or_path, "r", encoding="utf-8") as f:
                    cursor.copy_expert(sql, f)
            else:
                cursor.copy_expert(sql, file_or_path)
            if not self._autocommit:
                conn.commit()
            cursor.close()
        except Exception:
            conn.rollback()
            cursor.close()
            raise

    # ------------------------------------------------------------------ #
    #  Представление
    # ------------------------------------------------------------------ #

    def __repr__(self) -> str:
        db = self._db_params
        status = "подключён" if self.is_connected else "отключён"
        return (
            f"<PostgresDriver [{status}] "
            f"{db['user']}@{db['host']}:{db['port']}/{db['dbname']}>"
        )
