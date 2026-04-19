"""
Модель стола для мини-системы бронирования ресторана.

Описывает структуру таблицы ``tables`` и предоставляет методы
для CRUD-операций через PostgresDriver.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Any, Optional

# Поддержка запуска как напрямую, так и при импорте из пакета
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from postgres_driver import PostgresDriver


class Table:
    """Модель стола в ресторане.

    Каждый экземпляр описывает один конкретный стол: его номер,
    вместимость, расположение в зале и текущий статус.

    Атрибуты
    --------
    id : int или None
        Уникальный идентификатор (автоинкремент, назначается БД).
    number : int
        Номер стола в ресторане (уникальный).
    seats : int
        Количество посадочных мест.
    location : str
        Расположение: ``"hall"`` (основной зал), ``"terrace"`` (терраса),
        ``"vip"`` (VIP-зона), ``"bar"`` (барная зона).
    is_available : bool
        Стол доступен для бронирования. По умолчанию ``True``.
    description : str или None
        Произвольное описание (например, «у окна», «возле сцены»).
    created_at : datetime или None
        Дата и время добавления стола (назначается БД).
    updated_at : datetime или None
        Дата и время последнего обновления (назначается БД).
    """

    # ------------------------------------------------------------------ #
    #  Метаинформация о таблице
    # ------------------------------------------------------------------ #

    TABLE_NAME: str = "tables"

    DDL: str = """\
CREATE TABLE IF NOT EXISTS tables (
    id            SERIAL        PRIMARY KEY,
    number        INTEGER       NOT NULL UNIQUE,
    seats         INTEGER       NOT NULL CHECK (seats > 0),
    location      VARCHAR(20)   NOT NULL DEFAULT 'hall'
                                  CHECK (location IN ('hall', 'terrace', 'vip', 'bar')),
    is_available  BOOLEAN       NOT NULL DEFAULT TRUE,
    description   TEXT,
    created_at    TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP     NOT NULL DEFAULT NOW()
);
"""

    # Допустимые расположения
    LOCATIONS: tuple[str, ...] = ("hall", "terrace", "vip", "bar")

    # ------------------------------------------------------------------ #
    #  Конструктор
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        number: int,
        seats: int,
        location: str = "hall",
        is_available: bool = True,
        description: Optional[str] = None,
        *,
        id: Optional[int] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ) -> None:
        self.id = id

        if number <= 0:
            raise ValueError("Номер стола должен быть положительным числом.")
        self.number = number

        if seats <= 0:
            raise ValueError("Количество мест должно быть положительным числом.")
        self.seats = seats

        if location not in self.LOCATIONS:
            raise ValueError(
                f"Недопустимое расположение '{location}'. "
                f"Допустимые: {self.LOCATIONS}"
            )
        self.location = location

        self.is_available = is_available
        self.description = description
        self.created_at = created_at
        self.updated_at = updated_at

    # ------------------------------------------------------------------ #
    #  Сериализация / десериализация
    # ------------------------------------------------------------------ #

    def to_dict(self) -> dict[str, Any]:
        """Преобразовать объект стола в словарь для передачи в драйвер.

        Возвращает
        ---------
        dict
        """
        return {
            "number": self.number,
            "seats": self.seats,
            "location": self.location,
            "is_available": self.is_available,
            "description": self.description,
        }

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Table":
        """Создать объект Table из строки БД (словаря).

        Параметры
        ---------
        row : dict
            Словарь, полученный из ``PostgresDriver.fetchone`` / ``fetchall``.

        Возвращает
        ---------
        Table
        """
        return cls(
            id=row.get("id"),
            number=row["number"],
            seats=row["seats"],
            location=row.get("location", "hall"),
            is_available=row.get("is_available", True),
            description=row.get("description"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )

    # ------------------------------------------------------------------ #
    #  DDL
    # ------------------------------------------------------------------ #

    @staticmethod
    def create_table(db: PostgresDriver) -> None:
        """Создать таблицу столов, если она не существует.

        Параметры
        ---------
        db : PostgresDriver
            Подключённый драйвер базы данных.
        """
        db.execute(Table.DDL)

    @staticmethod
    def drop_table(db: PostgresDriver) -> None:
        """Удалить таблицу столов целиком.

        Параметры
        ---------
        db : PostgresDriver
            Подключённый драйвер базы данных.
        """
        db.execute(f"DROP TABLE IF EXISTS {Table.TABLE_NAME}")

    # ------------------------------------------------------------------ #
    #  CRUD
    # ------------------------------------------------------------------ #

    @staticmethod
    def save(db: PostgresDriver, table: "Table") -> "Table":
        """Сохранить новый стол в базу данных.

        После вставки обновляет ``table.id``, ``table.created_at`` и
        ``table.updated_at`` из возвращённых данных.

        Параметры
        ---------
        db : PostgresDriver
            Подключённый драйвер.
        table : Table
            Объект стола для сохранения.

        Возвращает
        ---------
        Table
            Тот же объект с заполненными ``id``, ``created_at``, ``updated_at``.

        Raises
        ------
        ValueError
            Если стол уже имеет ``id`` (уже сохранён).
        """
        if table.id is not None:
            raise ValueError(
                "Стол уже имеет id. Используйте Table.update_table() для обновления."
            )

        row = db.insert(
            Table.TABLE_NAME,
            table.to_dict(),
            returning="id, created_at, updated_at",
        )
        if row:
            table.id = row["id"]
            table.created_at = row["created_at"]
            table.updated_at = row["updated_at"]
        return table

    @staticmethod
    def get_by_id(db: PostgresDriver, table_id: int) -> Optional["Table"]:
        """Найти стол по идентификатору.

        Параметры
        ---------
        db : PostgresDriver
            Подключённый драйвер.
        table_id : int
            Идентификатор стола.

        Возвращает
        ---------
        Table или None
        """
        row = db.select_one(
            Table.TABLE_NAME, conditions={"id": table_id}
        )
        return Table.from_row(row) if row else None

    @staticmethod
    def get_by_number(db: PostgresDriver, number: int) -> Optional["Table"]:
        """Найти стол по номеру.

        Параметры
        ---------
        db : PostgresDriver
        number : int
            Номер стола в ресторане.

        Возвращает
        ---------
        Table или None
        """
        row = db.select_one(
            Table.TABLE_NAME, conditions={"number": number}
        )
        return Table.from_row(row) if row else None

    @staticmethod
    def get_all(
        db: PostgresDriver,
        *,
        location: Optional[str] = None,
        is_available: Optional[bool] = None,
        min_seats: Optional[int] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list["Table"]:
        """Получить список столов с необязательной фильтрацией.

        Параметры
        ---------
        db : PostgresDriver
        location : str или None
            Фильтр по расположению (``"hall"``, ``"terrace"``, ``"vip"``, ``"bar"``).
        is_available : bool или None
            Фильтр по доступности.
        min_seats : int или None
            Минимальное количество мест (``seats >= min_seats``).
        limit : int или None
            Ограничение количества строк.
        offset : int или None
            Смещение (для пагинации).

        Возвращает
        ---------
        list[Table]
        """
        conditions: dict[str, Any] = {}
        if location is not None:
            conditions["location"] = location
        if is_available is not None:
            conditions["is_available"] = is_available

        # min_seats — через произвольный запрос, т.к. это условие ">="
        if min_seats is not None:
            if conditions:
                # Есть и равенства, и >= — собираем запрос вручную
                where_parts: list[str] = []
                params: list[Any] = []
                for col, val in conditions.items():
                    where_parts.append(f"{col} = %s")
                    params.append(val)
                where_parts.append("seats >= %s")
                params.append(min_seats)
                query = (
                    f"SELECT * FROM {Table.TABLE_NAME} "
                    f"WHERE {' AND '.join(where_parts)} ORDER BY number ASC"
                )
                if limit is not None:
                    query += f" LIMIT {int(limit)}"
                if offset is not None:
                    query += f" OFFSET {int(offset)}"
                rows = db.fetchall(query, tuple(params))
                return [Table.from_row(r) for r in rows]

            # Только min_seats
            query = (
                f"SELECT * FROM {Table.TABLE_NAME} "
                f"WHERE seats >= %s ORDER BY number ASC"
            )
            params_list: list[Any] = [min_seats]
            if limit is not None:
                query += f" LIMIT {int(limit)}"
            if offset is not None:
                query += f" OFFSET {int(offset)}"
            rows = db.fetchall(query, tuple(params_list))
            return [Table.from_row(r) for r in rows]

        rows = db.select(
            Table.TABLE_NAME,
            conditions=conditions or None,
            order_by="number ASC",
            limit=limit,
            offset=offset,
        )
        return [Table.from_row(row) for row in rows]

    @staticmethod
    def update_table(db: PostgresDriver, table: "Table") -> "Table":
        """Обновить данные существующего стола в БД.

        ``updated_at`` обновляется базой автоматически (``NOW()``).

        Параметры
        ---------
        db : PostgresDriver
        table : Table
            Объект с изменёнными полями. Должен иметь ``id``.

        Возвращает
        ---------
        Table
            Объект с обновлённым ``updated_at``.

        Raises
        ------
        ValueError
            Если у стола нет ``id``.
        """
        if table.id is None:
            raise ValueError(
                "Нельзя обновить стол без id. Сначала сохраните его."
            )

        data = table.to_dict()
        set_parts = [f"{col} = %s" for col in data]
        set_parts.append("updated_at = NOW()")
        params = list(data.values()) + [table.id]

        query = (
            f"UPDATE {Table.TABLE_NAME} SET {', '.join(set_parts)} "
            f"WHERE id = %s RETURNING updated_at"
        )
        row = db._execute_returning(query, tuple(params))
        if row:
            table.updated_at = row["updated_at"]
        return table

    @staticmethod
    def delete_table(db: PostgresDriver, table: "Table") -> None:
        """Удалить стол из базы данных.

        Параметры
        ---------
        db : PostgresDriver
        table : Table
            Объект стола. Должен иметь ``id``.

        Raises
        ------
        ValueError
            Если у стола нет ``id``.
        """
        if table.id is None:
            raise ValueError("Нельзя удалить стол без id.")
        db.delete(Table.TABLE_NAME, conditions={"id": table.id})

    # ------------------------------------------------------------------ #
    #  Дополнительные операции
    # ------------------------------------------------------------------ #

    @staticmethod
    def mark_unavailable(db: PostgresDriver, table_id: int) -> Optional["Table"]:
        """Пометить стол как недоступный для бронирования.

        Параметры
        ---------
        db : PostgresDriver
        table_id : int

        Возвращает
        ---------
        Table или None
            Обновлённый стол, или ``None``, если не найден.
        """
        row = db.update(
            Table.TABLE_NAME,
            data={"is_available": False},
            conditions={"id": table_id},
            returning="*",
        )
        return Table.from_row(row) if row else None

    @staticmethod
    def mark_available(db: PostgresDriver, table_id: int) -> Optional["Table"]:
        """Пометить стол как доступный для бронирования.

        Параметры
        ---------
        db : PostgresDriver
        table_id : int

        Возвращает
        ---------
        Table или None
        """
        row = db.update(
            Table.TABLE_NAME,
            data={"is_available": True},
            conditions={"id": table_id},
            returning="*",
        )
        return Table.from_row(row) if row else None

    @staticmethod
    def count_tables(
        db: PostgresDriver,
        *,
        location: Optional[str] = None,
        is_available: Optional[bool] = None,
    ) -> int:
        """Подсчитать количество столов с необязательной фильтрацией.

        Параметры
        ---------
        db : PostgresDriver
        location : str или None
        is_available : bool или None

        Возвращает
        ---------
        int
        """
        conditions: dict[str, Any] = {}
        if location is not None:
            conditions["location"] = location
        if is_available is not None:
            conditions["is_available"] = is_available
        return db.count(Table.TABLE_NAME, conditions=conditions or None)

    @staticmethod
    def number_exists(db: PostgresDriver, number: int) -> bool:
        """Проверить, существует ли стол с данным номером.

        Параметры
        ---------
        db : PostgresDriver
        number : int

        Возвращает
        ---------
        bool
        """
        return db.exists(Table.TABLE_NAME, {"number": number})

    @staticmethod
    def total_seats(db: PostgresDriver) -> int:
        """Вернуть суммарное количество посадочных мест во всех доступных столах.

        Параметры
        ---------
        db : PostgresDriver

        Возвращает
        ---------
        int
        """
        result = db.fetchvalue(
            f"SELECT COALESCE(SUM(seats), 0) FROM {Table.TABLE_NAME} "
            f"WHERE is_available = TRUE"
        )
        return result if result is not None else 0

    # ------------------------------------------------------------------ #
    #  Представление
    # ------------------------------------------------------------------ #

    def __repr__(self) -> str:
        return (
            f"<Table id={self.id} number={self.number} "
            f"seats={self.seats} location='{self.location}' "
            f"available={self.is_available}>"
        )

    def __str__(self) -> str:
        status = "доступен" if self.is_available else "недоступен"
        loc_map = {"hall": "зал", "terrace": "терраса", "vip": "VIP", "bar": "бар"}
        loc = loc_map.get(self.location, self.location)
        return (
            f"Стол №{self.number} — {self.seats} мест, {loc} ({status})"
        )
