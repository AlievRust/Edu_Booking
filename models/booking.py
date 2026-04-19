"""
Модель бронирования для мини-системы бронирования ресторана.

Описывает структуру таблицы ``bookings`` и предоставляет методы
для CRUD-операций через PostgresDriver.

Внешние связи:
    - user_id  → users.id   (кто бронирует)
    - table_id → tables.id  (какой стол бронируется)
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime, time
from typing import Any, Optional

# Поддержка запуска как напрямую, так и при импорте из пакета
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from postgres_driver import PostgresDriver


class Booking:
    """Модель бронирования стола в ресторане.

    Каждая запись — одно бронирование: кто, какой стол, на какую дату
    и время, на сколько человек, с каким статусом.

    Атрибуты
    --------
    id : int или None
        Уникальный идентификатор (автоинкремент, назначается БД).
    user_id : int
        Идентификатор пользователя, создавшего бронирование.
    table_id : int
        Идентификатор забронированного стола.
    booking_date : date
        Дата бронирования.
    start_time : time
        Время начала бронирования.
    end_time : time
        Время окончания бронирования.
    guests_count : int
        Количество гостей.
    status : str
        Статус: ``"pending"`` (ожидает), ``"confirmed"`` (подтверждено),
        ``"cancelled"`` (отменено), ``"completed"`` (завершено).
        По умолчанию ``"pending"``.
    notes : str или None
        Примечания или пожелания клиента.
    created_at : datetime или None
        Дата и время создания записи (назначается БД).
    updated_at : datetime или None
        Дата и время последнего обновления (назначается БД).
    """

    # ------------------------------------------------------------------ #
    #  Метаинформация о таблице
    # ------------------------------------------------------------------ #

    TABLE_NAME: str = "bookings"

    DDL: str = """\
CREATE TABLE IF NOT EXISTS bookings (
    id            SERIAL        PRIMARY KEY,
    user_id       INTEGER       NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    table_id      INTEGER       NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    booking_date  DATE          NOT NULL,
    start_time    TIME          NOT NULL,
    end_time      TIME          NOT NULL,
    guests_count  INTEGER       NOT NULL CHECK (guests_count > 0),
    status        VARCHAR(20)   NOT NULL DEFAULT 'pending'
                                  CHECK (status IN ('pending', 'confirmed', 'cancelled', 'completed')),
    notes         TEXT,
    created_at    TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bookings_user_id   ON bookings(user_id);
CREATE INDEX IF NOT EXISTS idx_bookings_table_id  ON bookings(table_id);
CREATE INDEX IF NOT EXISTS idx_bookings_date      ON bookings(booking_date);
CREATE INDEX IF NOT EXISTS idx_bookings_status    ON bookings(status);
"""

    # Допустимые статусы
    STATUSES: tuple[str, ...] = ("pending", "confirmed", "cancelled", "completed")

    # ------------------------------------------------------------------ #
    #  Конструктор
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        user_id: int,
        table_id: int,
        booking_date: date,
        start_time: time,
        end_time: time,
        guests_count: int,
        status: str = "pending",
        notes: Optional[str] = None,
        *,
        id: Optional[int] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ) -> None:
        self.id = id
        self.user_id = user_id
        self.table_id = table_id
        self.booking_date = booking_date
        self.start_time = start_time
        self.end_time = end_time

        if guests_count <= 0:
            raise ValueError("Количество гостей должно быть положительным числом.")
        self.guests_count = guests_count

        if status not in self.STATUSES:
            raise ValueError(
                f"Недопустимый статус '{status}'. Допустимые: {self.STATUSES}"
            )
        self.status = status

        self.notes = notes
        self.created_at = created_at
        self.updated_at = updated_at

    # ------------------------------------------------------------------ #
    #  Сериализация / десериализация
    # ------------------------------------------------------------------ #

    def to_dict(self) -> dict[str, Any]:
        """Преобразовать объект бронирования в словарь для передачи в драйвер.

        Возвращает
        ---------
        dict
        """
        return {
            "user_id": self.user_id,
            "table_id": self.table_id,
            "booking_date": self.booking_date,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "guests_count": self.guests_count,
            "status": self.status,
            "notes": self.notes,
        }

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Booking":
        """Создать объект Booking из строки БД (словаря).

        Параметры
        ---------
        row : dict
            Словарь, полученный из ``PostgresDriver.fetchone`` / ``fetchall``.

        Возвращает
        ---------
        Booking
        """
        return cls(
            id=row.get("id"),
            user_id=row["user_id"],
            table_id=row["table_id"],
            booking_date=row["booking_date"],
            start_time=row["start_time"],
            end_time=row["end_time"],
            guests_count=row["guests_count"],
            status=row.get("status", "pending"),
            notes=row.get("notes"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )

    # ------------------------------------------------------------------ #
    #  DDL
    # ------------------------------------------------------------------ #

    @staticmethod
    def create_table(db: PostgresDriver) -> None:
        """Создать таблицу бронирований и индексы, если они не существуют.

        Параметры
        ---------
        db : PostgresDriver
            Подключённый драйвер базы данных.
        """
        db.execute(Booking.DDL)

    @staticmethod
    def drop_table(db: PostgresDriver) -> None:
        """Удалить таблицу бронирований целиком.

        Параметры
        ---------
        db : PostgresDriver
            Подключённый драйвер базы данных.
        """
        db.execute(f"DROP TABLE IF EXISTS {Booking.TABLE_NAME}")

    # ------------------------------------------------------------------ #
    #  CRUD
    # ------------------------------------------------------------------ #

    @staticmethod
    def save(db: PostgresDriver, booking: "Booking") -> "Booking":
        """Сохранить новое бронирование в базу данных.

        После вставки обновляет ``booking.id``, ``booking.created_at`` и
        ``booking.updated_at`` из возвращённых данных.

        Параметры
        ---------
        db : PostgresDriver
            Подключённый драйвер.
        booking : Booking
            Объект бронирования для сохранения.

        Возвращает
        ---------
        Booking
            Тот же объект с заполненными ``id``, ``created_at``, ``updated_at``.

        Raises
        ------
        ValueError
            Если бронирование уже имеет ``id`` (уже сохранено).
        """
        if booking.id is not None:
            raise ValueError(
                "Бронирование уже имеет id. "
                "Используйте Booking.update_booking() для обновления."
            )

        row = db.insert(
            Booking.TABLE_NAME,
            booking.to_dict(),
            returning="id, created_at, updated_at",
        )
        if row:
            booking.id = row["id"]
            booking.created_at = row["created_at"]
            booking.updated_at = row["updated_at"]
        return booking

    @staticmethod
    def get_by_id(db: PostgresDriver, booking_id: int) -> Optional["Booking"]:
        """Найти бронирование по идентификатору.

        Параметры
        ---------
        db : PostgresDriver
        booking_id : int

        Возвращает
        ---------
        Booking или None
        """
        row = db.select_one(
            Booking.TABLE_NAME, conditions={"id": booking_id}
        )
        return Booking.from_row(row) if row else None

    @staticmethod
    def get_by_user(
        db: PostgresDriver,
        user_id: int,
        *,
        status: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list["Booking"]:
        """Получить все бронирования конкретного пользователя.

        Параметры
        ---------
        db : PostgresDriver
        user_id : int
            Идентификатор пользователя.
        status : str или None
            Фильтр по статусу.
        limit : int или None
            Ограничение количества строк.
        offset : int или None
            Смещение (для пагинации).

        Возвращает
        ---------
        list[Booking]
        """
        conditions: dict[str, Any] = {"user_id": user_id}
        if status is not None:
            conditions["status"] = status
        rows = db.select(
            Booking.TABLE_NAME,
            conditions=conditions,
            order_by="booking_date ASC, start_time ASC",
            limit=limit,
            offset=offset,
        )
        return [Booking.from_row(r) for r in rows]

    @staticmethod
    def get_by_table(
        db: PostgresDriver,
        table_id: int,
        *,
        booking_date: Optional[date] = None,
        status: Optional[str] = None,
    ) -> list["Booking"]:
        """Получить бронирования конкретного стола.

        Параметры
        ---------
        db : PostgresDriver
        table_id : int
            Идентификатор стола.
        booking_date : date или None
            Фильтр по конкретной дате.
        status : str или None
            Фильтр по статусу.

        Возвращает
        ---------
        list[Booking]
        """
        conditions: dict[str, Any] = {"table_id": table_id}
        if booking_date is not None:
            conditions["booking_date"] = booking_date
        if status is not None:
            conditions["status"] = status
        rows = db.select(
            Booking.TABLE_NAME,
            conditions=conditions,
            order_by="booking_date ASC, start_time ASC",
        )
        return [Booking.from_row(r) for r in rows]

    @staticmethod
    def get_all(
        db: PostgresDriver,
        *,
        status: Optional[str] = None,
        booking_date: Optional[date] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list["Booking"]:
        """Получить все бронирования с необязательной фильтрацией.

        Параметры
        ---------
        db : PostgresDriver
        status : str или None
            Фильтр по статусу.
        booking_date : date или None
            Фильтр по дате.
        limit : int или None
            Ограничение количества строк.
        offset : int или None
            Смещение (для пагинации).

        Возвращает
        ---------
        list[Booking]
        """
        conditions: dict[str, Any] = {}
        if status is not None:
            conditions["status"] = status
        if booking_date is not None:
            conditions["booking_date"] = booking_date
        rows = db.select(
            Booking.TABLE_NAME,
            conditions=conditions or None,
            order_by="booking_date ASC, start_time ASC",
            limit=limit,
            offset=offset,
        )
        return [Booking.from_row(r) for r in rows]

    @staticmethod
    def update_booking(db: PostgresDriver, booking: "Booking") -> "Booking":
        """Обновить данные существующего бронирования в БД.

        ``updated_at`` обновляется базой автоматически (``NOW()``).

        Параметры
        ---------
        db : PostgresDriver
        booking : Booking
            Объект с изменёнными полями. Должен иметь ``id``.

        Возвращает
        ---------
        Booking
            Объект с обновлённым ``updated_at``.

        Raises
        ------
        ValueError
            Если у бронирования нет ``id``.
        """
        if booking.id is None:
            raise ValueError(
                "Нельзя обновить бронирование без id. Сначала сохраните его."
            )

        data = booking.to_dict()
        set_parts = [f"{col} = %s" for col in data]
        set_parts.append("updated_at = NOW()")
        params = list(data.values()) + [booking.id]

        query = (
            f"UPDATE {Booking.TABLE_NAME} SET {', '.join(set_parts)} "
            f"WHERE id = %s RETURNING updated_at"
        )
        row = db._execute_returning(query, tuple(params))
        if row:
            booking.updated_at = row["updated_at"]
        return booking

    @staticmethod
    def delete_booking(db: PostgresDriver, booking: "Booking") -> None:
        """Удалить бронирование из базы данных.

        Параметры
        ---------
        db : PostgresDriver
        booking : Booking
            Объект бронирования. Должен иметь ``id``.

        Raises
        ------
        ValueError
            Если у бронирования нет ``id``.
        """
        if booking.id is None:
            raise ValueError("Нельзя удалить бронирование без id.")
        db.delete(Booking.TABLE_NAME, conditions={"id": booking.id})

    # ------------------------------------------------------------------ #
    #  Изменение статуса
    # ------------------------------------------------------------------ #

    @staticmethod
    def confirm(db: PostgresDriver, booking_id: int) -> Optional["Booking"]:
        """Подтвердить бронирование (статус → ``"confirmed"``).

        Параметры
        ---------
        db : PostgresDriver
        booking_id : int

        Возвращает
        ---------
        Booking или None
        """
        row = db.update(
            Booking.TABLE_NAME,
            data={"status": "confirmed"},
            conditions={"id": booking_id},
            returning="*",
        )
        return Booking.from_row(row) if row else None

    @staticmethod
    def cancel(db: PostgresDriver, booking_id: int) -> Optional["Booking"]:
        """Отменить бронирование (статус → ``"cancelled"``).

        Параметры
        ---------
        db : PostgresDriver
        booking_id : int

        Возвращает
        ---------
        Booking или None
        """
        row = db.update(
            Booking.TABLE_NAME,
            data={"status": "cancelled"},
            conditions={"id": booking_id},
            returning="*",
        )
        return Booking.from_row(row) if row else None

    @staticmethod
    def complete(db: PostgresDriver, booking_id: int) -> Optional["Booking"]:
        """Завершить бронирование (статус → ``"completed"``).

        Параметры
        ---------
        db : PostgresDriver
        booking_id : int

        Возвращает
        ---------
        Booking или None
        """
        row = db.update(
            Booking.TABLE_NAME,
            data={"status": "completed"},
            conditions={"id": booking_id},
            returning="*",
        )
        return Booking.from_row(row) if row else None

    # ------------------------------------------------------------------ #
    #  Дополнительные операции
    # ------------------------------------------------------------------ #

    @staticmethod
    def count_bookings(
        db: PostgresDriver,
        *,
        user_id: Optional[int] = None,
        table_id: Optional[int] = None,
        status: Optional[str] = None,
        booking_date: Optional[date] = None,
    ) -> int:
        """Подсчитать количество бронирований с необязательной фильтрацией.

        Параметры
        ---------
        db : PostgresDriver
        user_id : int или None
        table_id : int или None
        status : str или None
        booking_date : date или None

        Возвращает
        ---------
        int
        """
        conditions: dict[str, Any] = {}
        if user_id is not None:
            conditions["user_id"] = user_id
        if table_id is not None:
            conditions["table_id"] = table_id
        if status is not None:
            conditions["status"] = status
        if booking_date is not None:
            conditions["booking_date"] = booking_date
        return db.count(Booking.TABLE_NAME, conditions=conditions or None)

    @staticmethod
    def has_overlap(
        db: PostgresDriver,
        table_id: int,
        booking_date: date,
        start_time: time,
        end_time: time,
        *,
        exclude_id: Optional[int] = None,
    ) -> bool:
        """Проверить, пересекается ли предлагаемое время с существующими бронированиями.

        Учитывает только активные бронирования (статус ``"pending"`` и
        ``"confirmed"``). Удобно для валидации перед созданием нового
        бронирования.

        Параметры
        ---------
        db : PostgresDriver
        table_id : int
            Идентификатор стола.
        booking_date : date
            Дата бронирования.
        start_time : time
            Начало интервала.
        end_time : time
            Конец интервала.
        exclude_id : int или None
            Исключить бронирование с этим id (для проверки при обновлении).

        Возвращает
        ---------
        bool
            ``True``, если есть пересечение по времени.
        """
        query = (
            f"SELECT EXISTS("
            f"SELECT 1 FROM {Booking.TABLE_NAME} "
            f"WHERE table_id = %s "
            f"  AND booking_date = %s "
            f"  AND status IN ('pending', 'confirmed') "
            f"  AND start_time < %s "
            f"  AND end_time > %s"
        )
        params: list[Any] = [table_id, booking_date, end_time, start_time]

        if exclude_id is not None:
            query += " AND id != %s"
            params.append(exclude_id)

        query += ")"
        return bool(db.fetchvalue(query, tuple(params)))

    @staticmethod
    def get_with_details(db: PostgresDriver, booking_id: int) -> Optional[dict[str, Any]]:
        """Получить бронирование с данными пользователя и стола одним запросом.

        Параметры
        ---------
        db : PostgresDriver
        booking_id : int

        Возвращает
        ---------
        dict или None
            Словарь с полями бронирования, а также вложенными ключами
            ``user_*`` и ``table_*``.
        """
        query = (
            "SELECT b.*, "
            "  u.first_name AS user_first_name, "
            "  u.last_name  AS user_last_name, "
            "  u.email      AS user_email, "
            "  u.phone      AS user_phone, "
            "  t.number     AS table_number, "
            "  t.seats      AS table_seats, "
            "  t.location   AS table_location, "
            "  t.description AS table_description "
            f"FROM {Booking.TABLE_NAME} b "
            "  JOIN users  u ON b.user_id  = u.id "
            "  JOIN tables t ON b.table_id = t.id "
            "WHERE b.id = %s"
        )
        return db.fetchone(query, (booking_id,))

    # ------------------------------------------------------------------ #
    #  Представление
    # ------------------------------------------------------------------ #

    def __repr__(self) -> str:
        return (
            f"<Booking id={self.id} user_id={self.user_id} "
            f"table_id={self.table_id} "
            f"date={self.booking_date} "
            f"time={self.start_time}-{self.end_time} "
            f"status='{self.status}'>"
        )

    def __str__(self) -> str:
        status_map = {
            "pending": "ожидает",
            "confirmed": "подтверждено",
            "cancelled": "отменено",
            "completed": "завершено",
        }
        st = status_map.get(self.status, self.status)
        return (
            f"Бронирование #{self.id or '?'} — "
            f"{self.booking_date} {self.start_time}-{self.end_time}, "
            f"гостей: {self.guests_count} ({st})"
        )
