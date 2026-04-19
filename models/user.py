"""
Модель пользователя для мини-системы бронирования.

Описывает структуру таблицы ``users`` и предоставляет методы
для CRUD-операций через PostgresDriver.
"""

from __future__ import annotations

import os
import re
import sys
from datetime import date, datetime
from typing import Any, Optional

# Поддержка запуска как напрямую, так и при импорте из пакета
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from postgres_driver import PostgresDriver


class User:
    """Модель пользователя мини-системы бронирования.

    Атрибуты
    --------
    id : int или None
        Уникальный идентификатор (автоинкремент, назначается БД).
    first_name : str
        Имя пользователя.
    last_name : str
        Фамилия пользователя.
    email : str
        Электронная почта (уникальная).
    phone : str или None
        Номер телефона в произвольном формате.
    password_hash : str
        Хеш пароля.
    role : str
        Роль: ``"client"`` (клиент), ``"admin"`` (администратор).
        По умолчанию ``"client"``.
    is_active : bool
        Аккаунт активен. По умолчанию ``True``.
    birth_date : date или None
        Дата рождения.
    created_at : datetime или None
        Дата и время регистрации (назначается БД).
    updated_at : datetime или None
        Дата и время последнего обновления (назначается БД).
    """

    # ------------------------------------------------------------------ #
    #  Метаинформация о таблице
    # ------------------------------------------------------------------ #

    TABLE_NAME: str = "users"

    DDL: str = """\
CREATE TABLE IF NOT EXISTS users (
    id            SERIAL        PRIMARY KEY,
    first_name    VARCHAR(100)  NOT NULL,
    last_name     VARCHAR(100)  NOT NULL,
    email         VARCHAR(255)  NOT NULL UNIQUE,
    phone         VARCHAR(30),
    password_hash VARCHAR(255)  NOT NULL,
    role          VARCHAR(20)   NOT NULL DEFAULT 'client'
                                  CHECK (role IN ('client', 'admin')),
    is_active     BOOLEAN       NOT NULL DEFAULT TRUE,
    birth_date    DATE,
    created_at    TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP     NOT NULL DEFAULT NOW()
);
"""

    # Допустимые роли
    ROLES: tuple[str, ...] = ("client", "admin")

    # ------------------------------------------------------------------ #
    #  Конструктор
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        first_name: str,
        last_name: str,
        email: str,
        password_hash: str,
        phone: Optional[str] = None,
        role: str = "client",
        is_active: bool = True,
        birth_date: Optional[date] = None,
        *,
        id: Optional[int] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ) -> None:
        self.id = id
        self.first_name = first_name
        self.last_name = last_name

        self.email = self._validate_email(email)
        self.phone = phone
        self.password_hash = password_hash

        if role not in self.ROLES:
            raise ValueError(
                f"Недопустимая роль '{role}'. Допустимые: {self.ROLES}"
            )
        self.role = role

        self.is_active = is_active
        self.birth_date = birth_date
        self.created_at = created_at
        self.updated_at = updated_at

    # ------------------------------------------------------------------ #
    #  Валидация
    # ------------------------------------------------------------------ #

    @staticmethod
    def _validate_email(email: str) -> str:
        """Проверить формат электронной почты.

        Параметры
        ---------
        email : str
            Адрес электронной почты.

        Возвращает
        ---------
        str
            Оригинальный email, если прошёл проверку.

        Raises
        ------
        ValueError
            Если формат некорректный.
        """
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        if not re.match(pattern, email):
            raise ValueError(f"Некорректный формат email: '{email}'")
        return email

    # ------------------------------------------------------------------ #
    #  Сериализация / десериализация
    # ------------------------------------------------------------------ #

    def to_dict(self, *, exclude_password: bool = True) -> dict[str, Any]:
        """Преобразовать объект пользователя в словарь.

        Параметры
        ---------
        exclude_password : bool
            Исключить ``password_hash`` из результата (по умолчанию ``True``).

        Возвращает
        ---------
        dict
            Словарь с полями пользователя, готовый для передачи в драйвер.
        """
        data: dict[str, Any] = {
            "first_name": self.first_name,
            "last_name": self.last_name,
            "email": self.email,
            "phone": self.phone,
            "password_hash": self.password_hash,
            "role": self.role,
            "is_active": self.is_active,
            "birth_date": self.birth_date,
        }
        if exclude_password:
            data.pop("password_hash", None)
        return data

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "User":
        """Создать объект User из строки БД (словаря).

        Параметры
        ---------
        row : dict
            Словарь, полученный из ``PostgresDriver.fetchone`` / ``fetchall``.

        Возвращает
        ---------
        User
        """
        return cls(
            id=row.get("id"),
            first_name=row["first_name"],
            last_name=row["last_name"],
            email=row["email"],
            password_hash=row.get("password_hash", ""),
            phone=row.get("phone"),
            role=row.get("role", "client"),
            is_active=row.get("is_active", True),
            birth_date=row.get("birth_date"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )

    # ------------------------------------------------------------------ #
    # DDL
    # ------------------------------------------------------------------ #

    @staticmethod
    def create_table(db: PostgresDriver) -> None:
        """Создать таблицу пользователей, если она не существует.

        Параметры
        ---------
        db : PostgresDriver
            Подключённый драйвер базы данных.
        """
        db.execute(User.DDL)

    @staticmethod
    def drop_table(db: PostgresDriver) -> None:
        """Удалить таблицу пользователей целиком.

        Параметры
        ---------
        db : PostgresDriver
            Подключённый драйвер базы данных.
        """
        db.execute(f"DROP TABLE IF EXISTS {User.TABLE_NAME}")

    # ------------------------------------------------------------------ #
    #  CRUD
    # ------------------------------------------------------------------ #

    @staticmethod
    def save(db: PostgresDriver, user: "User") -> "User":
        """Сохранить нового пользователя в базу данных.

        После вставки обновляет ``user.id``, ``user.created_at`` и
        ``user.updated_at`` из возвращённых данных.

        Параметры
        ---------
        db : PostgresDriver
            Подключённый драйвер.
        user : User
            Объект пользователя для сохранения.

        Возвращает
        ---------
        User
            Тот же объект с заполненными ``id``, ``created_at``, ``updated_at``.

        Raises
        ------
        ValueError
            Если пользователь уже имеет ``id`` (уже сохранён).
        """
        if user.id is not None:
            raise ValueError(
                "Пользователь уже имеет id. "
                "Используйте User.update_user() для обновления."
            )

        row = db.insert(
            User.TABLE_NAME,
            user.to_dict(exclude_password=False),
            returning="id, created_at, updated_at",
        )
        if row:
            user.id = row["id"]
            user.created_at = row["created_at"]
            user.updated_at = row["updated_at"]
        return user

    @staticmethod
    def get_by_id(
        db: PostgresDriver,
        user_id: int,
        *,
        with_password: bool = False,
    ) -> Optional["User"]:
        """Найти пользователя по идентификатору.

        Параметры
        ---------
        db : PostgresDriver
            Подключённый драйвер.
        user_id : int
            Идентификатор пользователя.
        with_password : bool
            Вернуть ``password_hash`` в объекте (по умолчанию ``False``).

        Возвращает
        ---------
        User или None
        """
        columns = (
            ["*"]
            if with_password
            else [
                "id", "first_name", "last_name", "email", "phone",
                "role", "is_active", "birth_date", "created_at", "updated_at",
            ]
        )
        row = db.select_one(
            User.TABLE_NAME, columns=columns, conditions={"id": user_id}
        )
        if row and not with_password:
            row.pop("password_hash", None)
        return User.from_row(row) if row else None

    @staticmethod
    def get_by_email(db: PostgresDriver, email: str) -> Optional["User"]:
        """Найти пользователя по email (включая хеш пароля).

        Удобно для аутентификации.

        Параметры
        ---------
        db : PostgresDriver
        email : str

        Возвращает
        ---------
        User или None
        """
        row = db.select_one(User.TABLE_NAME, conditions={"email": email})
        return User.from_row(row) if row else None

    @staticmethod
    def get_all(
        db: PostgresDriver,
        *,
        role: Optional[str] = None,
        is_active: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list["User"]:
        """Получить список пользователей с необязательной фильтрацией.

        Параметры
        ---------
        db : PostgresDriver
        role : str или None
            Фильтр по роли (``"client"`` / ``"admin"``).
        is_active : bool или None
            Фильтр по статусу активности.
        limit : int или None
            Ограничение количества строк.
        offset : int или None
            Смещение (для пагинации).

        Возвращает
        ---------
        list[User]
        """
        conditions: dict[str, Any] = {}
        if role is not None:
            conditions["role"] = role
        if is_active is not None:
            conditions["is_active"] = is_active

        columns = [
            "id", "first_name", "last_name", "email", "phone",
            "role", "is_active", "birth_date", "created_at", "updated_at",
        ]
        rows = db.select(
            User.TABLE_NAME,
            columns=columns,
            conditions=conditions or None,
            order_by="id ASC",
            limit=limit,
            offset=offset,
        )
        return [User.from_row(row) for row in rows]

    @staticmethod
    def update_user(db: PostgresDriver, user: "User") -> "User":
        """Обновить данные существующего пользователя в БД.

        ``updated_at`` обновляется базой автоматически (``NOW()``).

        Параметры
        ---------
        db : PostgresDriver
        user : User
            Объект с изменёнными полями. Должен иметь ``id``.

        Возвращает
        ---------
        User
            Объект с обновлённым ``updated_at``.

        Raises
        ------
        ValueError
            Если у пользователя нет ``id``.
        """
        if user.id is None:
            raise ValueError(
                "Нельзя обновить пользователя без id. Сначала сохраните его."
            )

        data = user.to_dict(exclude_password=False)
        set_parts = [f"{col} = %s" for col in data]
        set_parts.append("updated_at = NOW()")
        params = list(data.values()) + [user.id]

        query = (
            f"UPDATE {User.TABLE_NAME} SET {', '.join(set_parts)} "
            f"WHERE id = %s RETURNING updated_at"
        )
        row = db._execute_returning(query, tuple(params))
        if row:
            user.updated_at = row["updated_at"]
        return user

    @staticmethod
    def delete_user(db: PostgresDriver, user: "User") -> None:
        """Удалить пользователя из базы данных.

        Параметры
        ---------
        db : PostgresDriver
        user : User
            Объект пользователя. Должен иметь ``id``.

        Raises
        ------
        ValueError
            Если у пользователя нет ``id``.
        """
        if user.id is None:
            raise ValueError("Нельзя удалить пользователя без id.")
        db.delete(User.TABLE_NAME, conditions={"id": user.id})

    # ------------------------------------------------------------------ #
    #  Дополнительные операции
    # ------------------------------------------------------------------ #

    @staticmethod
    def deactivate(db: PostgresDriver, user_id: int) -> Optional["User"]:
        """Деактивировать аккаунт пользователя (мягкое удаление).

        Устанавливает ``is_active = False``.

        Параметры
        ---------
        db : PostgresDriver
        user_id : int

        Возвращает
        ---------
        User или None
            Обновлённый пользователь, или ``None``, если не найден.
        """
        row = db.update(
            User.TABLE_NAME,
            data={"is_active": False},
            conditions={"id": user_id},
            returning="*",
        )
        return User.from_row(row) if row else None

    @staticmethod
    def activate(db: PostgresDriver, user_id: int) -> Optional["User"]:
        """Активировать аккаунт пользователя.

        Параметры
        ---------
        db : PostgresDriver
        user_id : int

        Возвращает
        ---------
        User или None
        """
        row = db.update(
            User.TABLE_NAME,
            data={"is_active": True},
            conditions={"id": user_id},
            returning="*",
        )
        return User.from_row(row) if row else None

    @staticmethod
    def count_users(
        db: PostgresDriver,
        *,
        role: Optional[str] = None,
        is_active: Optional[bool] = None,
    ) -> int:
        """Подсчитать количество пользователей с необязательной фильтрацией.

        Параметры
        ---------
        db : PostgresDriver
        role : str или None
        is_active : bool или None

        Возвращает
        ---------
        int
        """
        conditions: dict[str, Any] = {}
        if role is not None:
            conditions["role"] = role
        if is_active is not None:
            conditions["is_active"] = is_active
        return db.count(User.TABLE_NAME, conditions=conditions or None)

    @staticmethod
    def email_exists(db: PostgresDriver, email: str) -> bool:
        """Проверить, занят ли данный email.

        Параметры
        ---------
        db : PostgresDriver
        email : str

        Возвращает
        ---------
        bool
        """
        return db.exists(User.TABLE_NAME, {"email": email})

    # ------------------------------------------------------------------ #
    #  Представление
    # ------------------------------------------------------------------ #

    def __repr__(self) -> str:
        return (
            f"<User id={self.id} email='{self.email}' "
            f"name='{self.first_name} {self.last_name}' "
            f"role='{self.role}' active={self.is_active}>"
        )

    def __str__(self) -> str:
        return f"{self.first_name} {self.last_name} ({self.email})"
