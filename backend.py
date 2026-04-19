from datetime import date, time
from typing import Any, Optional

from postgres_driver import PostgresDriver
from models.user import User
from models.tables import Table
from models.booking import Booking


# ====================================================================== #
#  Создание таблиц
# ====================================================================== #

def create_tables() -> None:
    """Создать все таблицы в правильном порядке (сначала без зависимостей)."""
    with PostgresDriver() as db:
        User.create_table(db)
        Table.create_table(db)
        Booking.create_table(db)


def drop_tables() -> None:
    """Удалить все таблицы в обратном порядке (сначала с зависимостями)."""
    with PostgresDriver() as db:
        Booking.drop_table(db)
        Table.drop_table(db)
        User.drop_table(db)


# ====================================================================== #
#  CRUD: Пользователи (Users)
# ====================================================================== #

def create_user(
    first_name: str,
    last_name: str,
    email: str,
    password_hash: str,
    phone: Optional[str] = None,
    role: str = "client",
    birth_date: Optional[date] = None,
) -> User:
    """Создать нового пользователя и сохранить его в базу.

    Параметры
    ---------
    first_name : str
        Имя.
    last_name : str
        Фамилия.
    email : str
        Электронная почта.
    password_hash : str
        Хеш пароля.
    phone : str или None
        Номер телефона.
    role : str
        Роль (``"client"`` или ``"admin"``).
    birth_date : date или None
        Дата рождения.

    Возвращает
    ---------
    User
        Сохранённый объект пользователя с заполненным ``id``.
    """
    user = User(
        first_name=first_name,
        last_name=last_name,
        email=email,
        password_hash=password_hash,
        phone=phone,
        role=role,
        birth_date=birth_date,
    )
    with PostgresDriver() as db:
        User.save(db, user)
    return user


def get_user_by_id(user_id: int, *, with_password: bool = False) -> Optional[User]:
    """Найти пользователя по идентификатору.

    Параметры
    ---------
    user_id : int
        Идентификатор пользователя.
    with_password : bool
        Вернуть хеш пароля (по умолчанию ``False``).

    Возвращает
    ---------
    User или None
    """
    with PostgresDriver() as db:
        return User.get_by_id(db, user_id, with_password=with_password)


def get_user_by_email(email: str) -> Optional[User]:
    """Найти пользователя по электронной почте (включая хеш пароля).

    Параметры
    ---------
    email : str

    Возвращает
    ---------
    User или None
    """
    with PostgresDriver() as db:
        return User.get_by_email(db, email)


def get_all_users(
    *,
    role: Optional[str] = None,
    is_active: Optional[bool] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> list[User]:
    """Получить список пользователей с необязательной фильтрацией.

    Параметры
    ---------
    role : str или None
        Фильтр по роли.
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
    with PostgresDriver() as db:
        return User.get_all(
            db, role=role, is_active=is_active, limit=limit, offset=offset
        )


def update_user(
    user_id: int,
    *,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    email: Optional[str] = None,
    phone: Optional[str] = None,
    role: Optional[str] = None,
    is_active: Optional[bool] = None,
    birth_date: Optional[date] = None,
) -> Optional[User]:
    """Обновить данные пользователя.

    Передаются только поля, которые нужно изменить.
    Остальные поля остаются без изменений.

    Параметры
    ---------
    user_id : int
        Идентификатор пользователя.
    first_name, last_name, email, phone, role, is_active, birth_date
        Новые значения (необязательные — только то, что меняем).

    Возвращает
    ---------
    User или None
        Обновлённый пользователь, или ``None``, если не найден.
    """
    with PostgresDriver() as db:
        user = User.get_by_id(db, user_id, with_password=True)
        if user is None:
            return None

        if first_name is not None:
            user.first_name = first_name
        if last_name is not None:
            user.last_name = last_name
        if email is not None:
            user.email = User._validate_email(email)
        if phone is not None:
            user.phone = phone
        if role is not None:
            user.role = role
        if is_active is not None:
            user.is_active = is_active
        if birth_date is not None:
            user.birth_date = birth_date

        User.update_user(db, user)
        return user


def delete_user(user_id: int) -> bool:
    """Удалить пользователя по идентификатору.

    Параметры
    ---------
    user_id : int

    Возвращает
    ---------
    bool
        ``True``, если пользователь был найден и удалён.
    """
    with PostgresDriver() as db:
        user = User.get_by_id(db, user_id)
        if user is None:
            return False
        User.delete_user(db, user)
        return True


def deactivate_user(user_id: int) -> Optional[User]:
    """Деактивировать аккаунт пользователя (мягкое удаление).

    Параметры
    ---------
    user_id : int

    Возвращает
    ---------
    User или None
    """
    with PostgresDriver() as db:
        return User.deactivate(db, user_id)


def activate_user(user_id: int) -> Optional[User]:
    """Активировать аккаунт пользователя.

    Параметры
    ---------
    user_id : int

    Возвращает
    ---------
    User или None
    """
    with PostgresDriver() as db:
        return User.activate(db, user_id)


# ====================================================================== #
#  CRUD: Столы (Tables)
# ====================================================================== #

def create_table(
    number: int,
    seats: int,
    location: str = "hall",
    description: Optional[str] = None,
) -> Table:
    """Добавить новый стол в ресторане.

    Параметры
    ---------
    number : int
        Номер стола (уникальный).
    seats : int
        Количество посадочных мест.
    location : str
        Расположение: ``"hall"`` / ``"terrace"`` / ``"vip"`` / ``"bar"``.
    description : str или None
        Произвольное описание.

    Возвращает
    ---------
    Table
        Сохранённый объект стола с заполненным ``id``.
    """
    table = Table(
        number=number,
        seats=seats,
        location=location,
        description=description,
    )
    with PostgresDriver() as db:
        Table.save(db, table)
    return table


def get_table_by_id(table_id: int) -> Optional[Table]:
    """Найти стол по идентификатору.

    Параметры
    ---------
    table_id : int

    Возвращает
    ---------
    Table или None
    """
    with PostgresDriver() as db:
        return Table.get_by_id(db, table_id)


def get_table_by_number(number: int) -> Optional[Table]:
    """Найти стол по номеру.

    Параметры
    ---------
    number : int

    Возвращает
    ---------
    Table или None
    """
    with PostgresDriver() as db:
        return Table.get_by_number(db, number)


def get_all_tables(
    *,
    location: Optional[str] = None,
    is_available: Optional[bool] = None,
    min_seats: Optional[int] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> list[Table]:
    """Получить список столов с необязательной фильтрацией.

    Параметры
    ---------
    location : str или None
        Фильтр по расположению.
    is_available : bool или None
        Фильтр по доступности.
    min_seats : int или None
        Минимальное количество мест.
    limit : int или None
        Ограничение количества строк.
    offset : int или None
        Смещение (для пагинации).

    Возвращает
    ---------
    list[Table]
    """
    with PostgresDriver() as db:
        return Table.get_all(
            db,
            location=location,
            is_available=is_available,
            min_seats=min_seats,
            limit=limit,
            offset=offset,
        )


def update_table(
    table_id: int,
    *,
    number: Optional[int] = None,
    seats: Optional[int] = None,
    location: Optional[str] = None,
    is_available: Optional[bool] = None,
    description: Optional[str] = None,
) -> Optional[Table]:
    """Обновить данные стола.

    Передаются только поля, которые нужно изменить.

    Параметры
    ---------
    table_id : int
        Идентификатор стола.
    number, seats, location, is_available, description
        Новые значения (необязательные).

    Возвращает
    ---------
    Table или None
        Обновлённый стол, или ``None``, если не найден.
    """
    with PostgresDriver() as db:
        table = Table.get_by_id(db, table_id)
        if table is None:
            return None

        if number is not None:
            table.number = number
        if seats is not None:
            table.seats = seats
        if location is not None:
            table.location = location
        if is_available is not None:
            table.is_available = is_available
        if description is not None:
            table.description = description

        Table.update_table(db, table)
        return table


def delete_table(table_id: int) -> bool:
    """Удалить стол по идентификатору.

    Параметры
    ---------
    table_id : int

    Возвращает
    ---------
    bool
        ``True``, если стол был найден и удалён.
    """
    with PostgresDriver() as db:
        table = Table.get_by_id(db, table_id)
        if table is None:
            return False
        Table.delete_table(db, table)
        return True


def mark_table_unavailable(table_id: int) -> Optional[Table]:
    """Пометить стол как недоступный.

    Параметры
    ---------
    table_id : int

    Возвращает
    ---------
    Table или None
    """
    with PostgresDriver() as db:
        return Table.mark_unavailable(db, table_id)


def mark_table_available(table_id: int) -> Optional[Table]:
    """Пометить стол как доступный.

    Параметры
    ---------
    table_id : int

    Возвращает
    ---------
    Table или None
    """
    with PostgresDriver() as db:
        return Table.mark_available(db, table_id)


# ====================================================================== #
#  Проверка доступности
# ====================================================================== #

def is_table_available(
    table_id: int,
    booking_date: date,
    start_time: time,
    end_time: time,
    *,
    exclude_booking_id: Optional[int] = None,
) -> bool:
    """Проверить, свободен ли столик на выбранную дату и время.

    Учитывает только активные бронирования (статусы ``"pending"`` и
    ``"confirmed"``). Отменённые и завершённые — не мешают.

    Параметры
    ---------
    table_id : int
        Идентификатор стола.
    booking_date : date
        Желаемая дата.
    start_time : time
        Желаемое время начала.
    end_time : time
        Желаемое время окончания.
    exclude_booking_id : int или None
        Исключить бронирование с этим id из проверки
        (полезно при обновлении существующего бронирования).

    Возвращает
    ---------
    bool
        ``True`` — стол свободен, можно бронировать.
        ``False`` — время уже занято.

    Пример
    -------
    >>> if is_table_available(1, date(2025, 7, 20), time(18, 0), time(20, 0)):
    ...     print("Свободен!")
    ... else:
    ...     print("Занят.")
    """
    with PostgresDriver() as db:
        return not Booking.has_overlap(
            db,
            table_id=table_id,
            booking_date=booking_date,
            start_time=start_time,
            end_time=end_time,
            exclude_id=exclude_booking_id,
        )


# ====================================================================== #
#  CRUD: Бронирования (Bookings)
# ====================================================================== #

def create_booking(
    user_id: int,
    table_id: int,
    booking_date: date,
    start_time: time,
    end_time: time,
    guests_count: int,
    notes: Optional[str] = None,
) -> Booking:
    """Создать новое бронирование со статусом ``"pending"`` (ожидает проверки).

    Бронирование создаётся в любом случае — пересечение по времени НЕ
    проверяется.  Для проверки и подтверждения используйте
    ``check_and_confirm()``.

    Параметры
    ---------
    user_id : int
        Идентификатор пользователя.
    table_id : int
        Идентификатор стола.
    booking_date : date
        Дата бронирования.
    start_time : time
        Время начала.
    end_time : time
        Время окончания.
    guests_count : int
        Количество гостей.
    notes : str или None
        Примечания.

    Возвращает
    ---------
    Booking
        Сохранённое бронирование со статусом ``"pending"``.
    """
    booking = Booking(
        user_id=user_id,
        table_id=table_id,
        booking_date=booking_date,
        start_time=start_time,
        end_time=end_time,
        guests_count=guests_count,
        notes=notes,
    )
    with PostgresDriver() as db:
        Booking.save(db, booking)
    return booking


def check_and_confirm(booking_id: int) -> Optional[str]:
    """Проверить доступность стола и подтвердить бронирование.

    Если стол свободен — переводит бронирование в статус ``"confirmed"``
    и возвращает строку ``"confirmed"``.

    Если время пересекается с другим подтверждённым или ожидающим
    бронированием — статус остаётся ``"pending"``, и возвращается
    строка ``"overlap"``.

    Если бронирование не найдено — возвращает ``None``.

    Параметры
    ---------
    booking_id : int

    Возвращает
    ---------
    str или None
        ``"confirmed"`` — бронирование подтверждено,
        ``"overlap"`` — есть пересечение, нужно изменить данные,
        ``None`` — бронирование не найдено.
    """
    with PostgresDriver() as db:
        booking = Booking.get_by_id(db, booking_id)
        if booking is None:
            return None

        # Проверяем пересечение (исключая само бронирование)
        if Booking.has_overlap(
            db,
            table_id=booking.table_id,
            booking_date=booking.booking_date,
            start_time=booking.start_time,
            end_time=booking.end_time,
            exclude_id=booking_id,
        ):
            return "overlap"

        # Свободно — подтверждаем
        Booking.confirm(db, booking_id)
        return "confirmed"


def get_booking_by_id(booking_id: int) -> Optional[Booking]:
    """Найти бронирование по идентификатору.

    Параметры
    ---------
    booking_id : int

    Возвращает
    ---------
    Booking или None
    """
    with PostgresDriver() as db:
        return Booking.get_by_id(db, booking_id)


def get_booking_with_details(booking_id: int) -> Optional[dict[str, Any]]:
    """Получить бронирование с полной информацией (пользователь + стол).

    Параметры
    ---------
    booking_id : int

    Возвращает
    ---------
    dict или None
        Словарь с полями бронирования, а также ``user_*`` и ``table_*``.
    """
    with PostgresDriver() as db:
        return Booking.get_with_details(db, booking_id)


def get_bookings_by_user(
    user_id: int,
    *,
    status: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> list[Booking]:
    """Получить все бронирования пользователя.

    Параметры
    ---------
    user_id : int
    status : str или None
        Фильтр по статусу.
    limit : int или None
    offset : int или None

    Возвращает
    ---------
    list[Booking]
    """
    with PostgresDriver() as db:
        return Booking.get_by_user(
            db, user_id, status=status, limit=limit, offset=offset
        )


def get_bookings_by_table(
    table_id: int,
    *,
    booking_date: Optional[date] = None,
    status: Optional[str] = None,
) -> list[Booking]:
    """Получить все бронирования стола.

    Параметры
    ---------
    table_id : int
    booking_date : date или None
        Фильтр по дате.
    status : str или None
        Фильтр по статусу.

    Возвращает
    ---------
    list[Booking]
    """
    with PostgresDriver() as db:
        return Booking.get_by_table(
            db, table_id, booking_date=booking_date, status=status
        )


def get_all_bookings(
    *,
    status: Optional[str] = None,
    booking_date: Optional[date] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> list[Booking]:
    """Получить все бронирования с необязательной фильтрацией.

    Параметры
    ---------
    status : str или None
    booking_date : date или None
    limit : int или None
    offset : int или None

    Возвращает
    ---------
    list[Booking]
    """
    with PostgresDriver() as db:
        return Booking.get_all(
            db,
            status=status,
            booking_date=booking_date,
            limit=limit,
            offset=offset,
        )


def update_booking(
    booking_id: int,
    *,
    table_id: Optional[int] = None,
    booking_date: Optional[date] = None,
    start_time: Optional[time] = None,
    end_time: Optional[time] = None,
    guests_count: Optional[int] = None,
    notes: Optional[str] = None,
) -> Optional[Booking]:
    """Обновить данные бронирования.

    Если меняются стол, дата или время — проверяет отсутствие пересечений.

    Параметры
    ---------
    booking_id : int
        Идентификатор бронирования.
    table_id, booking_date, start_time, end_time, guests_count, notes
        Новые значения (необязательные).

    Возвращает
    ---------
    Booking или None
        Обновлённое бронирование, или ``None``, если не найдено или время занято.
    """
    with PostgresDriver() as db:
        booking = Booking.get_by_id(db, booking_id)
        if booking is None:
            return None

        # Подставляем новые значения или оставляем текущие
        new_table_id = table_id if table_id is not None else booking.table_id
        new_date = booking_date if booking_date is not None else booking.booking_date
        new_start = start_time if start_time is not None else booking.start_time
        new_end = end_time if end_time is not None else booking.end_time

        # Проверяем пересечение, если изменилось что-то из времени/стола/даты
        time_changed = (
            table_id is not None
            or booking_date is not None
            or start_time is not None
            or end_time is not None
        )
        if time_changed:
            if Booking.has_overlap(
                db, new_table_id, new_date, new_start, new_end,
                exclude_id=booking_id,
            ):
                return None

        if table_id is not None:
            booking.table_id = table_id
        if booking_date is not None:
            booking.booking_date = booking_date
        if start_time is not None:
            booking.start_time = start_time
        if end_time is not None:
            booking.end_time = end_time
        if guests_count is not None:
            booking.guests_count = guests_count
        if notes is not None:
            booking.notes = notes

        Booking.update_booking(db, booking)
        return booking


def delete_booking(booking_id: int) -> bool:
    """Удалить бронирование по идентификатору.

    Параметры
    ---------
    booking_id : int

    Возвращает
    ---------
    bool
        ``True``, если бронирование было найдено и удалено.
    """
    with PostgresDriver() as db:
        booking = Booking.get_by_id(db, booking_id)
        if booking is None:
            return False
        Booking.delete_booking(db, booking)
        return True


def confirm_booking(booking_id: int) -> Optional[Booking]:
    """Подтвердить бронирование (статус → ``"confirmed"``).

    Параметры
    ---------
    booking_id : int

    Возвращает
    ---------
    Booking или None
    """
    with PostgresDriver() as db:
        return Booking.confirm(db, booking_id)


def cancel_booking(booking_id: int) -> Optional[Booking]:
    """Отменить бронирование (статус → ``"cancelled"``).

    Параметры
    ---------
    booking_id : int

    Возвращает
    ---------
    Booking или None
    """
    with PostgresDriver() as db:
        return Booking.cancel(db, booking_id)


def complete_booking(booking_id: int) -> Optional[Booking]:
    """Завершить бронирование (статус → ``"completed"``).

    Параметры
    ---------
    booking_id : int

    Возвращает
    ---------
    Booking или None
    """
    with PostgresDriver() as db:
        return Booking.complete(db, booking_id)


# ====================================================================== #
#  Точка входа
# ====================================================================== #

if __name__ == "__main__":
    create_tables()