"""
Графический интерфейс системы бронирования ресторана.

Вкладки:
    - Пользователи  — CRUD пользователей
    - Столы          — CRUD столов
    - Бронирования   — CRUD бронирований + управление статусами
    - Доступность    — проверка, свободен ли столик на выбранное время

Запуск:
    python app.py
"""

import os
import sys
import threading
import tkinter as tk
from datetime import date, time
from tkinter import ttk, messagebox

# Поддержка запуска из любой директории
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import backend

# ====================================================================== #
#  Стили и константы
# ====================================================================== #

WINDOW_TITLE = "Система бронирования ресторана"
WINDOW_MIN_SIZE = (1100, 700)

LOCATION_MAP = {"hall": "Зал", "terrace": "Терраса", "vip": "VIP", "bar": "Бар"}
LOCATION_MAP_R = {v: k for k, v in LOCATION_MAP.items()}

ROLE_MAP = {"client": "Клиент", "admin": "Администратор"}
ROLE_MAP_R = {v: k for k, v in ROLE_MAP.items()}

STATUS_MAP = {
    "pending": "Ожидает",
    "confirmed": "Подтверждено",
    "cancelled": "Отменено",
    "completed": "Завершено",
}
STATUS_MAP_R = {v: k for k, v in STATUS_MAP.items()}


# ====================================================================== #
#  Утилиты
# ====================================================================== #

def run_in_thread(fn):
    """Декоратор: запускает функцию в отдельном потоке, чтобы не блокировать GUI."""

    def wrapper(*args, **kwargs):
        def target():
            try:
                fn(*args, **kwargs)
            except Exception as exc:
                # Показываем ошибку в главном потоке
                try:
                    root = args[0].winfo_toplevel()
                    root.after(0, lambda: messagebox.showerror("Ошибка", str(exc)))
                except Exception:
                    pass

        t = threading.Thread(target=target, daemon=True)
        t.start()

    return wrapper


def show_info(title: str, message: str):
    """Показать информационное сообщение."""
    messagebox.showinfo(title, message)


def show_error(title: str, message: str):
    """Показать сообщение об ошибке."""
    messagebox.showerror(title, message)


def confirm(title: str, message: str) -> bool:
    """Показать диалог подтверждения. Возвращает True при 'Да'."""
    return messagebox.askyesno(title, message)


# ====================================================================== #
#  Главное приложение
# ====================================================================== #

class App(tk.Tk):
    """Главное окно приложения."""

    def __init__(self):
        super().__init__()
        self.title(WINDOW_TITLE)
        self.minsize(*WINDOW_MIN_SIZE)

        # Инициализация БД при запуске
        try:
            backend.create_tables()
        except Exception as exc:
            show_error("Ошибка БД", f"Не удалось создать таблицы:\n{exc}")

        # Стили
        style = ttk.Style(self)
        style.configure("Header.TLabel", font=("Segoe UI", 11, "bold"))
        style.configure("Status.TLabel", font=("Segoe UI", 9), foreground="gray")

        # Notebook (вкладки)
        nb = ttk.Notebook(self)
        nb.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)

        self.users_tab = UsersTab(nb)
        self.tables_tab = TablesTab(nb)
        self.bookings_tab = BookingsTab(nb)
        self.availability_tab = AvailabilityTab(nb)

        nb.add(self.users_tab, text="  Пользователи  ")
        nb.add(self.tables_tab, text="  Столы  ")
        nb.add(self.bookings_tab, text="  Бронирования  ")
        nb.add(self.availability_tab, text="  Доступность  ")


# ====================================================================== #
#  Вкладка: Пользователи
# ====================================================================== #

class UsersTab(ttk.Frame):
    """CRUD для пользователей."""

    def __init__(self, master):
        super().__init__(master)
        self._build_ui()
        self._refresh_list()

    # ------------------------------------------------------------------ #
    #  UI
    # ------------------------------------------------------------------ #

    def _build_ui(self):
        # ---- Верхняя панель: форма ----
        form = ttk.LabelFrame(self, text="Данные пользователя", padding=10)
        form.pack(fill=tk.X, padx=6, pady=(6, 3))

        labels = [
            "ID (для поиска/обновления):",
            "Имя:*", "Фамилия:*", "Email:*", "Телефон:",
            "Пароль:*", "Роль:", "Дата рождения (ГГГГ-ММ-ДД):",
        ]
        self.vars = {}
        keys = ["id", "first_name", "last_name", "email", "phone",
                "password", "role", "birth_date"]

        for i, (lbl, key) in enumerate(zip(labels, keys)):
            ttk.Label(form, text=lbl).grid(row=i, column=0, sticky=tk.W, pady=2)
            var = tk.StringVar()
            self.vars[key] = var
            if key == "role":
                cb = ttk.Combobox(form, textvariable=var,
                                  values=["Клиент", "Администратор"],
                                  state="readonly", width=28)
                cb.set("Клиент")
                cb.grid(row=i, column=1, sticky=tk.EW, pady=2, padx=(6, 0))
            else:
                ttk.Entry(form, textvariable=var, width=30).grid(
                    row=i, column=1, sticky=tk.EW, pady=2, padx=(6, 0)
                )
        form.columnconfigure(1, weight=1)

        # ---- Кнопки ----
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=6, pady=3)

        buttons = [
            ("Создать", self._on_create),
            ("Найти по ID", self._on_find),
            ("Найти по Email", self._on_find_email),
            ("Обновить", self._on_update),
            ("Удалить", self._on_delete),
            ("Активировать", self._on_activate),
            ("Деактивировать", self._on_deactivate),
            ("Обновить список", self._refresh_list),
        ]
        for text, cmd in buttons:
            ttk.Button(btn_frame, text=text, command=cmd).pack(
                side=tk.LEFT, padx=2, pady=2
            )

        # ---- Таблица ----
        tree_frame = ttk.LabelFrame(self, text="Список пользователей", padding=4)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=6, pady=(3, 6))

        cols = ("id", "first_name", "last_name", "email", "phone",
                "role", "is_active", "birth_date")
        self.tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=14)

        headers = {
            "id": "ID", "first_name": "Имя", "last_name": "Фамилия",
            "email": "Email", "phone": "Телефон",
            "role": "Роль", "is_active": "Активен",
            "birth_date": "Дата рождения",
        }
        widths = {
            "id": 40, "first_name": 100, "last_name": 110,
            "email": 180, "phone": 110,
            "role": 100, "is_active": 70, "birth_date": 110,
        }
        for col in cols:
            self.tree.heading(col, text=headers[col])
            self.tree.column(col, width=widths[col], anchor=tk.W)

        vsb = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)

        self.tree.bind("<<TreeviewSelect>>", self._on_select)

    # ------------------------------------------------------------------ #
    #  Утилиты формы
    # ------------------------------------------------------------------ #

    def _clear_form(self):
        """Очистить все поля формы."""
        for key in ("id", "first_name", "last_name", "email",
                     "phone", "password", "birth_date"):
            self.vars[key].set("")
        self.vars["role"].set("Клиент")

    # ------------------------------------------------------------------ #
    #  Обработчики
    # ------------------------------------------------------------------ #

    @run_in_thread
    def _refresh_list(self):
        try:
            users = backend.get_all_users()
            self.after(0, lambda: self._populate(users))
        except Exception as exc:
            show_error("Ошибка", str(exc))

    def _populate(self, users):
        self.tree.delete(*self.tree.get_children())
        for u in users:
            self.tree.insert("", tk.END, values=(
                u.id, u.first_name, u.last_name, u.email,
                u.phone or "",
                ROLE_MAP.get(u.role, u.role),
                "Да" if u.is_active else "Нет",
                u.birth_date or "",
            ))

    def _on_select(self, _event):
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], "values")
        self.vars["id"].set(vals[0])
        self.vars["first_name"].set(vals[1])
        self.vars["last_name"].set(vals[2])
        self.vars["email"].set(vals[3])
        self.vars["phone"].set(vals[4] if vals[4] != "" else "")
        self.vars["role"].set(vals[5])
        self.vars["birth_date"].set(vals[7] if vals[7] != "" else "")

    @run_in_thread
    def _on_create(self):
        try:
            fn = self.vars["first_name"].get().strip()
            ln = self.vars["last_name"].get().strip()
            em = self.vars["email"].get().strip()
            ph = self.vars["phone"].get().strip() or None
            pw = self.vars["password"].get().strip()
            ro = ROLE_MAP_R.get(self.vars["role"].get(), "client")
            bd_str = self.vars["birth_date"].get().strip()
            bd = bd_str if bd_str else None

            if not fn or not ln or not em or not pw:
                show_error("Валидация", "Заполните обязательные поля: Имя, Фамилия, Email, Пароль")
                return

            user = backend.create_user(fn, ln, em, pw, phone=ph, role=ro, birth_date=bd)
            self.after(0, self._clear_form)
            self.after(0, lambda: show_info("Создано", f"Пользователь создан (ID={user.id})"))
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка создания", str(exc))

    @run_in_thread
    def _on_find(self):
        try:
            uid = self.vars["id"].get().strip()
            if not uid:
                show_error("Ввод", "Введите ID пользователя")
                return
            user = backend.get_user_by_id(int(uid), with_password=True)
            if user is None:
                show_info("Поиск", "Пользователь не найден")
                return
            self.after(0, lambda: self._fill_form(user))
        except Exception as exc:
            show_error("Ошибка", str(exc))

    @run_in_thread
    def _on_find_email(self):
        try:
            em = self.vars["email"].get().strip()
            if not em:
                show_error("Ввод", "Введите Email для поиска")
                return
            user = backend.get_user_by_email(em)
            if user is None:
                show_info("Поиск", "Пользователь не найден")
                return
            self.after(0, lambda: self._fill_form(user))
        except Exception as exc:
            show_error("Ошибка", str(exc))

    def _fill_form(self, u):
        self.vars["id"].set(u.id or "")
        self.vars["first_name"].set(u.first_name or "")
        self.vars["last_name"].set(u.last_name or "")
        self.vars["email"].set(u.email or "")
        self.vars["phone"].set(u.phone or "")
        self.vars["password"].set(u.password_hash if hasattr(u, "password_hash") else "")
        self.vars["role"].set(ROLE_MAP.get(u.role, u.role))
        self.vars["birth_date"].set(str(u.birth_date) if u.birth_date else "")

    @run_in_thread
    def _on_update(self):
        try:
            uid_str = self.vars["id"].get().strip()
            if not uid_str:
                show_error("Ввод", "Введите ID пользователя для обновления")
                return
            uid = int(uid_str)

            kwargs = {}
            fn = self.vars["first_name"].get().strip()
            ln = self.vars["last_name"].get().strip()
            em = self.vars["email"].get().strip()
            ph = self.vars["phone"].get().strip()
            ro = ROLE_MAP_R.get(self.vars["role"].get())
            bd_str = self.vars["birth_date"].get().strip()

            if fn: kwargs["first_name"] = fn
            if ln: kwargs["last_name"] = ln
            if em: kwargs["email"] = em
            if ph: kwargs["phone"] = ph
            if ro: kwargs["role"] = ro
            if bd_str: kwargs["birth_date"] = bd_str

            result = backend.update_user(uid, **kwargs)
            if result is None:
                show_error("Ошибка", "Пользователь не найден")
                return
            self.after(0, self._clear_form)
            self.after(0, lambda: show_info("Обновлено", f"Пользователь ID={uid} обновлён"))
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка обновления", str(exc))

    @run_in_thread
    def _on_delete(self):
        try:
            uid_str = self.vars["id"].get().strip()
            if not uid_str:
                show_error("Ввод", "Введите ID пользователя для удаления")
                return
            uid = int(uid_str)
            if not confirm("Удаление", f"Удалить пользователя ID={uid}?"):
                return
            ok = backend.delete_user(uid)
            if ok:
                self.after(0, self._clear_form)
                self.after(0, lambda: show_info("Удалено", "Пользователь удалён"))
            else:
                show_info("Удаление", "Пользователь не найден")
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка удаления", str(exc))

    @run_in_thread
    def _on_activate(self):
        try:
            uid_str = self.vars["id"].get().strip()
            if not uid_str:
                show_error("Ввод", "Введите ID")
                return
            result = backend.activate_user(int(uid_str))
            if result:
                self.after(0, lambda: show_info("Активация", "Пользователь активирован"))
            else:
                show_info("Активация", "Пользователь не найден")
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка", str(exc))

    @run_in_thread
    def _on_deactivate(self):
        try:
            uid_str = self.vars["id"].get().strip()
            if not uid_str:
                show_error("Ввод", "Введите ID")
                return
            result = backend.deactivate_user(int(uid_str))
            if result:
                self.after(0, lambda: show_info("Деактивация", "Пользователь деактивирован"))
            else:
                show_info("Деактивация", "Пользователь не найден")
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка", str(exc))


# ====================================================================== #
#  Вкладка: Столы
# ====================================================================== #

class TablesTab(ttk.Frame):
    """CRUD для столов."""

    def __init__(self, master):
        super().__init__(master)
        self._build_ui()
        self._refresh_list()

    def _build_ui(self):
        # ---- Форма ----
        form = ttk.LabelFrame(self, text="Данные стола", padding=10)
        form.pack(fill=tk.X, padx=6, pady=(6, 3))

        labels = ["ID (для поиска/обновления):", "Номер:*", "Мест:*",
                  "Расположение:", "Описание:"]
        keys = ["id", "number", "seats", "location", "description"]
        self.vars = {}

        for i, (lbl, key) in enumerate(zip(labels, keys)):
            ttk.Label(form, text=lbl).grid(row=i, column=0, sticky=tk.W, pady=2)
            var = tk.StringVar()
            self.vars[key] = var
            if key == "location":
                cb = ttk.Combobox(form, textvariable=var,
                                  values=list(LOCATION_MAP.values()),
                                  state="readonly", width=28)
                cb.set("Зал")
                cb.grid(row=i, column=1, sticky=tk.EW, pady=2, padx=(6, 0))
            else:
                ttk.Entry(form, textvariable=var, width=30).grid(
                    row=i, column=1, sticky=tk.EW, pady=2, padx=(6, 0)
                )
        form.columnconfigure(1, weight=1)

        # ---- Кнопки ----
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=6, pady=3)

        buttons = [
            ("Создать", self._on_create),
            ("Найти по ID", self._on_find),
            ("Найти по номеру", self._on_find_by_number),
            ("Обновить", self._on_update),
            ("Удалить", self._on_delete),
            ("Недоступен", self._on_mark_unavailable),
            ("Доступен", self._on_mark_available),
            ("Обновить список", self._refresh_list),
        ]
        for text, cmd in buttons:
            ttk.Button(btn_frame, text=text, command=cmd).pack(
                side=tk.LEFT, padx=2, pady=2
            )

        # ---- Таблица ----
        tree_frame = ttk.LabelFrame(self, text="Список столов", padding=4)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=6, pady=(3, 6))

        cols = ("id", "number", "seats", "location", "is_available", "description")
        self.tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=14)

        headers = {
            "id": "ID", "number": "Номер", "seats": "Мест",
            "location": "Расположение", "is_available": "Доступен",
            "description": "Описание",
        }
        widths = {
            "id": 40, "number": 70, "seats": 60,
            "location": 130, "is_available": 80, "description": 250,
        }
        for col in cols:
            self.tree.heading(col, text=headers[col])
            self.tree.column(col, width=widths[col], anchor=tk.W)

        vsb = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)

        self.tree.bind("<<TreeviewSelect>>", self._on_select)

    # ------------------------------------------------------------------ #
    #  Утилиты формы
    # ------------------------------------------------------------------ #

    def _clear_form(self):
        """Очистить все поля формы."""
        for key in ("id", "number", "seats", "description"):
            self.vars[key].set("")
        self.vars["location"].set("Зал")

    # ------------------------------------------------------------------ #

    @run_in_thread
    def _refresh_list(self):
        try:
            tables = backend.get_all_tables()
            self.after(0, lambda: self._populate(tables))
        except Exception as exc:
            show_error("Ошибка", str(exc))

    def _populate(self, tables):
        self.tree.delete(*self.tree.get_children())
        for t in tables:
            self.tree.insert("", tk.END, values=(
                t.id, t.number, t.seats,
                LOCATION_MAP.get(t.location, t.location),
                "Да" if t.is_available else "Нет",
                t.description or "",
            ))

    def _on_select(self, _event):
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], "values")
        self.vars["id"].set(vals[0])
        self.vars["number"].set(vals[1])
        self.vars["seats"].set(vals[2])
        self.vars["location"].set(vals[3])
        self.vars["description"].set(vals[5] if vals[5] != "" else "")

    def _fill_form(self, t):
        self.vars["id"].set(t.id or "")
        self.vars["number"].set(t.number or "")
        self.vars["seats"].set(t.seats or "")
        self.vars["location"].set(LOCATION_MAP.get(t.location, t.location))
        self.vars["description"].set(t.description or "")

    @run_in_thread
    def _on_create(self):
        try:
            num = self.vars["number"].get().strip()
            seats = self.vars["seats"].get().strip()
            loc = LOCATION_MAP_R.get(self.vars["location"].get(), "hall")
            desc = self.vars["description"].get().strip() or None

            if not num or not seats:
                show_error("Валидация", "Заполните: Номер, Мест")
                return

            table = backend.create_table(int(num), int(seats), loc, desc)
            self.after(0, self._clear_form)
            self.after(0, lambda: show_info("Создано", f"Стол создан (ID={table.id})"))
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка создания", str(exc))

    @run_in_thread
    def _on_find(self):
        try:
            tid = self.vars["id"].get().strip()
            if not tid:
                show_error("Ввод", "Введите ID стола")
                return
            table = backend.get_table_by_id(int(tid))
            if table is None:
                show_info("Поиск", "Стол не найден")
                return
            self.after(0, lambda: self._fill_form(table))
        except Exception as exc:
            show_error("Ошибка", str(exc))

    @run_in_thread
    def _on_find_by_number(self):
        try:
            num = self.vars["number"].get().strip()
            if not num:
                show_error("Ввод", "Введите номер стола")
                return
            table = backend.get_table_by_number(int(num))
            if table is None:
                show_info("Поиск", "Стол не найден")
                return
            self.after(0, lambda: self._fill_form(table))
        except Exception as exc:
            show_error("Ошибка", str(exc))

    @run_in_thread
    def _on_update(self):
        try:
            tid_str = self.vars["id"].get().strip()
            if not tid_str:
                show_error("Ввод", "Введите ID стола для обновления")
                return
            tid = int(tid_str)

            kwargs = {}
            num = self.vars["number"].get().strip()
            seats = self.vars["seats"].get().strip()
            loc = self.vars["location"].get()
            desc = self.vars["description"].get().strip()

            if num: kwargs["number"] = int(num)
            if seats: kwargs["seats"] = int(seats)
            if loc: kwargs["location"] = LOCATION_MAP_R.get(loc, loc)
            if desc: kwargs["description"] = desc

            result = backend.update_table(tid, **kwargs)
            if result is None:
                show_error("Ошибка", "Стол не найден")
                return
            self.after(0, self._clear_form)
            self.after(0, lambda: show_info("Обновлено", f"Стол ID={tid} обновлён"))
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка обновления", str(exc))

    @run_in_thread
    def _on_delete(self):
        try:
            tid_str = self.vars["id"].get().strip()
            if not tid_str:
                show_error("Ввод", "Введите ID стола для удаления")
                return
            tid = int(tid_str)
            if not confirm("Удаление", f"Удалить стол ID={tid}?"):
                return
            ok = backend.delete_table(tid)
            if ok:
                self.after(0, self._clear_form)
                self.after(0, lambda: show_info("Удалено", "Стол удалён"))
            else:
                show_info("Удаление", "Стол не найден")
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка удаления", str(exc))

    @run_in_thread
    def _on_mark_unavailable(self):
        try:
            tid_str = self.vars["id"].get().strip()
            if not tid_str:
                show_error("Ввод", "Введите ID стола")
                return
            result = backend.mark_table_unavailable(int(tid_str))
            if result:
                self.after(0, lambda: show_info("Статус", "Стол помечен как недоступный"))
            else:
                show_info("Статус", "Стол не найден")
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка", str(exc))

    @run_in_thread
    def _on_mark_available(self):
        try:
            tid_str = self.vars["id"].get().strip()
            if not tid_str:
                show_error("Ввод", "Введите ID стола")
                return
            result = backend.mark_table_available(int(tid_str))
            if result:
                self.after(0, lambda: show_info("Статус", "Стол помечен как доступный"))
            else:
                show_info("Статус", "Стол не найден")
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка", str(exc))


# ====================================================================== #
#  Вкладка: Бронирования
# ====================================================================== #

class BookingsTab(ttk.Frame):
    """CRUD для бронирований + управление статусами."""

    def __init__(self, master):
        super().__init__(master)
        self._build_ui()
        self._refresh_list()

    def _build_ui(self):
        # ---- Справочники для combo-боксов ----
        self._users_cache = []     # [(id, "Фамилия Имя (email)"), ...]
        self._tables_cache = []    # [(id, "№X —Loc (Y мест)"), ...]

        # ---- Форма ----
        form = ttk.LabelFrame(self, text="Данные бронирования", padding=10)
        form.pack(fill=tk.X, padx=6, pady=(6, 3))

        self.vars = {}

        # Строка 0: ID бронирования
        ttk.Label(form, text="ID бронирования (поиск):").grid(
            row=0, column=0, sticky=tk.W, pady=2, padx=(0, 4))
        self.vars["id"] = tk.StringVar()
        ttk.Entry(form, textvariable=self.vars["id"], width=22).grid(
            row=0, column=1, sticky=tk.EW, pady=2)

        # Строка 1: Пользователь (combo)
        ttk.Label(form, text="Пользователь:*").grid(
            row=1, column=0, sticky=tk.W, pady=2, padx=(0, 4))
        self.vars["user_combo"] = tk.StringVar()
        self.user_combo = ttk.Combobox(
            form, textvariable=self.vars["user_combo"],
            state="readonly", width=30, postcommand=self._ensure_users_loaded)
        self.user_combo.grid(row=1, column=1, sticky=tk.EW, pady=2)

        # Строка 2: Стол (combo)
        ttk.Label(form, text="Стол:*").grid(
            row=2, column=0, sticky=tk.W, pady=2, padx=(0, 4))
        self.vars["table_combo"] = tk.StringVar()
        self.table_combo = ttk.Combobox(
            form, textvariable=self.vars["table_combo"],
            state="readonly", width=30, postcommand=self._ensure_tables_loaded)
        self.table_combo.grid(row=2, column=1, sticky=tk.EW, pady=2)

        # Остальные поля в сетке 2 колонки
        rest_labels = [
            ("Дата (ГГГГ-ММ-ДД):*", "booking_date"),
            ("Начало (ЧЧ:ММ):*", "start_time"),
            ("Конец (ЧЧ:ММ):*", "end_time"),
            ("Кол-во гостей:*", "guests_count"),
            ("Примечания:", "notes"),
        ]
        for i, (lbl, key) in enumerate(rest_labels):
            r, c = divmod(i, 2)
            row = 3 + r
            col = c * 2
            ttk.Label(form, text=lbl).grid(
                row=row, column=col, sticky=tk.W, pady=2, padx=(0, 4))
            var = tk.StringVar()
            self.vars[key] = var
            ttk.Entry(form, textvariable=var, width=22).grid(
                row=row, column=col + 1, sticky=tk.EW, pady=2)

        form.columnconfigure(1, weight=1)
        form.columnconfigure(3, weight=1)

        # Загружаем справочники
        self._load_users()
        self._load_tables()

        # ---- Кнопки CRUD ----
        btn_frame1 = ttk.Frame(self)
        btn_frame1.pack(fill=tk.X, padx=6, pady=3)

        for text, cmd in [
            ("Создать", self._on_create),
            ("Найти по ID", self._on_find),
            ("💾 Сохранить", self._on_update),
            ("Удалить", self._on_delete),
            ("Подробности (JOIN)", self._on_details),
        ]:
            ttk.Button(btn_frame1, text=text, command=cmd).pack(
                side=tk.LEFT, padx=2, pady=2
            )

        # ---- Кнопки статусов ----
        btn_frame2 = ttk.Frame(self)
        btn_frame2.pack(fill=tk.X, padx=6, pady=(0, 3))

        ttk.Label(btn_frame2, text="Изменить статус:", style="Header.TLabel").pack(
            side=tk.LEFT, padx=(0, 8)
        )
        for text, cmd in [
            ("✓ Проверить и подтвердить", self._on_check_and_confirm),
            ("✗ Отменить", self._on_cancel),
            ("✔ Завершить", self._on_complete),
            ("Обновить список", self._refresh_list),
        ]:
            ttk.Button(btn_frame2, text=text, command=cmd).pack(
                side=tk.LEFT, padx=2, pady=2
            )

        # ---- Фильтр ----
        filter_frame = ttk.Frame(self)
        filter_frame.pack(fill=tk.X, padx=6, pady=(0, 3))

        ttk.Label(filter_frame, text="Фильтр — статус:").pack(side=tk.LEFT, padx=(0, 4))
        self.filter_status = tk.StringVar()
        cb = ttk.Combobox(filter_frame, textvariable=self.filter_status,
                          values=["", "pending", "confirmed", "cancelled", "completed"],
                          state="readonly", width=15)
        cb.pack(side=tk.LEFT, padx=(0, 12))
        cb.bind("<<ComboboxSelected>>", lambda _: self._refresh_list())

        ttk.Label(filter_frame, text="Дата:").pack(side=tk.LEFT, padx=(0, 4))
        self.filter_date = tk.StringVar()
        ttk.Entry(filter_frame, textvariable=self.filter_date, width=12).pack(
            side=tk.LEFT, padx=(0, 8)
        )
        ttk.Button(filter_frame, text="Применить", command=self._refresh_list).pack(
            side=tk.LEFT, padx=2
        )

        # ---- Таблица ----
        tree_frame = ttk.LabelFrame(self, text="Список бронирований", padding=4)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=6, pady=(3, 6))

        cols = ("id", "user_id", "table_id", "booking_date",
                "start_time", "end_time", "guests_count", "status", "notes")
        self.tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=12)

        headers = {
            "id": "ID", "user_id": "Пользователь", "table_id": "Стол",
            "booking_date": "Дата", "start_time": "Начало",
            "end_time": "Конец", "guests_count": "Гости",
            "status": "Статус", "notes": "Примечания",
        }
        widths = {
            "id": 40, "user_id": 90, "table_id": 50,
            "booking_date": 100, "start_time": 70,
            "end_time": 70, "guests_count": 55,
            "status": 110, "notes": 200,
        }
        for col in cols:
            self.tree.heading(col, text=headers[col])
            self.tree.column(col, width=widths[col], anchor=tk.W)

        vsb = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)

        self.tree.bind("<<TreeviewSelect>>", self._on_select)

    # ------------------------------------------------------------------ #
    #  Утилиты формы
    # ------------------------------------------------------------------ #

    def _load_users(self):
        """Загрузить список пользователей из БД в кэш и combo."""
        try:
            users = backend.get_all_users()
            self._users_cache = [
                (u.id, f"{u.last_name} {u.first_name} ({u.email})")
                for u in users
            ]
            self.user_combo["values"] = [label for _, label in self._users_cache]
        except Exception:
            self._users_cache = []
            self.user_combo["values"] = []

    def _load_tables(self):
        """Загрузить список столов из БД в кэш и combo."""
        try:
            tables = backend.get_all_tables()
            self._tables_cache = [
                (t.id, f"№{t.number} — {LOCATION_MAP.get(t.location, t.location)} ({t.seats} мест)")
                for t in tables
            ]
            self.table_combo["values"] = [label for _, label in self._tables_cache]
        except Exception:
            self._tables_cache = []
            self.table_combo["values"] = []

    def _ensure_users_loaded(self):
        """Callback при открытии списка — обновляет данные."""
        self._load_users()

    def _ensure_tables_loaded(self):
        """Callback при открытии списка — обновляет данные."""
        self._load_tables()

    def _get_selected_user_id(self) -> int | None:
        """Получить ID выбранного пользователя из combo."""
        val = self.vars["user_combo"].get()
        for uid, label in self._users_cache:
            if label == val:
                return uid
        return None

    def _get_selected_table_id(self) -> int | None:
        """Получить ID выбранного стола из combo."""
        val = self.vars["table_combo"].get()
        for tid, label in self._tables_cache:
            if label == val:
                return tid
        return None

    def _get_table_seats(self, table_id: int) -> int | None:
        """Получить количество мест за выбранным столом."""
        try:
            table = backend.get_table_by_id(table_id)
            return table.seats if table else None
        except Exception:
            return None

    def _select_user_by_id(self, uid: int):
        """Выбрать пользователя в combo по ID."""
        for _id, label in self._users_cache:
            if _id == uid:
                self.vars["user_combo"].set(label)
                return
        self.vars["user_combo"].set("")

    def _select_table_by_id(self, tid: int):
        """Выбрать стол в combo по ID."""
        for _id, label in self._tables_cache:
            if _id == tid:
                self.vars["table_combo"].set(label)
                return
        self.vars["table_combo"].set("")

    def _clear_form(self):
        """Очистить все поля формы."""
        for key in ("id", "booking_date", "start_time", "end_time",
                     "guests_count", "notes"):
            self.vars[key].set("")
        self.vars["user_combo"].set("")
        self.vars["table_combo"].set("")

    # ------------------------------------------------------------------ #

    @run_in_thread
    def _refresh_list(self):
        try:
            kwargs = {}
            st = self.filter_status.get()
            dt_str = getattr(self, "filter_date", None) and self.filter_date.get().strip()
            if st:
                kwargs["status"] = st
            if dt_str:
                kwargs["booking_date"] = dt_str
            bookings = backend.get_all_bookings(**kwargs)
            self.after(0, lambda: self._populate(bookings))
        except Exception as exc:
            show_error("Ошибка", str(exc))

    def _populate(self, bookings):
        self.tree.delete(*self.tree.get_children())
        for b in bookings:
            self.tree.insert("", tk.END, values=(
                b.id, b.user_id, b.table_id,
                b.booking_date, b.start_time, b.end_time,
                b.guests_count,
                STATUS_MAP.get(b.status, b.status),
                b.notes or "",
            ))

    def _on_select(self, _event):
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], "values")
        self.vars["id"].set(vals[0])
        self._select_user_by_id(int(vals[1]))
        self._select_table_by_id(int(vals[2]))
        self.vars["booking_date"].set(vals[3])
        self.vars["start_time"].set(vals[4])
        self.vars["end_time"].set(vals[5])
        self.vars["guests_count"].set(vals[6])
        self.vars["notes"].set(vals[8] if len(vals) > 8 and vals[8] != "" else "")

    @run_in_thread
    def _on_create(self):
        try:
            uid = self._get_selected_user_id()
            tid = self._get_selected_table_id()
            bd = self.vars["booking_date"].get().strip()
            st = self.vars["start_time"].get().strip()
            et = self.vars["end_time"].get().strip()
            gc = self.vars["guests_count"].get().strip()
            notes = self.vars["notes"].get().strip() or None

            if not all([uid, tid, bd, st, et, gc]):
                show_error("Валидация", "Заполните все обязательные поля (выберите пользователя и стол)")
                return

            gc_int = int(gc)

            # Проверка: гостей больше, чем мест за столом
            seats = self._get_table_seats(tid)
            seats_warning = ""
            if seats is not None and gc_int > seats:
                seats_warning = (
                    f"⚠ Внимание: гостей ({gc_int}) больше, чем мест за столом ({seats})!"
                )
                # Добавляем пометку в примечания
                auto_note = f"[Мест: {seats}, гостей: {gc_int}]"
                notes = f"{auto_note} {notes}" if notes else auto_note

            booking = backend.create_booking(
                uid, tid, bd, st, et, gc_int, notes
            )
            self.after(0, self._clear_form)

            if seats_warning:
                self.after(0, lambda: show_error(
                    "⚠ Несоответствие мест",
                    f"Бронирование создано (ID={booking.id})\n\n"
                    f"{seats_warning}\n\n"
                    f"Пометка добавлена в примечания."
                ))
            else:
                self.after(0, lambda: show_info(
                    "Создано",
                    f"Бронирование создано (ID={booking.id})\n"
                    f"Статус: ОЖИДАЕТ\n\n"
                    f"Нажмите «Проверить и подтвердить», чтобы подтвердить."
                ))
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка создания", str(exc))

    @run_in_thread
    def _on_find(self):
        try:
            bid = self.vars["id"].get().strip()
            if not bid:
                show_error("Ввод", "Введите ID бронирования")
                return
            booking = backend.get_booking_by_id(int(bid))
            if booking is None:
                show_info("Поиск", "Бронирование не найдено")
                return
            self.after(0, lambda: self._fill_form(booking))
        except Exception as exc:
            show_error("Ошибка", str(exc))

    def _fill_form(self, b):
        self.vars["id"].set(b.id or "")
        self._select_user_by_id(b.user_id)
        self._select_table_by_id(b.table_id)
        self.vars["booking_date"].set(str(b.booking_date) if b.booking_date else "")
        self.vars["start_time"].set(str(b.start_time) if b.start_time else "")
        self.vars["end_time"].set(str(b.end_time) if b.end_time else "")
        self.vars["guests_count"].set(b.guests_count or "")
        self.vars["notes"].set(b.notes or "")

    @run_in_thread
    def _on_update(self):
        try:
            bid_str = self.vars["id"].get().strip()
            if not bid_str:
                show_error("Ввод", "Введите ID бронирования для обновления")
                return
            bid = int(bid_str)

            kwargs = {}
            tid = self._get_selected_table_id()
            bd = self.vars["booking_date"].get().strip()
            st = self.vars["start_time"].get().strip()
            et = self.vars["end_time"].get().strip()
            gc = self.vars["guests_count"].get().strip()
            notes = self.vars["notes"].get().strip()

            if tid: kwargs["table_id"] = tid
            if bd: kwargs["booking_date"] = bd
            if st: kwargs["start_time"] = st
            if et: kwargs["end_time"] = et
            if gc: kwargs["guests_count"] = int(gc)
            if notes: kwargs["notes"] = notes

            # Проверка соответствия мест и гостей
            seats_warning = ""
            check_tid = tid
            check_gc = int(gc) if gc else None
            if check_tid and check_gc is not None:
                seats = self._get_table_seats(check_tid)
                if seats is not None and check_gc > seats:
                    seats_warning = (
                        f"⚠ Гостей ({check_gc}) больше, чем мест за столом ({seats})!"
                    )
                    # Добавляем пометку в notes
                    auto_note = f"[Мест: {seats}, гостей: {check_gc}]"
                    kwargs["notes"] = f"{auto_note} {notes}" if notes else auto_note

            result = backend.update_booking(bid, **kwargs)
            if result is None:
                self.after(0, lambda: show_error(
                    "Ошибка", "Не найдено или время занято"
                ))
                return
            self.after(0, self._clear_form)
            if seats_warning:
                self.after(0, lambda: show_error(
                    "⚠ Несоответствие мест",
                    f"Бронирование ID={bid} обновлено\n\n"
                    f"{seats_warning}\n\n"
                    f"Пометка добавлена в примечания."
                ))
            else:
                self.after(0, lambda: show_info("Обновлено", f"Бронирование ID={bid} обновлено"))
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка обновления", str(exc))

    @run_in_thread
    def _on_delete(self):
        try:
            bid_str = self.vars["id"].get().strip()
            if not bid_str:
                show_error("Ввод", "Введите ID бронирования")
                return
            bid = int(bid_str)
            if not confirm("Удаление", f"Удалить бронирование ID={bid}?"):
                return
            ok = backend.delete_booking(bid)
            if ok:
                self.after(0, self._clear_form)
                self.after(0, lambda: show_info("Удалено", "Бронирование удалено"))
            else:
                show_info("Удаление", "Бронирование не найдено")
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка", str(exc))

    @run_in_thread
    def _on_details(self):
        try:
            bid_str = self.vars["id"].get().strip()
            if not bid_str:
                show_error("Ввод", "Введите ID бронирования")
                return
            details = backend.get_booking_with_details(int(bid_str))
            if details is None:
                show_info("Поиск", "Бронирование не найдено")
                return

            lines = [
                f"Бронирование ID: {details.get('id')}",
                f"Дата: {details.get('booking_date')}",
                f"Время: {details.get('start_time')} — {details.get('end_time')}",
                f"Гостей: {details.get('guests_count')}",
                f"Статус: {STATUS_MAP.get(details.get('status', ''), details.get('status', ''))}",
                f"Примечания: {details.get('notes') or '—'}",
                "",
                "— Пользователь —",
                f"Имя: {details.get('user_first_name', '')} {details.get('user_last_name', '')}",
                f"Email: {details.get('user_email', '')}",
                f"Телефон: {details.get('user_phone') or '—'}",
                "",
                "— Стол —",
                f"Номер: {details.get('table_number')}",
                f"Мест: {details.get('table_seats')}",
                f"Расположение: {LOCATION_MAP.get(details.get('table_location', ''), details.get('table_location', ''))}",
                f"Описание: {details.get('table_description') or '—'}",
            ]
            self.after(0, lambda: show_info("Подробности", "\n".join(lines)))
        except Exception as exc:
            show_error("Ошибка", str(exc))

    @run_in_thread
    def _on_check_and_confirm(self):
        try:
            bid_str = self.vars["id"].get().strip()
            if not bid_str:
                show_error("Ввод", "Введите ID бронирования")
                return
            bid = int(bid_str)

            result = backend.check_and_confirm(bid)

            if result is None:
                show_info("Статус", "Бронирование не найдено")
                return

            if result == "confirmed":
                self.after(0, lambda: show_info(
                    "✅ Подтверждено",
                    f"Стол свободен!\nБронирование ID={bid} подтверждено."
                ))
            elif result == "overlap":
                self.after(0, lambda: show_error(
                    "❌ Пересечение времени",
                    f"Стол ЗАНЯТ на это время.\n"
                    f"Бронирование ID={bid} осталось в статусе «Ожидает».\n\n"
                    f"Измените данные (стол, дату или время) и попробуйте снова."
                ))
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка", str(exc))

    @run_in_thread
    def _on_cancel(self):
        try:
            bid_str = self.vars["id"].get().strip()
            if not bid_str:
                show_error("Ввод", "Введите ID")
                return
            result = backend.cancel_booking(int(bid_str))
            if result:
                self.after(0, lambda: show_info("Статус", "Бронирование отменено"))
            else:
                show_info("Статус", "Бронирование не найдено")
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка", str(exc))

    @run_in_thread
    def _on_complete(self):
        try:
            bid_str = self.vars["id"].get().strip()
            if not bid_str:
                show_error("Ввод", "Введите ID")
                return
            result = backend.complete_booking(int(bid_str))
            if result:
                self.after(0, lambda: show_info("Статус", "Бронирование завершено"))
            else:
                show_info("Статус", "Бронирование не найдено")
            self._refresh_list()
        except Exception as exc:
            show_error("Ошибка", str(exc))


# ====================================================================== #
#  Вкладка: Доступность
# ====================================================================== #

class AvailabilityTab(ttk.Frame):
    """Проверка доступности столика на выбранную дату и время."""

    def __init__(self, master):
        super().__init__(master)
        self._build_ui()

    def _build_ui(self):
        # ---- Форма проверки ----
        form = ttk.LabelFrame(self, text="Проверить доступность столика", padding=16)
        form.pack(fill=tk.X, padx=20, pady=20)

        labels = [
            "ID стола:*", "Дата (ГГГГ-ММ-ДД):*",
            "Начало (ЧЧ:ММ):*", "Конец (ЧЧ:ММ):*",
        ]
        keys = ["table_id", "booking_date", "start_time", "end_time"]
        self.vars = {}

        for i, (lbl, key) in enumerate(zip(labels, keys)):
            ttk.Label(form, text=lbl, font=("Segoe UI", 10)).grid(
                row=i, column=0, sticky=tk.W, pady=6, padx=(0, 10)
            )
            var = tk.StringVar()
            self.vars[key] = var
            ttk.Entry(form, textvariable=var, width=25, font=("Segoe UI", 10)).grid(
                row=i, column=1, sticky=tk.EW, pady=6
            )
        form.columnconfigure(1, weight=1)

        ttk.Button(form, text="🔍  Проверить доступность", command=self._on_check).grid(
            row=len(labels), column=0, columnspan=2, pady=(16, 0)
        )

        # ---- Результат ----
        result_frame = ttk.LabelFrame(self, text="Результат", padding=16)
        result_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 20))

        self.result_label = ttk.Label(
            result_frame, text="Введите данные и нажмите «Проверить доступность»",
            font=("Segoe UI", 14), anchor=tk.CENTER, justify=tk.CENTER,
        )
        self.result_label.pack(fill=tk.BOTH, expand=True)

        # ---- Таблица существующих бронирований стола ----
        detail_frame = ttk.LabelFrame(
            self, text="Существующие бронирования этого стола на выбранную дату", padding=8
        )
        detail_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 20))

        cols = ("id", "user_id", "start_time", "end_time", "guests_count", "status", "notes")
        self.tree = ttk.Treeview(detail_frame, columns=cols, show="headings", height=8)

        for col, hdr, w in [
            ("id", "ID", 50), ("user_id", "Пользователь", 100),
            ("start_time", "Начало", 80), ("end_time", "Конец", 80),
            ("guests_count", "Гости", 60),
            ("status", "Статус", 110), ("notes", "Примечания", 200),
        ]:
            self.tree.heading(col, text=hdr)
            self.tree.column(col, width=w, anchor=tk.W)

        vsb = ttk.Scrollbar(detail_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)

    # ------------------------------------------------------------------ #

    @run_in_thread
    def _on_check(self):
        try:
            tid = self.vars["table_id"].get().strip()
            bd = self.vars["booking_date"].get().strip()
            st = self.vars["start_time"].get().strip()
            et = self.vars["end_time"].get().strip()

            if not all([tid, bd, st, et]):
                show_error("Ввод", "Заполните все поля")
                return

            available = backend.is_table_available(int(tid), bd, st, et)

            if available:
                self.after(0, lambda: self.result_label.configure(
                    text="✅  Столик СВОБОДЕН на выбранное время!\nМожно бронировать.",
                    foreground="green",
                ))
            else:
                self.after(0, lambda: self.result_label.configure(
                    text="❌  Столик ЗАНЯТ на выбранное время.\nВыберите другое время.",
                    foreground="red",
                ))

            # Показываем бронирования стола на эту дату
            bookings = backend.get_bookings_by_table(int(tid), booking_date=bd)
            self.after(0, lambda: self._populate(bookings))
        except Exception as exc:
            show_error("Ошибка", str(exc))

    def _populate(self, bookings):
        self.tree.delete(*self.tree.get_children())
        for b in bookings:
            self.tree.insert("", tk.END, values=(
                b.id, b.user_id, b.start_time, b.end_time,
                b.guests_count,
                STATUS_MAP.get(b.status, b.status),
                b.notes or "",
            ))


# ====================================================================== #
#  Точка входа
# ====================================================================== #

if __name__ == "__main__":
    app = App()
    app.mainloop()
