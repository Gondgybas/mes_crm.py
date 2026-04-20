#!/usr/bin/env python3
"""MetalWorks MES v5.6"""

import argparse, datetime, hashlib, json, logging, mimetypes
import os, threading, uuid, math
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Integer, String, Text, create_engine, func, Table, case
)
from sqlalchemy.orm import (
    DeclarativeBase, Session, relationship, sessionmaker, joinedload
)

BASE_DIR = Path(__file__).parent
DB_PATH = os.environ.get("MES_DB_PATH", str(BASE_DIR / "mes_v5.db"))
DB_URL = f"sqlite:///{DB_PATH}"
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)
WEB_HOST = os.environ.get("MES_WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.environ.get("MES_WEB_PORT", "8000"))
SECRET_KEY = os.environ.get("MES_SECRET", "mes-factory-secret-2024")
MAX_UPLOAD_MB = 50

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("MES")

# Московское время UTC+3
MSK_OFFSET = datetime.timezone(datetime.timedelta(hours=3))

def now_msk():
    return datetime.datetime.now(MSK_OFFSET).replace(tzinfo=None)


class Base(DeclarativeBase):
    pass

role_permissions = Table("role_permissions", Base.metadata,
    Column("role_config_id", Integer, ForeignKey("role_configs.id"), primary_key=True),
    Column("permission_id", Integer, ForeignKey("permissions.id"), primary_key=True))

user_stations = Table("user_stations", Base.metadata,
    Column("user_id", Integer, ForeignKey("users.id"), primary_key=True),
    Column("resource_id", Integer, ForeignKey("resources.id"), primary_key=True))


class OperationTypeCfg(Base):
    __tablename__ = "operation_type_cfgs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), unique=True, nullable=False)
    sort_order = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    writeoff_mode = Column(String(50), default="Детали")


class Permission(Base):
    __tablename__ = "permissions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(100), unique=True, nullable=False)
    name = Column(String(200), nullable=False)
    category = Column(String(100), nullable=False, default="Общие")


class RoleConfig(Base):
    __tablename__ = "role_configs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    role = Column(String(50), unique=True, nullable=False)
    display_name = Column(String(100), nullable=False)
    description = Column(Text, default="")
    is_system = Column(Boolean, default=False)
    allowed_writeoff_types = Column(Text, default='["Материал","Детали"]')
    permissions = relationship("Permission", secondary=role_permissions, lazy="joined")

    def get_wo_types(self):
        try: return json.loads(self.allowed_writeoff_types or "[]")
        except: return []

    def set_wo_types(self, t):
        self.allowed_writeoff_types = json.dumps(t)


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(100), unique=True, nullable=False, index=True)
    password_hash = Column(String(256), nullable=False)
    full_name = Column(String(200), nullable=False)
    role = Column(String(50), nullable=False, default="operator")
    is_active = Column(Boolean, default=True)
    tab_number = Column(String(50), default="")
    created_at = Column(DateTime, default=now_msk)
    allowed_stations = relationship("Resource", secondary=user_stations, lazy="joined")

    @staticmethod
    def hash_pw(pw):
        return hashlib.sha256((pw + SECRET_KEY).encode()).hexdigest()

    def check_pw(self, pw):
        return self.password_hash == self.hash_pw(pw)


class Customer(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    short_name = Column(String(100), default="")
    inn = Column(String(20), default="")
    contact_person = Column(String(200), default="")
    phone = Column(String(50), default="")
    email = Column(String(200), default="")
    address = Column(Text, default="")
    notes = Column(Text, default="")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=now_msk)
    orders = relationship("Order", back_populates="customer")


class MetalGrade(Base):
    __tablename__ = "metal_grades"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(50), unique=True, nullable=False)
    name = Column(String(200), nullable=False)
    density = Column(Float, nullable=False, default=7.85)


class MaterialCategory(Base):
    __tablename__ = "material_categories"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    material_type = Column(String(50), nullable=False)
    sort_order = Column(Integer, default=0)
    description = Column(Text, default="")
    custom_fields = Column(Text, default="[]")

    def get_custom_fields(self):
        try: return json.loads(self.custom_fields or "[]")
        except: return []

    def set_custom_fields(self, fields):
        self.custom_fields = json.dumps(fields, ensure_ascii=False)


class Material(Base):
    __tablename__ = "materials"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(200), nullable=False)
    material_type = Column(String(50), nullable=False, default="Лист")
    category_id = Column(Integer, ForeignKey("material_categories.id"), nullable=True)
    primary_unit = Column(String(20), nullable=False, default="кг")
    metal_grade_id = Column(Integer, ForeignKey("metal_grades.id"), nullable=True)
    thickness_mm = Column(Float, nullable=True)
    width_mm = Column(Float, nullable=True)
    length_mm = Column(Float, nullable=True)
    sheet_weight_kg = Column(Float, nullable=True)
    diameter_mm = Column(Float, nullable=True)
    wall_mm = Column(Float, nullable=True)
    quantity_kg = Column(Float, default=0.0)
    quantity_sheets = Column(Integer, default=0)
    quantity_pcs = Column(Float, default=0.0)
    reserved_kg = Column(Float, default=0.0)
    reserved_sheets = Column(Integer, default=0)
    min_stock_kg = Column(Float, default=0.0)
    min_stock_sheets = Column(Integer, default=0)
    color_ral = Column(String(50), default="")
    paint_type = Column(String(100), default="")
    description = Column(Text, default="")
    custom_data = Column(Text, default="{}")
    created_at = Column(DateTime, default=now_msk)
    metal_grade = relationship("MetalGrade", lazy="joined")
    category = relationship("MaterialCategory", lazy="joined")

    def get_custom_data(self):
        try: return json.loads(self.custom_data or "{}")
        except: return {}

    def set_custom_data(self, d):
        self.custom_data = json.dumps(d, ensure_ascii=False)

    @property
    def available_kg(self):
        return round(self.quantity_kg - self.reserved_kg, 2)

    @property
    def available_sheets(self):
        return self.quantity_sheets - self.reserved_sheets

    @property
    def low_stock(self):
        if self.material_type == "Лист":
            return self.available_sheets <= self.min_stock_sheets
        return self.available_kg <= self.min_stock_kg

    def calc_sheet_weight(self):
        if self.thickness_mm and self.width_mm and self.length_mm and self.metal_grade:
            return round(
                (self.length_mm * self.width_mm * self.thickness_mm) / 1_000_000 * self.metal_grade.density, 2)
        return self.sheet_weight_kg or 0.0


class MaterialMovement(Base):
    __tablename__ = "material_movements"
    id = Column(Integer, primary_key=True, autoincrement=True)
    material_id = Column(Integer, ForeignKey("materials.id"), nullable=False, index=True)
    movement_type = Column(String(50), nullable=False)
    quantity_kg = Column(Float, default=0.0)
    quantity_sheets = Column(Integer, default=0)
    quantity_pcs = Column(Float, default=0.0)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    resource_id = Column(Integer, ForeignKey("resources.id"), nullable=True)
    note = Column(Text, default="")
    created_at = Column(DateTime, default=now_msk)
    material = relationship("Material")
    order = relationship("Order")
    user = relationship("User")
    resource = relationship("Resource")


class PartTemplate(Base):
    __tablename__ = "part_templates"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    part_number = Column(String(100), default="")
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True)
    description = Column(Text, default="")
    created_at = Column(DateTime, default=now_msk)
    operation_times = Column(Text, default="{}")
    is_assembly = Column(Boolean, default=False)
    customer = relationship("Customer", lazy="joined")
    components = relationship("AssemblyComponent", foreign_keys="[AssemblyComponent.assembly_id]",
                              cascade="all, delete-orphan", lazy="joined")
    materials = relationship("PartTemplateMaterial", back_populates="part_template",
                             cascade="all, delete-orphan", lazy="joined")
    files = relationship("PartTemplateFile", back_populates="part_template",
                         cascade="all, delete-orphan", lazy="joined")

    def get_op_times(self):
        try: return json.loads(self.operation_times or "{}")
        except: return {}

    def set_op_times(self, d):
        self.operation_times = json.dumps(d)

    @property
    def display_name(self):
        if self.part_number:
            return f"{self.name} ({self.part_number})"
        return self.name


class PartTemplateMaterial(Base):
    __tablename__ = "part_template_materials"
    id = Column(Integer, primary_key=True, autoincrement=True)
    part_template_id = Column(Integer, ForeignKey("part_templates.id"), nullable=False)
    material_id = Column(Integer, ForeignKey("materials.id"), nullable=False)
    sheets_input = Column(Integer, default=1)
    parts_per_sheets = Column(Integer, default=1)
    sheets_per_one = Column(Float, default=0.0)
    part_template = relationship("PartTemplate", back_populates="materials")
    material = relationship("Material", lazy="joined")

    def calc_sheets_for_qty(self, qty):
        if self.parts_per_sheets and self.parts_per_sheets > 0:
            return math.ceil(qty * self.sheets_input / self.parts_per_sheets)
        return 0


class PartTemplateFile(Base):
    __tablename__ = "part_template_files"
    id = Column(Integer, primary_key=True, autoincrement=True)
    part_template_id = Column(Integer, ForeignKey("part_templates.id"), nullable=False)
    filename = Column(String(300), nullable=False)
    original_name = Column(String(300), nullable=False)
    file_type = Column(String(100), default="Чертёж")
    file_size = Column(Integer, default=0)
    mime_type = Column(String(100), default="")
    uploaded_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    uploaded_at = Column(DateTime, default=now_msk)
    description = Column(Text, default="")
    part_template = relationship("PartTemplate", back_populates="files")
    uploader = relationship("User")

class AssemblyComponent(Base):
    __tablename__ = "assembly_components"
    id = Column(Integer, primary_key=True, autoincrement=True)
    assembly_id = Column(Integer, ForeignKey("part_templates.id"), nullable=False)
    component_id = Column(Integer, ForeignKey("part_templates.id"), nullable=False)
    quantity = Column(Integer, nullable=False, default=1)
    sort_order = Column(Integer, default=0)
    component = relationship("PartTemplate", foreign_keys=[component_id], lazy="joined")



class Resource(Base):
    __tablename__ = "resources"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    resource_type = Column(String(100), nullable=False)
    code = Column(String(50), default="")
    is_available = Column(Boolean, default=True)
    description = Column(Text, default="")
    allowed_operations = Column(Text, default="[]")
    shift_hours = Column(Float, default=8.0)
    shifts_per_day = Column(Integer, default=1)

    def get_allowed_ops(self):
        try: return json.loads(self.allowed_operations or "[]")
        except: return []

    def set_allowed_ops(self, ops):
        self.allowed_operations = json.dumps(ops)

    @property
    def daily_capacity_min(self):
        return self.shift_hours * 60 * self.shifts_per_day


ORDER_STATUSES = ["Черновик", "Новый", "Ожидает", "В работе", "Завершён", "Отгружен", "Отменён", "Приостановлен"]
PRIORITIES = ["Низкий", "Обычный", "Высокий", "Срочный", "Критический"]


class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_number = Column(String(50), unique=True, nullable=False, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True)
    status = Column(String(50), nullable=False, default="Черновик")
    priority = Column(String(50), nullable=False, default="Обычный")
    total_amount = Column(Float, default=0.0)
    description = Column(Text, default="")
    notes = Column(Text, default="")
    deadline = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=now_msk)
    updated_at = Column(DateTime, default=now_msk, onupdate=now_msk)
    completed_at = Column(DateTime, nullable=True)
    customer = relationship("Customer", back_populates="orders")
    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")
    operations = relationship("ProductionOp", back_populates="order", cascade="all, delete-orphan")
    reservations = relationship("Reservation", back_populates="order", cascade="all, delete-orphan")
    files = relationship("OrderFile", back_populates="order", cascade="all, delete-orphan")

    @property
    def display_name(self):
        cname = self.customer.name if self.customer else "—"
        return f"{cname} — {self.description[:40]}" if self.description else cname

    @property
    def is_overdue(self):
        if self.deadline and self.status not in ("Завершён", "Отменён", "Отгружен"):
            return now_msk() > self.deadline
        return False


class OrderItem(Base):
    __tablename__ = "order_items"
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)
    part_template_id = Column(Integer, ForeignKey("part_templates.id"), nullable=False)
    quantity = Column(Integer, nullable=False, default=1)
    completed_qty = Column(Integer, default=0)
    rejected_qty = Column(Integer, default=0)
    description = Column(Text, default="")
    order = relationship("Order", back_populates="items")
    part_template = relationship("PartTemplate", lazy="joined")
    station_logs = relationship("PartStationLog", back_populates="order_item", cascade="all, delete-orphan")

    @property
    def surplus(self):
        return max(0, self.completed_qty - self.quantity)


class OrderFile(Base):
    __tablename__ = "order_files"
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)
    filename = Column(String(300), nullable=False)
    original_name = Column(String(300), nullable=False)
    file_type = Column(String(100), default="")
    file_size = Column(Integer, default=0)
    mime_type = Column(String(100), default="")
    uploaded_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    uploaded_at = Column(DateTime, default=now_msk)
    description = Column(Text, default="")
    order = relationship("Order", back_populates="files")
    uploader = relationship("User")


class Reservation(Base):
    __tablename__ = "reservations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False, index=True)
    order_item_id = Column(Integer, ForeignKey("order_items.id"), nullable=True)
    material_id = Column(Integer, ForeignKey("materials.id"), nullable=False, index=True)
    part_template_id = Column(Integer, ForeignKey("part_templates.id"), nullable=True)
    quantity_kg = Column(Float, default=0.0)
    quantity_sheets = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    reserved_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    note = Column(Text, default="")
    created_at = Column(DateTime, default=now_msk)
    order = relationship("Order", back_populates="reservations")
    order_item = relationship("OrderItem")
    material = relationship("Material")
    part_template = relationship("PartTemplate")
    reserver = relationship("User", foreign_keys=[reserved_by])


OP_STATUSES = ["Ожидает", "Запланирована", "В работе", "Завершена", "Частично", "Пауза"]


class ProductionOp(Base):
    __tablename__ = "production_ops"
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False, index=True)
    order_item_id = Column(Integer, ForeignKey("order_items.id"), nullable=True)
    operation_type = Column(String(200), nullable=False)
    status = Column(String(50), nullable=False, default="Ожидает")
    resource_id = Column(Integer, ForeignKey("resources.id"), nullable=True)
    sequence = Column(Integer, default=0)
    sort_order = Column(Integer, default=0)
    planned_qty = Column(Integer, default=0)
    completed_qty = Column(Integer, default=0)
    rejected_qty = Column(Integer, default=0)
    estimated_minutes = Column(Integer, default=60)
    actual_minutes = Column(Integer, nullable=True)
    assigned_to = Column(Integer, ForeignKey("users.id"), nullable=True)
    description = Column(Text, default="")
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    paused_at = Column(DateTime, nullable=True)
    total_pause_minutes = Column(Integer, default=0)
    order = relationship("Order", back_populates="operations")
    order_item = relationship("OrderItem")
    resource = relationship("Resource")
    operator = relationship("User")


class PartStationLog(Base):
    __tablename__ = "part_station_logs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_item_id = Column(Integer, ForeignKey("order_items.id"), nullable=False, index=True)
    resource_id = Column(Integer, ForeignKey("resources.id"), nullable=True)
    operation_type = Column(String(200), default="")
    good_qty = Column(Integer, default=0)
    rejected_qty = Column(Integer, default=0)
    is_anomaly = Column(Boolean, default=False)
    anomaly_note = Column(Text, default="")
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    note = Column(Text, default="")
    created_at = Column(DateTime, default=now_msk)
    order_item = relationship("OrderItem", back_populates="station_logs")
    resource = relationship("Resource")
    user = relationship("User")


class WriteOff(Base):
    __tablename__ = "writeoffs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    writeoff_type = Column(String(50), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    resource_id = Column(Integer, ForeignKey("resources.id"), nullable=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=True)
    order_item_id = Column(Integer, ForeignKey("order_items.id"), nullable=True)
    material_id = Column(Integer, ForeignKey("materials.id"), nullable=True)
    reservation_id = Column(Integer, ForeignKey("reservations.id"), nullable=True)
    quantity_sheets = Column(Integer, default=0)
    quantity_kg = Column(Float, default=0.0)
    quantity_pcs = Column(Float, default=0.0)
    parts_good = Column(Integer, default=0)
    parts_rejected = Column(Integer, default=0)
    is_anomaly = Column(Boolean, default=False)
    anomaly_note = Column(Text, default="")
    is_cancelled = Column(Boolean, default=False)
    cancelled_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    cancelled_at = Column(DateTime, nullable=True)
    note = Column(Text, default="")
    created_at = Column(DateTime, default=now_msk)
    user = relationship("User", foreign_keys=[user_id])
    cancelled_user = relationship("User", foreign_keys=[cancelled_by])
    resource = relationship("Resource")
    order = relationship("Order")
    order_item = relationship("OrderItem")
    material = relationship("Material")
    reservation = relationship("Reservation")


class AuditLog(Base):
    __tablename__ = "audit_logs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    action = Column(String(100), nullable=False, index=True)
    entity_type = Column(String(50), nullable=True)
    entity_id = Column(Integer, nullable=True)
    details = Column(Text, default="")
    created_at = Column(DateTime, default=now_msk, index=True)
    user = relationship("User")


engine = create_engine(DB_URL, echo=False, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


@contextmanager
def get_db():
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except:
        db.rollback()
        raise
    finally:
        db.close()


def audit(db, uid, action, etype="", eid=0, details=""):
    db.add(AuditLog(user_id=uid, action=action, entity_type=etype, entity_id=eid, details=details))
    db.flush()


DEFAULT_OP_TYPES = [
    "Лазерная резка", "Плазменная резка", "Координатная пробивка", "Гибка",
    "Сверление", "Фрезеровка", "Токарка", "Сварка", "Сборка",
    "Покраска", "Финишная обработка", "ОТК"
]

DEFAULT_PERMS = [
    ("mat.view", "Просмотр склада", "Склад"), ("mat.receive", "Поступление", "Склад"),
    ("mat.consume", "Списание материалов", "Склад"), ("mat.create", "Создание материалов", "Склад"),
    ("mat.edit", "Редактирование материалов", "Склад"),
    ("order.view", "Просмотр заказов", "Заказы"), ("order.create", "Создание заказов", "Заказы"),
    ("order.edit", "Редактирование заказов", "Заказы"), ("order.delete", "Удаление заказов", "Заказы"), ("order.status", "Смена статуса", "Заказы"),
    ("order.files", "Файлы заказов", "Заказы"), ("order.reports", "Отчёты", "Заказы"),
    ("reserve.view", "Просмотр резервов", "Резервы"), ("reserve.create", "Создание резервов", "Резервы"),
    ("reserve.edit", "Редактирование резервов", "Резервы"), ("reserve.cancel", "Отмена резервов", "Резервы"),
    ("op.view", "Просмотр операций", "Операции"), ("op.create", "Создание операций", "Операции"),
    ("op.edit", "Редактирование операций", "Операции"), ("op.start", "Запуск операций", "Операции"),
    ("op.complete", "Завершение операций", "Операции"), ("op.rollback", "Откат операций", "Операции"),
    ("op.reorder", "Порядок операций", "Операции"),
    ("parts.view", "Просмотр деталей", "Детали"), ("parts.create", "Создание деталей", "Детали"),
    ("parts.edit", "Редактирование деталей", "Детали"), ("parts.log", "Учёт деталей", "Детали"),
    ("parts.files", "Просмотр файлов деталей", "Детали"),
    ("writeoff.material", "Списание материала", "Списания"), ("writeoff.parts", "Списание деталей", "Списания"),
    ("writeoff.cancel", "Отмена списания", "Списания"),
    ("cust.view", "Просмотр клиентов", "Клиенты"), ("cust.create", "Создание клиентов", "Клиенты"),
    ("cust.edit", "Редактирование клиентов", "Клиенты"),
    ("res.view", "Просмотр ресурсов", "Ресурсы"), ("res.create", "Создание ресурсов", "Ресурсы"),
    ("res.edit", "Редактирование ресурсов", "Ресурсы"), ("res.delete", "Удаление ресурсов", "Ресурсы"),
    ("load.view", "Загруженность", "Загруженность"),
    ("admin.users", "Пользователи", "Админ"), ("admin.roles", "Роли", "Админ"),
    ("admin.grades", "Марки металла", "Админ"), ("admin.categories", "Категории склада", "Админ"),
    ("admin.logs", "Логи", "Админ"), ("admin.op_types", "Типы операций", "Админ"),
]

ALL_P = [p[0] for p in DEFAULT_PERMS]
MASTER_P = [p for p in ALL_P if not p.startswith("admin.")]
OPER_P = ["mat.view", "mat.consume", "order.view", "reserve.view", "op.view", "op.start", "op.complete",
           "parts.view", "parts.log", "parts.files", "writeoff.material", "writeoff.parts", "res.view", "load.view"]
VIEW_P = ["mat.view", "order.view", "op.view", "parts.view", "parts.files", "cust.view", "res.view", "load.view", "admin.logs"]
ROLE_LABELS = {"admin": "Администратор", "master": "Мастер", "operator": "Оператор", "viewer": "Наблюдатель"}


def init_database():
    Base.metadata.create_all(engine)
    # Миграция: добавляем writeoff_mode в operation_type_cfgs
    from sqlalchemy import inspect as sa_inspect, text
    insp = sa_inspect(engine)
    cols = [c["name"] for c in insp.get_columns("operation_type_cfgs")]
    if "writeoff_mode" not in cols:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE operation_type_cfgs ADD COLUMN writeoff_mode VARCHAR(50) DEFAULT 'Детали'"))
            conn.commit()
        log.info("Migration: added writeoff_mode to operation_type_cfgs")

    # Миграция: is_assembly в part_templates
    pt_cols = [c["name"] for c in insp.get_columns("part_templates")]
    if "is_assembly" not in pt_cols:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE part_templates ADD COLUMN is_assembly BOOLEAN DEFAULT 0"))
            conn.commit()
        log.info("Migration: added is_assembly to part_templates")
    # Миграция: таблица assembly_components
    if not insp.has_table("assembly_components"):
        Base.metadata.tables["assembly_components"].create(engine)
        log.info("Migration: created assembly_components table")

    with get_db() as db:
        if db.query(OperationTypeCfg).count() == 0:
            for i, n in enumerate(DEFAULT_OP_TYPES):
                db.add(OperationTypeCfg(name=n, sort_order=i))
            db.flush()
        # Ensure new permissions exist
        existing_codes = {p.code for p in db.query(Permission).all()}
        for code, name, cat in DEFAULT_PERMS:
            if code not in existing_codes:
                db.add(Permission(code=code, name=name, category=cat))
        db.flush()
        if db.query(RoleConfig).count() == 0:
            all_p = {p.code: p for p in db.query(Permission).all()}
            for role, disp, pcodes, wo in [
                ("admin", "Администратор", ALL_P, ["Материал", "Детали"]),
                ("master", "Мастер", MASTER_P, ["Материал", "Детали"]),
                ("operator", "Оператор", OPER_P, ["Материал", "Детали"]),
                ("viewer", "Наблюдатель", VIEW_P, [])
            ]:
                rc = RoleConfig(role=role, display_name=disp, is_system=True)
                rc.permissions = [all_p[c] for c in pcodes if c in all_p]
                rc.set_wo_types(wo)
                db.add(rc)
            db.flush()
        if db.query(User).count() == 0:
            db.add_all([
                User(username="admin", password_hash=User.hash_pw("admin"),
                     full_name="Администратор", role="admin", tab_number="001"),
                User(username="master", password_hash=User.hash_pw("master"),
                     full_name="Мастер Петров И.А.", role="master", tab_number="010"),
                User(username="operator1", password_hash=User.hash_pw("operator"),
                     full_name="Оператор Иванов С.В.", role="operator", tab_number="101"),
            ])
            db.flush()
        if db.query(MetalGrade).count() == 0:
            db.add_all([
                MetalGrade(code="Ст3", name="Сталь 3", density=7.85),
                MetalGrade(code="09Г2С", name="09Г2С", density=7.85),
                MetalGrade(code="AISI304", name="Нерж. AISI 304", density=7.93),
                MetalGrade(code="АМг2", name="Алюминий АМг2", density=2.68),
            ])
            db.flush()
        if db.query(MaterialCategory).count() == 0:
            sheet_cat = MaterialCategory(name="Листовой металл", material_type="Лист", sort_order=1)
            sheet_cat.set_custom_fields([
                {"key": "grade", "label": "Марка", "type": "grade_select"},
                {"key": "thickness", "label": "Толщина (мм)", "type": "number"},
                {"key": "width", "label": "Ширина (мм)", "type": "number"},
                {"key": "length", "label": "Длина (мм)", "type": "number"},
            ])
            db.add(sheet_cat)
            tube_cat = MaterialCategory(name="Трубы", material_type="Труба", sort_order=2)
            tube_cat.set_custom_fields([
                {"key": "grade", "label": "Марка", "type": "grade_select"},
                {"key": "diameter", "label": "Диаметр (мм)", "type": "number"},
                {"key": "wall", "label": "Стенка (мм)", "type": "number"},
                {"key": "length", "label": "Длина (мм)", "type": "number"},
            ])
            db.add(tube_cat)
            rod_cat = MaterialCategory(name="Прутки", material_type="Пруток", sort_order=3)
            rod_cat.set_custom_fields([
                {"key": "grade", "label": "Марка", "type": "grade_select"},
                {"key": "diameter", "label": "Диаметр (мм)", "type": "number"},
                {"key": "length", "label": "Длина (мм)", "type": "number"},
            ])
            db.add(rod_cat)
            hw_cat = MaterialCategory(name="Метизы", material_type="Метиз", sort_order=4)
            hw_cat.set_custom_fields([
                {"key": "standard", "label": "Стандарт", "type": "text"},
                {"key": "size", "label": "Размер", "type": "text"},
            ])
            db.add(hw_cat)
            paint_cat = MaterialCategory(name="Краска", material_type="Краска", sort_order=5)
            paint_cat.set_custom_fields([
                {"key": "color_ral", "label": "RAL", "type": "text"},
                {"key": "paint_type", "label": "Тип краски", "type": "text"},
            ])
            db.add(paint_cat)
            other_cat = MaterialCategory(name="Прочее", material_type="Прочее", sort_order=6)
            other_cat.set_custom_fields([])
            db.add(other_cat)
            db.flush()
        if db.query(Material).count() == 0:
            cat_sh = db.query(MaterialCategory).filter_by(material_type="Лист").first()
            st3 = db.query(MetalGrade).filter_by(code="Ст3").first()
            for code, name, t, w, l in [
                ("Ст3-3-1250x2500", "Лист Ст3 3мм 1250×2500", 3, 1250, 2500),
                ("Ст3-5-1500x6000", "Лист Ст3 5мм 1500×6000", 5, 1500, 6000),
                ("Ст3-8-1500x6000", "Лист Ст3 8мм 1500×6000", 8, 1500, 6000),
            ]:
                m = Material(code=code, name=name, material_type="Лист",
                             category_id=cat_sh.id if cat_sh else None,
                             primary_unit="лист", metal_grade_id=st3.id if st3 else None,
                             thickness_mm=t, width_mm=w, length_mm=l, quantity_sheets=20)
                m.metal_grade = st3
                m.sheet_weight_kg = m.calc_sheet_weight()
                m.quantity_kg = round(20 * (m.sheet_weight_kg or 0), 2)
                cd = {"grade": str(st3.id) if st3 else "", "thickness": t, "width": w, "length": l}
                m.set_custom_data(cd)
                db.add(m)
            cat_pa = db.query(MaterialCategory).filter_by(material_type="Краска").first()
            mp = Material(code="RAL9005", name="Порошковая RAL 9005", material_type="Краска",
                          category_id=cat_pa.id if cat_pa else None,
                          primary_unit="кг", quantity_kg=25, color_ral="9005", paint_type="Порошковая")
            mp.set_custom_data({"color_ral": "9005", "paint_type": "Порошковая"})
            db.add(mp)
            db.flush()
        if db.query(Resource).count() == 0:
            for name, rtype, ops, sh, sd in [
                ("Лазер Trumpf", "Лазерный станок", ["Лазерная резка"], 12, 2),
                ("Плазма Hypertherm", "Плазменный станок", ["Плазменная резка"], 12, 2),
                ("КП Amada", "Координатно-пробивной", ["Координатная пробивка"], 8, 1),
                ("Листогиб Amada 1", "Листогиб", ["Гибка"], 8, 2),
                ("Листогиб Amada 2", "Листогиб", ["Гибка"], 8, 2),
                ("Сверлильный Heller", "Сверлильный", ["Сверление"], 8, 1),
                ("Фрезерный DMG", "Фрезерный", ["Фрезеровка"], 8, 1),
                ("Токарный Mazak", "Токарный", ["Токарка"], 8, 1),
                ("Сварка пост 1", "Сварочный пост", ["Сварка"], 8, 2),
                ("Сварка пост 2", "Сварочный пост", ["Сварка"], 8, 2),
                ("Сборка пост 1", "Сборочный пост", ["Сборка"], 8, 1),
                ("Покраска камера", "Покрасочная камера", ["Покраска"], 8, 1),
                ("Финиш/Упаковка", "Финишный участок", ["Финишная обработка"], 8, 1),
                ("ОТК", "ОТК", ["ОТК"], 8, 1),
            ]:
                r = Resource(name=name, resource_type=rtype, shift_hours=sh, shifts_per_day=sd)
                r.set_allowed_ops(ops)
                db.add(r)
            db.flush()
        if db.query(Customer).count() == 0:
            db.add_all([
                Customer(name="ООО СтройМонтаж", short_name="СтройМонтаж",
                         contact_person="Петров А.И.", phone="+7-999-111-2233"),
                Customer(name="ИП Сидоров В.П.", short_name="Сидоров",
                         contact_person="Сидоров В.П.", phone="+7-999-444-5566"),
            ])
            db.flush()
    log.info("Database initialized")


# ═══════════════════════════════════════════════════════════════
#  WEB APPLICATION
# ═══════════════════════════════════════════════════════════════
def create_app():
    from fastapi import FastAPI, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect, UploadFile, File, Form
    from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
    from pydantic import BaseModel

    app = FastAPI(title="MetalWorks MES v5.6")

    class WSManager:
        def __init__(self): self.connections = []
        async def connect(self, ws): await ws.accept(); self.connections.append(ws)
        def disconnect(self, ws):
            if ws in self.connections: self.connections.remove(ws)
        async def broadcast(self, msg):
            for ws in self.connections[:]:
                try: await ws.send_json(msg)
                except:
                    if ws in self.connections: self.connections.remove(ws)

    wsmgr = WSManager()

    def db_dep():
        db = SessionLocal()
        try: yield db
        finally: db.close()

    class IdReq(BaseModel):
        id: int

    def pt_display(pt):
        if pt and pt.part_number:
            return f"{pt.name} ({pt.part_number})"
        return pt.name if pt else "?"

    def find_resources_for_op(db, op_name):
        all_res = db.query(Resource).filter(Resource.is_available == True).all()
        return [r for r in all_res if op_name in r.get_allowed_ops()]

    def auto_create_reservations(db, order_item, user_id=None):
        pt = order_item.part_template
        if not pt: return

        def _reserve_materials(template, qty_multiplier, label_pt_id):
            for ptm in (template.materials or []):
                sheets = ptm.calc_sheets_for_qty(order_item.quantity * qty_multiplier)
                mat = db.query(Material).get(ptm.material_id)
                if not mat: continue
                kg = round(sheets * (mat.sheet_weight_kg or 0), 2)
                db.add(Reservation(order_id=order_item.order_id, order_item_id=order_item.id,
                                   material_id=mat.id, part_template_id=label_pt_id,
                                   quantity_sheets=sheets, quantity_kg=kg,
                                   reserved_by=user_id, is_active=True))
                mat.reserved_sheets += sheets
                mat.reserved_kg += kg
                db.add(MaterialMovement(material_id=mat.id, movement_type="Резерв",
                                        quantity_sheets=sheets, quantity_kg=kg,
                                        order_id=order_item.order_id, user_id=user_id,
                                        note=f"Авто-резерв: {template.name} x{order_item.quantity * qty_multiplier}"))

        # Собственные материалы детали/сборки
        _reserve_materials(pt, 1, pt.id)

        # Материалы компонентов сборки
        if pt.is_assembly:
            for comp in (pt.components or []):
                comp_pt = db.query(PartTemplate).get(comp.component_id)
                if comp_pt:
                    _reserve_materials(comp_pt, comp.quantity, comp_pt.id)

        db.flush()

    def auto_create_operations(db, order_item):
        pt = order_item.part_template
        if not pt: return
        op_times = pt.get_op_times()
        seq = 0
        for op_name, entry in op_times.items():
            per_one = entry.get("per_one", 0) if isinstance(entry, dict) else float(entry or 0)
            if per_one <= 0: continue
            seq += 1
            total_min = math.ceil(per_one * order_item.quantity)
            matching = find_resources_for_op(db, op_name)
            res_id = matching[0].id if len(matching) == 1 else None
            db.add(ProductionOp(
                order_id=order_item.order_id, order_item_id=order_item.id,
                operation_type=op_name, sequence=seq, sort_order=seq,
                planned_qty=order_item.quantity, estimated_minutes=total_min,
                resource_id=res_id))
        db.flush()

    def remove_item_reservations(db, order_item_id):
        for r in db.query(Reservation).filter(
                Reservation.order_item_id == order_item_id,
                Reservation.is_active == True).all():
            mat = db.query(Material).get(r.material_id)
            if mat:
                mat.reserved_sheets = max(0, mat.reserved_sheets - r.quantity_sheets)
                mat.reserved_kg = max(0, mat.reserved_kg - r.quantity_kg)
            r.is_active = False
        db.flush()

    def remove_item_operations(db, order_item_id):
        db.query(ProductionOp).filter(ProductionOp.order_item_id == order_item_id).delete()
        db.flush()

    def recalc_linked_items(db, pt_id, user_id=None):
        items = db.query(OrderItem).join(Order).filter(
            OrderItem.part_template_id == pt_id,
            Order.status.notin_(["Завершён", "Отменён", "Отгружен"])
        ).all()
        for it in items:
            remove_item_reservations(db, it.id)
            auto_create_reservations(db, it, user_id)
            ops = db.query(ProductionOp).filter(ProductionOp.order_item_id == it.id).all()
            all_pending = all(o.status == "Ожидает" for o in ops)
            if all_pending:
                remove_item_operations(db, it.id)
                auto_create_operations(db, it)
        db.flush()

    def check_sequence_anomaly(db, order_item_id, resource_id, good_qty):
        item = db.query(OrderItem).get(order_item_id)
        if not item: return False, ""
        ops = db.query(ProductionOp).filter(
            ProductionOp.order_item_id == order_item_id
        ).order_by(ProductionOp.sequence).all()
        if len(ops) < 2: return False, ""
        cur_idx = -1
        for i, op in enumerate(ops):
            if op.resource_id == resource_id:
                cur_idx = i; break
        if cur_idx <= 0: return False, ""
        prev_op = ops[cur_idx - 1]
        prev_logs = db.query(func.coalesce(func.sum(PartStationLog.good_qty), 0)).filter(
            PartStationLog.order_item_id == order_item_id,
            PartStationLog.resource_id == prev_op.resource_id).scalar()
        cur_logs = db.query(func.coalesce(func.sum(PartStationLog.good_qty), 0)).filter(
            PartStationLog.order_item_id == order_item_id,
            PartStationLog.resource_id == resource_id).scalar()
        if cur_logs + good_qty > prev_logs:
            return True, f"На {prev_op.operation_type} сделано {prev_logs}, а на текущем будет {cur_logs + good_qty}"
        return False, ""

    def get_first_op_resource_id(db, order_item_id):
        first_op = db.query(ProductionOp).filter(
            ProductionOp.order_item_id == order_item_id
        ).order_by(ProductionOp.sequence).first()
        return first_op.resource_id if first_op else None

    # ─── Auth ───────────────────────────────────────
    @app.post("/api/auth/login")
    async def api_login(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        user = db.query(User).filter(User.username == data["username"], User.is_active == True).first()
        if not user or not user.check_pw(data["password"]):
            raise HTTPException(401, "Неверный логин или пароль")
        rc = db.query(RoleConfig).filter(RoleConfig.role == user.role).first()
        perms = [p.code for p in rc.permissions] if rc else []
        wo_types = rc.get_wo_types() if rc else []
        stations = [{"id": s.id, "name": s.name} for s in user.allowed_stations]
        audit(db, user.id, "Вход", "user", user.id, user.username)
        db.commit()
        return {"id": user.id, "username": user.username, "role": user.role,
                "role_label": rc.display_name if rc else user.role,
                "full_name": user.full_name, "permissions": perms,
                "stations": stations, "writeoff_types": wo_types}

    # ─── Op Types ───────────────────────────────────
    @app.get("/api/op-types")
    def api_op_types(db: Session = Depends(db_dep)):
        return [{"id": o.id, "name": o.name, "sort_order": o.sort_order, "is_active": o.is_active,
                 "writeoff_mode": o.writeoff_mode or "Детали"}
                for o in db.query(OperationTypeCfg).order_by(OperationTypeCfg.sort_order).all()]

    @app.post("/api/op-types/save")
    async def api_save_ot(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        oid = data.get("id")
        if oid: o = db.query(OperationTypeCfg).get(oid)
        else: o = OperationTypeCfg(); db.add(o)
        o.name = data.get("name", o.name or "")
        o.sort_order = int(data.get("sort_order", o.sort_order or 0))
        o.is_active = data.get("is_active", True)
        o.writeoff_mode = data.get("writeoff_mode", o.writeoff_mode or "Детали")
        db.flush(); db.commit()
        return {"id": o.id}

    @app.post("/api/op-types/delete")
    async def api_del_ot(req: IdReq, db: Session = Depends(db_dep)):
        o = db.query(OperationTypeCfg).get(req.id)
        if o: db.delete(o); db.commit()
        return {"status": "ok"}

    # ─── Users ──────────────────────────────────────
    @app.get("/api/users")
    def api_users(db: Session = Depends(db_dep)):
        return [{"id": u.id, "username": u.username, "full_name": u.full_name,
                 "role": u.role, "role_label": ROLE_LABELS.get(u.role, u.role),
                 "is_active": u.is_active, "tab_number": u.tab_number,
                 "stations": [s.id for s in u.allowed_stations]}
                for u in db.query(User).order_by(User.username).all()]

    @app.post("/api/users/save")
    async def api_save_user(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        uid = data.get("id")
        if uid:
            u = db.query(User).get(uid)
            u.full_name = data.get("full_name", u.full_name)
            u.role = data.get("role", u.role)
            u.is_active = data.get("is_active", u.is_active)
            u.tab_number = data.get("tab_number", u.tab_number)
            if data.get("password"):
                u.password_hash = User.hash_pw(data["password"])
        else:
            u = User(username=data["username"],
                     password_hash=User.hash_pw(data.get("password", "1234")),
                     full_name=data.get("full_name", ""),
                     role=data.get("role", "operator"),
                     tab_number=data.get("tab_number", ""))
            db.add(u)
        sids = data.get("stations", [])
        u.allowed_stations = db.query(Resource).filter(Resource.id.in_(sids)).all() if sids else []
        db.flush(); db.commit()
        return {"id": u.id}

    # ─── Roles ──────────────────────────────────────
    @app.get("/api/roles")
    def api_roles(db: Session = Depends(db_dep)):
        return [{"id": r.id, "role": r.role, "display_name": r.display_name,
                 "is_system": r.is_system,
                 "permissions": [p.code for p in r.permissions],
                 "writeoff_types": r.get_wo_types()}
                for r in db.query(RoleConfig).order_by(RoleConfig.role).all()]

    @app.get("/api/permissions")
    def api_permissions(db: Session = Depends(db_dep)):
        return [{"id": p.id, "code": p.code, "name": p.name, "category": p.category}
                for p in db.query(Permission).order_by(Permission.category, Permission.code).all()]

    @app.post("/api/roles/save")
    async def api_save_role(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        rid = data.get("id")
        if rid:
            rc = db.query(RoleConfig).get(rid)
            if not rc: raise HTTPException(404)
            if "display_name" in data: rc.display_name = data["display_name"]
        else:
            role_key = data.get("role", "").strip().lower()
            if not role_key: raise HTTPException(400, "Укажите код роли")
            if db.query(RoleConfig).filter(RoleConfig.role == role_key).first():
                raise HTTPException(400, "Роль уже существует")
            rc = RoleConfig(role=role_key, display_name=data.get("display_name", role_key), is_system=False)
            db.add(rc); db.flush()
        perm_codes = data.get("permissions", [])
        rc.permissions = db.query(Permission).filter(Permission.code.in_(perm_codes)).all() if perm_codes else []
        rc.set_wo_types(data.get("writeoff_types", rc.get_wo_types()))
        db.flush(); db.commit()
        ROLE_LABELS[rc.role] = rc.display_name
        return {"id": rc.id, "role": rc.role}

    @app.post("/api/roles/delete")
    async def api_del_role(req: IdReq, db: Session = Depends(db_dep)):
        rc = db.query(RoleConfig).get(req.id)
        if not rc: raise HTTPException(404)
        if rc.is_system: raise HTTPException(400, "Системную роль нельзя удалить")
        if db.query(User).filter(User.role == rc.role).count() > 0:
            raise HTTPException(400, "Есть пользователи с этой ролью")
        db.delete(rc); db.commit()
        return {"status": "ok"}

    # ─── Grades ─────────────────────────────────────
    @app.get("/api/grades")
    def api_grades(db: Session = Depends(db_dep)):
        return [{"id": g.id, "code": g.code, "name": g.name, "density": g.density}
                for g in db.query(MetalGrade).order_by(MetalGrade.code).all()]

    @app.post("/api/grades/save")
    async def api_save_grade(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        gid = data.get("id")
        if gid: g = db.query(MetalGrade).get(gid)
        else: g = MetalGrade(); db.add(g)
        g.code = data.get("code", g.code or "")
        g.name = data.get("name", g.name or "")
        g.density = float(data.get("density", g.density or 7.85))
        db.flush(); db.commit()
        return {"id": g.id}

    @app.post("/api/grades/delete")
    async def api_del_grade(req: IdReq, db: Session = Depends(db_dep)):
        g = db.query(MetalGrade).get(req.id)
        if g: db.delete(g); db.commit()
        return {"status": "ok"}

    # ─── Material Categories ────────────────────────
    @app.get("/api/material-categories")
    def api_mat_cats(db: Session = Depends(db_dep)):
        return [{"id": c.id, "name": c.name, "type": c.material_type,
                 "sort_order": c.sort_order, "description": c.description,
                 "custom_fields": c.get_custom_fields()}
                for c in db.query(MaterialCategory).order_by(MaterialCategory.sort_order).all()]

    @app.post("/api/material-categories/save")
    async def api_save_cat(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        cid = data.get("id")
        if cid: c = db.query(MaterialCategory).get(cid)
        else: c = MaterialCategory(); db.add(c)
        c.name = data.get("name", c.name or "")
        c.material_type = data.get("type", c.material_type or "Прочее")
        c.sort_order = int(data.get("sort_order", c.sort_order or 0))
        c.description = data.get("description", c.description or "")
        if "custom_fields" in data:
            c.set_custom_fields(data["custom_fields"])
        db.flush(); db.commit()
        return {"id": c.id}

    # ─── Customers ──────────────────────────────────
    @app.get("/api/customers")
    def api_customers(search: str = "", db: Session = Depends(db_dep)):
        q = db.query(Customer)
        if search:
            sq = f"%{search}%"
            q = q.filter((Customer.name.ilike(sq)) | (Customer.short_name.ilike(sq)) | (Customer.inn.ilike(sq)))
        return [{"id": c.id, "name": c.name, "short_name": c.short_name,
                 "inn": c.inn, "contact_person": c.contact_person,
                 "phone": c.phone, "email": c.email, "address": c.address,
                 "notes": c.notes, "is_active": c.is_active}
                for c in q.order_by(Customer.name).all()]

    @app.post("/api/customers/save")
    async def api_save_cust(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        cid = data.get("id")
        if cid: c = db.query(Customer).get(cid)
        else: c = Customer(); db.add(c)
        for k in ["name", "short_name", "inn", "contact_person", "phone", "email", "address", "notes"]:
            if k in data: setattr(c, k, data[k])
        db.flush(); db.commit()
        return {"id": c.id}

    # ─── Materials ──────────────────────────────────
    @app.get("/api/materials")
    def api_materials(cat_id: int = 0, db: Session = Depends(db_dep)):
        q = db.query(Material).options(joinedload(Material.metal_grade), joinedload(Material.category))
        if cat_id: q = q.filter(Material.category_id == cat_id)
        return [{"id": m.id, "code": m.code, "name": m.name, "type": m.material_type,
                 "unit": m.primary_unit, "category_id": m.category_id,
                 "category": m.category.name if m.category else "—",
                 "grade": m.metal_grade.code if m.metal_grade else "",
                 "grade_id": m.metal_grade_id,
                 "thickness": m.thickness_mm, "width": m.width_mm, "length": m.length_mm,
                 "sheet_weight": m.sheet_weight_kg,
                 "diameter": m.diameter_mm, "wall": m.wall_mm,
                 "color_ral": m.color_ral, "paint_type": m.paint_type,
                 "qty_kg": m.quantity_kg, "qty_sheets": m.quantity_sheets, "qty_pcs": m.quantity_pcs,
                 "reserved_kg": m.reserved_kg, "reserved_sheets": m.reserved_sheets,
                 "available_kg": m.available_kg, "available_sheets": m.available_sheets,
                 "min_kg": m.min_stock_kg, "min_sheets": m.min_stock_sheets,
                 "low_stock": m.low_stock, "description": m.description,
                 "custom_data": m.get_custom_data()}
                for m in q.order_by(Material.code).all()]

    @app.get("/api/materials/need-for-orders")
    def api_mat_need(db: Session = Depends(db_dep)):
        result = []
        for m in db.query(Material).all():
            if m.material_type == "Лист":
                if m.available_sheets < 0:
                    result.append({"id": m.id, "code": m.code, "name": m.name,
                                   "deficit": abs(m.available_sheets), "unit": "л"})
            else:
                if m.available_kg < 0:
                    result.append({"id": m.id, "code": m.code, "name": m.name,
                                   "deficit": round(abs(m.available_kg), 2), "unit": "кг"})
        return result

    @app.post("/api/materials/save")
    async def api_save_mat(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        mid = data.get("id")
        if mid: m = db.query(Material).get(mid)
        else: m = Material(); db.add(m)
        for k in ["code", "name", "description", "color_ral", "paint_type"]:
            if k in data: setattr(m, k, data[k])
        m.material_type = data.get("material_type", m.material_type or "Лист")
        m.primary_unit = data.get("primary_unit", m.primary_unit or "кг")
        m.category_id = data.get("category_id") or None
        m.metal_grade_id = data.get("grade_id") or None
        for k in ["thickness", "width", "length", "diameter", "wall"]:
            setattr(m, k + "_mm", data.get(k) or None)
        m.min_stock_kg = float(data.get("min_stock_kg", m.min_stock_kg or 0))
        m.min_stock_sheets = int(data.get("min_stock_sheets", m.min_stock_sheets or 0))
        if "custom_data" in data:
            m.set_custom_data(data["custom_data"])
        if m.material_type == "Лист" and m.thickness_mm and m.width_mm and m.length_mm and m.metal_grade_id:
            grade = db.query(MetalGrade).get(m.metal_grade_id)
            if grade:
                m.metal_grade = grade
                m.sheet_weight_kg = m.calc_sheet_weight()
        db.flush(); db.commit()
        return {"id": m.id, "sheet_weight": m.sheet_weight_kg}

    @app.post("/api/materials/receive")
    async def api_receive(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        uid = data.get("user_id", 1)
        mat = db.query(Material).get(data["material_id"])
        if not mat: raise HTTPException(404)
        sh = int(data.get("sheets", 0)); kg = float(data.get("kg", 0)); pcs = float(data.get("pcs", 0))
        if sh > 0 and mat.material_type == "Лист":
            add_kg = round(sh * (mat.sheet_weight_kg or 0), 2)
            mat.quantity_sheets += sh; mat.quantity_kg += add_kg
            db.add(MaterialMovement(material_id=mat.id, movement_type="Поступление",
                                    quantity_sheets=sh, quantity_kg=add_kg,
                                    user_id=uid, note=data.get("note", "")))
            audit(db, uid, "Поступление", "material", mat.id, f"+{sh}л {mat.name}")
        elif kg > 0:
            mat.quantity_kg += kg
            db.add(MaterialMovement(material_id=mat.id, movement_type="Поступление",
                                    quantity_kg=kg, user_id=uid, note=data.get("note", "")))
        elif pcs > 0:
            mat.quantity_pcs += pcs
            db.add(MaterialMovement(material_id=mat.id, movement_type="Поступление",
                                    quantity_pcs=pcs, user_id=uid, note=data.get("note", "")))
        else:
            raise HTTPException(400, "Укажите количество")
        db.flush(); db.commit()
        return {"status": "ok"}

    @app.post("/api/materials/adjust")
    async def api_adjust_mat(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        uid = data.get("user_id", 1)
        mat = db.query(Material).get(data["material_id"])
        if not mat: raise HTTPException(404)
        new_sh = int(data.get("new_sheets", mat.quantity_sheets))
        new_kg = float(data.get("new_kg", mat.quantity_kg))
        new_pcs = float(data.get("new_pcs", mat.quantity_pcs))
        diff_sh = new_sh - mat.quantity_sheets
        diff_kg = round(new_kg - mat.quantity_kg, 2)
        diff_pcs = round(new_pcs - mat.quantity_pcs, 2)
        note = data.get("note", "")
        mat.quantity_sheets = new_sh; mat.quantity_kg = new_kg; mat.quantity_pcs = new_pcs
        db.add(MaterialMovement(material_id=mat.id, movement_type="Корректировка",
                                quantity_sheets=diff_sh, quantity_kg=diff_kg, quantity_pcs=diff_pcs,
                                user_id=uid, note=note))
        audit(db, uid, "Корректировка", "material", mat.id, f"{mat.name}: Δл={diff_sh} Δкг={diff_kg} | {note}")
        db.flush(); db.commit()
        return {"status": "ok"}

    @app.get("/api/materials/edit-history")
    def api_edit_history(material_id: int = 0, movement_type: str = "",
                         user_id: int = 0, date_from: str = "", date_to: str = "",
                         db: Session = Depends(db_dep)):
        q = db.query(MaterialMovement).options(
            joinedload(MaterialMovement.user), joinedload(MaterialMovement.material),
            joinedload(MaterialMovement.order), joinedload(MaterialMovement.resource))
        if material_id: q = q.filter(MaterialMovement.material_id == material_id)
        if movement_type: q = q.filter(MaterialMovement.movement_type == movement_type)
        if user_id: q = q.filter(MaterialMovement.user_id == user_id)
        if date_from:
            q = q.filter(MaterialMovement.created_at >= datetime.datetime.fromisoformat(date_from))
        if date_to:
            q = q.filter(MaterialMovement.created_at <= datetime.datetime.fromisoformat(date_to + "T23:59:59"))
        return [{"id": mv.id, "material": mv.material.name if mv.material else "",
                 "material_code": mv.material.code if mv.material else "",
                 "type": mv.movement_type, "kg": mv.quantity_kg,
                 "sheets": mv.quantity_sheets, "pcs": mv.quantity_pcs,
                 "order": mv.order.order_number if mv.order else "",
                 "user": mv.user.full_name if mv.user else "",
                 "note": mv.note, "date": mv.created_at.isoformat()}
                for mv in q.order_by(MaterialMovement.created_at.desc()).limit(500).all()]

    @app.get("/api/materials/movement-types")
    def api_mvt_types(db: Session = Depends(db_dep)):
        return [t[0] for t in db.query(MaterialMovement.movement_type).distinct().order_by(MaterialMovement.movement_type).all()]

    @app.get("/api/materials/{mid}/movements")
    def api_movements(mid: int, db: Session = Depends(db_dep)):
        mvs = db.query(MaterialMovement).options(
            joinedload(MaterialMovement.user), joinedload(MaterialMovement.order),
            joinedload(MaterialMovement.resource)
        ).filter(MaterialMovement.material_id == mid).order_by(MaterialMovement.created_at.desc()).limit(200).all()
        return [{"id": mv.id, "type": mv.movement_type, "kg": mv.quantity_kg,
                 "sheets": mv.quantity_sheets, "pcs": mv.quantity_pcs,
                 "order": mv.order.order_number if mv.order else "",
                 "user": mv.user.full_name if mv.user else "",
                 "resource": mv.resource.name if mv.resource else "",
                 "note": mv.note, "date": mv.created_at.isoformat()} for mv in mvs]

    # ─── Part Templates ─────────────────────────────
    @app.get("/api/part-templates")
    def api_part_templates(customer_id: int = 0, search: str = "", db: Session = Depends(db_dep)):
        q = db.query(PartTemplate).options(
            joinedload(PartTemplate.customer),
            joinedload(PartTemplate.materials).joinedload(PartTemplateMaterial.material),
            joinedload(PartTemplate.files))
        if customer_id: q = q.filter(PartTemplate.customer_id == customer_id)
        if search:
            sq = f"%{search}%"
            q = q.outerjoin(Customer, PartTemplate.customer_id == Customer.id).filter(
                (PartTemplate.name.ilike(sq)) | (PartTemplate.part_number.ilike(sq)) | (Customer.name.ilike(sq)))
        return [{"id": p.id, "name": p.name, "part_number": p.part_number,
                 "display_name": pt_display(p),
                 "customer_id": p.customer_id,
                 "customer_name": p.customer.name if p.customer else "—",
                 "description": p.description, "operation_times": p.get_op_times(),
                 "materials": [{"id": pm.id, "material_id": pm.material_id,
                                "material_code": pm.material.code if pm.material else "",
                                "material_name": pm.material.name if pm.material else "",
                                "material_id_val": pm.material_id,
                                "sheets_input": pm.sheets_input,
                                "parts_per_sheets": pm.parts_per_sheets,
                                "sheets_per_one": pm.sheets_per_one}
                               for pm in (p.materials or [])],
                 "files": [{"id": f.id, "name": f.original_name, "type": f.file_type,
                            "size": f.file_size, "date": f.uploaded_at.isoformat()}
                           for f in (p.files or [])],
                 "is_assembly": p.is_assembly or False,
                 "components": [{"id": ac.id, "component_id": ac.component_id,
                                 "component_name": pt_display(ac.component) if ac.component else "?",
                                 "quantity": ac.quantity, "sort_order": ac.sort_order}
                                for ac in (p.components or [])]}
                for p in q.order_by(PartTemplate.name).all()]

    @app.post("/api/part-templates/save")
    async def api_save_pt(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        pid = data.get("id")
        if pid: p = db.query(PartTemplate).get(pid)
        else: p = PartTemplate(); db.add(p)
        p.name = data.get("name", p.name or "")
        p.part_number = data.get("part_number", p.part_number or "")
        p.customer_id = data.get("customer_id") or None
        p.description = data.get("description", p.description or "")
        if "operation_times" in data: p.set_op_times(data["operation_times"])
        p.is_assembly = data.get("is_assembly", p.is_assembly or False)
        if "materials" in data:
            db.query(PartTemplateMaterial).filter(PartTemplateMaterial.part_template_id == p.id).delete()
            db.flush()
            for md in data["materials"]:
                shi = int(md.get("sheets_input", 1)); pps = int(md.get("parts_per_sheets", 1))
                db.add(PartTemplateMaterial(
                    part_template_id=p.id, material_id=int(md["material_id"]),
                    sheets_input=shi, parts_per_sheets=pps,
                    sheets_per_one=round(shi / pps, 6) if pps > 0 else 0))
        if "components" in data:
            db.query(AssemblyComponent).filter(AssemblyComponent.assembly_id == p.id).delete()
            db.flush()
            for i, cd in enumerate(data["components"]):
                db.add(AssemblyComponent(assembly_id=p.id, component_id=int(cd["component_id"]),
                                         quantity=int(cd.get("quantity", 1)), sort_order=i))
        db.flush()
        if pid: recalc_linked_items(db, p.id, data.get("user_id"))
        db.commit()
        return {"id": p.id}

    @app.post("/api/part-templates/delete")
    async def api_del_pt(req: IdReq, db: Session = Depends(db_dep)):
        p = db.query(PartTemplate).get(req.id)
        if p: db.delete(p); db.commit()
        return {"status": "ok"}

    @app.post("/api/part-templates/{ptid}/upload")
    async def api_pt_upload(ptid: int, file: UploadFile = File(...),
                            file_type: str = Form("Чертёж"), description: str = Form(""),
                            user_id: int = Form(1), db: Session = Depends(db_dep)):
        pt = db.query(PartTemplate).get(ptid)
        if not pt: raise HTTPException(404)
        ext = Path(file.filename).suffix
        stored = f"pt_{ptid}_{uuid.uuid4().hex}{ext}"
        content = await file.read()
        with open(UPLOAD_DIR / stored, "wb") as f: f.write(content)
        mime = mimetypes.guess_type(file.filename)[0] or "application/octet-stream"
        ptf = PartTemplateFile(part_template_id=ptid, filename=stored, original_name=file.filename,
                               file_type=file_type, file_size=len(content), mime_type=mime,
                               uploaded_by=user_id, description=description)
        db.add(ptf); db.flush(); db.commit()
        return {"id": ptf.id, "name": ptf.original_name}

    @app.get("/api/part-template-files/{fid}/download")
    def api_pt_download(fid: int, db: Session = Depends(db_dep)):
        f = db.query(PartTemplateFile).get(fid)
        if not f: raise HTTPException(404)
        fpath = UPLOAD_DIR / f.filename
        if not fpath.exists(): raise HTTPException(404)
        return FileResponse(fpath, filename=f.original_name, media_type=f.mime_type)

    @app.post("/api/part-template-files/delete")
    async def api_pt_del_file(req: IdReq, db: Session = Depends(db_dep)):
        f = db.query(PartTemplateFile).get(req.id)
        if f:
            fp = UPLOAD_DIR / f.filename
            if fp.exists(): fp.unlink()
            db.delete(f); db.commit()
        return {"status": "ok"}

    # ─── Orders ─────────────────────────────────────
    @app.get("/api/orders")
    def api_orders(db: Session = Depends(db_dep)):
        orders = db.query(Order).options(
            joinedload(Order.customer),
            joinedload(Order.items).joinedload(OrderItem.part_template).joinedload(
                PartTemplate.materials).joinedload(PartTemplateMaterial.material),
            joinedload(Order.files)
        ).order_by(Order.created_at.desc()).all()
        return [{"id": o.id, "number": o.order_number,
                 "customer": o.customer.name if o.customer else "—",
                 "customer_id": o.customer_id, "display": o.display_name,
                 "overdue": o.is_overdue, "status": o.status, "priority": o.priority,
                 "total_amount": o.total_amount, "description": o.description, "notes": o.notes,
                 "deadline": o.deadline.isoformat() if o.deadline else None,
                 "completed_at": o.completed_at.isoformat() if o.completed_at else None,
                 "items": [{"id": it.id,
                            "part_name": pt_display(it.part_template),
                            "part_number": it.part_template.part_number if it.part_template else "",
                            "template_id": it.part_template_id,
                            "quantity": it.quantity, "completed": it.completed_qty,
                            "rejected": it.rejected_qty, "surplus": it.surplus,
                            "materials": [{"code": pm.material.code, "name": pm.material.name,
                                           "material_id": pm.material_id,
                                           "sheets_needed": pm.calc_sheets_for_qty(it.quantity),
                                           "kg_needed": round(pm.calc_sheets_for_qty(it.quantity) * (pm.material.sheet_weight_kg or 0), 2)}
                                          for pm in (it.part_template.materials or [])]
                            if it.part_template else []}
                           for it in (o.items or [])],
                 "files": [{"id": f.id, "name": f.original_name, "type": f.file_type,
                            "size": f.file_size, "date": f.uploaded_at.isoformat()}
                           for f in (o.files or [])],
                 "created": o.created_at.isoformat()} for o in orders]

    @app.post("/api/orders/save")
    async def api_save_order(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        uid = data.get("user_id", 1); oid = data.get("id")
        if oid:
            o = db.query(Order).get(oid)
            for k in ["description", "notes"]:
                if k in data: setattr(o, k, data[k])
            if "customer_id" in data: o.customer_id = data["customer_id"] or None
            if "priority" in data: o.priority = data["priority"]
            if "total_amount" in data: o.total_amount = float(data["total_amount"] or 0)
            if "deadline" in data:
                o.deadline = datetime.datetime.fromisoformat(data["deadline"]) if data["deadline"] else None
            audit(db, uid, "Редактирование заказа", "order", o.id, o.order_number)
        else:
            cnt = db.query(Order).count()
            num = f"ORD-{now_msk().strftime('%y%m')}-{cnt + 1:04d}"
            dl = datetime.datetime.fromisoformat(data["deadline"]) if data.get("deadline") else None
            o = Order(order_number=num, customer_id=data.get("customer_id") or None,
                      description=data.get("description", ""), priority=data.get("priority", "Обычный"),
                      total_amount=float(data.get("total_amount", 0) or 0),
                      deadline=dl, notes=data.get("notes", ""))
            db.add(o); db.flush()
            audit(db, uid, "Создание заказа", "order", o.id, num)
        db.flush(); db.commit()
        return {"id": o.id, "number": getattr(o, 'order_number', '')}

    @app.post("/api/orders/{oid}/status")
    async def api_order_status(oid: int, request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        force = data.get("force", False)
        o = db.query(Order).get(oid)
        if not o: raise HTTPException(404)
        new_status = data["status"]
        if new_status == "Завершён" and not force:
            active_res = db.query(Reservation).filter(Reservation.order_id == oid, Reservation.is_active == True).all()
            unconsumed = []
            for r in active_res:
                consumed_sh = db.query(func.coalesce(func.sum(WriteOff.quantity_sheets), 0)).filter(
                    WriteOff.reservation_id == r.id, WriteOff.is_cancelled == False).scalar() or 0
                if consumed_sh < r.quantity_sheets: unconsumed.append(r)
            if unconsumed:
                return {"status": "warning", "message": f"Не списано {len(unconsumed)} резервов. Завершить заказ?", "unconsumed": len(unconsumed)}
        old = o.status; o.status = new_status
        if o.status == "Завершён": o.completed_at = now_msk()
        audit(db, data.get("user_id", 1), "Смена статуса", "order", oid, f"{old} → {o.status}")
        db.flush(); db.commit()
        return {"status": "ok"}

    @app.post("/api/orders/delete")
    async def api_delete_order(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        uid = data.get("user_id", 1)
        oid = data.get("id")
        o = db.query(Order).get(oid)
        if not o:
            raise HTTPException(404, "Заказ не найден")
        # Снимаем все активные резервы — возвращаем материал
        for r in db.query(Reservation).filter(Reservation.order_id == oid, Reservation.is_active == True).all():
            mat = db.query(Material).get(r.material_id)
            if mat:
                mat.reserved_sheets = max(0, mat.reserved_sheets - r.quantity_sheets)
                mat.reserved_kg = max(0, mat.reserved_kg - r.quantity_kg)
                db.add(MaterialMovement(material_id=mat.id, movement_type="Снятие резерва",
                                        quantity_sheets=r.quantity_sheets, quantity_kg=r.quantity_kg,
                                        order_id=oid, user_id=uid,
                                        note=f"Удаление заказа {o.order_number}"))
            r.is_active = False
        # Удаляем файлы с диска
        for f in db.query(OrderFile).filter(OrderFile.order_id == oid).all():
            fp = UPLOAD_DIR / f.filename
            if fp.exists():
                fp.unlink()
        audit(db, uid, "Удаление заказа", "order", oid, o.order_number)
        db.delete(o)
        db.flush()
        db.commit()
        return {"status": "ok"}

    # ─── Order Items ────────────────────────────────
    @app.post("/api/order-items/save")
    async def api_save_item(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        uid = data.get("user_id", 1); iid = data.get("id")
        if iid:
            it = db.query(OrderItem).get(iid)
            old_qty = it.quantity; it.quantity = int(data.get("quantity", it.quantity))
            if it.quantity != old_qty:
                remove_item_reservations(db, it.id); remove_item_operations(db, it.id)
                auto_create_reservations(db, it, uid); auto_create_operations(db, it)
        else:
            it = OrderItem(order_id=data["order_id"], part_template_id=data["part_template_id"],
                           quantity=int(data.get("quantity", 1)))
            db.add(it); db.flush()
            auto_create_reservations(db, it, uid); auto_create_operations(db, it)
        db.flush()
        unassigned = db.query(ProductionOp).filter(ProductionOp.order_item_id == it.id, ProductionOp.resource_id.is_(None)).count()
        audit(db, uid, "Позиция заказа", "order_item", it.id, f"{pt_display(it.part_template)} x{it.quantity}")
        db.commit()
        return {"id": it.id, "unassigned_ops": unassigned}

    @app.post("/api/order-items/delete")
    async def api_del_item(req: IdReq, db: Session = Depends(db_dep)):
        it = db.query(OrderItem).get(req.id)
        if it:
            remove_item_reservations(db, it.id); remove_item_operations(db, it.id)
            db.delete(it); db.commit()
        return {"status": "ok"}

    # ─── Order Files ────────────────────────────────
    @app.post("/api/orders/{oid}/upload")
    async def api_upload(oid: int, file: UploadFile = File(...),
                         file_type: str = Form("Чертёж"), description: str = Form(""),
                         user_id: int = Form(1), db: Session = Depends(db_dep)):
        order = db.query(Order).get(oid)
        if not order: raise HTTPException(404)
        ext = Path(file.filename).suffix
        stored = f"{oid}_{uuid.uuid4().hex}{ext}"
        content = await file.read()
        with open(UPLOAD_DIR / stored, "wb") as f: f.write(content)
        mime = mimetypes.guess_type(file.filename)[0] or "application/octet-stream"
        of = OrderFile(order_id=oid, filename=stored, original_name=file.filename,
                       file_type=file_type, file_size=len(content), mime_type=mime,
                       uploaded_by=user_id, description=description)
        db.add(of); db.flush(); db.commit()
        return {"id": of.id}

    @app.get("/api/files/{fid}/download")
    def api_download(fid: int, db: Session = Depends(db_dep)):
        f = db.query(OrderFile).get(fid)
        if not f: raise HTTPException(404)
        fpath = UPLOAD_DIR / f.filename
        if not fpath.exists(): raise HTTPException(404)
        return FileResponse(fpath, filename=f.original_name, media_type=f.mime_type)

    @app.post("/api/files/delete")
    async def api_del_file(req: IdReq, db: Session = Depends(db_dep)):
        f = db.query(OrderFile).get(req.id)
        if f:
            fp = UPLOAD_DIR / f.filename
            if fp.exists(): fp.unlink()
            db.delete(f); db.commit()
        return {"status": "ok"}

    # ─── Reservations ───────────────────────────────
    @app.get("/api/reservations")
    def api_reservations(active_only: int = 1, db: Session = Depends(db_dep)):
        q = db.query(Reservation).options(
            joinedload(Reservation.order).joinedload(Order.customer),
            joinedload(Reservation.material), joinedload(Reservation.part_template),
            joinedload(Reservation.reserver), joinedload(Reservation.order_item))
        if active_only: q = q.filter(Reservation.is_active == True)
        rs_all = q.order_by(Reservation.created_at.desc()).all()
        consumed_map = {}
        consumed_q = db.query(
            WriteOff.reservation_id, func.sum(WriteOff.quantity_sheets), func.sum(WriteOff.quantity_kg)
        ).filter(WriteOff.reservation_id.isnot(None), WriteOff.is_cancelled == False).group_by(WriteOff.reservation_id).all()
        for rid, sh, kg in consumed_q:
            consumed_map[rid] = {"sheets": int(sh or 0), "kg": round(kg or 0, 2)}
        return [{"id": r.id, "order_id": r.order_id,
                 "order_display": r.order.display_name if r.order else "",
                 "order_status": r.order.status if r.order else "",
                 "material": r.material.name if r.material else "",
                 "material_code": r.material.code if r.material else "",
                 "material_id": r.material_id,
                 "material_type": r.material.material_type if r.material else "",
                 "part_name": pt_display(r.part_template) if r.part_template else "",
                 "kg": r.quantity_kg, "sheets": r.quantity_sheets,
                 "consumed_sheets": consumed_map.get(r.id, {}).get("sheets", 0),
                 "consumed_kg": consumed_map.get(r.id, {}).get("kg", 0),
                 "remaining_sheets": r.quantity_sheets - consumed_map.get(r.id, {}).get("sheets", 0),
                 "remaining_kg": round(r.quantity_kg - consumed_map.get(r.id, {}).get("kg", 0), 2),
                 "active": r.is_active, "note": r.note,
                 "reserved_by": r.reserver.full_name if r.reserver else "",
                 "created": r.created_at.isoformat()} for r in rs_all]

    @app.post("/api/reservations/create")
    async def api_create_res(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        uid = data.get("user_id", 1)
        order = db.query(Order).get(data["order_id"])
        if not order or order.status != "В работе": raise HTTPException(400, "Заказ должен быть «В работе»")
        mat = db.query(Material).get(data["material_id"])
        if not mat: raise HTTPException(404)
        sheets = int(data.get("sheets", 0))
        kg = round(sheets * (mat.sheet_weight_kg or 0), 2) if sheets else float(data.get("kg", 0))
        if sheets > 0: mat.reserved_sheets += sheets; mat.reserved_kg += kg
        elif kg > 0: mat.reserved_kg += kg
        else: raise HTTPException(400, "Укажите количество")
        res = Reservation(order_id=data["order_id"], material_id=mat.id,
                          order_item_id=data.get("order_item_id"),
                          part_template_id=data.get("part_template_id"),
                          quantity_sheets=sheets, quantity_kg=kg,
                          reserved_by=uid, note=data.get("note", ""))
        db.add(res)
        db.add(MaterialMovement(material_id=mat.id, movement_type="Резерв",
                                quantity_sheets=sheets, quantity_kg=kg,
                                order_id=data["order_id"], user_id=uid))
        db.flush(); db.commit()
        return {"id": res.id}

    @app.post("/api/reservations/{rid}/edit")
    async def api_edit_res(rid: int, request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        res = db.query(Reservation).get(rid)
        if not res or not res.is_active: raise HTTPException(404)
        mat = db.query(Material).get(res.material_id)
        mat.reserved_sheets -= res.quantity_sheets; mat.reserved_kg -= res.quantity_kg
        new_sh = int(data.get("sheets", res.quantity_sheets))
        new_kg = round(new_sh * (mat.sheet_weight_kg or 0), 2)
        mat.reserved_sheets += new_sh; mat.reserved_kg += new_kg
        res.quantity_sheets = new_sh; res.quantity_kg = new_kg
        res.note = data.get("note", res.note)
        db.flush(); db.commit()
        return {"status": "ok"}

    @app.post("/api/reservations/{rid}/cancel")
    async def api_cancel_res(rid: int, request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        uid = data.get("user_id", 1)
        res = db.query(Reservation).get(rid)
        if not res or not res.is_active: raise HTTPException(400)
        mat = db.query(Material).get(res.material_id)
        mat.reserved_sheets = max(0, mat.reserved_sheets - res.quantity_sheets)
        mat.reserved_kg = max(0, mat.reserved_kg - res.quantity_kg)
        res.is_active = False
        db.add(MaterialMovement(material_id=mat.id, movement_type="Снятие резерва",
                                quantity_sheets=res.quantity_sheets, quantity_kg=res.quantity_kg,
                                order_id=res.order_id, user_id=uid))
        db.flush(); db.commit()
        return {"status": "ok"}

    @app.get("/api/reservations/by-item/{item_id}")
    def api_res_by_item(item_id: int, db: Session = Depends(db_dep)):
        rs = db.query(Reservation).options(joinedload(Reservation.material)).filter(
            Reservation.order_item_id == item_id, Reservation.is_active == True).all()
        rs = db.query(Reservation).options(
            joinedload(Reservation.material), joinedload(Reservation.part_template)
        ).filter(Reservation.order_item_id == item_id, Reservation.is_active == True).all()
        return [{"id": r.id, "material_id": r.material_id,
                 "material": r.material.name if r.material else "",
                 "material_code": r.material.code if r.material else "",
                 "sheets": r.quantity_sheets, "kg": r.quantity_kg,
                 "part_template_id": r.part_template_id,
                 "part_name": pt_display(r.part_template) if r.part_template else "—"} for r in rs]

    # ─── Resources ──────────────────────────────────
    @app.get("/api/resources")
    def api_resources(db: Session = Depends(db_dep)):
        return [{"id": r.id, "name": r.name, "type": r.resource_type, "code": r.code,
                 "available": r.is_available, "allowed_ops": r.get_allowed_ops(),
                 "shift_hours": r.shift_hours, "shifts_per_day": r.shifts_per_day,
                 "daily_min": r.daily_capacity_min, "description": r.description}
                for r in db.query(Resource).order_by(Resource.name).all()]

    @app.post("/api/resources/save")
    async def api_save_res(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        rid = data.get("id")
        if rid: r = db.query(Resource).get(rid)
        else: r = Resource(resource_type=data.get("resource_type", "ОТК")); db.add(r)
        r.name = data.get("name", r.name or "")
        r.resource_type = data.get("resource_type", r.resource_type)
        r.code = data.get("code", r.code or "")
        r.is_available = data.get("is_available", r.is_available)
        r.description = data.get("description", r.description or "")
        r.shift_hours = float(data.get("shift_hours", r.shift_hours or 8))
        r.shifts_per_day = int(data.get("shifts_per_day", r.shifts_per_day or 1))
        if "allowed_ops" in data: r.set_allowed_ops(data["allowed_ops"])
        db.flush(); db.commit()
        return {"id": r.id}

    @app.post("/api/resources/delete")
    async def api_del_res(req: IdReq, db: Session = Depends(db_dep)):
        r = db.query(Resource).get(req.id)
        if not r: raise HTTPException(404)
        if db.query(ProductionOp).filter(ProductionOp.resource_id == req.id).count() > 0:
            raise HTTPException(400, "Есть операции на этом ресурсе")
        db.delete(r); db.commit()
        return {"status": "ok"}

    # ─── Operations ─────────────────────────────────
    @app.get("/api/operations")
    def api_operations(order_id: int = 0, active_only: int = 0, resource_id: int = 0,
                       db: Session = Depends(db_dep)):
        q = db.query(ProductionOp).options(
            joinedload(ProductionOp.order).joinedload(Order.customer),
            joinedload(ProductionOp.resource), joinedload(ProductionOp.operator),
            joinedload(ProductionOp.order_item).joinedload(OrderItem.part_template))
        if order_id: q = q.filter(ProductionOp.order_id == order_id)
        if active_only: q = q.join(Order).filter(Order.status == "В работе")
        if resource_id: q = q.filter(ProductionOp.resource_id == resource_id)
        return [{"id": o.id, "order_id": o.order_id,
                 "order_number": o.order.order_number if o.order else "",
                 "order_display": o.order.display_name if o.order else "",
                 "item": pt_display(o.order_item.part_template) if o.order_item and o.order_item.part_template else "",
                 "item_id": o.order_item_id,
                 "type": o.operation_type, "status": o.status,
                 "resource": o.resource.name if o.resource else "—",
                 "resource_id": o.resource_id,
                 "operator": o.operator.full_name if o.operator else "",
                 "sequence": o.sequence, "sort_order": o.sort_order,
                 "planned_qty": o.planned_qty, "completed_qty": o.completed_qty,
                 "rejected_qty": o.rejected_qty,
                 "estimated_min": o.estimated_minutes, "actual_min": o.actual_minutes,
                 "started_at": o.started_at.isoformat() if o.started_at else None,
                 "completed_at": o.completed_at.isoformat() if o.completed_at else None,
                 "paused_at": o.paused_at.isoformat() if o.paused_at else None,
                 "total_pause_min": o.total_pause_minutes}
                for o in q.order_by(ProductionOp.resource_id, ProductionOp.sort_order, ProductionOp.sequence).all()]

    @app.post("/api/operations/save")
    async def api_save_op(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        opid = data.get("id")
        if opid: op = db.query(ProductionOp).get(opid)
        else: op = ProductionOp(order_id=data["order_id"]); db.add(op)
        for k in ["operation_type", "sequence", "sort_order", "planned_qty",
                   "estimated_minutes", "resource_id", "order_item_id", "description"]:
            if k in data: setattr(op, k, data[k])
        db.flush(); db.commit()
        return {"id": op.id}

    @app.post("/api/operations/delete")
    async def api_del_op(req: IdReq, db: Session = Depends(db_dep)):
        op = db.query(ProductionOp).get(req.id)
        if op: db.delete(op); db.commit()
        return {"status": "ok"}

    @app.post("/api/operations/{opid}/start")
    async def api_start_op(opid: int, request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        op = db.query(ProductionOp).get(opid)
        if not op: raise HTTPException(404)
        n = now_msk()
        if op.status == "Пауза" and op.paused_at:
            pause_dur = int((n - op.paused_at).total_seconds() / 60)
            op.total_pause_minutes = (op.total_pause_minutes or 0) + pause_dur
            op.paused_at = None
        else:
            op.started_at = n
        op.status = "В работе"; op.assigned_to = data.get("user_id", 1)
        audit(db, data.get("user_id", 1), "Старт операции", "op", opid, op.operation_type)
        db.flush(); db.commit()
        return {"status": "ok"}

    @app.post("/api/operations/{opid}/pause")
    async def api_pause_op(opid: int, request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        op = db.query(ProductionOp).get(opid)
        if not op: raise HTTPException(404)
        op.status = "Пауза"; op.paused_at = now_msk()
        audit(db, data.get("user_id", 1), "Пауза операции", "op", opid, op.operation_type)
        db.flush(); db.commit()
        return {"status": "ok"}

    @app.post("/api/operations/{opid}/complete")
    async def api_complete_op(opid: int, request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        op = db.query(ProductionOp).get(opid)
        if not op: raise HTTPException(404)
        n = now_msk()
        op.status = "Завершена"; op.completed_at = n
        if op.started_at:
            total_elapsed = int((n - op.started_at).total_seconds() / 60)
            op.actual_minutes = total_elapsed - (op.total_pause_minutes or 0)
        audit(db, data.get("user_id", 1), "Завершение операции", "op", opid, op.operation_type)
        db.flush(); db.commit()
        return {"status": "ok"}

    @app.post("/api/operations/{opid}/rollback")
    async def api_rollback_op(opid: int, request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        op = db.query(ProductionOp).get(opid)
        if not op: raise HTTPException(404)
        if op.status not in ("Завершена", "В работе", "Пауза"): raise HTTPException(400, "Откат невозможен")
        old = op.status; op.status = "Ожидает"
        op.completed_at = None; op.actual_minutes = None; op.started_at = None
        op.paused_at = None; op.total_pause_minutes = 0
        audit(db, data.get("user_id", 1), "Откат операции", "op", opid, f"{old} → Ожидает")
        db.flush(); db.commit()
        return {"status": "ok"}

    @app.post("/api/operations/reorder")
    async def api_reorder_ops(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        for item in data.get("order", []):
            op = db.query(ProductionOp).get(item["id"])
            if op: op.sort_order = item["sort_order"]
        db.flush(); db.commit()
        return {"status": "ok"}

    # ─── Resources for operation type ───────────────
    @app.get("/api/resources/for-operation/{op_type}")
    def api_resources_for_op(op_type: str, db: Session = Depends(db_dep)):
        matching = find_resources_for_op(db, op_type)
        return [{"id": r.id, "name": r.name, "type": r.resource_type} for r in matching]

    # ─── Part Station Logs ──────────────────────────
    @app.get("/api/part-station-logs")
    def api_part_station_logs(active_only: int = 1, db: Session = Depends(db_dep)):
        q = db.query(OrderItem).options(
            joinedload(OrderItem.order).joinedload(Order.customer),
            joinedload(OrderItem.part_template),
            joinedload(OrderItem.station_logs).joinedload(PartStationLog.resource),
            joinedload(OrderItem.station_logs).joinedload(PartStationLog.user))
        if active_only: q = q.join(Order).filter(Order.status == "В работе")
        result = []
        for it in q.all():
            first_res_id = get_first_op_resource_id(db, it.id)
            by_res = {}; first_res_good = 0
            for l in (it.station_logs or []):
                rn = l.resource.name if l.resource else "—"
                if rn not in by_res: by_res[rn] = {"good": 0, "rejected": 0, "logs": []}
                by_res[rn]["good"] += l.good_qty
                by_res[rn]["rejected"] += l.rejected_qty
                by_res[rn]["logs"].append({
                    "id": l.id, "operation": l.operation_type, "good": l.good_qty,
                    "rejected": l.rejected_qty, "anomaly": l.is_anomaly,
                    "anomaly_note": l.anomaly_note,
                    "user": l.user.full_name if l.user else "",
                    "note": l.note, "date": l.created_at.isoformat()})
                if l.resource_id == first_res_id: first_res_good += l.good_qty
            surplus = max(0, first_res_good - it.quantity) if first_res_id else it.surplus
            result.append({
                "item_id": it.id, "order_id": it.order_id,
                "order_number": it.order.order_number if it.order else "",
                "order_display": it.order.display_name if it.order else "",
                "order_status": it.order.status if it.order else "",
                "part_name": pt_display(it.part_template),
                "is_assembly": it.part_template.is_assembly if it.part_template else False,
                "components": [{"name": pt_display(ac.component), "qty": ac.quantity}
                               for ac in (it.part_template.components or [])] if it.part_template and it.part_template.is_assembly else [],
                "quantity": it.quantity, "completed": it.completed_qty,
                "rejected": it.rejected_qty, "surplus": surplus,
                "by_resource": by_res})
        return result

    @app.get("/api/part-station-logs/surplus")
    def api_surplus(db: Session = Depends(db_dep)):
        items = db.query(OrderItem).options(
            joinedload(OrderItem.order).joinedload(Order.customer),
            joinedload(OrderItem.part_template),
            joinedload(OrderItem.station_logs).joinedload(PartStationLog.resource)
        ).join(Order).filter(Order.status.notin_(["Отменён"])).all()
        result = {}
        for it in items:
            first_res_id = get_first_op_resource_id(db, it.id)
            if not first_res_id:
                if it.completed_qty > it.quantity:
                    name = pt_display(it.part_template)
                    surplus = it.completed_qty - it.quantity
                    if name not in result: result[name] = {"part_name": name, "total_surplus": 0, "orders": []}
                    result[name]["total_surplus"] += surplus
                    result[name]["orders"].append({"order": it.order.display_name if it.order else "—",
                        "planned": it.quantity, "completed_first": it.completed_qty, "surplus": surplus})
                continue
            first_good = sum(l.good_qty for l in (it.station_logs or []) if l.resource_id == first_res_id)
            surplus = first_good - it.quantity
            if surplus <= 0: continue
            name = pt_display(it.part_template)
            if name not in result: result[name] = {"part_name": name, "total_surplus": 0, "orders": []}
            result[name]["total_surplus"] += surplus
            result[name]["orders"].append({"order": it.order.display_name if it.order else "—",
                "planned": it.quantity, "completed_first": first_good, "surplus": surplus})
        return list(result.values())

    # ─── Writeoffs ──────────────────────────────────
    @app.get("/api/writeoffs")
    def api_writeoffs(wtype: str = "", db: Session = Depends(db_dep)):
        q = db.query(WriteOff).options(
            joinedload(WriteOff.user), joinedload(WriteOff.resource),
            joinedload(WriteOff.order).joinedload(Order.customer),
            joinedload(WriteOff.material),
            joinedload(WriteOff.order_item).joinedload(OrderItem.part_template),
            joinedload(WriteOff.reservation), joinedload(WriteOff.cancelled_user))
        if wtype: q = q.filter(WriteOff.writeoff_type == wtype)
        return [{"id": w.id, "type": w.writeoff_type,
                 "user": w.user.full_name if w.user else "",
                 "resource": w.resource.name if w.resource else "",
                 "order_display": w.order.display_name if w.order else "",
                 "material": w.material.name if w.material else "",
                 "material_type": w.material.material_type if w.material else "",
                 "sheets": w.quantity_sheets, "kg": w.quantity_kg, "pcs": w.quantity_pcs,
                 "part_name": pt_display(w.order_item.part_template) if w.order_item and w.order_item.part_template else "",
                 "parts_good": w.parts_good, "parts_rejected": w.parts_rejected,
                 "is_anomaly": w.is_anomaly, "anomaly_note": w.anomaly_note,
                 "is_cancelled": w.is_cancelled,
                 "cancelled_by": w.cancelled_user.full_name if w.cancelled_user else "",
                 "cancelled_at": w.cancelled_at.isoformat() if w.cancelled_at else None,
                 "note": w.note, "date": w.created_at.isoformat()}
                for w in q.order_by(WriteOff.created_at.desc()).limit(500).all()]

    @app.get("/api/orders/{oid}/items-for-writeoff")
    def api_items_for_wo(oid: int, db: Session = Depends(db_dep)):
        items = db.query(OrderItem).options(joinedload(OrderItem.part_template)).filter(OrderItem.order_id == oid).all()
        return [{"id": it.id, "part_name": pt_display(it.part_template),
                 "template_id": it.part_template_id,
                 "quantity": it.quantity, "completed": it.completed_qty} for it in items]

    @app.get("/api/orders/{oid}/resources-for-writeoff")
    def api_res_for_wo(oid: int, db: Session = Depends(db_dep)):
        ops = db.query(ProductionOp).options(joinedload(ProductionOp.resource)).filter(
            ProductionOp.order_id == oid, ProductionOp.resource_id.isnot(None)).all()
        seen = set(); result = []
        for op in ops:
            if op.resource_id not in seen:
                seen.add(op.resource_id)
                result.append({"id": op.resource_id, "name": op.resource.name if op.resource else "?",
                                "allowed_ops": op.resource.get_allowed_ops() if op.resource else []})
        return result

    @app.post("/api/writeoffs/create")
    async def api_create_wo(request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        uid = data.get("user_id", 1); wtype = data["writeoff_type"]
        wo = WriteOff(writeoff_type=wtype, user_id=uid, resource_id=data.get("resource_id"),
                      order_id=data.get("order_id"), order_item_id=data.get("order_item_id"),
                      note=data.get("note", ""))
        if wtype == "Материал":
            res_id = data.get("reservation_id")
            reservation = db.query(Reservation).get(res_id) if res_id else None
            mat = db.query(Material).get(data["material_id"])
            if not mat: raise HTTPException(404)
            wo.material_id = mat.id; wo.reservation_id = res_id
            sh = int(data.get("sheets", 0)); kg = float(data.get("kg", 0))
            if sh > 0:
                sub_kg = round(sh * (mat.sheet_weight_kg or 0), 2)
                mat.quantity_sheets -= sh; mat.quantity_kg -= sub_kg
                wo.quantity_sheets = sh; wo.quantity_kg = sub_kg
                if reservation and reservation.is_active:
                    reservation.quantity_sheets = max(0, reservation.quantity_sheets - sh)
                    reservation.quantity_kg = max(0, reservation.quantity_kg - sub_kg)
                    mat.reserved_sheets = max(0, mat.reserved_sheets - sh)
                    mat.reserved_kg = max(0, mat.reserved_kg - sub_kg)
                    if reservation.quantity_sheets <= 0: reservation.is_active = False
                db.add(MaterialMovement(material_id=mat.id, movement_type="Списание",
                                        quantity_sheets=sh, quantity_kg=sub_kg,
                                        order_id=data.get("order_id"), user_id=uid,
                                        resource_id=data.get("resource_id")))
            elif kg > 0:
                mat.quantity_kg -= kg; wo.quantity_kg = kg
            else:
                raise HTTPException(400, "Укажите количество")
            audit(db, uid, "Списание материала", "writeoff", 0, f"{mat.name}: {sh}л/{kg}кг")
        elif wtype == "Детали":
            good = int(data.get("parts_good", 0)); rej = int(data.get("parts_rejected", 0))
            if good == 0 and rej == 0:
                raise HTTPException(400, "Укажите количество годных или брак (не может быть 0)")
            wo.parts_good = good; wo.parts_rejected = rej
            is_anom, anom_note = check_sequence_anomaly(db, data["order_item_id"], data.get("resource_id"), good)
            wo.is_anomaly = is_anom; wo.anomaly_note = anom_note
            db.add(PartStationLog(order_item_id=data["order_item_id"], resource_id=data.get("resource_id"),
                                  operation_type=data.get("operation_type", ""),
                                  good_qty=good, rejected_qty=rej, is_anomaly=is_anom, anomaly_note=anom_note,
                                  user_id=uid, note=data.get("note", "")))
            item = db.query(OrderItem).get(data["order_item_id"])
            if item: item.completed_qty += good; item.rejected_qty += rej
            audit(db, uid, "Списание деталей", "writeoff", 0,
                  f"+{good} годн +{rej} брак" + (f" ⚠ {anom_note}" if is_anom else ""))
        db.add(wo); db.flush(); db.commit()
        return {"id": wo.id, "is_anomaly": wo.is_anomaly, "anomaly_note": wo.anomaly_note}

    @app.post("/api/writeoffs/{wid}/cancel")
    async def api_cancel_wo(wid: int, request: Request, db: Session = Depends(db_dep)):
        data = await request.json()
        uid = data.get("user_id", 1)
        wo = db.query(WriteOff).get(wid)
        if not wo: raise HTTPException(404)
        if wo.is_cancelled: raise HTTPException(400, "Уже отменено")
        wo.is_cancelled = True; wo.cancelled_by = uid; wo.cancelled_at = now_msk()
        if wo.writeoff_type == "Материал":
            mat = db.query(Material).get(wo.material_id)
            if mat:
                mat.quantity_sheets += wo.quantity_sheets
                mat.quantity_kg += wo.quantity_kg
                mat.quantity_pcs += wo.quantity_pcs
                db.add(MaterialMovement(material_id=mat.id, movement_type="Возврат (отмена списания)",
                    quantity_sheets=wo.quantity_sheets, quantity_kg=wo.quantity_kg, quantity_pcs=wo.quantity_pcs,
                    order_id=wo.order_id, user_id=uid, note=f"Отмена списания #{wo.id}"))
            if wo.reservation_id:
                res = db.query(Reservation).get(wo.reservation_id)
                if res:
                    res.quantity_sheets += wo.quantity_sheets
                    res.quantity_kg += wo.quantity_kg
                    if not res.is_active: res.is_active = True
                    if mat:
                        mat.reserved_sheets += wo.quantity_sheets
                        mat.reserved_kg += wo.quantity_kg
        elif wo.writeoff_type == "Детали":
            item = db.query(OrderItem).get(wo.order_item_id)
            if item:
                item.completed_qty = max(0, item.completed_qty - wo.parts_good)
                item.rejected_qty = max(0, item.rejected_qty - wo.parts_rejected)
            logs = db.query(PartStationLog).filter(
                PartStationLog.order_item_id == wo.order_item_id,
                PartStationLog.resource_id == wo.resource_id,
                PartStationLog.good_qty == wo.parts_good,
                PartStationLog.rejected_qty == wo.parts_rejected,
                PartStationLog.user_id == wo.user_id
            ).order_by(PartStationLog.created_at.desc()).first()
            if logs: db.delete(logs)
        audit(db, uid, "Отмена списания", "writeoff", wid, f"Тип={wo.writeoff_type}")
        db.flush(); db.commit()
        return {"status": "ok"}

    # ─── Analytics ──────────────────────────────────
    @app.get("/api/analytics/dashboard")
    def api_dashboard(db: Session = Depends(db_dep)):
        n = now_msk(); today = n.replace(hour=0, minute=0, second=0)
        return {
            "orders_total": db.query(Order).count(),
            "orders_active": db.query(Order).filter(Order.status.in_(["Новый", "Ожидает", "В работе"])).count(),
            "orders_completed": db.query(Order).filter(Order.status == "Завершён").count(),
            "orders_overdue": db.query(Order).filter(Order.deadline < n, Order.status.notin_(["Завершён", "Отменён", "Отгружен"])).count(),
            "ops_pending": db.query(ProductionOp).filter(ProductionOp.status.in_(["Ожидает", "Запланирована"])).count(),
            "ops_in_progress": db.query(ProductionOp).filter(ProductionOp.status == "В работе").count(),
            "ops_completed_today": db.query(ProductionOp).filter(ProductionOp.status == "Завершена", ProductionOp.completed_at >= today).count(),
            "parts_today": db.query(func.coalesce(func.sum(PartStationLog.good_qty), 0)).filter(PartStationLog.created_at >= today).scalar(),
            "rejected_today": db.query(func.coalesce(func.sum(PartStationLog.rejected_qty), 0)).filter(PartStationLog.created_at >= today).scalar(),
            "low_stock": db.query(Material).filter(Material.material_type == "Лист",
                (Material.quantity_sheets - Material.reserved_sheets) <= Material.min_stock_sheets).count()
        }

    @app.get("/api/analytics/dashboard/detail/{widget}")
    def api_dashboard_detail(widget: str, db: Session = Depends(db_dep)):
        n = now_msk(); today = n.replace(hour=0, minute=0, second=0)
        if widget == "orders_active":
            return [{"number": o.order_number, "customer": o.customer.name if o.customer else "—",
                     "status": o.status, "deadline": o.deadline.isoformat() if o.deadline else None, "overdue": o.is_overdue}
                    for o in db.query(Order).options(joinedload(Order.customer)).filter(Order.status.in_(["Новый", "Ожидает", "В работе"])).all()]
        elif widget == "orders_overdue":
            return [{"number": o.order_number, "customer": o.customer.name if o.customer else "—",
                     "status": o.status, "deadline": o.deadline.isoformat() if o.deadline else None}
                    for o in db.query(Order).options(joinedload(Order.customer)).filter(Order.deadline < n, Order.status.notin_(["Завершён", "Отменён", "Отгружен"])).all()]
        elif widget == "ops_in_progress":
            return [{"order": o.order.order_number if o.order else "", "type": o.operation_type,
                     "resource": o.resource.name if o.resource else "—"}
                    for o in db.query(ProductionOp).options(joinedload(ProductionOp.order), joinedload(ProductionOp.resource)).filter(ProductionOp.status == "В работе").all()]
        elif widget == "low_stock":
            return [{"code": m.code, "name": m.name, "available": m.available_sheets, "min": m.min_stock_sheets}
                    for m in db.query(Material).filter(Material.material_type == "Лист",
                    (Material.quantity_sheets - Material.reserved_sheets) <= Material.min_stock_sheets).all()]
        elif widget == "parts_today":
            return [{"part": pt_display(l.order_item.part_template) if l.order_item and l.order_item.part_template else "?",
                     "resource": l.resource.name if l.resource else "—", "good": l.good_qty, "rejected": l.rejected_qty}
                    for l in db.query(PartStationLog).options(
                    joinedload(PartStationLog.order_item).joinedload(OrderItem.part_template),
                    joinedload(PartStationLog.resource)).filter(PartStationLog.created_at >= today).all()]
        return []

    @app.get("/api/analytics/operations")
    def api_op_stats(db: Session = Depends(db_dep)):
        results = []
        for ot in db.query(OperationTypeCfg).order_by(OperationTypeCfg.sort_order).all():
            total = db.query(ProductionOp).filter(ProductionOp.operation_type == ot.name).count()
            completed = db.query(ProductionOp).filter(ProductionOp.operation_type == ot.name, ProductionOp.status == "Завершена").count()
            avg_t = db.query(func.avg(ProductionOp.actual_minutes)).filter(ProductionOp.operation_type == ot.name, ProductionOp.actual_minutes.isnot(None)).scalar()
            results.append({"type": ot.name, "total": total, "completed": completed, "avg_min": round(avg_t or 0, 1)})
        return results

    @app.get("/api/analytics/load")
    def api_load(db: Session = Depends(db_dep)):
        resources = db.query(Resource).filter(Resource.is_available == True).order_by(Resource.name).all()
        pending_ops = db.query(ProductionOp).options(joinedload(ProductionOp.resource)).filter(
            ProductionOp.status.in_(["Ожидает", "Запланирована", "В работе"]),
            ProductionOp.resource_id.isnot(None)).order_by(ProductionOp.resource_id, ProductionOp.sort_order).all()
        result = []
        for res in resources:
            daily_cap = res.daily_capacity_min
            if daily_cap <= 0: daily_cap = 480
            res_ops = [o for o in pending_ops if o.resource_id == res.id]
            total_min = sum(o.estimated_minutes for o in res_ops)
            days_needed = math.ceil(total_min / daily_cap) if total_min > 0 else 0
            day_loads = []; remaining = total_min; today = datetime.date.today()
            for d in range(max(days_needed, 1)):
                dt = today + datetime.timedelta(days=d)
                day_min = min(remaining, daily_cap)
                pct = round(day_min / daily_cap * 100) if daily_cap > 0 else 0
                day_loads.append({"date": dt.isoformat(), "label": dt.strftime("%d.%m"), "minutes": day_min, "pct": pct})
                remaining -= day_min
                if remaining <= 0: break
            result.append({"resource_id": res.id, "resource_name": res.name,
                           "total_min": total_min, "daily_cap": daily_cap,
                           "days_needed": days_needed, "ops_count": len(res_ops), "day_loads": day_loads})
        return result

    @app.get("/api/orders/{oid}/stats")
    def api_order_stats(oid: int, db: Session = Depends(db_dep)):
        order = db.query(Order).get(oid)
        if not order: raise HTTPException(404)
        ops = db.query(ProductionOp).options(joinedload(ProductionOp.resource)).filter(ProductionOp.order_id == oid).order_by(ProductionOp.sequence).all()
        n = now_msk(); by_resource = {}; first_start = None; last_complete = None
        for op in ops:
            rn = op.resource.name if op.resource else "Не назначен"
            shift_h = op.resource.shift_hours if op.resource else 8
            if rn not in by_resource:
                by_resource[rn] = {"work_min": 0, "pause_min": 0, "estimated_min": 0, "completed": 0, "total": 0, "shift_hours": shift_h}
            by_resource[rn]["total"] += 1; by_resource[rn]["estimated_min"] += op.estimated_minutes
            if op.status == "Пауза":
                if op.started_at and op.paused_at:
                    elapsed = int((op.paused_at - op.started_at).total_seconds() / 60)
                    by_resource[rn]["work_min"] += elapsed - (op.total_pause_minutes or 0)
            elif op.status == "Завершена" and op.actual_minutes is not None:
                by_resource[rn]["work_min"] += op.actual_minutes; by_resource[rn]["completed"] += 1
            elif op.status == "В работе" and op.started_at:
                elapsed = int((n - op.started_at).total_seconds() / 60)
                by_resource[rn]["work_min"] += elapsed - (op.total_pause_minutes or 0)
            if op.started_at:
                if first_start is None or op.started_at < first_start: first_start = op.started_at
            if op.completed_at:
                if last_complete is None or op.completed_at > last_complete: last_complete = op.completed_at
        end_time = order.completed_at or n
        total_elapsed_min = int((end_time - first_start).total_seconds() / 60) if first_start else 0
        resources_stats = []
        for rn, data in by_resource.items():
            shift_min = data["shift_hours"] * 60
            shifts = round(data["work_min"] / shift_min, 2) if shift_min > 0 else 0
            resources_stats.append({"resource": rn, "work_hours": round(data["work_min"] / 60, 2),
                                    "work_shifts": shifts, "estimated_hours": round(data["estimated_min"] / 60, 2),
                                    "completed_ops": data["completed"], "total_ops": data["total"],
                                    "shift_hours": data["shift_hours"]})
        return {"order_number": order.order_number, "customer": order.customer.name if order.customer else "—",
                "status": order.status,
                "first_start": first_start.isoformat() if first_start else None,
                "last_complete": last_complete.isoformat() if last_complete else None,
                "order_completed": order.completed_at.isoformat() if order.completed_at else None,
                "total_elapsed_hours": round(total_elapsed_min / 60, 2),
                "total_elapsed_shifts": round(total_elapsed_min / 480, 2),
                "resources": resources_stats}

    @app.get("/api/reports/customers")
    def api_report_customers(date_from: str = "", date_to: str = "", customer_id: int = 0, db: Session = Depends(db_dep)):
        q = db.query(Order).options(joinedload(Order.customer), joinedload(Order.items).joinedload(OrderItem.part_template))
        if date_from: q = q.filter(Order.created_at >= datetime.datetime.fromisoformat(date_from))
        if date_to: q = q.filter(Order.created_at <= datetime.datetime.fromisoformat(date_to + "T23:59:59"))
        if customer_id: q = q.filter(Order.customer_id == customer_id)
        orders = q.order_by(Order.created_at.desc()).all()
        by_cust = {}
        total_summary = {"orders_count": 0, "total_amount": 0, "total_parts": 0, "completed_parts": 0}
        for o in orders:
            cn = o.customer.name if o.customer else "Без клиента"
            if cn not in by_cust:
                by_cust[cn] = {"customer": cn, "orders_count": 0, "total_amount": 0, "total_parts": 0, "completed_parts": 0, "details": {}, "orders": []}
            by_cust[cn]["orders_count"] += 1; by_cust[cn]["total_amount"] += o.total_amount or 0
            total_summary["orders_count"] += 1; total_summary["total_amount"] += o.total_amount or 0
            for it in (o.items or []):
                pn = pt_display(it.part_template)
                by_cust[cn]["total_parts"] += it.quantity; by_cust[cn]["completed_parts"] += it.completed_qty
                total_summary["total_parts"] += it.quantity; total_summary["completed_parts"] += it.completed_qty
                if pn not in by_cust[cn]["details"]: by_cust[cn]["details"][pn] = {"qty": 0, "completed": 0}
                by_cust[cn]["details"][pn]["qty"] += it.quantity; by_cust[cn]["details"][pn]["completed"] += it.completed_qty
        for cn, data in by_cust.items():
            mat_used = db.query(Material.name, func.sum(WriteOff.quantity_sheets), func.sum(WriteOff.quantity_kg)
            ).join(WriteOff, WriteOff.material_id == Material.id
            ).join(Order, WriteOff.order_id == Order.id).filter(
                Order.customer_id == (db.query(Customer.id).filter(Customer.name == cn).scalar()),
                WriteOff.is_cancelled == False
            ).group_by(Material.name).all()
            data["materials_used"] = [{"material": m[0], "sheets": m[1] or 0, "kg": round(m[2] or 0, 2)} for m in mat_used]
            data["details"] = [{"part": k, **v} for k, v in data["details"].items()]
        return {"customers": list(by_cust.values()), "summary": total_summary}

    # ─── Logs ───────────────────────────────────────
    @app.get("/api/logs")
    def api_logs(limit: int = 300, action: str = "", user_id: int = 0, db: Session = Depends(db_dep)):
        q = db.query(AuditLog).options(joinedload(AuditLog.user))
        if action: q = q.filter(AuditLog.action.contains(action))
        if user_id: q = q.filter(AuditLog.user_id == user_id)
        return [{"id": l.id, "action": l.action,
                 "user": l.user.full_name if l.user else "Система",
                 "user_id": l.user_id,
                 "entity": l.entity_type, "entity_id": l.entity_id,
                 "details": l.details, "date": l.created_at.isoformat()}
                for l in q.order_by(AuditLog.created_at.desc()).limit(limit).all()]

    @app.get("/api/logs/actions")
    def api_log_actions(db: Session = Depends(db_dep)):
        return [a[0] for a in db.query(AuditLog.action).distinct().order_by(AuditLog.action).all()]

    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket):
        await wsmgr.connect(ws)
        try:
            while True: await ws.receive_text()
        except WebSocketDisconnect: wsmgr.disconnect(ws)

    @app.get("/", response_class=HTMLResponse)
    def index():
        return HTML_APP

    return app

# ═══════════════════════════════════════════════════════════════
#  HTML SPA
# ═══════════════════════════════════════════════════════════════

HTML_APP = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MetalWorks MES v5.6</title>
<style>
:root{--bg:#111827;--s1:#1f2937;--s2:#374151;--s3:#4b5563;--accent:#ef4444;--accent2:#dc2626;--text:#f9fafb;--text2:#9ca3af;--text3:#6b7280;--ok:#10b981;--warn:#f59e0b;--err:#ef4444;--info:#3b82f6;--r:8px;--shadow:0 1px 3px rgba(0,0,0,.3)}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;font-size:14px}
.light{--bg:#f3f4f6;--s1:#fff;--s2:#e5e7eb;--s3:#d1d5db;--text:#111827;--text2:#4b5563;--text3:#9ca3af;--shadow:0 1px 3px rgba(0,0,0,.1)}
.header{background:var(--s1);padding:10px 20px;display:flex;justify-content:space-between;align-items:center;border-bottom:2px solid var(--accent);position:sticky;top:0;z-index:100;box-shadow:var(--shadow)}
.header h1{font-size:1.15em;display:flex;align-items:center;gap:8px}
.header h1 span{color:var(--accent)}
.hdr-r{display:flex;gap:8px;align-items:center;font-size:.85em}
.ws-dot{width:8px;height:8px;border-radius:50%;background:var(--err);display:inline-block;margin-right:4px}
.ws-dot.on{background:var(--ok)}
.btn{padding:6px 14px;border-radius:var(--r);border:1px solid var(--s2);background:var(--s1);color:var(--text);cursor:pointer;font-size:.85em;font-weight:500;transition:all .15s;display:inline-flex;align-items:center;gap:4px}
.btn:hover{background:var(--s2)}.btn.primary{background:var(--accent);border-color:var(--accent);color:#fff}
.btn.primary:hover{background:var(--accent2)}.btn.ok{background:var(--ok);border-color:var(--ok);color:#fff}
.btn.sm{padding:4px 8px;font-size:.8em}.btn.warn{background:var(--warn);border-color:var(--warn);color:#111}
select.ctl,input.ctl{padding:6px 10px;border-radius:var(--r);border:1px solid var(--s2);background:var(--s1);color:var(--text);font-size:.85em}
.nav{background:var(--s1);display:flex;border-bottom:1px solid var(--s2);overflow-x:auto;flex-wrap:wrap}
.nav button{background:none;border:none;color:var(--text3);padding:10px 16px;cursor:pointer;font-size:.85em;border-bottom:3px solid transparent;white-space:nowrap;transition:all .15s}
.nav button.active{color:var(--accent);border-bottom-color:var(--accent);font-weight:600}
.nav button:hover{color:var(--text)}
main{padding:16px;max-width:1600px;margin:0 auto}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:20px}
.card{background:var(--s1);border-radius:var(--r);padding:14px;border-left:4px solid var(--accent);box-shadow:var(--shadow);cursor:pointer;transition:transform .1s}
.card:hover{transform:scale(1.02)}
.card.ok{border-left-color:var(--ok)}.card.warn{border-left-color:var(--warn)}.card.err{border-left-color:var(--err)}.card.info{border-left-color:var(--info)}
.card h4{font-size:.7em;color:var(--text3);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px}
.card .val{font-size:1.6em;font-weight:700}
table{width:100%;border-collapse:collapse;background:var(--s1);border-radius:var(--r);overflow:hidden;box-shadow:var(--shadow);margin-bottom:16px;font-size:.85em}
th{background:var(--s2);text-align:left;padding:8px 10px;font-size:.75em;text-transform:uppercase;color:var(--text2);letter-spacing:.5px;position:sticky;top:0}
td{padding:7px 10px;border-top:1px solid rgba(255,255,255,.04)}
tr:hover td{background:rgba(239,68,68,.04)}
.tbl-wrap{overflow-x:auto;max-height:65vh;overflow-y:auto;border-radius:var(--r)}
.badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.75em;font-weight:600}
.badge.b-ok{background:#10b98120;color:#10b981}.badge.b-warn{background:#f59e0b20;color:#f59e0b}
.badge.b-err{background:#ef444420;color:#ef4444}.badge.b-info{background:#3b82f620;color:#3b82f6}
.badge.b-gray{background:#6b728020;color:#6b7280}.badge.b-purple{background:#8b5cf620;color:#8b5cf6}
.toolbar{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;align-items:center}
.toolbar .spacer{flex:1}
.modal-bg{position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.6);display:none;z-index:200;justify-content:center;align-items:flex-start;padding-top:4vh;overflow-y:auto}
.modal-bg.show{display:flex}
.modal{background:var(--s1);border-radius:var(--r);padding:24px;width:95%;max-width:780px;max-height:88vh;overflow-y:auto;border:1px solid var(--s2);box-shadow:0 20px 60px rgba(0,0,0,.5)}
.modal h2{margin-bottom:16px;color:var(--accent);font-size:1.1em}
.modal .form-row{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px}
.modal .form-row.full{grid-template-columns:1fr}
.modal .form-row.triple{grid-template-columns:1fr 1fr 1fr}
.modal label{display:block;font-size:.8em;color:var(--text2);margin-bottom:3px;font-weight:500}
.modal input,.modal select,.modal textarea{width:100%;padding:8px 10px;border-radius:var(--r);border:1px solid var(--s2);background:var(--bg);color:var(--text);font-size:.9em}
.modal input:focus,.modal select:focus,.modal textarea:focus{border-color:var(--accent);outline:none}
.modal textarea{resize:vertical;min-height:60px}
.modal .actions{display:flex;gap:8px;justify-content:flex-end;margin-top:16px;padding-top:12px;border-top:1px solid var(--s2)}
.section-hdr{font-size:.9em;font-weight:600;color:var(--text2);margin:16px 0 8px;padding-bottom:4px;border-bottom:1px solid var(--s2)}
.info-box{background:var(--bg);border:1px solid var(--s2);border-radius:var(--r);padding:12px;margin-bottom:12px;font-size:.85em}
.low{color:var(--err);font-weight:700}
.overdue-row td{background:rgba(239,68,68,.08)!important}
.anomaly{background:rgba(239,68,68,.12)!important;border-left:3px solid var(--err)}
.cancelled-row td{opacity:.5;text-decoration:line-through}
#toast{position:fixed;bottom:20px;right:20px;z-index:300}
.toast{background:var(--s1);color:var(--text);padding:10px 20px;border-radius:var(--r);margin-top:8px;border-left:4px solid var(--accent);box-shadow:var(--shadow);font-size:.9em;animation:fadeUp .3s}
.toast.ok{border-left-color:var(--ok)}.toast.err{border-left-color:var(--err)}
@keyframes fadeUp{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
.check-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:6px;padding:8px 0}
.check-grid label{font-size:.85em;display:flex;align-items:center;gap:8px;padding:4px 8px;border-radius:4px;cursor:pointer;background:var(--bg);border:1px solid var(--s2)}
.check-grid label:hover{border-color:var(--accent)}
.check-grid input[type=checkbox]{accent-color:var(--accent);width:16px;height:16px;flex-shrink:0}
.sub-tabs{display:flex;gap:4px;margin-bottom:12px;flex-wrap:wrap}
.sub-tabs button{background:var(--bg);border:1px solid var(--s2);color:var(--text3);padding:6px 14px;border-radius:var(--r);cursor:pointer;font-size:.85em}
.sub-tabs button.active{background:var(--accent);border-color:var(--accent);color:#fff}
.filter-bar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:12px;padding:8px;background:var(--s1);border-radius:var(--r);border:1px solid var(--s2)}
.filter-bar label{font-size:.8em;color:var(--text2)}
.filter-bar select,.filter-bar input{padding:4px 8px;border-radius:4px;border:1px solid var(--s2);background:var(--bg);color:var(--text);font-size:.85em}
.mat-row{display:flex;gap:8px;align-items:end;margin-bottom:8px;padding:8px;background:var(--s1);border:1px solid var(--s2);border-radius:var(--r)}
.mat-row select,.mat-row input{padding:6px;border-radius:4px;border:1px solid var(--s2);background:var(--bg);color:var(--text);font-size:.85em}
.load-bar{height:20px;border-radius:4px;margin:2px 0;min-width:4px;display:inline-block;vertical-align:middle}
.load-100{background:var(--err)}.load-80{background:var(--warn)}.load-50{background:var(--info)}.load-0{background:var(--s2)}
.ss-wrap{position:relative;width:100%}
.ss-input{width:100%;padding:8px 30px 8px 10px;border-radius:var(--r);border:1px solid var(--s2);background:var(--bg);color:var(--text);font-size:.9em;cursor:pointer}
.ss-input:focus{border-color:var(--accent);outline:none}
.ss-arrow{position:absolute;right:10px;top:50%;transform:translateY(-50%);pointer-events:none;color:var(--text3);font-size:.7em}
.ss-drop{position:fixed;max-height:220px;overflow-y:auto;background:var(--s1);border:1px solid var(--s2);border-radius:0 0 var(--r) var(--r);z-index:250;display:none;box-shadow:0 8px 24px rgba(0,0,0,.4);min-width:250px}
.ss-drop.open{display:block}
.ss-search-bar{padding:6px;border-bottom:1px solid var(--s2);position:sticky;top:0;background:var(--s1);z-index:1}
.ss-search-bar input{width:100%;padding:6px 8px;border:1px solid var(--s2);border-radius:4px;background:var(--bg);color:var(--text);font-size:.85em}
.ss-search-bar input:focus{border-color:var(--accent);outline:none}
.ss-opt{padding:6px 10px;cursor:pointer;font-size:.85em;border-bottom:1px solid rgba(255,255,255,.03)}
.ss-opt:hover,.ss-opt.hl{background:var(--accent);color:#fff}
.ss-opt.selected{background:rgba(239,68,68,.15);font-weight:600}
.ss-empty{padding:8px 10px;color:var(--text3);font-size:.85em}
.stat-card{background:var(--bg);border:1px solid var(--s2);border-radius:var(--r);padding:12px;margin-bottom:8px}
.stat-card .stat-label{font-size:.75em;color:var(--text3);text-transform:uppercase}
.stat-card .stat-val{font-size:1.4em;font-weight:700;color:var(--accent)}
.cf-row{display:flex;gap:8px;align-items:end;margin-bottom:6px;padding:8px;background:var(--bg);border:1px solid var(--s2);border-radius:var(--r)}
.cf-row input,.cf-row select{padding:6px;border-radius:4px;border:1px solid var(--s2);background:var(--s1);color:var(--text);font-size:.85em}
</style>
</head>
<body>
<div id="loginScreen" style="display:flex;justify-content:center;align-items:center;min-height:100vh">
<div style="background:var(--s1);padding:32px;border-radius:var(--r);width:360px;box-shadow:var(--shadow)">
<h2 style="text-align:center;margin-bottom:20px">⚙ <span style="color:var(--accent)">MetalWorks</span> MES</h2>
<div style="margin-bottom:12px"><label style="font-size:.85em;color:var(--text2)">Логин</label><input id="loginUser" class="ctl" style="width:100%;padding:10px" value="admin"></div>
<div style="margin-bottom:16px"><label style="font-size:.85em;color:var(--text2)">Пароль</label><input id="loginPass" class="ctl" type="password" style="width:100%;padding:10px" value="admin"></div>
<button class="btn primary" style="width:100%;padding:12px;font-size:1em" onclick="doLogin()">Войти</button>
<div id="loginErr" style="color:var(--err);text-align:center;margin-top:8px;font-size:.85em"></div>
</div></div>
<div id="appShell" style="display:none">
<div class="header"><h1>⚙ <span>MetalWorks</span> MES</h1>
<div class="hdr-r"><span class="ws-dot" id="wsDot"></span><span id="userInfo"></span>
<select class="ctl" id="themeCtl" onchange="toggleTheme()"><option value="dark">🌙</option><option value="light">☀</option></select>
<button class="btn" onclick="refreshPage()">↻</button><button class="btn" onclick="doLogout()">Выход</button></div></div>
<div class="nav" id="mainNav"></div>
<main id="mainContent"></main></div>
<div id="toast"></div>
<div class="modal-bg" id="modalBg"><div class="modal" id="modal"></div></div>

<script>
let U=null,ws=null,curPage='dashboard';

// ═══ SearchSelect ═══
let ssCounter=0;var ssInstances={};
function SS(id,options,selected,placeholder,onChange){
  var uid='ss_'+id+'_'+(ssCounter++);
  ssInstances[uid]={options:options,selected:selected!=null?String(selected):'',onChange:onChange,id:id};
  var selOpt=options.find(function(o){return String(o.v)===String(selected)});
  var dispVal=selOpt?selOpt.t:'';
  return '<div class="ss-wrap" id="'+uid+'">'+
    '<input type="text" class="ss-input" id="'+uid+'_inp" value="'+dispVal.replace(/"/g,'&quot;')+'" placeholder="'+(placeholder||'— выберите —')+'" onfocus="ssOpen(\''+uid+'\')" readonly>'+
    '<span class="ss-arrow">▼</span><input type="hidden" id="'+id+'" value="'+(selected!=null?selected:'')+'">'+
    '<div class="ss-drop" id="'+uid+'_drop">'+
      '<div class="ss-search-bar"><input type="text" placeholder="🔍 Поиск..." id="'+uid+'_search" oninput="ssFilter(\''+uid+'\',this.value)" onkeydown="ssKey(event,\''+uid+'\')"></div>'+
      '<div id="'+uid+'_list"></div>'+
    '</div></div>';
}
function ssOpen(uid){
  document.querySelectorAll('.ss-drop.open').forEach(function(d){d.classList.remove('open')});
  var wrap=document.getElementById(uid);if(!wrap)return;
  var inp=document.getElementById(uid+'_inp');if(!inp)return;
  var drop=document.getElementById(uid+'_drop');
  var rect=inp.getBoundingClientRect();
  drop.style.left=rect.left+'px';
  drop.style.width=Math.max(rect.width,250)+'px';
  var spaceBelow=window.innerHeight-rect.bottom;
  if(spaceBelow<230&&rect.top>230){drop.style.top='';drop.style.bottom=(window.innerHeight-rect.top)+'px'}
  else{drop.style.top=rect.bottom+'px';drop.style.bottom=''}
  drop.classList.add('open');
  var si=document.getElementById(uid+'_search');si.value='';setTimeout(function(){si.focus()},50);ssFilter(uid,'');
}
function ssClose(uid){var d=document.getElementById(uid+'_drop');if(d)d.classList.remove('open')}
function ssFilter(uid,q){var inst=ssInstances[uid];if(!inst)return;var list=document.getElementById(uid+'_list');var ql=q.toLowerCase();
  var f=inst.options.filter(function(o){return o.t.toLowerCase().indexOf(ql)>=0});
  if(!f.length){list.innerHTML='<div class="ss-empty">Ничего не найдено</div>';return}
  list.innerHTML=f.map(function(o,i){return '<div class="ss-opt '+(String(o.v)===String(inst.selected)?'selected':'')+' '+(i===0?'hl':'')+'" data-v="'+String(o.v).replace(/"/g,'&quot;')+'" onclick="ssPick(\''+uid+'\',\''+String(o.v).replace(/'/g,"\\'")+'\')">'+(o.t||'—')+'</div>'}).join('');}
function ssKey(e,uid){var list=document.getElementById(uid+'_list');var opts=Array.from(list.querySelectorAll('.ss-opt'));var hi=opts.findIndex(function(o){return o.classList.contains('hl')});
  if(e.key==='ArrowDown'){e.preventDefault();if(hi<opts.length-1){opts.forEach(function(o){o.classList.remove('hl')});opts[hi+1].classList.add('hl');opts[hi+1].scrollIntoView({block:'nearest'})}}
  else if(e.key==='ArrowUp'){e.preventDefault();if(hi>0){opts.forEach(function(o){o.classList.remove('hl')});opts[hi-1].classList.add('hl');opts[hi-1].scrollIntoView({block:'nearest'})}}
  else if(e.key==='Enter'){e.preventDefault();if(hi>=0)ssPick(uid,opts[hi].dataset.v)}
  else if(e.key==='Escape')ssClose(uid)}
function ssPick(uid,val){var inst=ssInstances[uid];if(!inst)return;inst.selected=val;var opt=inst.options.find(function(o){return String(o.v)===String(val)});
  document.getElementById(uid+'_inp').value=opt?opt.t:'';document.getElementById(inst.id).value=val;ssClose(uid);if(inst.onChange)inst.onChange(val)}
function ssVal(id){return(document.getElementById(id)||{}).value||''}
document.addEventListener('click',function(e){if(!e.target.closest('.ss-wrap')&&!e.target.closest('.ss-drop'))document.querySelectorAll('.ss-drop.open').forEach(function(d){d.classList.remove('open')})});

// ═══ Utils ═══
function api(url,method,body){var o={method:method||'GET',headers:{'Content-Type':'application/json'}};if(body)o.body=JSON.stringify(body);
  return fetch(url,o).then(function(r){if(!r.ok)return r.json().catch(function(){return{}}).then(function(e){throw new Error(e.detail||r.statusText)});return r.json()})}
function apiUpload(url,fd){return fetch(url,{method:'POST',body:fd}).then(function(r){if(!r.ok)return r.json().catch(function(){return{}}).then(function(e){throw new Error(e.detail||r.statusText)});return r.json()})}
function hasPerm(c){return U&&(U.role==='admin'||U.permissions.indexOf(c)>=0)}
function toast(m,t){var d=document.createElement('div');d.className='toast '+(t||'');d.textContent=m;document.getElementById('toast').appendChild(d);setTimeout(function(){d.remove()},4000)}
function openModal(h){document.getElementById('modal').innerHTML=h;document.getElementById('modalBg').classList.add('show')}
function closeModal(){document.getElementById('modalBg').classList.remove('show')}
// Московское время: сервер уже отдаёт MSK, показываем как есть
function fmtD(iso){if(!iso)return'—';var d=new Date(iso);return d.toLocaleDateString('ru-RU',{timeZone:'Europe/Moscow'})}
function fmtDT(iso){if(!iso)return'—';var d=new Date(iso);return d.toLocaleString('ru-RU',{timeZone:'Europe/Moscow'})}
function fmtN(n){return n==null?'—':Number(n).toLocaleString('ru-RU')}
function fmtMoney(n){return n?Number(n).toLocaleString('ru-RU',{minimumFractionDigits:2})+'₽':'—'}
function fmtMinToH(m){if(!m&&m!==0)return'—';var h=Math.floor(m/60);var mn=m%60;return h+'ч '+mn+'м'}
function toggleTheme(){document.body.classList.toggle('light',document.getElementById('themeCtl').value==='light')}
function refreshPage(){loadPage(curPage)}
function statusBadge(s){var m={'Черновик':'b-gray','Новый':'b-info','Ожидает':'b-purple','В работе':'b-warn','Завершён':'b-ok','Отгружен':'b-ok','Отменён':'b-err','Приостановлен':'b-gray','Запланирована':'b-purple','Завершена':'b-ok','Частично':'b-warn','Пауза':'b-gray','Низкий':'b-gray','Обычный':'b-info','Высокий':'b-warn','Срочный':'b-err','Критический':'b-err'};return '<span class="badge '+(m[s]||'b-gray')+'">'+s+'</span>'}
function esc(s){return(s||'').replace(/'/g,"\\'").replace(/"/g,'&quot;')}
var STATUSES=['Черновик','Новый','Ожидает','В работе','Завершён','Отгружен','Отменён','Приостановлен'];
var PRIORITIES=['Низкий','Обычный','Высокий','Срочный','Критический'];

// ═══ Auth ═══
function doLogin(){api('/api/auth/login','POST',{username:document.getElementById('loginUser').value,password:document.getElementById('loginPass').value}).then(function(u){
  U=u;document.getElementById('loginScreen').style.display='none';document.getElementById('appShell').style.display='block';
  document.getElementById('userInfo').textContent=U.full_name+' ('+U.role_label+')';buildNav();connectWS();loadPage('dashboard')}).catch(function(e){document.getElementById('loginErr').textContent=e.message})}
function doLogout(){U=null;location.reload()}
document.getElementById('loginPass').addEventListener('keydown',function(e){if(e.key==='Enter')doLogin()});
function connectWS(){try{var p=location.protocol==='https:'?'wss':'ws';ws=new WebSocket(p+'://'+location.host+'/ws');
  ws.onopen=function(){document.getElementById('wsDot').classList.add('on')};ws.onclose=function(){document.getElementById('wsDot').classList.remove('on');setTimeout(connectWS,5000)};ws.onerror=function(){}}catch(e){}}

var PAGES=[{id:'dashboard',icon:'📊',label:'Панель',perm:null},{id:'orders',icon:'📋',label:'Заказы',perm:'order.view'},
  {id:'parts_db',icon:'🔩',label:'Детали: БД',perm:'parts.view'},{id:'warehouse',icon:'📦',label:'Склад',perm:'mat.view'},
  {id:'operations',icon:'🔧',label:'Операции',perm:'op.view'},{id:'reservations',icon:'🔒',label:'Резервы',perm:'reserve.view'},
  {id:'parts_log',icon:'📝',label:'Учёт деталей',perm:'parts.log'},{id:'writeoffs',icon:'📤',label:'Списания',perm:'writeoff.material'},
  {id:'load',icon:'📈',label:'Загруженность',perm:'load.view'},{id:'customers',icon:'🏢',label:'Клиенты',perm:'cust.view'},
  {id:'resources',icon:'🏭',label:'Ресурсы',perm:'res.view'},{id:'logs',icon:'📜',label:'Логи',perm:'admin.logs'},
  {id:'settings',icon:'⚙',label:'Настройки',perm:'admin.users'}];
function buildNav(){document.getElementById('mainNav').innerHTML=PAGES.filter(function(p){return!p.perm||hasPerm(p.perm)}).map(function(p){
  return '<button data-p="'+p.id+'" onclick="navTo(\''+p.id+'\')" class="'+(p.id===curPage?'active':'')+'">'+p.icon+' '+p.label+'</button>'}).join('')}
function navTo(p){curPage=p;document.querySelectorAll('.nav button').forEach(function(b){b.classList.toggle('active',b.dataset.p===p)});loadPage(p)}
function loadPage(p){var c=document.getElementById('mainContent');try{switch(p){
  case'dashboard':pgDashboard(c);break;case'orders':pgOrders(c);break;case'parts_db':pgPartsDB(c);break;
  case'warehouse':pgWarehouse(c);break;case'operations':pgOperations(c);break;case'reservations':pgReservations(c);break;
  case'parts_log':pgPartsLog(c);break;case'writeoffs':pgWriteoffs(c);break;case'load':pgLoad(c);break;
  case'customers':pgCustomers(c);break;case'resources':pgResources(c);break;case'logs':pgLogs(c);break;
  case'settings':pgSettings(c);break;default:c.innerHTML='<p>Не найдено</p>'}}catch(e){c.innerHTML='<p style="color:var(--err)">Ошибка: '+e.message+'</p>';console.error(e)}}

// ═══ ПАНЕЛЬ ═══
function pgDashboard(c){
  Promise.all([api('/api/analytics/dashboard'),api('/api/analytics/operations')]).then(function(arr){var d=arr[0],ops=arr[1];
  var widgets=[{k:'orders_total',lbl:'Заказы всего',cls:'info',v:d.orders_total},{k:'orders_active',lbl:'Активные',cls:'ok',v:d.orders_active},
    {k:'orders_completed',lbl:'Выполнено',cls:'ok',v:d.orders_completed},{k:'orders_overdue',lbl:'Просрочено',cls:d.orders_overdue?'err':'',v:d.orders_overdue},
    {k:'ops_pending',lbl:'Ожидает операций',cls:'warn',v:d.ops_pending},{k:'ops_in_progress',lbl:'В работе',cls:'',v:d.ops_in_progress},
    {k:'ops_completed_today',lbl:'Завершено сегодня',cls:'ok',v:d.ops_completed_today},{k:'parts_today',lbl:'Деталей сегодня',cls:'ok',v:d.parts_today},
    {k:'rejected_today',lbl:'Брак сегодня',cls:d.rejected_today?'err':'',v:d.rejected_today},{k:'low_stock',lbl:'Мало на складе',cls:d.low_stock?'err':'ok',v:d.low_stock}];
  c.innerHTML='<div class="cards">'+widgets.map(function(w){return '<div class="card '+w.cls+'" onclick="dashDetail(\''+w.k+'\')"><h4>'+w.lbl+'</h4><div class="val">'+w.v+'</div></div>'}).join('')+'</div>'+
  '<div class="section-hdr">Статистика по операциям</div><div class="tbl-wrap"><table><thead><tr><th>Тип</th><th>Всего</th><th>Выполнено</th><th>Ср. время</th></tr></thead>'+
  '<tbody>'+ops.map(function(o){return '<tr><td>'+o.type+'</td><td>'+o.total+'</td><td>'+o.completed+'</td><td>'+fmtMinToH(Math.round(o.avg_min))+'</td></tr>'}).join('')+'</tbody></table></div>'})}
function dashDetail(w){api('/api/analytics/dashboard/detail/'+w).then(function(data){if(!data.length){toast('Нет данных');return}
  var keys=Object.keys(data[0]);var h='<h2>📊 Детализация</h2><div class="tbl-wrap"><table><thead><tr>';keys.forEach(function(k){h+='<th>'+k+'</th>'});
  h+='</tr></thead><tbody>';data.forEach(function(r){var cls=r.overdue?'class="overdue-row"':'';h+='<tr '+cls+'>';keys.forEach(function(k){var v=r[k];if(v===true)v='⚠';if(v===false)v='';h+='<td>'+(v||'—')+'</td>'});h+='</tr>'});
  h+='</tbody></table></div><div class="actions"><button class="btn" onclick="closeModal()">Закрыть</button></div>';openModal(h)}).catch(function(e){toast(e.message,'err')})}

// ═══ ЗАКАЗЫ ═══
function pgOrders(c){api('/api/orders').then(function(orders){
  c.innerHTML='<div class="toolbar">'+(hasPerm('order.create')?'<button class="btn primary" onclick="modalOrder()">+ Новый заказ</button>':'')+
    (hasPerm('order.reports')?'<button class="btn" style="background:var(--info);border-color:var(--info);color:#fff" onclick="modalReports()">📊 Отчёты</button>':'')+'</div>'+
  '<div class="tbl-wrap"><table><thead><tr><th>№</th><th>Клиент</th><th>Описание</th><th>Сумма</th><th>Поз.</th><th>Приор.</th><th>Статус</th><th>Дедлайн</th><th>Заверш.</th><th>📎</th><th></th></tr></thead>'+
  '<tbody>'+orders.map(function(o){return '<tr '+(o.overdue?'class="overdue-row"':'')+'>'+
    '<td><strong>'+o.number+'</strong></td><td>'+o.customer+'</td><td title="'+esc(o.description)+'">'+(o.description||'').substring(0,35)+'</td>'+
    '<td>'+fmtMoney(o.total_amount)+'</td><td>'+(o.items||[]).length+'</td><td>'+statusBadge(o.priority)+'</td><td>'+statusBadge(o.status)+'</td>'+
    '<td '+(o.overdue?'class="low"':'')+'>'+fmtD(o.deadline)+(o.overdue?' ⚠':'')+'</td>'+
    '<td>'+fmtD(o.completed_at)+'</td>'+
    '<td>'+((o.files||[]).length?'📎'+(o.files||[]).length:'—')+'</td>'+
    '<td style="white-space:nowrap">'+
      '<button class="btn sm" onclick="modalOrderDetail('+o.id+')">📋</button>'+
      '<button class="btn sm" onclick="modalOrderStats('+o.id+')" title="Статистика">📈</button>'+
      (hasPerm('order.edit')?'<button class="btn sm" onclick="modalOrder('+o.id+')">✏</button>':'')+
      (hasPerm('order.delete')?'<button class="btn sm" onclick="delOrder('+o.id+',\''+esc(o.number)+'\')" style="color:var(--err)" title="Удалить заказ">🗑</button>':'')+
      (hasPerm('order.status')?'<select class="ctl" style="padding:3px;font-size:.8em" onchange="chgStatus('+o.id+',this.value)">'+STATUSES.map(function(s){return '<option '+(s===o.status?'selected':'')+'>'+s+'</option>'}).join('')+'</select>':'')+
    '</td></tr>'}).join('')+'</tbody></table></div>'})}
function chgStatus(oid,s){
  api('/api/orders/'+oid+'/status','POST',{status:s,user_id:U.id}).then(function(r){
    if(r.status==='warning'){if(confirm(r.message)){api('/api/orders/'+oid+'/status','POST',{status:s,user_id:U.id,force:true}).then(function(){toast('Обновлён','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}else{refreshPage()}}
    else{toast('Обновлён','ok');refreshPage()}
  }).catch(function(e){toast(e.message,'err')})}
  
  function delOrder(oid,num){if(!confirm('Удалить заказ '+num+'?\n\nВсе позиции, операции, резервы и файлы будут удалены.\nМатериалы из резервов вернутся на склад.'))return;
  api('/api/orders/delete','POST',{id:oid,user_id:U.id}).then(function(){toast('Заказ '+num+' удалён','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}

function modalOrder(oid){
  api('/api/customers').then(function(custs){
    var p1=oid?api('/api/orders'):Promise.resolve(null);
    p1.then(function(os){var o=os?os.find(function(x){return x.id===oid}):null;
    var custOpts=[{v:'',t:'— не выбран —'}].concat(custs.map(function(c){return{v:String(c.id),t:c.name}}));
    var prioOpts=PRIORITIES.map(function(p){return{v:p,t:p}});
    openModal('<h2>'+(o?'✏':'+')+' заказ</h2>'+
    '<div class="form-row"><div><label>Клиент</label>'+SS('f_cust',custOpts,o?String(o.customer_id||''):'','Клиент')+'</div>'+
      '<div><label>Приоритет</label>'+SS('f_prio',prioOpts,o?o.priority:'Обычный','Приоритет')+'</div></div>'+
    '<div class="form-row"><div><label>Дедлайн</label><input type="date" id="f_dl" value="'+(o&&o.deadline?o.deadline.split('T')[0]:'')+'"></div>'+
      '<div><label>Сумма (₽)</label><input type="number" id="f_amount" step="0.01" value="'+(o?o.total_amount||'':'')+'"></div></div>'+
    '<div class="form-row full"><div><label>Описание</label><textarea id="f_desc" rows="2">'+(o?o.description:'')+'</textarea></div></div>'+
    '<div class="form-row full"><div><label>Примечания</label><textarea id="f_notes" rows="2">'+(o?o.notes||'':'')+'</textarea></div></div>'+
    '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="saveOrder('+(oid||0)+')">Сохранить</button></div>')})})}
function saveOrder(oid){var dl=document.getElementById('f_dl').value;
  var b={user_id:U.id,customer_id:+ssVal('f_cust')||null,priority:ssVal('f_prio')||'Обычный',
    description:document.getElementById('f_desc').value,notes:document.getElementById('f_notes').value,
    total_amount:+document.getElementById('f_amount').value||0,deadline:dl?dl+'T23:59:00':null};if(oid)b.id=oid;
  api('/api/orders/save','POST',b).then(function(){closeModal();toast('Сохранено','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}

function modalOrderStats(oid){
  api('/api/orders/'+oid+'/stats').then(function(st){
  var h='<h2>📈 Статистика: '+st.order_number+'</h2>'+
  '<div class="info-box"><strong>'+st.customer+'</strong> | '+statusBadge(st.status)+
    (st.first_start?' | Начало: '+fmtDT(st.first_start):'')+
    (st.order_completed?' | Завершён: '+fmtDT(st.order_completed):'')+'</div>'+
  '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:8px;margin-bottom:16px">'+
    '<div class="stat-card"><div class="stat-label">Общее время</div><div class="stat-val">'+st.total_elapsed_hours+' ч</div><div style="font-size:.8em;color:var(--text3)">'+st.total_elapsed_shifts+' смен</div></div></div>'+
  '<div class="section-hdr">По участкам</div>'+
  '<div class="tbl-wrap"><table><thead><tr><th>Участок</th><th>Работа (ч)</th><th>Смен</th><th>План (ч)</th><th>Смена</th><th>Операций</th></tr></thead>'+
  '<tbody>'+st.resources.map(function(r){return '<tr><td><strong>'+r.resource+'</strong></td><td>'+r.work_hours+'</td><td>'+r.work_shifts+'</td><td>'+r.estimated_hours+'</td><td>'+r.shift_hours+'ч</td><td>'+r.completed_ops+'/'+r.total_ops+'</td></tr>'}).join('')+'</tbody></table></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Закрыть</button></div>';
  openModal(h)}).catch(function(e){toast(e.message,'err')})}

// ═══ Детализация заказа ═══
function modalOrderDetail(oid){
  Promise.all([api('/api/orders'),api('/api/operations?order_id='+oid),api('/api/resources')]).then(function(arr){
  var orders=arr[0],ops=arr[1],resources=arr[2];
  var o=orders.find(function(x){return x.id===oid});if(!o)return;
  var resOpts=[{v:'',t:'— не назначен —'}].concat(resources.map(function(r){return{v:String(r.id),t:r.name}}));
  window._orderResources=resources;
  openModal('<h2>📋 '+o.number+' — '+o.customer+'</h2>'+
  '<div class="info-box">'+statusBadge(o.priority)+' '+statusBadge(o.status)+(o.overdue?' <span class="low">⚠ ПРОСРОЧЕН</span>':'')+' | Дедлайн: '+fmtD(o.deadline)+' | Сумма: '+fmtMoney(o.total_amount)+
    (o.completed_at?' | <strong>Завершён:</strong> '+fmtDT(o.completed_at):'')+
    '<br>'+(o.description||'')+'</div>'+
  '<div class="section-hdr">Позиции '+(hasPerm('order.edit')?'<button class="btn sm" onclick="modalAddItem('+oid+','+(o.customer_id||0)+')">+ Добавить</button>':'')+'</div>'+
  '<table><thead><tr><th>Деталь</th><th>Кол-во</th><th>Готово</th><th>Изл.</th><th>Материалы</th><th></th></tr></thead>'+
  '<tbody>'+(o.items||[]).map(function(it){return '<tr><td><strong>'+it.part_name+'</strong></td><td>'+it.quantity+'</td><td>'+it.completed+'/'+it.quantity+'</td>'+
    '<td class="'+(it.surplus>0?'low':'')+'">'+(it.surplus>0?'+'+it.surplus:'—')+'</td>'+
    '<td style="font-size:.8em">'+((it.materials||[]).map(function(m){return m.code+': '+m.sheets_needed+'л'}).join('<br>')||'—')+'</td>'+
    '<td>'+(hasPerm('order.edit')?'<button class="btn sm" onclick="modalEditItem('+oid+','+it.id+')">✏</button><button class="btn sm" onclick="delItem('+it.id+','+oid+')">🗑</button>':'')+'</td></tr>'}).join('')+'</tbody></table>'+
  '<div class="section-hdr">Операции</div>'+
  '<table><thead><tr><th>#</th><th>Деталь</th><th>Тип</th><th>Ресурс</th><th>План</th><th>Время</th><th>Статус</th><th></th></tr></thead>'+
  '<tbody>'+ops.map(function(op){return '<tr><td>'+op.sequence+'</td><td>'+(op.item||'—')+'</td><td>'+op.type+'</td>'+
    '<td><div style="min-width:160px">'+SS('op_res_'+op.id,resOpts,String(op.resource_id||''),'Ресурс...',function(v){updateOpRes(op.id,v)})+'</div></td>'+
    '<td>'+op.planned_qty+'</td>'+
    '<td>'+fmtMinToH(op.estimated_min)+'</td>'+
    '<td>'+statusBadge(op.status)+'</td>'+
    '<td>'+(hasPerm('op.create')?'<button class="btn sm" onclick="delOp('+op.id+','+oid+')">🗑</button>':'')+'</td></tr>'}).join('')+'</tbody></table>'+
  '<div class="section-hdr">Файлы '+(hasPerm('order.files')?'<button class="btn sm" onclick="modalUpload('+oid+')">📎</button>':'')+'</div>'+
  '<div>'+((o.files||[]).map(function(f){return '<div style="display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid var(--s2);font-size:.85em">'+
    '<a href="/api/files/'+f.id+'/download" target="_blank" style="color:var(--info)">📄 '+f.name+'</a>'+
    (hasPerm('order.files')?'<button class="btn sm" onclick="delFile('+f.id+','+oid+')">🗑</button>':'')+'</div>'}).join('')||'<div style="color:var(--text3);padding:8px">Нет файлов</div>')+'</div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Закрыть</button></div>')})}

function updateOpRes(opid,val){api('/api/operations/save','POST',{id:opid,resource_id:+val||null}).then(function(){toast('Ресурс назначен','ok')}).catch(function(e){toast(e.message,'err')})}

function modalAddItem(oid,custId){
  var url='/api/part-templates'+(custId?'?customer_id='+custId:'');
  api(url).then(function(pts){
    var p2=custId?api('/api/part-templates'):Promise.resolve(pts);
    p2.then(function(allPts){
    var useList=pts.length?pts:allPts;
    if(!useList.length){toast('Нет деталей','err');return}
    var ptOpts=useList.map(function(p){return{v:String(p.id),t:p.display_name+' ['+p.customer_name+']'}});
    openModal('<h2>+ Добавить деталь</h2>'+
    '<div class="form-row"><div><label>Деталь</label>'+SS('fi_pt',ptOpts,'','Поиск детали...')+'</div>'+
      '<div><label>Количество</label><input type="number" id="fi_qty" value="1" min="1"></div></div>'+
    '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="saveItem('+oid+')">Добавить</button></div>')})})}
function saveItem(oid){api('/api/order-items/save','POST',{order_id:oid,part_template_id:+ssVal('fi_pt'),quantity:+document.getElementById('fi_qty').value,user_id:U.id}).then(function(r){
  if(r.unassigned_ops>0){toast('⚠ '+r.unassigned_ops+' операций без ресурса — назначьте вручную','err')}
  closeModal();toast('Добавлено','ok');modalOrderDetail(oid)}).catch(function(e){toast(e.message,'err')})}
function modalEditItem(oid,iid){api('/api/orders').then(function(os){var o=os.find(function(x){return x.id===oid});var it=(o.items||[]).find(function(x){return x.id===iid});if(!it)return;
  openModal('<h2>✏ '+it.part_name+'</h2><div class="form-row"><div><label>Количество</label><input type="number" id="fei_qty" value="'+it.quantity+'" min="1"></div><div></div></div>'+
  '<div class="info-box">Резервы и операции пересоздадутся</div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="updateItem('+oid+','+iid+')">Сохранить</button></div>')})}
function updateItem(oid,iid){api('/api/order-items/save','POST',{id:iid,quantity:+document.getElementById('fei_qty').value,user_id:U.id}).then(function(){closeModal();toast('OK','ok');modalOrderDetail(oid)}).catch(function(e){toast(e.message,'err')})}
function delItem(iid,oid){if(!confirm('Удалить?'))return;api('/api/order-items/delete','POST',{id:iid}).then(function(){modalOrderDetail(oid)})}
function delOp(opid,oid){if(!confirm('Удалить?'))return;api('/api/operations/delete','POST',{id:opid}).then(function(){modalOrderDetail(oid)})}
function modalUpload(oid){openModal('<h2>📎 Загрузка</h2><div class="form-row full"><div><label>Файл</label><input type="file" id="fu_file" style="padding:8px"></div></div>'+
  '<div class="form-row"><div><label>Тип</label><select id="fu_type"><option>Чертёж</option><option>3D Модель</option><option>УП (NC)</option><option>Фото</option><option>Спецификация</option><option>Прочее</option></select></div>'+
    '<div><label>Описание</label><input id="fu_desc"></div></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="doUpload('+oid+')">Загрузить</button></div>')}
function doUpload(oid){var f=document.getElementById('fu_file').files[0];if(!f){toast('Файл','err');return}
  var fd=new FormData();fd.append('file',f);fd.append('file_type',document.getElementById('fu_type').value);fd.append('description',document.getElementById('fu_desc').value);fd.append('user_id',U.id);
  apiUpload('/api/orders/'+oid+'/upload',fd).then(function(){closeModal();toast('Загружено','ok');modalOrderDetail(oid)}).catch(function(e){toast(e.message,'err')})}
function delFile(fid,oid){if(!confirm('Удалить?'))return;api('/api/files/delete','POST',{id:fid}).then(function(){modalOrderDetail(oid)})}

function modalReports(){api('/api/customers').then(function(custs){
  var custOpts=[{v:'0',t:'Все'}].concat(custs.map(function(c){return{v:String(c.id),t:c.name}}));
  openModal('<h2>📊 Отчёты</h2>'+
  '<div class="form-row triple"><div><label>С</label><input type="date" id="rpt_from"></div><div><label>По</label><input type="date" id="rpt_to"></div>'+
    '<div><label>Клиент</label>'+SS('rpt_cust',custOpts,'0','Все')+'</div></div>'+
  '<div class="actions" style="justify-content:flex-start"><button class="btn primary" onclick="loadReport()">Сформировать</button></div>'+
  '<div id="rptResult"></div><div class="actions"><button class="btn" onclick="closeModal()">Закрыть</button></div>')})}
function loadReport(){var f=document.getElementById('rpt_from').value;var t=document.getElementById('rpt_to').value;var cid=+ssVal('rpt_cust');
  var url='/api/reports/customers?';if(f)url+='date_from='+f+'&';if(t)url+='date_to='+t+'&';if(cid)url+='customer_id='+cid+'&';
  api(url).then(function(resp){var data=resp.customers||[];var summary=resp.summary||{};var h='';
  if(!data.length)h='<div class="info-box">Нет данных</div>';
  else{
    h+='<div class="section-hdr">📊 Итого за период</div><div class="info-box"><strong>Заказов:</strong> '+summary.orders_count+' | <strong>Сумма:</strong> '+fmtMoney(summary.total_amount)+' | <strong>План деталей:</strong> '+summary.total_parts+' | <strong>Готово:</strong> '+summary.completed_parts+'</div>';
    data.forEach(function(cd){h+='<div class="section-hdr">'+cd.customer+'</div>'+
    '<div class="info-box"><strong>Заказов:</strong> '+cd.orders_count+' | <strong>Сумма:</strong> '+fmtMoney(cd.total_amount)+' | <strong>План:</strong> '+cd.total_parts+' | <strong>Готово:</strong> '+cd.completed_parts+'</div>'+
    '<table><thead><tr><th>Деталь</th><th>Кол-во</th><th>Готово</th></tr></thead><tbody>'+(cd.details||[]).map(function(d){return '<tr><td>'+d.part+'</td><td>'+d.qty+'</td><td>'+d.completed+'</td></tr>'}).join('')+'</tbody></table>'+
    ((cd.materials_used||[]).length?'<table><thead><tr><th>Материал</th><th>Листов</th><th>Кг</th></tr></thead><tbody>'+cd.materials_used.map(function(m){return '<tr><td>'+m.material+'</td><td>'+m.sheets+'</td><td>'+fmtN(m.kg)+'</td></tr>'}).join('')+'</tbody></table>':'')})}
  document.getElementById('rptResult').innerHTML=h}).catch(function(e){toast(e.message,'err')})}

// ═══ ДЕТАЛИ БД ═══
var ptSearch='',ptSearchTimer=null;
function pgPartsDB(c){
  api('/api/part-templates?search='+encodeURIComponent(ptSearch)).then(function(pts){
  c.innerHTML='<div class="toolbar">'+(hasPerm('parts.create')?'<button class="btn primary" onclick="modalPartTpl()">+ Новая деталь</button>':'')+
    '<span class="spacer"></span>'+
    '<input class="ctl" id="ptSearchInput" style="width:280px" placeholder="🔍 Поиск..." value="'+esc(ptSearch)+'"></div>'+
  '<div class="tbl-wrap"><table><thead><tr><th>Наименование</th><th>Чертёж</th><th>Заказчик</th><th>Материалы</th><th>Операции</th><th>📎</th><th></th></tr></thead>'+
  '<tbody>'+pts.map(function(p){
    var mats=(p.materials||[]).map(function(m){return m.material_code+': '+m.sheets_input+'л→'+m.parts_per_sheets+'шт'}).join('<br>')||'—';
    var opT=p.operation_times||{};var opStr=Object.entries(opT).map(function(e){return e[0]+': '+(typeof e[1]==='object'?fmtMinToH(Math.round(e[1].per_one)):e[1])}).join(', ')||'—';
    var filesHtml='';
    if((p.files||[]).length&&hasPerm('parts.files')){
      filesHtml='<button class="btn sm" onclick="modalPTFiles('+p.id+',\''+esc(p.display_name)+'\')">📎'+p.files.length+'</button>';
    } else if((p.files||[]).length){filesHtml='📎'+p.files.length}else{filesHtml='—'}
    return '<tr><td><strong>'+p.display_name+'</strong></td><td>'+(p.part_number||'—')+'</td><td>'+p.customer_name+'</td>'+
    '<td style="font-size:.8em">'+mats+'</td><td style="font-size:.8em">'+opStr+'</td>'+
    '<td>'+filesHtml+'</td>'+
    '<td>'+(hasPerm('parts.edit')?'<button class="btn sm" onclick="modalPartTpl('+p.id+')">✏</button><button class="btn sm" onclick="delPT('+p.id+')">🗑</button>':'')+'</td></tr>'}).join('')+'</tbody></table></div>';
  // Attach input event after render
  var inp=document.getElementById('ptSearchInput');
  if(inp){inp.addEventListener('input',function(){ptSearch=this.value;clearTimeout(ptSearchTimer);ptSearchTimer=setTimeout(function(){pgPartsDB(document.getElementById('mainContent'))},400)});
    // restore cursor
    inp.focus();inp.setSelectionRange(inp.value.length,inp.value.length)}
  })}

// Модальное окно просмотра файлов детали (без редактирования)
function modalPTFiles(ptid,name){
  api('/api/part-templates').then(function(pts){
  var p=pts.find(function(x){return x.id===ptid});if(!p)return;
  var h='<h2>📎 Файлы: '+name+'</h2>';
  if(!(p.files||[]).length){h+='<div class="info-box">Нет файлов</div>'}
  else{h+='<div>'+(p.files||[]).map(function(f){return '<div style="display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid var(--s2);font-size:.9em">'+
    '<div><a href="/api/part-template-files/'+f.id+'/download" target="_blank" style="color:var(--info)">📄 '+f.name+'</a><span style="color:var(--text3);margin-left:8px;font-size:.8em">'+f.type+'</span></div>'+
    '<span style="color:var(--text3);font-size:.8em">'+fmtDT(f.date)+'</span></div>'}).join('')+'</div>'}
  h+='<div class="actions"><button class="btn" onclick="closeModal()">Закрыть</button></div>';openModal(h)})}

var ptMaterials=[],ptComponents=[];
function modalPartTpl(pid){
  Promise.all([api('/api/customers'),api('/api/materials'),api('/api/op-types'),api('/api/part-templates')]).then(function(arr){
  var custs=arr[0],mats=arr[1],opTypes=arr[2],allPts=arr[3];
  var p=pid?allPts.find(function(x){return x.id===pid}):null;
  ptMaterials=p?(p.materials||[]).map(function(m){return{material_id:m.material_id_val||m.material_id,sheets_input:m.sheets_input,parts_per_sheets:m.parts_per_sheets}}):[];
  ptComponents=p?(p.components||[]).map(function(c){return{component_id:c.component_id,component_name:c.component_name,quantity:c.quantity}}):[];
  window._allPTs=allPts;window._allMats=mats;
  var opT=p?p.operation_times:{};var activeOps=opTypes.filter(function(o){return o.is_active});
  var custOpts=[{v:'',t:'— не привязан —'}].concat(custs.map(function(c){return{v:String(c.id),t:c.name}}));
  var h='<h2>'+(p?'✏':'+')+' Деталь</h2>'+
  '<div class="form-row"><div><label>Наименование</label><input id="fp_name" value="'+(p?p.name:'')+'"></div>'+
    '<div><label>Чертёжный номер</label><input id="fp_num" value="'+(p?p.part_number:'')+'"></div></div>'+
  '<div class="form-row"><div><label>Заказчик</label>'+SS('fp_cust',custOpts,p?String(p.customer_id||''):'','Заказчик')+'</div><div></div></div>'+
'<div style="margin-bottom:12px;padding:8px;background:var(--bg);border:1px solid var(--s2);border-radius:var(--r)"><label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:.9em"><input type="checkbox" id="fp_is_asm" '+(p&&p.is_assembly?'checked':'')+' onchange="toggleAsmUI()" style="width:18px;height:18px;accent-color:var(--accent)"> Это сборка (состоит из нескольких деталей)</label></div>'+
  '<div id="fp_asm_section" style="display:'+(p&&p.is_assembly?'block':'none')+'">'+
    '<div class="section-hdr">Компоненты сборки <button class="btn sm" onclick="addPTComp()">+</button></div><div id="fp_comps_list"></div></div>'+
  '<div class="section-hdr">Материалы <button class="btn sm" onclick="addPTMat()">+</button></div><div id="fp_mats_list"></div>'+
  '<div class="section-hdr">Калькулятор операций</div><div class="info-box">Партия + общее время → авто мин/шт</div>';
  activeOps.forEach(function(ot){var entry=opT[ot.name];var qty=entry&&typeof entry==='object'?entry.qty||1:1;
    var totalMin=entry&&typeof entry==='object'?entry.total_min||'':'';var perOne=entry&&typeof entry==='object'?entry.per_one||'':'';
    h+='<div style="background:var(--bg);border:1px solid var(--s2);border-radius:var(--r);padding:8px;margin-bottom:6px">'+
      '<div style="font-weight:600;font-size:.85em;margin-bottom:4px;color:var(--accent)">'+ot.name+'</div>'+
      '<div class="form-row triple" style="margin:0">'+
        '<div><label>Партия</label><input type="number" class="fp_op_qty" data-op="'+ot.name+'" value="'+qty+'" min="1" oninput="calcOneOp(\''+ot.name+'\')"></div>'+
        '<div><label>Общее (мин)</label><input type="number" class="fp_op_total" data-op="'+ot.name+'" value="'+totalMin+'" min="0" step="0.1" oninput="calcOneOp(\''+ot.name+'\')"></div>'+
        '<div><label>= На 1 шт</label><input class="fp_op_one" data-op="'+ot.name+'" value="'+perOne+'" disabled style="font-weight:700;color:var(--ok)"></div></div></div>'});
  h+='<div class="section-hdr">Файлы '+(p?'<button class="btn sm" onclick="modalPTUpload('+p.id+')">📎 Загрузить</button>':'')+'</div>'+
  '<div id="fp_files">'+(p?(p.files||[]).map(function(f){return '<div style="display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid var(--s2);font-size:.85em">'+
    '<a href="/api/part-template-files/'+f.id+'/download" target="_blank" style="color:var(--info)">📄 '+f.name+'</a>'+
    '<button class="btn sm" onclick="delPTFile('+f.id+','+p.id+')">🗑</button></div>'}).join('')||'<div style="color:var(--text3)">Нет файлов</div>':'<div class="info-box">Сохраните деталь, чтобы загрузить файлы</div>')+'</div>'+
  '<div class="form-row full"><div><label>Описание</label><textarea id="fp_desc" rows="2">'+(p?p.description:'')+'</textarea></div></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="savePT('+(pid||0)+')">Сохранить</button></div>';
  openModal(h);renderPTMats(mats);renderPTComps();activeOps.forEach(function(ot){calcOneOp(ot.name)})})}

function calcOneOp(n){var q=document.querySelector('.fp_op_qty[data-op="'+n+'"]');var t=document.querySelector('.fp_op_total[data-op="'+n+'"]');var o=document.querySelector('.fp_op_one[data-op="'+n+'"]');
  if(!q||!t||!o)return;var qty=+q.value||1;var tot=+t.value||0;o.value=qty>0?(tot/qty).toFixed(2):''}

function renderPTMats(allMats){var el=document.getElementById('fp_mats_list');if(!el)return;
  var matOpts=allMats.map(function(m){return{v:String(m.id),t:m.code+' — '+m.name}});
  el.innerHTML=ptMaterials.map(function(m,i){
    var ssHtml=SS('ptm_'+i,matOpts,String(m.material_id),'Материал...');
    return '<div class="mat-row">'+
    '<div style="flex:1"><label>Материал</label>'+ssHtml+'</div>'+
    '<div><label>Листов</label><input type="number" value="'+m.sheets_input+'" min="1" style="width:60px" onchange="ptMaterials['+i+'].sheets_input=+this.value"></div>'+
    '<div><label>Штук</label><input type="number" value="'+m.parts_per_sheets+'" min="1" style="width:60px" onchange="ptMaterials['+i+'].parts_per_sheets=+this.value"></div>'+
    '<button class="btn sm" onclick="ptMaterials.splice('+i+',1);renderPTMats(window._allMats)">🗑</button></div>'
  }).join('');window._allMats=allMats}
function addPTMat(){if(!window._allMats||!window._allMats.length){toast('Нет материалов','err');return}
  ptMaterials.push({material_id:window._allMats[0].id,sheets_input:1,parts_per_sheets:1});renderPTMats(window._allMats)}

function toggleAsmUI(){var ch=document.getElementById('fp_is_asm');
  document.getElementById('fp_asm_section').style.display=ch.checked?'block':'none'}

function renderPTComps(){var el=document.getElementById('fp_comps_list');if(!el)return;
  var allPts=(window._allPTs||[]).filter(function(p){return!p.is_assembly});
  var ptOpts=allPts.map(function(p){return{v:String(p.id),t:p.display_name+' ['+p.customer_name+']'}});
  el.innerHTML=ptComponents.map(function(c,i){
    return '<div class="mat-row">'+
    '<div style="flex:1"><label>Деталь</label>'+SS('ptc_'+i,ptOpts,String(c.component_id),'Деталь...')+'</div>'+
    '<div><label>Кол-во/сб</label><input type="number" value="'+c.quantity+'" min="1" style="width:60px" onchange="ptComponents['+i+'].quantity=+this.value"></div>'+
    '<button class="btn sm" onclick="ptComponents.splice('+i+',1);renderPTComps()">🗑</button></div>'
  }).join('')}

function addPTComp(){var allPts=(window._allPTs||[]).filter(function(p){return!p.is_assembly});
  if(!allPts.length){toast('Нет деталей для компонентов','err');return}
  ptComponents.push({component_id:allPts[0].id,component_name:allPts[0].display_name,quantity:1});renderPTComps()}

function savePT(pid){var opTimes={};
  document.querySelectorAll('.fp_op_total').forEach(function(el){var t=+el.value;if(!t)return;var n=el.dataset.op;
    var q=+document.querySelector('.fp_op_qty[data-op="'+n+'"]').value||1;
    opTimes[n]={qty:q,total_min:t,per_one:q>0?Math.round(t/q*100)/100:0}});
  ptMaterials.forEach(function(m,i){var v=ssVal('ptm_'+i);if(v)m.material_id=+v});
  var b={name:document.getElementById('fp_name').value,part_number:document.getElementById('fp_num').value,
    customer_id:+ssVal('fp_cust')||null,description:document.getElementById('fp_desc').value,
    operation_times:opTimes,materials:ptMaterials,user_id:U.id,
    is_assembly:document.getElementById('fp_is_asm').checked,
    components:ptComponents.map(function(c,i){var v=ssVal('ptc_'+i);return{component_id:+(v||c.component_id),quantity:c.quantity}})};
  if(pid)b.id=pid;
  api('/api/part-templates/save','POST',b).then(function(){closeModal();toast('Сохранено','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}
function delPT(pid){if(!confirm('Удалить?'))return;api('/api/part-templates/delete','POST',{id:pid}).then(function(){refreshPage()})}

function modalPTUpload(ptid){openModal('<h2>📎 Файл детали</h2>'+
  '<div class="form-row full"><div><label>Файл</label><input type="file" id="fpu_file" style="padding:8px"></div></div>'+
  '<div class="form-row"><div><label>Тип</label><select id="fpu_type"><option>Чертёж</option><option>3D Модель</option><option>Развёртка</option><option>Фото</option><option>Прочее</option></select></div>'+
    '<div><label>Описание</label><input id="fpu_desc"></div></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="doPTUpload('+ptid+')">Загрузить</button></div>')}
function doPTUpload(ptid){var f=document.getElementById('fpu_file').files[0];if(!f){toast('Файл','err');return}
  var fd=new FormData();fd.append('file',f);fd.append('file_type',document.getElementById('fpu_type').value);
  fd.append('description',document.getElementById('fpu_desc').value);fd.append('user_id',U.id);
  apiUpload('/api/part-templates/'+ptid+'/upload',fd).then(function(){closeModal();toast('Загружено','ok');modalPartTpl(ptid)}).catch(function(e){toast(e.message,'err')})}
function delPTFile(fid,ptid){if(!confirm('Удалить?'))return;api('/api/part-template-files/delete','POST',{id:fid}).then(function(){modalPartTpl(ptid)})}

// ═══ СКЛАД ═══
var whCatId=0;
function pgWarehouse(c){
  Promise.all([api('/api/material-categories'),api('/api/materials'),api('/api/materials/need-for-orders')]).then(function(arr){
  var cats=arr[0],mats=arr[1],need=arr[2];
  if(!whCatId&&cats.length)whCatId=cats[0].id;var filtered=whCatId?mats.filter(function(m){return m.category_id===whCatId}):mats;
  var cat=cats.find(function(ct){return ct.id===whCatId});
  var catFields=cat?cat.custom_fields:[];
  c.innerHTML='<div class="toolbar">'+(hasPerm('mat.create')?'<button class="btn primary" onclick="modalMaterial()">+ Новый</button>':'')+
    (hasPerm('mat.receive')?'<button class="btn ok" onclick="modalReceive()">📥 Поступление</button>':'')+
    (hasPerm('mat.edit')?'<button class="btn" style="background:var(--info);border-color:var(--info);color:#fff" onclick="modalAdjust()">🔧 Изменить количество</button>':'')+
    '<button class="btn" onclick="modalEditHistory()">📜 История</button>'+
    (need.length?'<button class="btn warn" onclick="modalNeedMat()">⚠ Дефицит ('+need.length+')</button>':'')+'</div>'+
  '<div class="sub-tabs">'+cats.map(function(ct){return '<button class="'+(ct.id===whCatId?'active':'')+'" onclick="whCatId='+ct.id+';pgWarehouse(document.getElementById(\'mainContent\'))">'+ct.name+'</button>'}).join('')+'</div>'+
  '<div class="tbl-wrap"><table><thead><tr><th>Код</th><th>Наименование</th>'+
    catFields.map(function(f){return '<th>'+f.label+'</th>'}).join('')+
    '<th>Кол</th><th>Рез</th><th>Св</th><th></th></tr></thead>'+
  '<tbody>'+filtered.map(function(m){var cd=m.custom_data||{};
    var fieldCols=catFields.map(function(f){
      var val=cd[f.key]||'';
      if(f.type==='grade_select'&&val){return '<td>'+(m.grade||val)+'</td>'}
      return '<td>'+val+'</td>'}).join('');
    var qtyCol='';
    if(cat&&cat.type==='Лист')qtyCol='<td>'+m.qty_sheets+'л / '+fmtN(m.qty_kg)+'кг</td><td>'+m.reserved_sheets+'</td><td class="'+(m.low_stock?'low':'')+'">'+m.available_sheets+'</td>';
    else if(cat&&cat.type==='Краска')qtyCol='<td>'+fmtN(m.qty_kg)+'кг</td><td>—</td><td>'+fmtN(m.available_kg)+'</td>';
    else if(cat&&cat.type==='Метиз')qtyCol='<td>'+fmtN(m.qty_pcs)+'шт</td><td>—</td><td>—</td>';
    else qtyCol='<td>'+fmtN(m.qty_kg||m.qty_pcs)+' '+m.unit+'</td><td>—</td><td>—</td>';
    return '<tr><td><strong>'+m.code+'</strong></td><td>'+m.name+'</td>'+fieldCols+qtyCol+
    '<td><button class="btn sm" onclick="modalMovements('+m.id+',\''+esc(m.name)+'\')">📜</button>'+
    (hasPerm('mat.edit')?'<button class="btn sm" onclick="modalMaterial('+m.id+')">✏</button>':'')+'</td></tr>'}).join('')+'</tbody></table></div>'})}

function modalNeedMat(){api('/api/materials/need-for-orders').then(function(need){
  openModal('<h2>⚠ Дефицит</h2><table><thead><tr><th>Код</th><th>Название</th><th>Дефицит</th></tr></thead>'+
  '<tbody>'+need.map(function(n){return '<tr><td><strong>'+n.code+'</strong></td><td>'+n.name+'</td><td class="low">'+n.deficit+' '+n.unit+'</td></tr>'}).join('')+'</tbody></table>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Закрыть</button></div>')})}

function modalAdjust(){api('/api/materials').then(function(mats){
  var matOpts=mats.map(function(m){return{v:String(m.id),t:m.code+' — '+m.name,type:m.type,qty_sheets:m.qty_sheets,qty_kg:m.qty_kg,qty_pcs:m.qty_pcs}});
  openModal('<h2>🔧 Изменить количество</h2>'+
  '<div class="form-row full"><div><label>Материал</label>'+SS('fa_mat',matOpts,'','Поиск материала...',function(v){adjChg(v)})+'</div></div>'+
  '<div id="fa_fields"></div>'+
  '<div class="form-row full"><div><label>Комментарий (обязательно)</label><textarea id="fa_note" rows="2" placeholder="Причина корректировки..."></textarea></div></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="submitAdjust()">Сохранить</button></div>');
  window._adjMats=matOpts})}
function adjChg(val){if(!window._adjMats)return;var m=window._adjMats.find(function(x){return x.v===val});if(!m)return;
  var h='';
  if(m.type==='Лист')h='<div class="form-row"><div><label>Текущее: '+m.qty_sheets+'л / '+fmtN(m.qty_kg)+'кг</label></div></div>'+
    '<div class="form-row"><div><label>Новое кол-во (листов)</label><input type="number" id="fa_sheets" value="'+m.qty_sheets+'"></div>'+
    '<div><label>Новое кол-во (кг)</label><input type="number" id="fa_kg" step="0.01" value="'+m.qty_kg+'"></div></div>';
  else if(m.type==='Краска'||m.type==='Труба'||m.type==='Пруток')h='<div class="form-row"><div><label>Текущее: '+fmtN(m.qty_kg)+' кг</label></div></div>'+
    '<div class="form-row"><div><label>Новое (кг)</label><input type="number" id="fa_kg" step="0.01" value="'+m.qty_kg+'"></div><div></div></div>';
  else h='<div class="form-row"><div><label>Текущее: '+fmtN(m.qty_pcs)+' шт</label></div></div>'+
    '<div class="form-row"><div><label>Новое (шт)</label><input type="number" id="fa_pcs" step="0.01" value="'+m.qty_pcs+'"></div><div></div></div>';
  document.getElementById('fa_fields').innerHTML=h}
function submitAdjust(){var mid=+ssVal('fa_mat');if(!mid){toast('Выберите материал','err');return}
  var note=document.getElementById('fa_note').value.trim();if(!note){toast('Укажите причину корректировки','err');return}
  var b={material_id:mid,user_id:U.id,note:note};
  var shEl=document.getElementById('fa_sheets'),kgEl=document.getElementById('fa_kg'),pcEl=document.getElementById('fa_pcs');
  if(shEl)b.new_sheets=+shEl.value;if(kgEl)b.new_kg=+kgEl.value;if(pcEl)b.new_pcs=+pcEl.value;
  api('/api/materials/adjust','POST',b).then(function(){closeModal();toast('Количество изменено','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}

function modalEditHistory(){
  Promise.all([api('/api/materials'),api('/api/materials/movement-types'),api('/api/users')]).then(function(arr){
  var mats=arr[0],mvTypes=arr[1],users=arr[2];
  var matOpts=[{v:'0',t:'Все'}].concat(mats.map(function(m){return{v:String(m.id),t:m.code+' — '+m.name}}));
  var typeOpts=[{v:'',t:'Все'}].concat(mvTypes.map(function(t){return{v:t,t:t}}));
  var userOpts=[{v:'0',t:'Все'}].concat(users.map(function(u){return{v:String(u.id),t:u.full_name}}));
  openModal('<h2>📜 История движений материалов</h2>'+
  '<div class="filter-bar"><label>Материал:</label><div style="min-width:200px">'+SS('eh_mat',matOpts,'0','Все')+'</div>'+
    '<label>Тип:</label><div style="min-width:140px">'+SS('eh_type',typeOpts,'','Все')+'</div>'+
    '<label>Кто:</label><div style="min-width:150px">'+SS('eh_user',userOpts,'0','Все')+'</div></div>'+
  '<div class="filter-bar"><label>С:</label><input type="date" id="eh_from">'+
    '<label>По:</label><input type="date" id="eh_to">'+
    '<button class="btn primary sm" onclick="loadEditHistory()">🔍 Найти</button></div>'+
  '<div id="ehResult" style="max-height:50vh;overflow-y:auto"></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Закрыть</button></div>');
  loadEditHistory()})}
function loadEditHistory(){var url='/api/materials/edit-history?';
  var mid=+ssVal('eh_mat');var mt=ssVal('eh_type');var uid=+ssVal('eh_user');
  var df=document.getElementById('eh_from').value;var dt=document.getElementById('eh_to').value;
  if(mid)url+='material_id='+mid+'&';if(mt)url+='movement_type='+mt+'&';if(uid)url+='user_id='+uid+'&';
  if(df)url+='date_from='+df+'&';if(dt)url+='date_to='+dt+'&';
  api(url).then(function(data){
  document.getElementById('ehResult').innerHTML=data.length?
    '<table><thead><tr><th>Дата</th><th>Тип</th><th>Материал</th><th>Л</th><th>Кг</th><th>Шт</th><th>Заказ</th><th>Кто</th><th>Комментарий</th></tr></thead>'+
    '<tbody>'+data.map(function(m){return '<tr><td style="font-size:.8em;white-space:nowrap">'+fmtDT(m.date)+'</td><td>'+statusBadge(m.type)+'</td><td style="font-size:.8em">'+m.material_code+'</td>'+
      '<td>'+(m.sheets||'—')+'</td><td>'+fmtN(m.kg)+'</td><td>'+(m.pcs||'—')+'</td><td>'+(m.order||'—')+'</td><td>'+(m.user||'—')+'</td><td style="font-size:.85em">'+m.note+'</td></tr>'}).join('')+'</tbody></table>'
    :'<div class="info-box">Нет записей</div>'}).catch(function(e){toast(e.message,'err')})}

function modalMaterial(mid){
  Promise.all([api('/api/grades'),api('/api/material-categories')]).then(function(arr){
  var grades=arr[0],cats=arr[1];
  var p1=mid?api('/api/materials'):Promise.resolve(null);
  p1.then(function(ms){var m=ms?ms.find(function(x){return x.id===mid}):null;
  var catOpts=cats.map(function(c){return{v:String(c.id),t:c.name}});
  var UNITS=['кг','лист','м','шт','л'];var unitOpts=UNITS.map(function(u){return{v:u,t:u}});
  var gradeOpts=[{v:'',t:'— нет —'}].concat(grades.map(function(g){return{v:String(g.id),t:g.code+' (ρ='+g.density+')'}}));
  var curCatId=m?String(m.category_id||''):(cats.length?String(cats[0].id):'');
  window._matGrades=gradeOpts;window._matCats=cats;window._matCustomData=m?m.custom_data:{};

  function buildCF(catId){
    var cat=cats.find(function(c){return String(c.id)===String(catId)});
    if(!cat)return '';var fields=cat.custom_fields||[];if(!fields.length)return '';
    var customData=window._matCustomData||{};
    var h='<div class="section-hdr">Параметры: '+cat.name+'</div>';
    fields.forEach(function(f){var val=customData[f.key]||'';
      if(f.type==='grade_select')h+='<div class="form-row"><div><label>'+f.label+'</label>'+SS('cf_'+f.key,gradeOpts,val,'Марка')+'</div><div></div></div>';
      else if(f.type==='number')h+='<div class="form-row"><div><label>'+f.label+'</label><input type="number" id="cf_'+f.key+'" step="0.1" value="'+val+'"></div><div></div></div>';
      else h+='<div class="form-row"><div><label>'+f.label+'</label><input id="cf_'+f.key+'" value="'+val+'"></div><div></div></div>';});
    return h}

  openModal('<h2>'+(m?'✏':'+')+' Материал</h2>'+
  '<div class="form-row"><div><label>Код</label><input id="fm_code" value="'+(m?m.code:'')+'"></div><div><label>Наименование</label><input id="fm_name" value="'+(m?m.name:'')+'"></div></div>'+
  '<div class="form-row"><div><label>Категория</label>'+SS('fm_cat',catOpts,curCatId,'Категория',function(v){document.getElementById("fm_custom_area").innerHTML=buildCF(v)})+'</div>'+
    '<div><label>Ед.</label>'+SS('fm_unit',unitOpts,m?m.unit:'кг','Ед.')+'</div></div>'+
  '<div id="fm_custom_area">'+buildCF(curCatId)+'</div>'+
  '<div class="form-row"><div><label>Мин. остаток (л)</label><input type="number" id="fm_minsh" value="'+(m?m.min_sheets:0)+'"></div>'+
    '<div><label>Мин. остаток (кг)</label><input type="number" id="fm_minkg" step="0.1" value="'+(m?m.min_kg:0)+'"></div></div>'+
  '<div class="form-row full"><div><label>Описание</label><textarea id="fm_desc" rows="2">'+(m?m.description:'')+'</textarea></div></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="saveMat('+(mid||0)+')">Сохранить</button></div>')})})}

function saveMat(mid){
  var catId=+ssVal('fm_cat')||null;var cats=window._matCats||[];var cat=cats.find(function(c){return c.id===catId});
  var customData={};var gradeId=null;var mtype='Прочее';
  if(cat){mtype=cat.type;(cat.custom_fields||[]).forEach(function(f){
    if(f.type==='grade_select'){var v=ssVal('cf_'+f.key);customData[f.key]=v;if(f.key==='grade')gradeId=+v||null}
    else if(f.type==='number'){var el=document.getElementById('cf_'+f.key);customData[f.key]=el?+el.value||0:0}
    else{var el=document.getElementById('cf_'+f.key);customData[f.key]=el?el.value:''}})}
  var b={code:document.getElementById('fm_code').value,name:document.getElementById('fm_name').value,
    material_type:mtype,primary_unit:ssVal('fm_unit'),category_id:catId,grade_id:gradeId,
    thickness:customData.thickness||null,width:customData.width||null,length:customData.length||null,
    diameter:customData.diameter||null,wall:customData.wall||null,
    min_stock_sheets:+document.getElementById('fm_minsh').value,min_stock_kg:+document.getElementById('fm_minkg').value,
    description:document.getElementById('fm_desc').value,
    color_ral:customData.color_ral||'',paint_type:customData.paint_type||'',
    custom_data:customData};if(mid)b.id=mid;
  api('/api/materials/save','POST',b).then(function(){closeModal();toast('Сохранено','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}

function modalReceive(){api('/api/materials').then(function(mats){
  var matOpts=mats.map(function(m){return{v:String(m.id),t:m.code+' — '+m.name,type:m.type,sw:m.sheet_weight||0}});
  openModal('<h2>📥 Поступление</h2>'+
  '<div class="form-row full"><div><label>Материал</label>'+SS('fr_mat',matOpts,'','Поиск...',function(v){recvChg(v)})+'</div></div>'+
  '<div id="fr_sh" class="form-row"><div><label>Листов</label><input type="number" id="fr_sheets" min="1" value="1" oninput="recvCalc()"></div>'+
    '<div><label>Авто вес</label><input id="fr_autokg" disabled></div></div>'+
  '<div id="fr_kgr" class="form-row" style="display:none"><div><label>Кг</label><input type="number" id="fr_kg" step="0.01"></div><div></div></div>'+
  '<div id="fr_pcr" class="form-row" style="display:none"><div><label>Шт/м/л</label><input type="number" id="fr_pcs" step="0.01"></div><div></div></div>'+
  '<div class="form-row full"><div><label>Примечание</label><input id="fr_note"></div></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="submitRecv()">Принять</button></div>');
  window._recvMats=matOpts})}
function recvChg(val){if(!window._recvMats)return;var m=window._recvMats.find(function(x){return x.v===val});if(!m)return;
  document.getElementById('fr_sh').style.display=m.type==='Лист'?'':'none';
  document.getElementById('fr_kgr').style.display=['Краска','Труба','Пруток'].indexOf(m.type)>=0?'':'none';
  document.getElementById('fr_pcr').style.display=['Метиз','Прочее'].indexOf(m.type)>=0?'':'none';recvCalc()}
function recvCalc(){if(!window._recvMats)return;var m=window._recvMats.find(function(x){return x.v===ssVal('fr_mat')});
  var sw=m?m.sw:0;var sh=+document.getElementById('fr_sheets').value||0;
  document.getElementById('fr_autokg').value=sw?(sh*sw).toFixed(2)+' кг':'—'}
function submitRecv(){var mid=+ssVal('fr_mat');if(!mid){toast('Выберите','err');return}
  var m=window._recvMats.find(function(x){return x.v===String(mid)});var b={material_id:mid,user_id:U.id,note:document.getElementById('fr_note').value};
  if(m.type==='Лист')b.sheets=+document.getElementById('fr_sheets').value;
  else if(['Краска','Труба','Пруток'].indexOf(m.type)>=0)b.kg=+document.getElementById('fr_kg').value;
  else b.pcs=+document.getElementById('fr_pcs').value;
  api('/api/materials/receive','POST',b).then(function(){closeModal();toast('Принято','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}

function modalMovements(mid,name){api('/api/materials/'+mid+'/movements').then(function(mvs){
  openModal('<h2>📜 '+name+'</h2><div class="tbl-wrap" style="max-height:60vh"><table><thead><tr><th>Дата</th><th>Тип</th><th>Л</th><th>Кг</th><th>Заказ</th><th>Кто</th><th>Прим.</th></tr></thead>'+
  '<tbody>'+mvs.map(function(m){return '<tr><td style="font-size:.8em">'+fmtDT(m.date)+'</td><td>'+statusBadge(m.type)+'</td><td>'+(m.sheets||'—')+'</td><td>'+fmtN(m.kg)+'</td>'+
    '<td>'+(m.order||'—')+'</td><td>'+(m.user||'—')+'</td><td style="font-size:.85em">'+m.note+'</td></tr>'}).join('')+'</tbody></table></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Закрыть</button></div>')})}

// ═══ ОПЕРАЦИИ (+ фильтр по типу) ═══
var opsResFilter=0,opsTypeFilter='';
function pgOperations(c){
  Promise.all([api('/api/resources'),api('/api/op-types')]).then(function(arr2){
  var resources=arr2[0],opTypes=arr2[1];
  window._opTypesData={};opTypes.forEach(function(ot){window._opTypesData[ot.name]=ot});
  var url='/api/operations?active_only=1';if(opsResFilter)url+='&resource_id='+opsResFilter;
  api(url).then(function(ops){
  window._opsData={};ops.forEach(function(o){window._opsData[o.id]=o});
  if(opsTypeFilter)ops=ops.filter(function(o){return o.type===opsTypeFilter});
  var byRes={};
  ops.forEach(function(o){var rn=o.resource||'Не назначен';if(!byRes[rn])byRes[rn]={ops:[],rid:o.resource_id,shift_h:0};byRes[rn].ops.push(o)});
  resources.forEach(function(r){if(byRes[r.name])byRes[r.name].shift_h=r.shift_hours});
  var resOpts=[{v:'0',t:'Все участки'}].concat(resources.map(function(r){return{v:String(r.id),t:r.name}}));
  var typeOpts=[{v:'',t:'Все типы'}].concat(opTypes.filter(function(o){return o.is_active}).map(function(o){return{v:o.name,t:o.name}}));
  var html='<div class="toolbar"><div class="info-box" style="margin:0;padding:6px 12px">Операции «В работе»</div><span class="spacer"></span></div>'+
  '<div class="filter-bar"><label>Участок:</label><div style="min-width:200px">'+SS('ops_res',resOpts,String(opsResFilter),'Все',function(v){opsResFilter=+v;pgOperations(document.getElementById('mainContent'))})+'</div>'+
    '<label>Тип:</label><div style="min-width:200px">'+SS('ops_type',typeOpts,opsTypeFilter,'Все типы',function(v){opsTypeFilter=v;pgOperations(document.getElementById('mainContent'))})+'</div></div>';
  Object.keys(byRes).forEach(function(rname){var data=byRes[rname];var totalMin=data.ops.reduce(function(s,o){return s+o.estimated_min},0);
    var shiftMin=data.shift_h*60;var totalShifts=shiftMin>0?Math.ceil(totalMin/shiftMin):0;
    html+='<div class="section-hdr">'+rname+'<span style="font-weight:400;font-size:.85em;color:var(--text3);margin-left:12px">'+fmtMinToH(totalMin)+' | ~'+totalShifts+' смен</span></div>'+
    '<div class="tbl-wrap"><table><thead><tr><th>↕</th><th>Заказ</th><th>Деталь</th><th>Тип</th><th>План</th><th>Гот</th><th>Брак</th><th>Время</th><th>Ст.</th><th></th></tr></thead><tbody>'+
    data.ops.map(function(o){return '<tr draggable="true" data-opid="'+o.id+'" ondragstart="dragOp(event)" ondragover="event.preventDefault()" ondrop="dropOp(event)">'+
      '<td style="cursor:grab">☰</td><td>'+(o.order_display||o.order_number)+'</td><td>'+(o.item||'—')+'</td><td>'+o.type+'</td>'+
      '<td>'+o.planned_qty+'</td><td>'+o.completed_qty+'</td><td class="'+(o.rejected_qty?'low':'')+'">'+o.rejected_qty+'</td><td>'+fmtMinToH(o.estimated_min)+'</td><td>'+statusBadge(o.status)+'</td>'+
      '<td style="white-space:nowrap">'+
        (o.status==='Ожидает'||o.status==='Запланирована'||o.status==='Пауза'?'<button class="btn sm warn" onclick="startOp('+o.id+')">▶</button>':'')+
        (o.status==='В работе'?'<button class="btn sm ok" onclick="completeOp('+o.id+')">✓</button><button class="btn sm" onclick="pauseOp('+o.id+')">⏸</button>':'')+
        ((o.status==='В работе'||o.status==='Пауза')&&o.item_id&&U.writeoff_types.length>0&&(window._opTypesData[o.type]||{}).writeoff_mode&&(window._opTypesData[o.type]||{}).writeoff_mode!=='Нет'?'<button class="btn sm" style="background:var(--info);border-color:var(--info);color:#fff" onclick="modalOpWriteoff('+o.id+')" title="Списание">📤</button>':'')+
        (['Завершена','В работе','Пауза'].indexOf(o.status)>=0&&hasPerm('op.rollback')?'<button class="btn sm" onclick="rollbackOp('+o.id+')" style="color:var(--err)">↩</button>':'')+
      '</td></tr>'}).join('')+'</tbody></table></div>'});
  if(!Object.keys(byRes).length)html+='<div class="info-box">Нет операций</div>';c.innerHTML=html})})}

var draggedOpId=null;function dragOp(e){draggedOpId=e.target.closest('tr').dataset.opid;e.dataTransfer.effectAllowed='move'}
function dropOp(e){e.preventDefault();var tr=e.target.closest('tr');if(!tr||!draggedOpId)return;
  var tbody=tr.closest('tbody'),rows=Array.from(tbody.querySelectorAll('tr'));
  var order=rows.map(function(r,i){return{id:+r.dataset.opid,sort_order:i}});
  var di=order.findIndex(function(o){return o.id===+draggedOpId}),ti=order.findIndex(function(o){return o.id===+tr.dataset.opid});
  if(di<0||ti<0)return;var mv=order.splice(di,1)[0];order.splice(ti,0,mv);order.forEach(function(o,i){o.sort_order=i});draggedOpId=null;
  api('/api/operations/reorder','POST',{order:order}).then(function(){toast('OK','ok');refreshPage()}).catch(function(e2){toast(e2.message,'err')})}
function startOp(id){api('/api/operations/'+id+'/start','POST',{user_id:U.id}).then(function(){toast('Запущена','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}
function pauseOp(id){api('/api/operations/'+id+'/pause','POST',{user_id:U.id}).then(function(){toast('Пауза','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}
function completeOp(id){api('/api/operations/'+id+'/complete','POST',{user_id:U.id}).then(function(){toast('Завершена','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}
function rollbackOp(id){if(!confirm('Откатить?'))return;api('/api/operations/'+id+'/rollback','POST',{user_id:U.id}).then(function(){toast('Откат','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}

function modalOpWriteoff(opid){
  var op=window._opsData[opid];if(!op){toast('Операция не найдена','err');return}
  var otCfg=window._opTypesData[op.type]||{};
  var mode=otCfg.writeoff_mode||'Детали';
  if(mode==='Нет'){toast('Списание не настроено для «'+op.type+'»','err');return}
  var showMat=mode.indexOf('Материал')>=0&&U.writeoff_types.indexOf('Материал')>=0;
  var showParts=mode.indexOf('Детали')>=0&&U.writeoff_types.indexOf('Детали')>=0;
  if(!showMat&&!showParts){toast('Нет прав на списание','err');return}
  var p=showMat&&op.item_id?api('/api/reservations/by-item/'+op.item_id):Promise.resolve([]);
  p.then(function(reservations){
    window._opWoReservations=reservations;
    // Группируем по компоненту
    var byComp={};reservations.forEach(function(r){
      var key=r.part_template_id||0;var name=r.part_name||'Общие';
      if(!byComp[key])byComp[key]={name:name,items:[]};byComp[key].items.push(r)});
    var compKeys=Object.keys(byComp);var hasComponents=compKeys.length>1;

    var h='<h2>📤 Списание: '+op.type+'</h2>'+
      '<div class="info-box"><strong>Заказ:</strong> '+(op.order_display||op.order_number)+
      ' | <strong>Деталь:</strong> '+(op.item||'—')+
      ' | <strong>Участок:</strong> '+(op.resource||'—')+'</div>';

    if(showMat&&reservations.length){
      h+='<div class="section-hdr">📦 Материал</div>';
      if(hasComponents){
        h+='<div class="form-row full"><div><label>Компонент сборки</label><select id="fopwo_comp" onchange="opWoCompChg()">'+
          compKeys.map(function(k){return '<option value="'+k+'">'+byComp[k].name+' ('+byComp[k].items.length+' мат.)</option>'}).join('')+
          '</select></div></div>'
      }
      h+='<div id="fopwo_mat_area"></div>'+
        '<div class="form-row"><div><label>Листов</label><input type="number" id="fopwo_sheets" min="0" value="0"></div><div></div></div>'
    } else if(showMat){
      h+='<div class="section-hdr">📦 Материал</div><div class="info-box" style="color:var(--text3)">Нет активных резервов</div>'
    }
    if(showParts){
      h+='<div class="section-hdr">🔩 Детали</div>'+
        '<div class="form-row"><div><label>Годных</label><input type="number" id="fopwo_good" value="0" min="0"></div>'+
        '<div><label>Брак</label><input type="number" id="fopwo_rej" value="0" min="0"></div></div>'
    }
    h+='<div class="form-row full"><div><label>Примечание</label><input id="fopwo_note"></div></div>'+
      '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button>'+
      '<button class="btn primary" onclick="submitOpWriteoff('+opid+','+showMat+','+showParts+')">Списать</button></div>';
    openModal(h);
    // Инициализируем материалы
    if(showMat&&reservations.length){
      window._opWoByComp=byComp;window._opWoCompKeys=compKeys;
      opWoCompChg()
    }
  }).catch(function(e){toast(e.message,'err')})}

function opWoCompChg(){
  var sel=document.getElementById('fopwo_comp');
  var key=sel?sel.value:window._opWoCompKeys[0];
  var items=window._opWoByComp[key].items;
  document.getElementById('fopwo_mat_area').innerHTML=
    '<div class="form-row full"><div><label>Материал</label><select id="fopwo_mat">'+
    items.map(function(r){return '<option value="'+r.material_id+'" data-rid="'+r.id+'" data-sh="'+r.sheets+'">'+
      r.material_code+' — '+r.material+' (ост. '+r.sheets+'л / '+fmtN(r.kg)+'кг)</option>'}).join('')+
    '</select></div></div>'}

function submitOpWriteoff(opid,showMat,showParts){
  var op=window._opsData[opid];if(!op)return;
  var note=(document.getElementById('fopwo_note')||{}).value||'';
  var promises=[];
  if(showMat){
    var matSel=document.getElementById('fopwo_mat');
    var shVal=+(document.getElementById('fopwo_sheets')||{}).value||0;
    if(matSel&&shVal>0){
      var matOpt=matSel.options[matSel.selectedIndex];
      promises.push(api('/api/writeoffs/create','POST',{
        writeoff_type:'Материал',user_id:U.id,order_id:op.order_id,
        order_item_id:op.item_id,resource_id:op.resource_id,
        material_id:+matSel.value,reservation_id:+(matOpt.dataset.rid)||null,
        sheets:shVal,note:'['+op.type+'] '+note}))
    }
  }
  if(showParts){
    var good=+(document.getElementById('fopwo_good')||{}).value||0;
    var rej=+(document.getElementById('fopwo_rej')||{}).value||0;
    if(good>0||rej>0){
      promises.push(api('/api/writeoffs/create','POST',{
        writeoff_type:'Детали',user_id:U.id,order_id:op.order_id,
        order_item_id:op.item_id,resource_id:op.resource_id,
        operation_type:op.type,parts_good:good,parts_rejected:rej,
        note:'['+op.type+'] '+note}))
    }
  }
  if(!promises.length){toast('Укажите количество','err');return}
  Promise.all(promises).then(function(results){
    closeModal();
    var anom=results.find(function(r){return r.is_anomaly});
    if(anom)toast('⚠ '+anom.anomaly_note,'err');
    else toast('Списано с «'+op.type+'»','ok');
    refreshPage()
  }).catch(function(e){toast(e.message,'err')})}

// ═══ РЕЗЕРВЫ ═══
var resFilter={type:'',order:'',status:'',part:''};
function pgReservations(c){api('/api/reservations?active_only=1').then(function(rs){
  if(resFilter.type)rs=rs.filter(function(r){return r.material_type===resFilter.type});
  if(resFilter.order)rs=rs.filter(function(r){return r.order_display.toLowerCase().indexOf(resFilter.order.toLowerCase())>=0});
  if(resFilter.status)rs=rs.filter(function(r){return r.order_status===resFilter.status});
  if(resFilter.part)rs=rs.filter(function(r){return r.part_name.toLowerCase().indexOf(resFilter.part.toLowerCase())>=0});
  c.innerHTML='<div class="toolbar">'+(hasPerm('reserve.create')?'<button class="btn primary" onclick="modalCreateRes()">+ Резерв</button>':'')+'</div>'+
  '<div class="filter-bar"><label>Тип:</label><select onchange="resFilter.type=this.value;pgReservations(document.getElementById(\'mainContent\'))">'+
    '<option value="">Все</option>'+['Лист','Труба','Пруток','Метиз','Краска','Прочее'].map(function(t){return '<option '+(resFilter.type===t?'selected':'')+'>'+t+'</option>'}).join('')+'</select>'+
    '<label>Заказ:</label><input style="width:150px" value="'+esc(resFilter.order)+'" onchange="resFilter.order=this.value;pgReservations(document.getElementById(\'mainContent\'))">'+
    '<label>Деталь:</label><input style="width:150px" value="'+esc(resFilter.part)+'" onchange="resFilter.part=this.value;pgReservations(document.getElementById(\'mainContent\'))">'+
    '<label>Статус:</label><select onchange="resFilter.status=this.value;pgReservations(document.getElementById(\'mainContent\'))">'+
    '<option value="">Все</option>'+STATUSES.map(function(s){return '<option '+(resFilter.status===s?'selected':'')+'>'+s+'</option>'}).join('')+'</select></div>'+
  '<div class="tbl-wrap"><table><thead><tr><th>Заказ</th><th>Ст.</th><th>Деталь</th><th>Материал</th><th>Зарез. (л)</th><th>Списано</th><th>Остаток</th><th>Кем</th><th></th></tr></thead>'+
  '<tbody>'+rs.map(function(r){return '<tr><td>'+r.order_display+'</td><td>'+statusBadge(r.order_status)+'</td><td>'+(r.part_name||'—')+'</td>'+
    '<td>'+r.material_code+' — '+r.material+'</td><td>'+(r.sheets||'—')+'</td>'+
    '<td>'+(r.consumed_sheets||0)+'</td><td class="'+(r.remaining_sheets>0?'low':'')+'">'+(r.remaining_sheets||0)+'</td>'+
    '<td>'+(r.reserved_by||'—')+'</td>'+
    '<td>'+(hasPerm('reserve.edit')?'<button class="btn sm" onclick="modalEditRes('+r.id+','+r.sheets+','+r.kg+',\''+esc(r.note)+'\')">✏</button>':'')+
      (hasPerm('reserve.cancel')?'<button class="btn sm" onclick="cancelRes('+r.id+')">❌</button>':'')+'</td></tr>'}).join('')+'</tbody></table></div>'})}

function modalCreateRes(){api('/api/orders').then(function(orders){var active=orders.filter(function(o){return o.status==='В работе'});
  if(!active.length){toast('Нет заказов «В работе»','err');return}
  var ordOpts=active.map(function(o){return{v:String(o.id),t:o.display,items:o.items}});
  openModal('<h2>🔒 Резерв</h2>'+
  '<div class="form-row full"><div><label>Заказ</label>'+SS('fres_ord',ordOpts,'','Заказ...',function(v){resOrdChg2(v)})+'</div></div>'+
  '<div class="form-row full"><div><label>Деталь</label><select id="fres_item" onchange="resItemChg2()"><option value="">— сначала заказ —</option></select></div></div>'+
  '<div class="form-row full"><div><label>Материал</label><select id="fres_mat"><option value="">— сначала деталь —</option></select></div></div>'+
  '<div class="form-row"><div><label>Листов</label><input type="number" id="fres_sh" min="1" value="1"></div><div><label>Прим.</label><input id="fres_note"></div></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="submitCreateRes()">Зарезервировать</button></div>');
  window._resOrds=ordOpts})}
function resOrdChg2(val){var ord=window._resOrds.find(function(o){return o.v===val});if(!ord)return;
  document.getElementById('fres_item').innerHTML='<option value="">— выберите —</option>'+
    (ord.items||[]).map(function(it){return '<option value="'+it.id+'" data-mats=\''+JSON.stringify(it.materials||[]).replace(/'/g,"&#39;")+'\' data-tid="'+it.template_id+'">'+it.part_name+' (x'+it.quantity+')</option>'}).join('');
  document.getElementById('fres_mat').innerHTML='<option value="">— сначала деталь —</option>'}
function resItemChg2(){var sel=document.getElementById('fres_item');var opt=sel.options[sel.selectedIndex];
  var mats=[];try{mats=JSON.parse(opt.dataset.mats||'[]')}catch(e){}
  document.getElementById('fres_mat').innerHTML=mats.map(function(m){return '<option value="'+m.material_id+'">'+m.code+' — '+m.name+' ('+m.sheets_needed+'л)</option>'}).join('')||'<option value="">Нет</option>'}
function submitCreateRes(){var ordId=+ssVal('fres_ord');var itemSel=document.getElementById('fres_item');
  var tid=+(itemSel.options[itemSel.selectedIndex]||{}).dataset?.tid||null;var matId=+document.getElementById('fres_mat').value;
  if(!ordId||!matId){toast('Заполните','err');return}
  api('/api/reservations/create','POST',{user_id:U.id,order_id:ordId,order_item_id:+itemSel.value||null,
    part_template_id:tid,material_id:matId,sheets:+document.getElementById('fres_sh').value,note:document.getElementById('fres_note').value}).then(function(){
    closeModal();toast('OK','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}
function modalEditRes(rid,sheets,kg,note){openModal('<h2>✏ Резерв #'+rid+'</h2>'+
  '<div class="form-row"><div><label>Листов</label><input type="number" id="fre_sh" value="'+sheets+'" min="0"></div><div><label>Кг</label><input value="'+fmtN(kg)+'" disabled></div></div>'+
  '<div class="form-row full"><div><label>Прим.</label><input id="fre_note" value="'+note+'"></div></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="submitEditRes('+rid+')">Сохранить</button></div>')}
function submitEditRes(rid){api('/api/reservations/'+rid+'/edit','POST',{user_id:U.id,sheets:+document.getElementById('fre_sh').value,note:document.getElementById('fre_note').value}).then(function(){
  closeModal();toast('OK','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}
function cancelRes(rid){if(!confirm('Снять?'))return;api('/api/reservations/'+rid+'/cancel','POST',{user_id:U.id}).then(function(){toast('OK','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}

// ═══ УЧЁТ ДЕТАЛЕЙ ═══
var plFilter={name:'',active_only:1},plSearchTimer=null;
function pgPartsLog(c){api('/api/part-station-logs?active_only='+plFilter.active_only).then(function(data){
  var filtered=data;if(plFilter.name)filtered=filtered.filter(function(d){return d.part_name.toLowerCase().indexOf(plFilter.name.toLowerCase())>=0});
  var totalSurplus=filtered.reduce(function(s,d){return s+d.surplus},0);
  c.innerHTML='<div class="toolbar">'+(totalSurplus>0?'<button class="btn warn" onclick="modalSurplus()">📊 Излишки (+'+totalSurplus+')</button>':'')+'</div>'+
  '<div class="filter-bar"><label>Деталь:</label><input id="plNameInput" style="width:150px" value="'+esc(plFilter.name)+'">'+
    '<label>Заказы:</label><select onchange="plFilter.active_only=+this.value;pgPartsLog(document.getElementById(\'mainContent\'))">'+
    '<option value="1" '+(plFilter.active_only?'selected':'')+'>В работе</option><option value="0" '+(!plFilter.active_only?'selected':'')+'>Все</option></select></div>'+
  '<div class="tbl-wrap"><table><thead><tr><th>Заказ</th><th>Ст.</th><th>Деталь</th><th>План</th><th>Факт</th><th>Брак</th><th>Изл.(1й уч.)</th><th>По участкам</th></tr></thead>'+
  '<tbody>'+filtered.map(function(d){var resBadges=Object.entries(d.by_resource||{}).map(function(e){var rn=e[0],rd=e[1];
    var ac=rd.logs.some(function(l){return l.anomaly})?'anomaly':'';
    return '<span class="badge b-info '+ac+'" title="'+rn+': '+rd.good+' / '+rd.rejected+' брак">'+rn+': '+rd.good+(rd.rejected?' ⚠'+rd.rejected:'')+'</span>'}).join(' ')||'—';
     d.components_html=(d.components||[]).length?'<div style="font-size:.8em;color:var(--text3);margin-top:2px">'+d.components.map(function(c){return '├ '+c.name+' ×'+c.qty}).join('<br>')+'</div>':'';
    return '<tr><td>'+d.order_display+'</td><td>'+statusBadge(d.order_status)+'</td><td><strong>'+(d.is_assembly?'🔧 ':'')+d.part_name+'</strong>'+(d.components_html||'')+'</td>'+
    '<td>'+d.quantity+'</td><td>'+d.completed+'</td><td class="'+(d.rejected?'low':'')+'">'+d.rejected+'</td>'+
    '<td class="'+(d.surplus>0?'low':'')+'">'+(d.surplus>0?'+'+d.surplus:'—')+'</td><td>'+resBadges+'</td></tr>'}).join('')+'</tbody></table></div>';
  var inp=document.getElementById('plNameInput');
  if(inp){inp.addEventListener('input',function(){plFilter.name=this.value;clearTimeout(plSearchTimer);plSearchTimer=setTimeout(function(){pgPartsLog(document.getElementById('mainContent'))},400)});
    inp.focus();inp.setSelectionRange(inp.value.length,inp.value.length)}
  })}
function modalSurplus(){api('/api/part-station-logs/surplus').then(function(data){if(!data.length){toast('Нет излишков');return}
  openModal('<h2>📊 Излишки (по 1-му участку)</h2><div class="tbl-wrap"><table><thead><tr><th>Деталь</th><th>Изл.</th><th>Заказы</th></tr></thead>'+
  '<tbody>'+data.map(function(d){return '<tr><td><strong>'+(d.is_assembly?'🔧 ':'')+d.part_name+'</strong>'+((d.components||[]).length?'<div style="font-size:.8em;color:var(--text3);margin-top:2px">'+(d.components||[]).map(function(c){return '├ '+c.name+' ×'+c.qty}).join('<br>')+'</div>':'')+'</td> class="low">+'+d.total_surplus+'</td>'+
    '<td style="font-size:.8em">'+(d.orders||[]).map(function(o){return o.order+': '+o.planned+'→'+o.completed_first+' (+'+o.surplus+')'}).join('<br>')+'</td></tr>'}).join('')+'</tbody></table></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Закрыть</button></div>')}).catch(function(e){toast(e.message,'err')})}

// ═══ СПИСАНИЯ ═══
var woTab='Материал';
function pgWriteoffs(c){var canMat=U.writeoff_types.indexOf('Материал')>=0,canParts=U.writeoff_types.indexOf('Детали')>=0;
  if(!canMat&&canParts)woTab='Детали';api('/api/writeoffs?wtype='+woTab).then(function(wos){
  c.innerHTML='<div class="toolbar">'+
    (canMat?'<button class="btn '+(woTab==='Материал'?'primary':'')+'" onclick="woTab=\'Материал\';pgWriteoffs(document.getElementById(\'mainContent\'))">📦 Материал</button>':'')+
    (canParts?'<button class="btn '+(woTab==='Детали'?'primary':'')+'" onclick="woTab=\'Детали\';pgWriteoffs(document.getElementById(\'mainContent\'))">🔩 Детали</button>':'')+
    '<span class="spacer"></span><button class="btn primary" onclick="modalWriteoff()">+ Списание</button></div>'+
  (woTab==='Материал'?'<div class="tbl-wrap"><table><thead><tr><th>Дата</th><th>Заказ</th><th>Деталь</th><th>Материал</th><th>Л</th><th>Кг</th><th>Уч.</th><th>Кто</th><th>Прим.</th><th></th></tr></thead>'+
  '<tbody>'+wos.map(function(w){return '<tr class="'+(w.is_cancelled?'cancelled-row':'')+'"><td style="font-size:.8em">'+fmtDT(w.date)+'</td><td>'+(w.order_display||'—')+'</td><td>'+(w.part_name||'—')+'</td><td>'+(w.material||'—')+'</td>'+
    '<td>'+(w.sheets||'—')+'</td><td>'+fmtN(w.kg)+'</td><td>'+(w.resource||'—')+'</td><td>'+w.user+'</td><td style="font-size:.85em">'+(w.note||'')+
    (w.is_cancelled?' <span class="badge b-err">Отменено: '+w.cancelled_by+'</span>':'')+'</td>'+
    '<td>'+(w.is_cancelled?'':'<button class="btn sm" onclick="cancelWO('+w.id+')" title="Отменить" '+(hasPerm('writeoff.cancel')?'':'disabled')+'>↩</button>')+'</td></tr>'}).join('')+'</tbody></table></div>'
  :'<div class="tbl-wrap"><table><thead><tr><th>Дата</th><th>Заказ</th><th>Деталь</th><th>Годн</th><th>Брак</th><th>Уч.</th><th>Кто</th><th>⚠</th><th>Прим.</th><th></th></tr></thead>'+
  '<tbody>'+wos.map(function(w){return '<tr class="'+(w.is_cancelled?'cancelled-row':(w.is_anomaly?'anomaly':''))+'"><td style="font-size:.8em">'+fmtDT(w.date)+'</td><td>'+(w.order_display||'—')+'</td><td>'+(w.part_name||'—')+'</td>'+
    '<td>'+w.parts_good+'</td><td class="'+(w.parts_rejected?'low':'')+'">'+w.parts_rejected+'</td><td>'+(w.resource||'—')+'</td><td>'+w.user+'</td>'+
    '<td>'+(w.is_anomaly?'<span class="low" title="'+w.anomaly_note+'">⚠</span>':'')+'</td>'+
    '<td style="font-size:.85em">'+(w.is_anomaly?'<span class="low">'+w.anomaly_note+'</span> ':'')+(w.note||'')+
    (w.is_cancelled?' <span class="badge b-err">Отменено: '+w.cancelled_by+'</span>':'')+'</td>'+
    '<td>'+(w.is_cancelled?'':'<button class="btn sm" onclick="cancelWO('+w.id+')" title="Отменить" '+(hasPerm('writeoff.cancel')?'':'disabled')+'>↩</button>')+'</td></tr>'}).join('')+'</tbody></table></div>')})}

function cancelWO(wid){if(!confirm('Отменить списание? Все параметры вернутся в состояние "до списания".'))return;
  api('/api/writeoffs/'+wid+'/cancel','POST',{user_id:U.id}).then(function(){toast('Списание отменено','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}

function modalWriteoff(){Promise.all([api('/api/orders'),api('/api/resources'),api('/api/op-types')]).then(function(arr){
  var orders=arr[0],resources=arr[1],opTypes=arr[2];
  var active=orders.filter(function(o){return o.status==='В работе'});var userSt=U.stations||[];
  window._woResources=resources;window._woOpTypes=opTypes.filter(function(o){return o.is_active});window._woAllResources=resources;
  var ordOpts=active.map(function(o){return{v:String(o.id),t:o.display}});
  openModal('<h2>+ Списание ('+woTab+')</h2>'+
  '<div class="form-row full"><div><label>Заказ</label>'+SS('fwo_ord',ordOpts,'','Заказ...',function(v){woOrdChg2(v)})+'</div></div>'+
  '<div class="form-row full"><div><label>Деталь</label><select id="fwo_item" onchange="woItemChg2()"><option value="">— сначала заказ —</option></select></div></div>'+
  '<div class="form-row"><div><label>Участок</label><div id="fwo_res_wrap"><select id="fwo_res_sel"><option value="">— сначала заказ —</option></select></div></div><div></div></div>'+
  (woTab==='Материал'?'<div class="form-row full"><div><label>Материал (из резервов)</label><select id="fwo_mat"><option value="">— сначала деталь —</option></select></div></div>'+
  '<div class="form-row"><div><label>Листов</label><input type="number" id="fwo_sheets" min="1" value="1"></div><div></div></div>'
  :'<div class="form-row"><div><label>Операция</label><div id="fwo_optype_wrap"><select id="fwo_optype_sel">'+window._woOpTypes.map(function(o){return '<option value="'+o.name+'">'+o.name+'</option>'}).join('')+'</select></div></div><div></div></div>'+
  '<div class="form-row"><div><label>Годных</label><input type="number" id="fwo_good" value="0" min="0"></div><div><label>Брак</label><input type="number" id="fwo_rej" value="0" min="0"></div></div>')+
  '<div class="form-row full"><div><label>Прим.</label><input id="fwo_note"></div></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="submitWO()">Списать</button></div>')})}

function woOrdChg2(val){var ordId=+val;if(!ordId)return;
  // Load resources for this order
  api('/api/orders/'+ordId+'/resources-for-writeoff').then(function(res){
    var sel=document.getElementById('fwo_res_sel');
    sel.innerHTML='<option value="">—</option>'+res.map(function(r){return '<option value="'+r.id+'" data-ops=\''+JSON.stringify(r.allowed_ops||[]).replace(/'/g,"&#39;")+'\'>'+r.name+'</option>'}).join('');
    sel.onchange=function(){woResChg2()};
  });
  api('/api/orders/'+ordId+'/items-for-writeoff').then(function(items){
    document.getElementById('fwo_item').innerHTML='<option value="">— выберите —</option>'+
      items.map(function(it){return '<option value="'+it.id+'" data-tid="'+it.template_id+'">'+it.part_name+' ('+it.quantity+'/'+it.completed+')</option>'}).join('')}).catch(function(e){toast(e.message,'err')});
  if(woTab==='Материал'){var ms=document.getElementById('fwo_mat');if(ms)ms.innerHTML='<option value="">— деталь —</option>'}}

function woResChg2(){
  if(woTab!=='Детали')return;
  var sel=document.getElementById('fwo_res_sel');if(!sel)return;
  var opt=sel.options[sel.selectedIndex];if(!opt)return;
  var ops=[];try{ops=JSON.parse(opt.dataset.ops||'[]')}catch(e){}
  var opSel=document.getElementById('fwo_optype_sel');
  if(opSel&&ops.length>=1){opSel.innerHTML=ops.map(function(o){return '<option value="'+o+'">'+o+'</option>'}).join('')}
}

function woItemChg2(){if(woTab!=='Материал')return;var itemId=+document.getElementById('fwo_item').value;if(!itemId)return;
  api('/api/reservations/by-item/'+itemId).then(function(rs){
    document.getElementById('fwo_mat').innerHTML=rs.map(function(r){return '<option value="'+r.material_id+'" data-rid="'+r.id+'">'+r.material_code+' — '+r.material+' ('+r.sheets+'л/'+fmtN(r.kg)+'кг)</option>'}).join('')||'<option value="">Нет</option>'}).catch(function(e){toast(e.message,'err')})}
function submitWO(){var ordId=+ssVal('fwo_ord');var itemId=+document.getElementById('fwo_item').value;
  if(!ordId){toast('Заказ','err');return}if(!itemId){toast('Деталь','err');return}
  var resId=+document.getElementById('fwo_res_sel').value||null;
  var b={writeoff_type:woTab,user_id:U.id,order_id:ordId,order_item_id:itemId,resource_id:resId,note:document.getElementById('fwo_note').value};
  if(woTab==='Материал'){var ms=document.getElementById('fwo_mat');var matId=+ms.value;if(!matId){toast('Материал','err');return}
    b.material_id=matId;b.reservation_id=+(ms.options[ms.selectedIndex].dataset.rid)||null;b.sheets=+document.getElementById('fwo_sheets').value}
  else{var sel=document.getElementById('fwo_optype_sel');b.operation_type=sel?sel.value:'';
    b.parts_good=+document.getElementById('fwo_good').value;b.parts_rejected=+document.getElementById('fwo_rej').value;
    if(b.parts_good===0&&b.parts_rejected===0){toast('Укажите количество годных или брак','err');return}}
  api('/api/writeoffs/create','POST',b).then(function(r){closeModal();if(r.is_anomaly)toast('⚠ '+r.anomaly_note,'err');else toast('OK','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}

// ═══ ЗАГРУЖЕННОСТЬ ═══
function pgLoad(c){api('/api/analytics/load').then(function(data){if(!data.length){c.innerHTML='<div class="info-box">Нет данных</div>';return}
  var allDates=new Set();data.forEach(function(r){r.day_loads.forEach(function(d){allDates.add(d.label)})});
  var today=new Date();for(var i=0;i<14;i++){var dt=new Date(today);dt.setDate(dt.getDate()+i);allDates.add(dt.toLocaleDateString('ru-RU',{day:'2-digit',month:'2-digit'}))}
  var dates=Array.from(allDates).sort(function(a,b){var da=a.split('.'),db=b.split('.');return(da[1]+da[0]).localeCompare(db[1]+db[0])});
  var h='<div class="section-hdr">📈 Загруженность</div><div class="info-box"><span style="color:var(--err)">■</span> >80% | <span style="color:var(--warn)">■</span> 50-80% | <span style="color:var(--info)">■</span> <50%</div>'+
  '<div class="tbl-wrap" style="max-height:75vh"><table><thead><tr><th style="min-width:180px;position:sticky;left:0;background:var(--s2);z-index:2">Участок</th>'+
    dates.map(function(d){return '<th style="min-width:50px;text-align:center;font-size:.7em">'+d+'</th>'}).join('')+'<th>Итого</th></tr></thead><tbody>';
  data.forEach(function(r){var loadMap={};r.day_loads.forEach(function(d){loadMap[d.label]=d});
    h+='<tr><td style="position:sticky;left:0;background:var(--s1);z-index:1;font-weight:600">'+r.resource_name+'<div style="font-size:.75em;color:var(--text3)">'+r.ops_count+' оп | '+fmtMinToH(r.total_min)+'</div></td>';
    dates.forEach(function(d){var dl=loadMap[d];if(dl){var cls=dl.pct>=80?'load-100':dl.pct>=50?'load-80':dl.pct>0?'load-50':'load-0';
      h+='<td style="text-align:center" title="'+d+': '+fmtMinToH(dl.minutes)+' ('+dl.pct+'%)"><div class="load-bar '+cls+'" style="width:'+Math.max(4,dl.pct)+'%"></div><div style="font-size:.7em;color:var(--text3)">'+dl.pct+'%</div></td>'}
    else h+='<td style="text-align:center"><div class="load-bar load-0" style="width:4px"></div></td>'});
    h+='<td style="font-weight:600">'+r.days_needed+' дн.</td></tr>'});
  h+='</tbody></table></div>';c.innerHTML=h})}

// ═══ КЛИЕНТЫ ═══
var custSearch='',custSearchTimer=null;
function pgCustomers(c){api('/api/customers?search='+encodeURIComponent(custSearch)).then(function(custs){
  c.innerHTML='<div class="toolbar">'+(hasPerm('cust.create')?'<button class="btn primary" onclick="modalCust()">+ Клиент</button>':'')+
    '<span class="spacer"></span>'+
    '<input class="ctl" id="custSearchInput" style="width:280px" placeholder="🔍 Поиск по названию..." value="'+esc(custSearch)+'"></div>'+
  '<div class="tbl-wrap"><table><thead><tr><th>Название</th><th>Сокр.</th><th>ИНН</th><th>Контакт</th><th>Тел.</th><th>Email</th><th></th></tr></thead>'+
  '<tbody>'+custs.map(function(cu){return '<tr><td><strong>'+cu.name+'</strong></td><td>'+(cu.short_name||'—')+'</td><td>'+(cu.inn||'—')+'</td>'+
    '<td>'+(cu.contact_person||'—')+'</td><td>'+(cu.phone||'—')+'</td><td>'+(cu.email||'—')+'</td>'+
    '<td>'+(hasPerm('cust.edit')?'<button class="btn sm" onclick="modalCust('+cu.id+')">✏</button>':'')+'</td></tr>'}).join('')+'</tbody></table></div>';
  var inp=document.getElementById('custSearchInput');
  if(inp){inp.addEventListener('input',function(){custSearch=this.value;clearTimeout(custSearchTimer);custSearchTimer=setTimeout(function(){pgCustomers(document.getElementById('mainContent'))},400)});
    inp.focus();inp.setSelectionRange(inp.value.length,inp.value.length)}
  })}
function modalCust(cid){var p1=cid?api('/api/customers'):Promise.resolve(null);p1.then(function(cs){var cu=cs?cs.find(function(x){return x.id===cid}):null;
  openModal('<h2>'+(cu?'✏':'+')+' Клиент</h2>'+
  '<div class="form-row"><div><label>Название</label><input id="fcu_name" value="'+(cu?cu.name:'')+'"></div><div><label>Сокр.</label><input id="fcu_short" value="'+(cu?cu.short_name:'')+'"></div></div>'+
  '<div class="form-row"><div><label>ИНН</label><input id="fcu_inn" value="'+(cu?cu.inn:'')+'"></div><div><label>Контакт</label><input id="fcu_cp" value="'+(cu?cu.contact_person:'')+'"></div></div>'+
  '<div class="form-row"><div><label>Тел.</label><input id="fcu_ph" value="'+(cu?cu.phone:'')+'"></div><div><label>Email</label><input id="fcu_em" value="'+(cu?cu.email:'')+'"></div></div>'+
  '<div class="form-row full"><div><label>Адрес</label><textarea id="fcu_addr" rows="2">'+(cu?cu.address||'':'')+'</textarea></div></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="saveCust('+(cid||0)+')">Сохранить</button></div>')})}
function saveCust(cid){var b={name:document.getElementById('fcu_name').value,short_name:document.getElementById('fcu_short').value,
  inn:document.getElementById('fcu_inn').value,contact_person:document.getElementById('fcu_cp').value,
  phone:document.getElementById('fcu_ph').value,email:document.getElementById('fcu_em').value,address:document.getElementById('fcu_addr').value};
  if(cid)b.id=cid;api('/api/customers/save','POST',b).then(function(){closeModal();toast('OK','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}

// ═══ РЕСУРСЫ (+ фильтр по типу + удаление) ═══
var resTypeFilter='';
function pgResources(c){api('/api/resources').then(function(rs){
  var types=[];rs.forEach(function(r){if(types.indexOf(r.type)<0)types.push(r.type)});types.sort();
  var filtered=resTypeFilter?rs.filter(function(r){return r.type===resTypeFilter}):rs;
  c.innerHTML='<div class="toolbar">'+(hasPerm('res.create')?'<button class="btn primary" onclick="modalRes()">+ Ресурс</button>':'')+'</div>'+
  '<div class="filter-bar"><label>Тип:</label><select onchange="resTypeFilter=this.value;pgResources(document.getElementById(\'mainContent\'))">'+
    '<option value="">Все</option>'+types.map(function(t){return '<option '+(resTypeFilter===t?'selected':'')+'>'+t+'</option>'}).join('')+'</select></div>'+
  '<div class="tbl-wrap"><table><thead><tr><th>Название</th><th>Тип</th><th>Код</th><th>Операции</th><th>Смена</th><th>Дост.</th><th></th></tr></thead>'+
  '<tbody>'+filtered.map(function(r){return '<tr><td><strong>'+r.name+'</strong></td><td>'+r.type+'</td><td>'+(r.code||'—')+'</td>'+
    '<td style="font-size:.8em">'+((r.allowed_ops||[]).join(', ')||'—')+'</td><td>'+r.shift_hours+'ч × '+r.shifts_per_day+'см</td><td>'+(r.available?'✅':'❌')+'</td>'+
    '<td>'+(hasPerm('res.edit')?'<button class="btn sm" onclick="modalRes('+r.id+')">✏</button>':'')+
    (hasPerm('res.delete')?'<button class="btn sm" onclick="delRes('+r.id+')" style="color:var(--err)">🗑</button>':'')+'</td></tr>'}).join('')+'</tbody></table></div>'})}
function delRes(rid){if(!confirm('Удалить ресурс?'))return;api('/api/resources/delete','POST',{id:rid}).then(function(){toast('Удалено','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}
function modalRes(rid){api('/api/op-types').then(function(opTypes){
  var p1=rid?api('/api/resources'):Promise.resolve(null);p1.then(function(rs){var r=rs?rs.find(function(x){return x.id===rid}):null;
  var curOps=r?r.allowed_ops:[];var activeOps=opTypes.filter(function(o){return o.is_active});
  var RES_TYPES=['Лазерный станок','Плазменный станок','Координатно-пробивной','Листогиб','Сверлильный','Фрезерный','Токарный','Сварочный пост','Сборочный пост','Покрасочная камера','Финишный участок','ОТК'];
  var typeOpts=RES_TYPES.map(function(t){return{v:t,t:t}});
  openModal('<h2>'+(r?'✏':'+')+' Ресурс</h2>'+
  '<div class="form-row"><div><label>Название</label><input id="frs_name" value="'+(r?r.name:'')+'"></div><div><label>Код</label><input id="frs_code" value="'+(r?r.code:'')+'"></div></div>'+
  '<div class="form-row"><div><label>Тип</label>'+SS('frs_type',typeOpts,r?r.type:RES_TYPES[0],'Тип')+'</div>'+
    '<div><label>Доступен</label><select id="frs_av"><option value="true" '+(!r||r.available?'selected':'')+'>Да</option><option value="false" '+(r&&!r.available?'selected':'')+'>Нет</option></select></div></div>'+
  '<div class="form-row"><div><label>Смена (ч)</label><input type="number" id="frs_sh" step="0.5" value="'+(r?r.shift_hours:8)+'"></div>'+
    '<div><label>Смен/сутки</label><input type="number" id="frs_shd" min="1" value="'+(r?r.shifts_per_day:1)+'"></div></div>'+
  '<div class="form-row full"><div><label>Описание</label><textarea id="frs_desc" rows="2">'+(r?r.description:'')+'</textarea></div></div>'+
  '<div class="section-hdr">Допустимые операции</div>'+
  '<div class="check-grid">'+activeOps.map(function(ot){return '<label><input type="checkbox" class="frs_op" value="'+ot.name+'" '+(curOps.indexOf(ot.name)>=0?'checked':'')+'> '+ot.name+'</label>'}).join('')+'</div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="saveRes('+(rid||0)+')">Сохранить</button></div>')})})}
function saveRes(rid){var ops=Array.from(document.querySelectorAll('.frs_op:checked')).map(function(cb){return cb.value});
  var b={name:document.getElementById('frs_name').value,code:document.getElementById('frs_code').value,
    resource_type:ssVal('frs_type'),is_available:document.getElementById('frs_av').value==='true',
    shift_hours:+document.getElementById('frs_sh').value,shifts_per_day:+document.getElementById('frs_shd').value,
    description:document.getElementById('frs_desc').value,allowed_ops:ops};if(rid)b.id=rid;
  api('/api/resources/save','POST',b).then(function(){closeModal();toast('OK','ok');refreshPage()}).catch(function(e){toast(e.message,'err')})}

// ═══ ЛОГИ (многоуровневая фильтрация) ═══
var logAction='',logUserId=0;
function pgLogs(c){Promise.all([api('/api/logs?limit=300&action='+encodeURIComponent(logAction)+'&user_id='+logUserId),api('/api/logs/actions'),api('/api/users')]).then(function(arr){
  var logs=arr[0],actions=arr[1],users=arr[2];
  var actOpts=[{v:'',t:'Все'}].concat(actions.map(function(a){return{v:a,t:a}}));
  var userOpts=[{v:'0',t:'Все'}].concat(users.map(function(u){return{v:String(u.id),t:u.full_name}}));
  c.innerHTML='<div class="filter-bar"><label>Действие:</label><div style="min-width:200px">'+SS('log_act',actOpts,logAction,'Все',function(v){logAction=v;pgLogs(document.getElementById('mainContent'))})+'</div>'+
    '<label>Кто:</label><div style="min-width:200px">'+SS('log_user',userOpts,String(logUserId),'Все',function(v){logUserId=+v;pgLogs(document.getElementById('mainContent'))})+'</div></div>'+
  '<div class="tbl-wrap"><table><thead><tr><th>Дата</th><th>Кто</th><th>Действие</th><th>Объект</th><th>ID</th><th>Детали</th></tr></thead>'+
  '<tbody>'+logs.map(function(l){return '<tr><td style="font-family:monospace;font-size:.8em;white-space:nowrap">'+fmtDT(l.date)+'</td><td>'+l.user+'</td>'+
    '<td><strong style="color:var(--accent)">'+l.action+'</strong></td><td>'+(l.entity||'—')+'</td><td>'+(l.entity_id||'—')+'</td>'+
    '<td style="font-size:.85em;max-width:400px;overflow:hidden;text-overflow:ellipsis" title="'+esc(l.details)+'">'+l.details+'</td></tr>'}).join('')+'</tbody></table></div>'})}

// ═══ НАСТРОЙКИ ═══
var setTab='users';
function pgSettings(c){c.innerHTML='<div class="toolbar">'+
    '<button class="btn '+(setTab==='users'?'primary':'')+'" onclick="setTab=\'users\';pgSettings(document.getElementById(\'mainContent\'))">👤 Пользователи</button>'+
    '<button class="btn '+(setTab==='roles'?'primary':'')+'" onclick="setTab=\'roles\';pgSettings(document.getElementById(\'mainContent\'))">🛡 Роли</button>'+
    '<button class="btn '+(setTab==='grades'?'primary':'')+'" onclick="setTab=\'grades\';pgSettings(document.getElementById(\'mainContent\'))">🔬 Марки</button>'+
    '<button class="btn '+(setTab==='categories'?'primary':'')+'" onclick="setTab=\'categories\';pgSettings(document.getElementById(\'mainContent\'))">📂 Категории</button>'+
    '<button class="btn '+(setTab==='op_types'?'primary':'')+'" onclick="setTab=\'op_types\';pgSettings(document.getElementById(\'mainContent\'))">🔧 Типы операций</button></div>'+
  '<div id="setContent"></div>';var sc=document.getElementById('setContent');
  switch(setTab){case'users':setUsers(sc);break;case'roles':setRoles(sc);break;case'grades':setGrades(sc);break;
    case'categories':setCategories(sc);break;case'op_types':setOpTypes(sc);break}}

function setUsers(sc){api('/api/users').then(function(users){
  sc.innerHTML='<button class="btn primary" onclick="modalUser()" style="margin-bottom:12px">+ Пользователь</button>'+
  '<div class="tbl-wrap"><table><thead><tr><th>Логин</th><th>ФИО</th><th>Таб.</th><th>Роль</th><th>Акт.</th><th></th></tr></thead>'+
  '<tbody>'+users.map(function(u){return '<tr><td><strong>'+u.username+'</strong></td><td>'+u.full_name+'</td><td>'+(u.tab_number||'—')+'</td>'+
    '<td>'+statusBadge(u.role_label)+'</td><td>'+(u.is_active?'✅':'❌')+'</td>'+
    '<td><button class="btn sm" onclick="modalUser('+u.id+')">✏</button></td></tr>'}).join('')+'</tbody></table></div>'})}
function modalUser(uid){Promise.all([api('/api/resources'),api('/api/roles')]).then(function(arr){
  var resources=arr[0],roles=arr[1];
  var p1=uid?api('/api/users'):Promise.resolve(null);p1.then(function(us){var u=us?us.find(function(x){return x.id===uid}):null;
  var uSt=u?u.stations:[];var roleOpts=roles.map(function(r){return{v:r.role,t:r.display_name+' ('+r.role+')'}});
  openModal('<h2>'+(u?'✏':'+')+' Пользователь</h2>'+
  '<div class="form-row"><div><label>Логин</label><input id="fu_l" value="'+(u?u.username:'')+'" '+(u?'disabled':'')+'></div>'+
    '<div><label>Пароль '+(u?'(пусто=не менять)':'')+'</label><input type="password" id="fu_p"></div></div>'+
  '<div class="form-row"><div><label>ФИО</label><input id="fu_n" value="'+(u?u.full_name:'')+'"></div><div><label>Таб.№</label><input id="fu_t" value="'+(u?u.tab_number:'')+'"></div></div>'+
  '<div class="form-row"><div><label>Роль</label>'+SS('fu_r',roleOpts,u?u.role:'operator','Роль')+'</div>'+
    '<div><label>Активен</label><select id="fu_a"><option value="true" '+(!u||u.is_active?'selected':'')+'>Да</option><option value="false" '+(u&&!u.is_active?'selected':'')+'>Нет</option></select></div></div>'+
  '<div class="section-hdr">Участки</div>'+
  '<div class="check-grid">'+resources.map(function(r){return '<label><input type="checkbox" class="fus_st" value="'+r.id+'" '+(uSt.indexOf(r.id)>=0?'checked':'')+'> '+r.name+'</label>'}).join('')+'</div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="saveUser('+(uid||0)+')">Сохранить</button></div>')})})}
function saveUser(uid){var b={full_name:document.getElementById('fu_n').value,tab_number:document.getElementById('fu_t').value,
  role:ssVal('fu_r'),is_active:document.getElementById('fu_a').value==='true',
  stations:Array.from(document.querySelectorAll('.fus_st:checked')).map(function(cb){return+cb.value})};
  if(uid)b.id=uid;else{b.username=document.getElementById('fu_l').value;b.password=document.getElementById('fu_p').value}
  var pw=document.getElementById('fu_p').value;if(pw&&uid)b.password=pw;
  api('/api/users/save','POST',b).then(function(){closeModal();toast('OK','ok');pgSettings(document.getElementById('mainContent'))}).catch(function(e){toast(e.message,'err')})}

function setRoles(sc){Promise.all([api('/api/roles'),api('/api/permissions')]).then(function(arr){var roles=arr[0],perms=arr[1];
  var cats={};perms.forEach(function(p){if(!cats[p.category])cats[p.category]=[];cats[p.category].push(p)});var WO=['Материал','Детали'];
  var h='<button class="btn primary" onclick="modalNewRole()" style="margin-bottom:12px">+ Новая роль</button>';
  h+=roles.map(function(r){return '<div style="background:var(--s1);border-radius:var(--r);padding:16px;margin-bottom:12px;border:1px solid var(--s2)">'+
    '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">'+
      '<h3 style="color:var(--accent)">'+r.display_name+' <span style="color:var(--text3);font-size:.8em">('+r.role+')'+(r.is_system?' 🔒':'')+'</span></h3>'+
      '<div style="display:flex;gap:6px"><button class="btn sm primary" onclick="saveRole('+r.id+')">💾</button>'+
        (!r.is_system?'<button class="btn sm" style="color:var(--err)" onclick="deleteRole('+r.id+')">🗑</button>':'')+'</div></div>'+
    '<div class="form-row" style="margin-bottom:8px"><div><label>Имя</label><input id="role_disp_'+r.id+'" value="'+r.display_name+'" style="padding:6px;border:1px solid var(--s2);border-radius:4px;background:var(--bg);color:var(--text);width:100%"></div><div></div></div>'+
    '<div class="section-hdr" style="margin-top:8px">Списания</div>'+
    '<div class="check-grid" id="role_wo_'+r.id+'">'+WO.map(function(t){return '<label><input type="checkbox" value="'+t+'" '+(r.writeoff_types.indexOf(t)>=0?'checked':'')+'> '+t+'</label>'}).join('')+'</div>'+
    '<div class="section-hdr">Разрешения</div><div id="role_perms_'+r.id+'">'+
    Object.entries(cats).map(function(e){var cat=e[0],ps=e[1];return '<div style="margin-bottom:8px"><strong style="font-size:.8em;color:var(--text2);text-transform:uppercase">'+cat+'</strong>'+
      '<div class="check-grid">'+ps.map(function(p){return '<label title="'+p.code+'"><input type="checkbox" value="'+p.code+'" '+(r.permissions.indexOf(p.code)>=0?'checked':'')+'> '+p.name+'</label>'}).join('')+'</div></div>'}).join('')+
    '</div></div>'}).join('');sc.innerHTML=h})}
function saveRole(rid){var d=document.getElementById('role_disp_'+rid);
  var b={id:rid,permissions:Array.from(document.querySelectorAll('#role_perms_'+rid+' input:checked')).map(function(cb){return cb.value}),
    writeoff_types:Array.from(document.querySelectorAll('#role_wo_'+rid+' input:checked')).map(function(cb){return cb.value})};
  if(d)b.display_name=d.value;api('/api/roles/save','POST',b).then(function(){toast('OK','ok')}).catch(function(e){toast(e.message,'err')})}
function deleteRole(rid){if(!confirm('Удалить?'))return;api('/api/roles/delete','POST',{id:rid}).then(function(){toast('OK','ok');pgSettings(document.getElementById('mainContent'))}).catch(function(e){toast(e.message,'err')})}
function modalNewRole(){api('/api/permissions').then(function(perms){var cats={};perms.forEach(function(p){if(!cats[p.category])cats[p.category]=[];cats[p.category].push(p)});
  openModal('<h2>+ Роль</h2>'+
  '<div class="form-row"><div><label>Код (лат.)</label><input id="fnr_code" placeholder="supervisor"></div><div><label>Имя</label><input id="fnr_disp" placeholder="Супервайзер"></div></div>'+
  '<div class="section-hdr">Списания</div><div class="check-grid" id="fnr_wo">'+['Материал','Детали'].map(function(t){return '<label><input type="checkbox" value="'+t+'"> '+t+'</label>'}).join('')+'</div>'+
  '<div class="section-hdr">Разрешения</div><div id="fnr_perms">'+
  Object.entries(cats).map(function(e){var cat=e[0],ps=e[1];return '<div style="margin-bottom:8px"><strong style="font-size:.8em;color:var(--text2);text-transform:uppercase">'+cat+'</strong>'+
    '<div class="check-grid">'+ps.map(function(p){return '<label title="'+p.code+'"><input type="checkbox" value="'+p.code+'"> '+p.name+'</label>'}).join('')+'</div></div>'}).join('')+'</div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="saveNewRole()">Создать</button></div>')})}
function saveNewRole(){var code=document.getElementById('fnr_code').value.trim().toLowerCase();var disp=document.getElementById('fnr_disp').value.trim();
  if(!code||!disp){toast('Код и имя','err');return}
  api('/api/roles/save','POST',{role:code,display_name:disp,
    permissions:Array.from(document.querySelectorAll('#fnr_perms input:checked')).map(function(cb){return cb.value}),
    writeoff_types:Array.from(document.querySelectorAll('#fnr_wo input:checked')).map(function(cb){return cb.value})}).then(function(){
    closeModal();toast('OK','ok');pgSettings(document.getElementById('mainContent'))}).catch(function(e){toast(e.message,'err')})}

function setGrades(sc){api('/api/grades').then(function(grades){
  sc.innerHTML='<button class="btn primary" onclick="modalGrade()" style="margin-bottom:12px">+ Марка</button>'+
  '<div class="tbl-wrap"><table><thead><tr><th>Код</th><th>Название</th><th>ρ</th><th></th></tr></thead>'+
  '<tbody>'+grades.map(function(g){return '<tr><td><strong>'+g.code+'</strong></td><td>'+g.name+'</td><td>'+g.density+'</td>'+
    '<td><button class="btn sm" onclick="modalGrade('+g.id+')">✏</button><button class="btn sm" onclick="delGrade('+g.id+')">🗑</button></td></tr>'}).join('')+'</tbody></table></div>'})}
function modalGrade(gid){var p1=gid?api('/api/grades'):Promise.resolve(null);p1.then(function(gs){var g=gs?gs.find(function(x){return x.id===gid}):null;
  openModal('<h2>'+(g?'✏':'+')+' Марка</h2><div class="form-row triple">'+
    '<div><label>Код</label><input id="fg_code" value="'+(g?g.code:'')+'"></div><div><label>Название</label><input id="fg_name" value="'+(g?g.name:'')+'"></div>'+
    '<div><label>Плотность</label><input type="number" id="fg_dens" step="0.01" value="'+(g?g.density:7.85)+'"></div></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="saveGrade('+(gid||0)+')">Сохранить</button></div>')})}
function saveGrade(gid){var b={code:document.getElementById('fg_code').value,name:document.getElementById('fg_name').value,density:+document.getElementById('fg_dens').value};if(gid)b.id=gid;
  api('/api/grades/save','POST',b).then(function(){closeModal();toast('OK','ok');pgSettings(document.getElementById('mainContent'))}).catch(function(e){toast(e.message,'err')})}
function delGrade(gid){if(!confirm('Удалить?'))return;api('/api/grades/delete','POST',{id:gid}).then(function(){pgSettings(document.getElementById('mainContent'))})}

var catFields=[];
function setCategories(sc){api('/api/material-categories').then(function(cats){
  sc.innerHTML='<button class="btn primary" onclick="modalCat()" style="margin-bottom:12px">+ Категория</button>'+
  '<div class="tbl-wrap"><table><thead><tr><th>Название</th><th>Тип</th><th>Порядок</th><th>Поля</th><th></th></tr></thead>'+
  '<tbody>'+cats.map(function(c){var flds=(c.custom_fields||[]).map(function(f){return f.label+' ('+f.type+')'}).join(', ')||'—';
    return '<tr><td><strong>'+c.name+'</strong></td><td>'+c.type+'</td><td>'+c.sort_order+'</td>'+
    '<td style="font-size:.8em">'+flds+'</td>'+
    '<td><button class="btn sm" onclick="modalCat('+c.id+')">✏</button></td></tr>'}).join('')+'</tbody></table></div>'})}
function modalCat(cid){
  var p1=cid?api('/api/material-categories'):Promise.resolve(null);
  p1.then(function(cs){var c=cs?cs.find(function(x){return x.id===cid}):null;
  catFields=c?(c.custom_fields||[]).map(function(f){return{key:f.key,label:f.label,type:f.type}}):[];
  var TYPES=['Лист','Труба','Пруток','Метиз','Краска','Прочее'];
  openModal('<h2>'+(c?'✏':'+')+' Категория</h2>'+
  '<div class="form-row"><div><label>Название</label><input id="fcat_name" value="'+(c?c.name:'')+'"></div>'+
    '<div><label>Тип</label><select id="fcat_type">'+TYPES.map(function(t){return '<option '+(c&&c.type===t?'selected':'')+'>'+t+'</option>'}).join('')+'</select></div></div>'+
  '<div class="form-row"><div><label>Порядок</label><input type="number" id="fcat_sort" value="'+(c?c.sort_order:0)+'"></div>'+
    '<div><label>Описание</label><input id="fcat_desc" value="'+(c?c.description:'')+'"></div></div>'+
  '<div class="section-hdr">Кастомные параметры <button class="btn sm" onclick="addCatField()">+ Поле</button></div>'+
  '<div class="info-box">Типы: <strong>text</strong> — текст, <strong>number</strong> — число, <strong>grade_select</strong> — выбор марки</div>'+
  '<div id="fcat_fields_list"></div>'+
  '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="saveCat('+(cid||0)+')">Сохранить</button></div>');
  renderCatFields()})}
function renderCatFields(){var el=document.getElementById('fcat_fields_list');if(!el)return;
  var FT=['text','number','grade_select'];
  el.innerHTML=catFields.map(function(f,i){
    return '<div class="cf-row">'+
    '<div style="flex:0 0 120px"><label>Ключ</label><input value="'+f.key+'" onchange="catFields['+i+'].key=this.value" style="width:100%"></div>'+
    '<div style="flex:1"><label>Название</label><input value="'+f.label+'" onchange="catFields['+i+'].label=this.value" style="width:100%"></div>'+
    '<div style="flex:0 0 140px"><label>Тип</label><select onchange="catFields['+i+'].type=this.value" style="width:100%">'+
      FT.map(function(t){return '<option '+(f.type===t?'selected':'')+'>'+t+'</option>'}).join('')+'</select></div>'+
    '<button class="btn sm" onclick="catFields.splice('+i+',1);renderCatFields()" style="align-self:end">🗑</button></div>'
  }).join('')}
function addCatField(){catFields.push({key:'field_'+(catFields.length+1),label:'Параметр '+(catFields.length+1),type:'text'});renderCatFields()}
function saveCat(cid){var b={name:document.getElementById('fcat_name').value,type:document.getElementById('fcat_type').value,
  sort_order:+document.getElementById('fcat_sort').value,description:document.getElementById('fcat_desc').value,
  custom_fields:catFields};if(cid)b.id=cid;
  api('/api/material-categories/save','POST',b).then(function(){closeModal();toast('OK','ok');pgSettings(document.getElementById('mainContent'))}).catch(function(e){toast(e.message,'err')})}

function setOpTypes(sc){api('/api/op-types').then(function(opTypes){
  sc.innerHTML='<button class="btn primary" onclick="modalOpType()" style="margin-bottom:12px">+ Тип</button>'+
  '<div class="tbl-wrap"><table><thead><tr><th>Название</th><th>Порядок</th><th>Акт.</th><th></th></tr></thead>'+
  '<tbody>'+opTypes.map(function(ot){return '<tr><td><strong>'+ot.name+'</strong></td><td>'+ot.sort_order+'</td><td>'+(ot.is_active?'✅':'❌')+'</td>'+
    '<td><button class="btn sm" onclick="modalOpType('+ot.id+')">✏</button><button class="btn sm" onclick="delOpType('+ot.id+')">🗑</button></td></tr>'}).join('')+'</tbody></table></div>'})}
function modalOpType(otid){var p1=otid?api('/api/op-types'):Promise.resolve(null);p1.then(function(ots){var ot=ots?ots.find(function(x){return x.id===otid}):null;
   openModal('<h2>'+(ot?'✏':'+')+' Тип операции</h2>'+
   '<div class="form-row"><div><label>Название</label><input id="fot_name" value="'+(ot?ot.name:'')+'"></div><div><label>Порядок</label><input type="number" id="fot_sort" value="'+(ot?ot.sort_order:0)+'"></div></div>'+
   '<div class="form-row"><div><label>Списание на участке</label><select id="fot_wmode">'+['Детали','Материал','Материал+Детали','Нет'].map(function(m){return '<option '+(ot&&ot.writeoff_mode===m?'selected':'')+'>'+m+'</option>'}).join('')+'</select></div>'+
   '<div><label>Активен</label><select id="fot_active"><option value="true" '+(!ot||ot.is_active?'selected':'')+'>Да</option><option value="false" '+(ot&&!ot.is_active?'selected':'')+'>Нет</option></select></div></div>'+
   '<div class="actions"><button class="btn" onclick="closeModal()">Отмена</button><button class="btn primary" onclick="saveOpType('+(otid||0)+')">Сохранить</button></div>')})}
function saveOpType(otid){var b={name:document.getElementById('fot_name').value,sort_order:+document.getElementById('fot_sort').value,
  is_active:document.getElementById('fot_active').value==='true',writeoff_mode:document.getElementById('fot_wmode').value};if(otid)b.id=otid;
  api('/api/op-types/save','POST',b).then(function(){closeModal();toast('OK','ok');pgSettings(document.getElementById('mainContent'))}).catch(function(e){toast(e.message,'err')})}
function delOpType(otid){if(!confirm('Удалить?'))return;api('/api/op-types/delete','POST',{id:otid}).then(function(){toast('OK','ok');pgSettings(document.getElementById('mainContent'))}).catch(function(e){toast(e.message,'err')})}

</script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run_desktop():
    import webbrowser, time
    web_thread = threading.Thread(target=run_web, daemon=True)
    web_thread.start()
    time.sleep(2)
    webbrowser.open(f"http://localhost:{WEB_PORT}")
    log.info("Desktop mode: opened browser. Press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass


def run_web():
    import uvicorn
    app = create_app()
    log.info(f"Starting MetalWorks MES v5.6 at http://{WEB_HOST}:{WEB_PORT}")
    uvicorn.run(app, host=WEB_HOST, port=WEB_PORT, log_level="info")


def main():
    parser = argparse.ArgumentParser(description="MetalWorks MES v5.6")
    parser.add_argument("--mode", choices=["web", "desktop"], default="web")
    args = parser.parse_args()
    init_database()
    if args.mode == "desktop":
        run_desktop()
    else:
        run_web()


if __name__ == "__main__":
    main()