# =====================================================
# vrp_core.py — VRP Core Engine
# =====================================================
# รวม Part A + B + C + E เป็นไฟล์เดียว
# ลบ print() ทั้งหมด → ใช้ progress callback
# รวม helper ที่ซ้ำกัน → ชุดเดียว
# Logic คำนวณ 100% เหมือนเดิม
# =====================================================
import pandas as pd
import numpy as np
import math
import time
import random
import logging
from io import BytesIO
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional, Callable
from collections import defaultdict, Counter

logger = logging.getLogger(__name__)

# =====================================================
# SECTION 1: CONSTANTS (รวมศูนย์จากทุก Part)
# =====================================================
# --- Sheet Names ---
SHEET_DEPOTS = 'Depots'
SHEET_ORDERS = 'นำเข้าข้อมูลออเดอร์'
SHEET_CUSTOMERS = '01.ออเดอร์'
SHEET_VEHICLES = '02.ข้อมูลรถ'
SHEET_DISTANCE = '03.ระยะทาง'
SHEET_PRIORITY = 'Priority'

# --- Default Values ---
DEFAULT_SERVICE_TIME = 15
DEFAULT_UNLOAD_TIME = 30
DEFAULT_SPEED_KMH = 35
ROAD_FACTOR = 1.4
SECOND_DEPOT_LOADING_TIME = 60

# --- Overtime Control ---
MAX_OVERTIME_MINUTES = 120  # ← ใหม่: soft limit สำหรับ overtime

# --- Zone & Province ---
INNER_PROVINCES = ['กรุงเทพมหานคร', 'กทม', 'นนทบุรี', 'ปทุมธานี', 'สมุทรปราการ']
ZONE_PROVINCES = {
    'BKK': ['กรุงเทพมหานคร', 'กทม'],
    'CENTRAL': ['นนทบุรี', 'ปทุมธานี', 'สมุทรปราการ', 'นครปฐม', 'สมุทรสาคร'],
    'EAST': ['ชลบุรี', 'ระยอง', 'ฉะเชิงเทรา', 'ปราจีนบุรี'],
    'WEST': ['กาญจนบุรี', 'ราชบุรี', 'เพชรบุรี', 'สุพรรณบุรี'],
    'NORTH': ['พระนครศรีอยุธยา', 'สระบุรี', 'ลพบุรี', 'สิงห์บุรี', 'อ่างทอง']
}

# --- Default Priority Config ---
DEFAULT_PRIORITY_CONFIG = {
    1: {'name': 'Critical', 'deadline': 600, 'hard': True},
    2: {'name': 'Urgent', 'deadline': 720, 'hard': True},
    3: {'name': 'Afternoon', 'deadline': 900, 'hard': False},
    4: {'name': 'Normal', 'deadline': None, 'hard': False},
    5: {'name': 'Flexible', 'deadline': 1080, 'hard': False},
}

# =====================================================
# SECTION 2: PROGRESS CALLBACK SYSTEM
# =====================================================

# =====================================================
# SECTION 3: UTILITY FUNCTIONS (รวมศูนย์ — ชุดเดียว)
# =====================================================
def time_str_to_minutes(time_val) -> int:
    """แปลง time value (หลายรูปแบบ) → นาที"""
    if pd.isna(time_val):
        return 480
    if isinstance(time_val, (int, float)):
        if time_val < 1:
            # Excel serial time format (0.333 = 08:00)
            total_minutes = int(round(time_val * 24 * 60))
            return total_minutes
        return int(time_val) if time_val < 1440 else 480
    time_str = str(time_val).strip()
    if hasattr(time_val, 'hour'):
        return time_val.hour * 60 + time_val.minute
    try:
        if ':' in time_str:
            parts = time_str.split(':')
            hours = int(parts[0])
            minutes = int(parts[1]) if len(parts) > 1 else 0
            return hours * 60 + minutes
    except (ValueError, IndexError):
        pass
    return 480


def minutes_to_time_str(minutes) -> str:
    """แปลง minutes → HH:MM string"""
    if minutes is None or minutes == '':
        return "TW Close"
    try:
        minutes = int(minutes)
        hours = minutes // 60
        mins = minutes % 60
        return f"{hours:02d}:{mins:02d}"
    except (ValueError, TypeError):
        return str(minutes)


def haversine_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """คำนวณระยะทาง Haversine (กม.)"""
    if lat1 == 0 or lng1 == 0 or lat2 == 0 or lng2 == 0:
        return 0
    R = 6371
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lng = math.radians(lng2 - lng1)
    a = (math.sin(delta_lat / 2) ** 2 +
         math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lng / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def get_road_distance(lat1, lng1, lat2, lng2) -> float:
    """ระยะทางถนนโดยประมาณ (haversine × ROAD_FACTOR)"""
    return haversine_distance(lat1, lng1, lat2, lng2) * ROAD_FACTOR


def normalize_province(province: str) -> str:
    """ทำให้ชื่อจังหวัดเป็นมาตรฐาน"""
    if pd.isna(province):
        return ""
    province = str(province).strip()
    if province in ['กทม', 'กทม.', 'กรุงเทพ', 'กรุงเทพฯ', 'Bangkok']:
        return 'กรุงเทพมหานคร'
    return province


def determine_zone(district: str, province: str) -> str:
    """กำหนดโซนจากอำเภอ/จังหวัด"""
    province = normalize_province(province)
    for zone, provinces in ZONE_PROVINCES.items():
        if province in provinces:
            return zone
    return 'OTHER'


def is_inner_province(province: str) -> bool:
    """ตรวจว่าเป็นจังหวัดปริมณฑลหรือไม่"""
    province = normalize_province(province)
    return province in INNER_PROVINCES or province == 'กรุงเทพมหานคร'


def normalize_depot_id(depot_id) -> str:
    """ทำให้ depot ID เป็นมาตรฐาน"""
    if pd.isna(depot_id):
        return "03T7"
    depot_str = str(depot_id).strip()
    if depot_str in ['329', '0329']:
        return '329'
    if depot_str in ['03T7', '3T7', 'T7']:
        return '03T7'
    return depot_str


# --- Break Time Helper (ใหม่ — ใช้ร่วมกันทุกจุด) ---
def apply_break_time(current_time: int, travel_time: float,
                     vehicle, enabled: bool = True) -> float:
    """
    คำนวณ arrival time โดยรวม break time ของคนขับ

    Parameters:
        enabled=True  → คิด break time (ใช้ตอน final metrics / แสดงผล)
        enabled=False → ไม่คิด break time (ใช้ตอน ALNS optimize เพื่อให้เน้น distance)

    Logic (เมื่อ enabled=True):
    - ถ้าออกเดินทางก่อนพัก แต่ถึงหลังเวลาพักเริ่ม → เลื่อน arrival ออกไป break_duration
    - ถ้าตอนออกเดินทางอยู่ในช่วงพักอยู่แล้ว → เริ่มเดินทางหลังพักเสร็จ
    - กรณีอื่น → ไม่กระทบ
    """
    arrival = current_time + travel_time

    if not enabled:
        return arrival

    break_start = getattr(vehicle, 'break_start', 720)
    break_duration = getattr(vehicle, 'break_duration', 60)
    break_end = break_start + break_duration

    if current_time < break_start and arrival >= break_start:
        # กำลังเดินทางแล้วชนเวลาพัก → เลื่อน arrival
        arrival += break_duration
    elif break_start <= current_time < break_end:
        # ยังอยู่ในช่วงพัก → เริ่มเดินทางหลังพักเสร็จ
        arrival = break_end + travel_time

    return arrival


# --- Priority Helpers (ชุดเดียว) ---
def get_order_deadline(order, priority_config: Dict[int, dict] = None) -> int:
    """หา deadline ของ order ตาม priority"""
    if priority_config is None:
        priority_config = DEFAULT_PRIORITY_CONFIG
    priority = getattr(order, 'priority', 4)
    config = priority_config.get(priority, {})
    if config and config.get('deadline') is not None:
        return config['deadline']
    else:
        return getattr(order, 'time_close', 1020)


def get_priority_name(priority: int, priority_config: Dict[int, dict] = None) -> str:
    """หาชื่อ Priority"""
    if priority_config is None:
        priority_config = DEFAULT_PRIORITY_CONFIG
    config = priority_config.get(priority, {})
    return config.get('name', f'Priority {priority}')


def is_hard_priority(order, priority_config: Dict = None) -> bool:
    """ตรวจว่า order มี hard priority constraint หรือไม่"""
    if priority_config is None:
        priority_config = DEFAULT_PRIORITY_CONFIG
    priority = getattr(order, 'priority', 4)
    config = priority_config.get(priority, {})
    return config.get('hard', False)


def can_meet_deadline(order, arrival_time: int, priority_config: Dict[int, dict] = None) -> bool:
    """ตรวจว่าถึงทัน deadline หรือไม่"""
    deadline = get_order_deadline(order, priority_config)
    return arrival_time <= deadline


def sort_orders_by_priority(orders: List, priority_config: Dict[int, dict] = None) -> List:
    """เรียง orders ตาม priority → deadline → น้ำหนัก"""
    if priority_config is None:
        priority_config = DEFAULT_PRIORITY_CONFIG

    def get_sort_key(order):
        priority = order.priority
        deadline = get_order_deadline(order, priority_config)
        weight = order.weight_kg
        return (priority, deadline, -weight)

    return sorted(orders, key=get_sort_key)


def get_distance(from_name: str, to_name: str, dist_matrix: Dict,
                 locations: Dict) -> float:
    """ดึงระยะทางจาก matrix หรือคำนวณจาก coordinates"""
    if from_name in dist_matrix and to_name in dist_matrix.get(from_name, {}):
        dist = dist_matrix[from_name].get(to_name, 0)
        if dist > 0:
            return dist
    if to_name in dist_matrix and from_name in dist_matrix.get(to_name, {}):
        dist = dist_matrix[to_name].get(from_name, 0)
        if dist > 0:
            return dist
    if from_name in locations and to_name in locations:
        lat1, lng1 = locations[from_name]
        lat2, lng2 = locations[to_name]
        return get_road_distance(lat1, lng1, lat2, lng2)
    return 50


def get_default_end_depot(depots: Dict) -> 'Depot':
    """หา depot ที่เป็น default end"""
    for depot in depots.values():
        if depot.is_default_end:
            return depot
    return list(depots.values())[0] if depots else None


# =====================================================
# SECTION 4: DATA CLASSES
# =====================================================
@dataclass
class Depot:
    depot_id: str
    name: str
    lat: float
    lng: float
    district: str = ""
    province: str = ""
    is_default_end: bool = False


@dataclass
class Vehicle:
    vehicle_id: str
    driver_name: str
    vehicle_type: str
    max_weight_kg: float
    max_volume_cbm: float
    capacity_drop: int
    extra_drop_charge: float
    max_drops: int
    start_time: int
    end_time: int
    break_start: int
    break_duration: int
    fixed_cost: float
    variable_cost: float


@dataclass
class Order:
    order_id: str
    customer_name: str
    plant: str
    lat: float
    lng: float
    weight_kg: float
    volume_cbm: float
    district: str
    province: str
    ship_to_code: str
    dn_numbers: List[str]
    time_open: int = 480
    time_close: int = 1020
    service_time: int = DEFAULT_SERVICE_TIME
    unload_time: int = DEFAULT_UNLOAD_TIME
    priority: int = 4
    zone: str = ""
    zone_speed: float = DEFAULT_SPEED_KMH


# =====================================================
# BRIDGE CLASSES: เชื่อม vrp_core ↔ app.py (Streamlit)
# =====================================================
@dataclass
class VRPData:
    """Container สำหรับข้อมูลที่โหลดมาทั้งหมด"""
    depots: Dict[str, Depot] = field(default_factory=dict)
    vehicles: List[Vehicle] = field(default_factory=list)
    orders: List[Order] = field(default_factory=list)
    dist_matrix: Dict = field(default_factory=dict)
    locations: Dict = field(default_factory=dict)
    priority_config: Dict[int, dict] = field(default_factory=lambda: {
        1: {'name': 'Critical',  'deadline': 600,  'hard': True},
        2: {'name': 'Urgent',    'deadline': 720,  'hard': True},
        3: {'name': 'Afternoon', 'deadline': 900,  'hard': False},
        4: {'name': 'Normal',    'deadline': None,  'hard': False},
        5: {'name': 'Flexible',  'deadline': 1080, 'hard': False},
    })
    raw_order_lines: pd.DataFrame = field(
        default_factory=pd.DataFrame
    )
    skipped_orders: List[Order] = field(default_factory=list)  # ← C1: เพิ่มใหม่


@dataclass
class VRPResult:
    """Container สำหรับผลลัพธ์ทั้งหมด"""
    solution: Optional['ALNSSolution'] = None
    data: Optional[VRPData] = None
    operator_stats: Dict = field(default_factory=dict)
    elapsed_time: float = 0.0
    is_valid: bool = False
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class VRPProgress:
    """
    Progress reporter — รองรับ callback สำหรับ Streamlit
    ถ้าไม่ set callback → ทำงานเงียบๆ ไม่ crash
    """
    def __init__(self):
        self._callback = None

    def set_callback(self, callback_fn):
        """
        callback_fn(phase, message, percentage, details)
        - phase: 'load' | 'validate' | 'construct' | 'alns' | 'post' | 'done'
        - message: str
        - percentage: int 0-100
        - details: dict
        """
        self._callback = callback_fn

    def report(self, phase: str, message: str,
               percentage: int = 0, details: dict = None):
        if self._callback:
            self._callback(phase, message, percentage, details or {})


@dataclass
class RouteStop:
    order: Order
    arrival_time: int = 0
    departure_time: int = 0
    distance_from_prev: float = 0
    cumulative_distance: float = 0
    wait_time: int = 0
    is_late: bool = False


@dataclass
class Route:
    """
    เส้นทางการขนส่ง - Single Source of Truth + Cache
    stops เป็นแหล่งข้อมูลหลัก
    orders_by_depot, total_weight, total_volume ใช้ cache
    """
    vehicle: Vehicle
    stops: List[RouteStop] = field(default_factory=list)
    pickup_sequence: List[Depot] = field(default_factory=list)
    total_distance: float = 0
    pickup_distance: float = 0
    delivery_distance: float = 0
    return_distance: float = 0

    # ── Cache fields (ไม่แสดงใน repr) ──
    _cached_orders_by_depot: Dict = field(
        default_factory=dict, repr=False, compare=False
    )
    _cached_total_weight: float = field(
        default=0.0, repr=False, compare=False
    )
    _cached_total_volume: float = field(
        default=0.0, repr=False, compare=False
    )
    _cache_valid: bool = field(
        default=False, repr=False, compare=False
    )

    # ──────────────────────────────────
    # Cache management
    # ──────────────────────────────────
    def _invalidate_cache(self):
        """เรียกทุกครั้งที่ stops เปลี่ยน"""
        self._cache_valid = False

    def _rebuild_cache(self):
        """สร้าง cache ใหม่จาก stops (เรียกครั้งเดียว จนกว่า stops จะเปลี่ยน)"""
        result = {}
        total_w = 0.0
        total_v = 0.0
        for stop in self.stops:
            order = stop.order
            plant = order.plant
            if plant not in result:
                result[plant] = []
            result[plant].append(order)
            total_w += order.weight_kg
            total_v += order.volume_cbm
        self._cached_orders_by_depot = result
        self._cached_total_weight = total_w
        self._cached_total_volume = total_v
        self._cache_valid = True

    def _ensure_cache(self):
        """ถ้า cache ไม่ valid → rebuild"""
        if not self._cache_valid:
            self._rebuild_cache()

    # ──────────────────────────────────
    # Properties (ใช้ cache)
    # ──────────────────────────────────
    @property
    def orders(self) -> List[Order]:
        """ไม่ cache เพราะเรียกไม่บ่อย + ต้องการ list ใหม่ทุกครั้ง"""
        return [stop.order for stop in self.stops]

    @property
    def total_weight(self) -> float:
        self._ensure_cache()
        return self._cached_total_weight

    @property
    def total_volume(self) -> float:
        self._ensure_cache()
        return self._cached_total_volume

    @property
    def num_stops(self) -> int:
        return len(self.stops)

    @property
    def orders_by_depot(self) -> Dict[str, List[Order]]:
        self._ensure_cache()
        return self._cached_orders_by_depot

    @property
    def depot_ids(self) -> List[str]:
        return list(self.orders_by_depot.keys())

    @property
    def is_multi_depot(self) -> bool:
        return len(self.depot_ids) > 1

    # ──────────────────────────────────
    # Mutation methods (ทุกตัว invalidate cache)
    # ──────────────────────────────────
    def add_stop(self, stop: RouteStop, position: int,
                 depot: Depot = None):
        self.stops.insert(position, stop)
        if depot and not any(
            d.depot_id == depot.depot_id for d in self.pickup_sequence
        ):
            self.pickup_sequence.append(depot)
        self._invalidate_cache()

    def add_order(self, order: Order, position: int,
                  depot: Depot = None):
        stop = RouteStop(order=order)
        self.add_stop(stop, position, depot)

    def remove_order(self, order: Order) -> bool:
        original_len = len(self.stops)
        self.stops = [s for s in self.stops if s.order != order]
        if len(self.stops) < original_len:
            # ⭐ D2 FIX: normalize ทั้งสองฝั่งก่อนเปรียบเทียบ
            active_plants = set(
                normalize_depot_id(s.order.plant) for s in self.stops
            )
            self.pickup_sequence = [
                d for d in self.pickup_sequence
                if normalize_depot_id(d.depot_id) in active_plants
            ]
            self._invalidate_cache()
            return True
        return False

    def remove_order_at(self, index: int) -> Optional[Order]:
        if 0 <= index < len(self.stops):
            order = self.stops[index].order
            self.remove_order(order)
            return order
        return None

    def clear_all(self):
        self.stops.clear()
        self.pickup_sequence.clear()
        self.total_distance = 0
        self.pickup_distance = 0
        self.delivery_distance = 0
        self.return_distance = 0
        self._invalidate_cache()

    # ──────────────────────────────────
    # Query methods (ไม่แก้ cache)
    # ──────────────────────────────────
    def has_order(self, order: Order) -> bool:
        return any(s.order == order for s in self.stops)

    def is_empty(self) -> bool:
        return len(self.stops) == 0

    def get_orders_for_depot(self, depot_id: str) -> List[Order]:
        return [
            s.order for s in self.stops
            if s.order.plant == depot_id
        ]

    def recalc_weight_volume(self):
        """Backward compatibility — invalidate เพื่อ force rebuild"""
        self._invalidate_cache()

# =====================================================
# SECTION 5: DATA RESULT CONTAINER
# =====================================================

# =====================================================
# SECTION 6: INPUT VALIDATION (ใหม่ — ละเอียด 15+ จุด)
# =====================================================
def validate_input(excel_file) -> Tuple[bool, List[str], List[str]]:
    """
    ตรวจสอบข้อมูลก่อนรัน Solver
    Returns:
        (is_valid, errors, warnings)
        errors   = ❌ ห้ามรัน ต้องแก้ก่อน
        warnings = ⚠️ รันได้ แต่ผลอาจไม่สมบูรณ์
    """
    errors = []
    warnings = []

    # ─── อ่านรายชื่อ Sheet ───
    try:
        xl = pd.ExcelFile(excel_file)
        sheet_names = xl.sheet_names
    except Exception as e:
        errors.append(f"❌ ไม่สามารถเปิดไฟล์ Excel ได้ — กรุณาตรวจสอบว่าเป็นไฟล์ .xlsx ที่ถูกต้อง")
        return False, errors, warnings

    # ═══════════════════════════════════════════
    # CHECK 1: ตรวจว่ามีครบทุก Sheet
    # ═══════════════════════════════════════════
    required_sheets = {
        SHEET_DEPOTS: 'ข้อมูลคลังสินค้า',
        SHEET_ORDERS: 'รายการสินค้า',
        SHEET_CUSTOMERS: 'ข้อมูลลูกค้า',
        SHEET_VEHICLES: 'ข้อมูลรถ',
        SHEET_DISTANCE: 'ตารางระยะทาง',
    }
    for sheet, thai_name in required_sheets.items():
        if sheet not in sheet_names:
            errors.append(
                f"❌ ไม่พบแท็บ '{sheet}' ({thai_name}) — "
                f"กรุณาตรวจสอบชื่อ Sheet ในไฟล์ Excel "
                f"(พบแท็บ: {', '.join(sheet_names)})"
            )

    if SHEET_PRIORITY not in sheet_names:
        warnings.append(
            f"⚠️ ไม่พบแท็บ 'Priority' — ระบบจะใช้ค่า Priority เริ่มต้น "
            f"(Critical/Urgent/Afternoon/Normal/Flexible)"
        )

    # ถ้าขาด sheet หลัก ไม่ต้องตรวจต่อ
    if errors:
        return False, errors, warnings

    # ═══════════════════════════════════════════
    # CHECK 2: ตรวจคอลัมน์ในแต่ละ Sheet
    # ═══════════════════════════════════════════
    column_requirements = {
        SHEET_ORDERS: {
            'required': ['Customer Name', 'Plant', 'น้ำหนัก'],
            'recommended': ['Latitude', 'Longitude', 'ปริมาตร CBM', 'DN Number',
                           'อำเภอ', 'จังหวัด', 'Ship to Code']
        },
        SHEET_CUSTOMERS: {
            'required': ['Customer Name', 'Latitude', 'Longitude'],
            'recommended': ['Priority', 'เวลาร้านเปิด', 'เวลาร้านปิด',
                           'เวลาลงสินค้า', 'เวลาเซ็นเอกสาร', 'ความเร็วฉลี่ยโซน',
                           'น้ำหนักสินค้า KG', 'ปริมาตร CBM', 'อำเภอ', 'จังหวัด']
        },
        SHEET_VEHICLES: {
            'required': ['Vehicle ID', 'MaxKG', 'MaxCBM'],
            'recommended': ['Max Drop', 'Vehicle Start Time', 'Vehicle End Time',
                           'Driver Name', 'Type Truck', 'Break Start Time',
                           'Break Duration']
        },
        SHEET_DEPOTS: {
            'required': ['DC_ID', 'Latitude', 'Longitude'],
            'recommended': ['DC_Name', 'Is_Default_End']
        }
    }

    for sheet, checks in column_requirements.items():
        if sheet not in sheet_names:
            continue
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet, nrows=0)
            cols = [str(c).strip() for c in df.columns]
        except Exception:
            errors.append(f"❌ ไม่สามารถอ่านแท็บ '{sheet}' ได้")
            continue

        for col in checks['required']:
            if col not in cols:
                errors.append(
                    f"❌ แท็บ '{sheet}' ขาดคอลัมน์ '{col}' (จำเป็น) — "
                    f"กรุณาเพิ่มคอลัมน์นี้ "
                    f"(คอลัมน์ที่พบ: {', '.join(cols[:8])}...)"
                )
        for col in checks['recommended']:
            if col not in cols:
                warnings.append(
                    f"⚠️ แท็บ '{sheet}' ไม่มีคอลัมน์ '{col}' — ระบบจะใช้ค่าเริ่มต้น"
                )

    # ถ้ายังมี error จากคอลัมน์ ไม่ต้องตรวจ data
    if errors:
        return False, errors, warnings

    # ═══════════════════════════════════════════
    # CHECK 3: ตรวจข้อมูล Depots
    # ═══════════════════════════════════════════
    try:
        df_depots = pd.read_excel(excel_file, sheet_name=SHEET_DEPOTS)
        df_depots.columns = df_depots.columns.str.strip()

        if len(df_depots) == 0:
            errors.append("❌ แท็บ 'Depots' ไม่มีข้อมูล — กรุณาเพิ่มข้อมูลคลังสินค้าอย่างน้อย 1 แห่ง")
        else:
            for idx, row in df_depots.iterrows():
                dc_id = str(row.get('DC_ID', '')).strip()
                lat = row.get('Latitude', 0)
                lng = row.get('Longitude', 0)

                if pd.isna(lat) or pd.isna(lng) or float(lat or 0) == 0 or float(lng or 0) == 0:
                    errors.append(
                        f"❌ คลัง '{dc_id}' (แถวที่ {idx + 2}) มีพิกัดเป็น 0 หรือว่าง — "
                        f"กรุณาใส่ค่า Latitude/Longitude"
                    )
                else:
                    lat_val = float(lat)
                    lng_val = float(lng)
                    if not (5 <= lat_val <= 21):
                        warnings.append(
                            f"⚠️ คลัง '{dc_id}' มี Latitude = {lat_val} "
                            f"ซึ่งอยู่นอกประเทศไทย (5-21) — กรุณาตรวจสอบ"
                        )
                    if not (97 <= lng_val <= 106):
                        warnings.append(
                            f"⚠️ คลัง '{dc_id}' มี Longitude = {lng_val} "
                            f"ซึ่งอยู่นอกประเทศไทย (97-106) — กรุณาตรวจสอบ"
                        )

            # ตรวจว่ามี default end depot
            has_default = False
            for col in ['Is_Default_End', 'is_default_end', 'IsDefaultEnd']:
                if col in df_depots.columns:
                    has_default = df_depots[col].any()
                    break
            if not has_default:
                warnings.append(
                    "⚠️ ไม่มีคลังที่ตั้งเป็น Is_Default_End = TRUE — "
                    "ระบบจะใช้คลังแรกเป็นจุดกลับเริ่มต้น"
                )
    except Exception:
        errors.append("❌ ไม่สามารถอ่านข้อมูลแท็บ 'Depots' ได้")

    # ═══════════════════════════════════════════
    # CHECK 4: ตรวจข้อมูลรถ
    # ═══════════════════════════════════════════
    try:
        df_vehicles = pd.read_excel(excel_file, sheet_name=SHEET_VEHICLES)
        df_vehicles.columns = df_vehicles.columns.str.strip()

        if len(df_vehicles) == 0:
            errors.append("❌ แท็บ '02.ข้อมูลรถ' ไม่มีข้อมูล — กรุณาเพิ่มข้อมูลรถอย่างน้อย 1 คัน")
        else:
            for idx, row in df_vehicles.iterrows():
                vid = str(row.get('Vehicle ID', f'V{idx+1}')).strip()
                max_kg = row.get('MaxKG', 0)
                max_cbm = row.get('MaxCBM', 0)

                if pd.isna(max_kg) or float(max_kg or 0) <= 0:
                    errors.append(
                        f"❌ รถ '{vid}' (แถวที่ {idx + 2}) มี MaxKG = {max_kg} — "
                        f"กรุณาใส่ค่าน้ำหนักบรรทุกสูงสุดที่มากกว่า 0"
                    )
                if pd.isna(max_cbm) or float(max_cbm or 0) <= 0:
                    errors.append(
                        f"❌ รถ '{vid}' (แถวที่ {idx + 2}) มี MaxCBM = {max_cbm} — "
                        f"กรุณาใส่ค่าปริมาตรบรรทุกสูงสุดที่มากกว่า 0"
                    )

                # ตรวจ start < end time
                start_time = row.get('Vehicle Start Time')
                end_time = row.get('Vehicle End Time')
                if pd.notna(start_time) and pd.notna(end_time):
                    st = time_str_to_minutes(start_time)
                    et = time_str_to_minutes(end_time)
                    if st >= et:
                        warnings.append(
                            f"⚠️ รถ '{vid}' มีเวลาเริ่ม ({minutes_to_time_str(st)}) "
                            f">= เวลาสิ้นสุด ({minutes_to_time_str(et)}) — กรุณาตรวจสอบ"
                        )
    except Exception:
        errors.append("❌ ไม่สามารถอ่านข้อมูลแท็บ '02.ข้อมูลรถ' ได้")

    # ═══════════════════════════════════════════
    # CHECK 5: ตรวจข้อมูลออเดอร์
    # ═══════════════════════════════════════════
    try:
        df_orders = pd.read_excel(excel_file, sheet_name=SHEET_ORDERS)
        df_orders.columns = df_orders.columns.str.strip()

        if len(df_orders) == 0:
            errors.append(
                "❌ แท็บ 'นำเข้าข้อมูลออเดอร์' ไม่มีข้อมูล — กรุณาเพิ่มรายการสินค้า"
            )
        else:
            # ตรวจ Customer Name ว่าง
            if 'Customer Name' in df_orders.columns:
                blank_count = df_orders['Customer Name'].isna().sum()
                blank_count += (df_orders['Customer Name'].astype(str).str.strip() == '').sum()
                if blank_count > 0:
                    warnings.append(
                        f"⚠️ พบ {blank_count} แถวที่ Customer Name ว่าง "
                        f"ในแท็บ 'นำเข้าข้อมูลออเดอร์' — แถวเหล่านี้จะถูกข้ามไป"
                    )

            # ตรวจน้ำหนัก <= 0
            if 'น้ำหนัก' in df_orders.columns:
                zero_weight = (pd.to_numeric(df_orders['น้ำหนัก'], errors='coerce').fillna(0) <= 0).sum()
                if zero_weight > 0:
                    warnings.append(
                        f"⚠️ พบ {zero_weight} แถวที่น้ำหนัก ≤ 0 — อาจทำให้การจัดรถไม่ถูกต้อง"
                    )

            # ตรวจ Plant ว่าตรงกับ Depots
            if 'Plant' in df_orders.columns:
                depot_ids_in_depots = set()
                try:
                    df_dep = pd.read_excel(excel_file, sheet_name=SHEET_DEPOTS)
                    df_dep.columns = df_dep.columns.str.strip()
                    for col in ['DC_ID', 'dc_id', 'Depot_ID']:
                        if col in df_dep.columns:
                            depot_ids_in_depots = set(
                                normalize_depot_id(x) for x in df_dep[col].dropna()
                            )
                            break
                except Exception:
                    pass

                if depot_ids_in_depots:
                    plants_in_orders = set(
                        normalize_depot_id(x) for x in df_orders['Plant'].dropna()
                    )
                    missing_plants = plants_in_orders - depot_ids_in_depots
                    if missing_plants:
                        warnings.append(
                            f"⚠️ พบ Plant ในออเดอร์ที่ไม่มีในแท็บ Depots: {missing_plants} — "
                            f"ออเดอร์จาก Plant เหล่านี้จะถูกข้ามไปเป็น Unassigned "
                            f"(คลังที่มี: {depot_ids_in_depots})"
                        )
    except Exception:
        errors.append("❌ ไม่สามารถอ่านข้อมูลแท็บ 'นำเข้าข้อมูลออเดอร์' ได้")

    # ═══════════════════════════════════════════
    # CHECK 6: ตรวจข้อมูลลูกค้า
    # ═══════════════════════════════════════════
    try:
        df_customers = pd.read_excel(excel_file, sheet_name=SHEET_CUSTOMERS)
        df_customers.columns = df_customers.columns.str.strip()

        if len(df_customers) == 0:
            errors.append(
                "❌ แท็บ '01.ออเดอร์' ไม่มีข้อมูล — กรุณาเพิ่มข้อมูลลูกค้า"
            )
        else:
            # ตรวจพิกัด
            zero_coord_count = 0
            out_of_range_count = 0
            for idx, row in df_customers.iterrows():
                name = str(row.get('Customer Name', '')).strip()
                lat = pd.to_numeric(row.get('Latitude', 0), errors='coerce') or 0
                lng = pd.to_numeric(row.get('Longitude', 0), errors='coerce') or 0

                if lat == 0 or lng == 0:
                    zero_coord_count += 1
                elif not (5 <= lat <= 21) or not (97 <= lng <= 106):
                    out_of_range_count += 1

            if zero_coord_count > 0:
                warnings.append(
                    f"⚠️ พบ {zero_coord_count} ลูกค้าที่พิกัดเป็น 0 ในแท็บ '01.ออเดอร์' — "
                    f"ระบบจะพยายามใช้พิกัดจากแท็บ 'นำเข้าข้อมูลออเดอร์' แทน"
                )
            if out_of_range_count > 0:
                warnings.append(
                    f"⚠️ พบ {out_of_range_count} ลูกค้าที่พิกัดอยู่นอกประเทศไทย — กรุณาตรวจสอบ"
                )

            # ตรวจ Priority อยู่ใน range 1-5
            if 'Priority' in df_customers.columns:
                priorities = pd.to_numeric(df_customers['Priority'], errors='coerce').dropna()
                invalid_priorities = priorities[(priorities < 1) | (priorities > 5)]
                if len(invalid_priorities) > 0:
                    warnings.append(
                        f"⚠️ พบ Priority ที่ไม่อยู่ในช่วง 1-5: {invalid_priorities.unique().tolist()} — "
                        f"ระบบจะใช้ค่า 4 (Normal) แทน"
                    )
    except Exception:
        errors.append("❌ ไม่สามารถอ่านข้อมูลแท็บ '01.ออเดอร์' ได้")

    # ═══════════════════════════════════════════
    # CHECK 7: ตรวจ Distance Matrix
    # ═══════════════════════════════════════════
    try:
        df_dist = pd.read_excel(excel_file, sheet_name=SHEET_DISTANCE, index_col=0)

        if df_dist.shape[0] == 0 or df_dist.shape[1] == 0:
            errors.append(
                "❌ แท็บ '03.ระยะทาง' ว่างเปล่า — กรุณาเพิ่มตารางระยะทาง"
            )
        else:
            matrix_names = set(str(x).strip() for x in df_dist.index)
            matrix_col_names = set(str(x).strip() for x in df_dist.columns)

            # ตรวจว่า Depot อยู่ใน Matrix
            try:
                df_dep = pd.read_excel(excel_file, sheet_name=SHEET_DEPOTS)
                df_dep.columns = df_dep.columns.str.strip()
                for col in ['DC_Name', 'dc_name', 'Name']:
                    if col in df_dep.columns:
                        depot_names = set(str(x).strip() for x in df_dep[col].dropna())
                        missing_depots = depot_names - matrix_names
                        if missing_depots:
                            errors.append(
                                f"❌ คลังต่อไปนี้ไม่อยู่ในตารางระยะทาง: {missing_depots} — "
                                f"กรุณาเพิ่มในแท็บ '03.ระยะทาง'"
                            )
                        break
            except Exception:
                pass

            # ตรวจว่าลูกค้าอยู่ใน Matrix
            try:
                df_cust = pd.read_excel(excel_file, sheet_name=SHEET_CUSTOMERS)
                df_cust.columns = df_cust.columns.str.strip()
                if 'Customer Name' in df_cust.columns:
                    customer_names = set(
                        str(x).strip() for x in df_cust['Customer Name'].dropna()
                    )
                    missing_customers = customer_names - matrix_names
                    if missing_customers:
                        # แสดงแค่ 5 ตัวอย่าง
                        sample = list(missing_customers)[:5]
                        suffix = f" และอีก {len(missing_customers) - 5} ราย" if len(missing_customers) > 5 else ""
                        warnings.append(
                            f"⚠️ พบ {len(missing_customers)} ลูกค้าที่ไม่อยู่ในตารางระยะทาง: "
                            f"{sample}{suffix} — "
                            f"ระบบจะคำนวณระยะทางจากพิกัดแทน (อาจไม่แม่นยำ)"
                        )
            except Exception:
                pass

            # ตรวจค่าผิดปกติ
            numeric_vals = pd.to_numeric(df_dist.values.flatten(), errors='coerce')
            numeric_vals = numeric_vals[~np.isnan(numeric_vals)]
            if len(numeric_vals) > 0:
                negative_count = (numeric_vals < 0).sum()
                if negative_count > 0:
                    errors.append(
                        f"❌ พบระยะทางติดลบ {negative_count} จุดในตาราง '03.ระยะทาง' — "
                        f"กรุณาแก้ไขให้เป็นค่าบวก"
                    )
                extreme_count = (numeric_vals > 2000).sum()
                if extreme_count > 0:
                    warnings.append(
                        f"⚠️ พบระยะทางเกิน 2,000 กม. จำนวน {extreme_count} จุด — "
                        f"กรุณาตรวจสอบว่าถูกต้อง (หน่วยเป็น กม.)"
                    )
    except Exception:
        errors.append("❌ ไม่สามารถอ่านข้อมูลแท็บ '03.ระยะทาง' ได้")

    # ═══════════════════════════════════════════
    # CHECK 8: ตรวจ Priority Config (ถ้ามี)
    # ═══════════════════════════════════════════
    if SHEET_PRIORITY in sheet_names:
        try:
            df_priority = pd.read_excel(excel_file, sheet_name=SHEET_PRIORITY)
            df_priority.columns = df_priority.columns.str.strip()

            if len(df_priority) == 0:
                warnings.append(
                    "⚠️ แท็บ 'Priority' ว่างเปล่า — ระบบจะใช้ค่าเริ่มต้น"
                )
            else:
                if 'Priority' in df_priority.columns:
                    priorities = df_priority['Priority'].dropna().astype(int).tolist()
                    if not all(1 <= p <= 5 for p in priorities):
                        warnings.append(
                            f"⚠️ Priority ในแท็บ 'Priority' ควรอยู่ในช่วง 1-5 "
                            f"(พบ: {priorities})"
                        )
        except Exception:
            warnings.append("⚠️ ไม่สามารถอ่านแท็บ 'Priority' — ระบบจะใช้ค่าเริ่มต้น")

    # ═══════════════════════════════════════════
    # CHECK 9: ตรวจ Cross-Sheet Consistency
    # ═══════════════════════════════════════════
    try:
        df_orders_raw = pd.read_excel(excel_file, sheet_name=SHEET_ORDERS)
        df_orders_raw.columns = df_orders_raw.columns.str.strip()
        df_customers_check = pd.read_excel(excel_file, sheet_name=SHEET_CUSTOMERS)
        df_customers_check.columns = df_customers_check.columns.str.strip()

        if 'Customer Name' in df_orders_raw.columns and 'Customer Name' in df_customers_check.columns:
            order_customers = set(
                str(x).strip() for x in df_orders_raw['Customer Name'].dropna()
            )
            master_customers = set(
                str(x).strip() for x in df_customers_check['Customer Name'].dropna()
            )
            missing_in_master = order_customers - master_customers
            if missing_in_master:
                sample = list(missing_in_master)[:3]
                suffix = f" และอีก {len(missing_in_master) - 3} ราย" if len(missing_in_master) > 3 else ""
                warnings.append(
                    f"⚠️ พบ {len(missing_in_master)} ลูกค้าในแท็บ 'นำเข้าข้อมูลออเดอร์' "
                    f"ที่ไม่อยู่ในแท็บ '01.ออเดอร์': {sample}{suffix} — "
                    f"ลูกค้าเหล่านี้จะไม่ได้รับข้อมูล Time Window, Priority, ความเร็วโซน"
                )
    except Exception:
        pass

    # ═══════════════════════════════════════════
    # CHECK 10: ตรวจ Capacity Feasibility
    # ═══════════════════════════════════════════
    try:
        df_orders_cap = pd.read_excel(excel_file, sheet_name=SHEET_ORDERS)
        df_orders_cap.columns = df_orders_cap.columns.str.strip()
        df_vehicles_cap = pd.read_excel(excel_file, sheet_name=SHEET_VEHICLES)
        df_vehicles_cap.columns = df_vehicles_cap.columns.str.strip()

        total_order_weight = pd.to_numeric(
            df_orders_cap.get('น้ำหนัก', pd.Series([0])), errors='coerce'
        ).fillna(0).sum()
        total_vehicle_capacity = pd.to_numeric(
            df_vehicles_cap.get('MaxKG', pd.Series([0])), errors='coerce'
        ).fillna(0).sum()

        if total_vehicle_capacity > 0 and total_order_weight > total_vehicle_capacity:
            warnings.append(
                f"⚠️ น้ำหนักสินค้ารวม ({total_order_weight:,.0f} kg) มากกว่า "
                f"ความจุรถรวม ({total_vehicle_capacity:,.0f} kg) — "
                f"อาจมีออเดอร์ที่จัดไม่ลง"
            )

        # ตรวจ single order > max vehicle capacity
        if 'น้ำหนัก' in df_orders_cap.columns and 'MaxKG' in df_vehicles_cap.columns:
            max_single_vehicle = pd.to_numeric(
                df_vehicles_cap['MaxKG'], errors='coerce'
            ).fillna(0).max()

            if max_single_vehicle > 0:
                # Aggregate by customer
                if 'Customer Name' in df_orders_cap.columns:
                    customer_weights = df_orders_cap.groupby('Customer Name')['น้ำหนัก'].apply(
                        lambda x: pd.to_numeric(x, errors='coerce').fillna(0).sum()
                    )
                    oversized = customer_weights[customer_weights > max_single_vehicle]
                    if len(oversized) > 0:
                        sample = oversized.head(3)
                        warnings.append(
                            f"⚠️ พบ {len(oversized)} ลูกค้าที่น้ำหนักรวมเกินรถที่ใหญ่ที่สุด "
                            f"({max_single_vehicle:,.0f} kg): "
                            f"{dict(sample)} — "
                            f"ออเดอร์เหล่านี้จะถูกข้ามไปเป็น Unassigned"
                        )
    except Exception:
        pass

    # ═══════════════════════════════════════════
    # FINAL RESULT
    # ═══════════════════════════════════════════
    is_valid = len(errors) == 0
    return is_valid, errors, warnings


# =====================================================
# SECTION 7: DATA LOADING FUNCTIONS (ลบ print ทั้งหมด)
# =====================================================
def get_default_depots() -> Dict[str, Depot]:
    """ค่าเริ่มต้น Depot"""
    return {
        '03T7': Depot(depot_id='03T7', name='DC รังสิต',
                      lat=14.02567, lng=100.61411,
                      district='คลองหลวง', province='ปทุมธานี',
                      is_default_end=True),
        '329': Depot(depot_id='329', name='DC ธัญบุรี',
                     lat=14.06792, lng=100.86592,
                     district='ธัญบุรี', province='ปทุมธานี',
                     is_default_end=False)
    }


def load_depots(excel_file) -> Dict[str, Depot]:
    """โหลดข้อมูล Depot จาก Excel"""
    try:
        df = pd.read_excel(excel_file, sheet_name=SHEET_DEPOTS)
        df.columns = df.columns.str.strip()
    except Exception:
        return get_default_depots()

    depots = {}
    for idx, row in df.iterrows():
        try:
            depot_id = None
            for col_name in ['DC_ID', 'dc_id', 'Depot_ID', 'depot_id']:
                if col_name in df.columns:
                    depot_id = str(row[col_name]).strip()
                    break
            if depot_id is None:
                depot_id = f'D{idx}'
            depot_id = normalize_depot_id(depot_id)

            depot_name = None
            for col_name in ['DC_Name', 'dc_name', 'Name', 'name']:
                if col_name in df.columns:
                    depot_name = str(row[col_name]).strip()
                    break
            if depot_name is None:
                depot_name = f'Depot {depot_id}'

            lat, lng = 0.0, 0.0
            for lat_col in ['Latitude', 'latitude', 'Lat', 'lat']:
                if lat_col in df.columns:
                    lat = float(row[lat_col] or 0)
                    break
            for lng_col in ['Longitude', 'longitude', 'Long', 'lng']:
                if lng_col in df.columns:
                    lng = float(row[lng_col] or 0)
                    break

            is_default = False
            for col in ['Is_Default_End', 'is_default_end', 'IsDefaultEnd']:
                if col in df.columns:
                    val = row[col]
                    is_default = bool(val) if pd.notna(val) else False
                    break

            depot = Depot(
                depot_id=depot_id, name=depot_name,
                lat=lat, lng=lng,
                district=str(row.get('District', '') or '').strip(),
                province=str(row.get('Province', '') or '').strip(),
                is_default_end=is_default
            )
            depots[depot_id] = depot
        except Exception:
            continue

    if not depots or all(d.lat == 0 for d in depots.values()):
        return get_default_depots()
    return depots


def get_default_vehicles() -> List[Vehicle]:
    """ค่าเริ่มต้น Vehicles"""
    vehicles = []
    for i in range(10):
        vehicles.append(Vehicle(
            vehicle_id=f'V{i + 1}', driver_name='Default Driver',
            vehicle_type='4W', max_weight_kg=1500, max_volume_cbm=5,
            capacity_drop=7, extra_drop_charge=150, max_drops=10,
            start_time=480, end_time=1020, break_start=720, break_duration=60,
            fixed_cost=1360, variable_cost=14
        ))
    return vehicles


def load_vehicles(excel_file) -> List[Vehicle]:
    """โหลดข้อมูล Vehicle จาก Excel"""
    try:
        df = pd.read_excel(excel_file, sheet_name=SHEET_VEHICLES)
        df.columns = df.columns.str.strip()
    except Exception:
        return get_default_vehicles()

    vehicles = []
    for idx, row in df.iterrows():
        try:
            fixed_cost_col = [c for c in df.columns if 'Fix cost' in str(c) or 'fixed' in str(c).lower()]
            variable_cost_col = [c for c in df.columns if 'Variable cost' in str(c) or 'variable' in str(c).lower()]
            fixed_cost = float(row[fixed_cost_col[0]]) if fixed_cost_col else 1360
            variable_cost = float(row[variable_cost_col[0]]) if variable_cost_col else 14

            vehicle = Vehicle(
                vehicle_id=str(row.get('Vehicle ID', f'V{idx + 1}')),
                driver_name=str(row.get('Driver Name', 'Unknown')),
                vehicle_type=str(row.get('Type Truck', '4W')),
                max_weight_kg=float(row.get('MaxKG', 1500) or 1500),
                max_volume_cbm=float(row.get('MaxCBM', 5) or 5),
                capacity_drop=int(row.get('Capacity Drop', 7) or 7),
                extra_drop_charge=float(row.get('Extra Drop Charge', 150) or 150),
                max_drops=int(row.get('Max Drop', 10) or 10),
                start_time=time_str_to_minutes(row.get('Vehicle Start Time', '08:00')),
                end_time=time_str_to_minutes(row.get('Vehicle End Time', '17:00')),
                break_start=time_str_to_minutes(row.get('Break Start Time', '12:00')),
                break_duration=int(row.get('Break Duration', 60) or 60),
                fixed_cost=fixed_cost,
                variable_cost=variable_cost
            )
            vehicles.append(vehicle)
        except Exception:
            continue
    return vehicles if vehicles else get_default_vehicles()


def load_customer_master(excel_file) -> Dict[str, dict]:
    """โหลด Customer Master จาก Sheet '01.ออเดอร์'"""
    try:
        df = pd.read_excel(excel_file, sheet_name=SHEET_CUSTOMERS)
        df.columns = df.columns.str.strip()
    except Exception:
        return {}

    customers = {}
    for idx, row in df.iterrows():
        try:
            customer_name = str(row.get('Customer Name', '')).strip()
            if not customer_name:
                continue
            customers[customer_name] = {
                'lat': float(row.get('Latitude', 0) or 0),
                'lng': float(row.get('Longitude', 0) or 0),
                'district': str(row.get('อำเภอ', '') or ''),
                'province': str(row.get('จังหวัด', '') or ''),
                'priority': int(row.get('Priority', 4) or 4),
                'time_open': time_str_to_minutes(row.get('เวลาร้านเปิด', '08:00')),
                'time_close': time_str_to_minutes(row.get('เวลาร้านปิด', '17:00')),
                'unload_time': int(row.get('เวลาลงสินค้า', DEFAULT_UNLOAD_TIME) or DEFAULT_UNLOAD_TIME),
                'service_time': int(row.get('เวลาเซ็นเอกสาร', DEFAULT_SERVICE_TIME) or DEFAULT_SERVICE_TIME),
                'zone_speed': float(row.get('ความเร็วฉลี่ยโซน', DEFAULT_SPEED_KMH) or DEFAULT_SPEED_KMH)
            }
        except Exception:
            continue
    return customers


def load_priority_config(excel_file) -> Dict[int, dict]:
    """โหลด Priority Config จาก Sheet 'Priority'"""
    try:
        df = pd.read_excel(excel_file, sheet_name=SHEET_PRIORITY)
        df.columns = df.columns.str.strip()
    except Exception:
        return DEFAULT_PRIORITY_CONFIG.copy()

    priority_config = {}
    for _, row in df.iterrows():
        try:
            priority = int(row.get('Priority', 4))

            name = None
            for col_name in ['Name', 'Priority Name', 'name']:
                if col_name in df.columns and pd.notna(row.get(col_name)):
                    name = str(row.get(col_name)).strip()
                    break
            if name is None:
                name = f'Priority {priority}'

            deadline_val = row.get('Deadline', None)
            if pd.isna(deadline_val):
                deadline = None
            elif str(deadline_val).strip().upper() in ['TW CLOSE', 'TW', 'TWCLOSE', '']:
                deadline = None
            else:
                deadline = time_str_to_minutes(deadline_val)

            is_hard = False
            for col_name in ['Hard constraint', 'Hard', 'HardConstraint', 'hard']:
                if col_name in df.columns:
                    hard_val = row.get(col_name, False)
                    if pd.isna(hard_val):
                        is_hard = False
                    elif isinstance(hard_val, bool):
                        is_hard = hard_val
                    elif isinstance(hard_val, str):
                        is_hard = hard_val.strip().upper() in ['TRUE', 'YES', '1', 'Y']
                    elif isinstance(hard_val, (int, float)):
                        is_hard = bool(hard_val)
                    break

            priority_config[priority] = {
                'name': name, 'deadline': deadline, 'hard': is_hard
            }
        except Exception:
            continue

    if not priority_config:
        return DEFAULT_PRIORITY_CONFIG.copy()
    return priority_config


def load_orders(excel_file, depots: Dict[str, Depot],
                customer_master: Dict[str, dict]) -> Tuple[List[Order], pd.DataFrame, List[Order]]:
    """
    โหลดและ aggregate orders จาก Sheet 'นำเข้าข้อมูลออเดอร์'
    Returns:
        (orders, raw_order_lines, skipped_orders)
        ⭐ C1 FIX: return skipped_orders แทน global variable
    """
    try:
        df = pd.read_excel(excel_file, sheet_name=SHEET_ORDERS)
        df.columns = df.columns.str.strip()
        raw_order_lines = df.copy()
    except Exception:
        return [], pd.DataFrame(), []

    has_priority_col = 'Priority' in df.columns

    # Aggregate by (Customer Name, Plant)
    aggregated = defaultdict(lambda: {
        'weight': 0, 'volume': 0, 'dn_numbers': [],
        'lat': 0, 'lng': 0, 'district': '', 'province': '',
        'ship_to_code': '', 'priority': 4
    })

    for idx, row in df.iterrows():
        try:
            customer_name = str(row.get('Customer Name', '')).strip()
            plant = normalize_depot_id(row.get('Plant', '03T7'))

            if not customer_name:
                continue

            key = (customer_name, plant)
            aggregated[key]['weight'] += float(row.get('น้ำหนัก', 0) or 0)
            aggregated[key]['volume'] += float(row.get('ปริมาตร CBM', 0) or 0)

            dn = str(row.get('DN Number', ''))
            if dn and dn != 'nan' and dn not in aggregated[key]['dn_numbers']:
                aggregated[key]['dn_numbers'].append(dn)

            if has_priority_col and pd.notna(row.get('Priority')):
                aggregated[key]['priority'] = int(row.get('Priority', 4))

            if aggregated[key]['lat'] == 0:
                aggregated[key]['lat'] = float(row.get('Latitude', 0) or 0)
                aggregated[key]['lng'] = float(row.get('Longitude', 0) or 0)
                aggregated[key]['district'] = str(row.get('อำเภอ', '') or '')
                aggregated[key]['province'] = str(row.get('จังหวัด', '') or '')
                aggregated[key]['ship_to_code'] = str(row.get('Ship to Code', '') or '')
        except Exception:
            continue

    # Create Order objects
    orders = []
    for (customer_name, plant), data in aggregated.items():
        try:
            cust_data = customer_master.get(customer_name, {})
            lat = data['lat'] if data['lat'] != 0 else cust_data.get('lat', 0)
            lng = data['lng'] if data['lng'] != 0 else cust_data.get('lng', 0)

            if lat == 0 or lng == 0:
                continue

            district = data['district'] or cust_data.get('district', '')
            province = data['province'] or cust_data.get('province', '')

            priority = data['priority']
            if priority == 4 and cust_data.get('priority'):
                priority = cust_data['priority']

            zone_speed = cust_data.get('zone_speed', DEFAULT_SPEED_KMH)
            if zone_speed == 0:
                zone_speed = DEFAULT_SPEED_KMH

            order = Order(
                order_id=f"{customer_name}_{plant}",
                customer_name=customer_name, plant=plant,
                lat=lat, lng=lng,
                weight_kg=data['weight'], volume_cbm=data['volume'],
                district=district, province=province,
                ship_to_code=data['ship_to_code'],
                dn_numbers=data['dn_numbers'],
                time_open=cust_data.get('time_open', 480),
                time_close=cust_data.get('time_close', 1020),
                service_time=cust_data.get('service_time', DEFAULT_SERVICE_TIME),
                unload_time=cust_data.get('unload_time', DEFAULT_UNLOAD_TIME),
                priority=priority,
                zone=determine_zone(district, province),
                zone_speed=zone_speed
            )
            orders.append(order)
        except Exception:
            continue

    # ══════════════════════════════════════════════
    # ⭐ C1 FIX: Filter orders ที่ Plant ไม่มีใน depots
    # ใช้ local variable แทน global
    # ══════════════════════════════════════════════
    skipped_orders = []

    if depots:
        valid_orders = []
        for order in orders:
            plant = getattr(order, 'plant', '')
            if plant and plant not in depots:
                skipped_orders.append(order)
            else:
                valid_orders.append(order)

        if skipped_orders:
            logger.info(
                f"⚠️ ข้าม {len(skipped_orders)} ออเดอร์ "
                f"(Plant ไม่ตรง) → จะไปเป็น Unassigned"
            )
            for o in skipped_orders[:5]:
                logger.info(
                    f"   • {o.customer_name[:40]} "
                    f"(Plant: {o.plant})"
                )
            if len(skipped_orders) > 5:
                logger.info(
                    f"   ... และอีก "
                    f"{len(skipped_orders) - 5} ออเดอร์"
                )
        orders = valid_orders

    return orders, raw_order_lines, skipped_orders


def load_distance_matrix(excel_file, depots, orders) -> Dict:
    """โหลด Distance Matrix"""
    dist_matrix = {}
    try:
        df = pd.read_excel(excel_file, sheet_name=SHEET_DISTANCE, index_col=0)
        for from_name in df.index:
            from_name_str = str(from_name).strip()
            dist_matrix[from_name_str] = {}
            for to_name in df.columns:
                to_name_str = str(to_name).strip()
                dist = df.loc[from_name, to_name]
                dist_matrix[from_name_str][to_name_str] = float(dist) if pd.notna(dist) else 0
    except Exception:
        pass
    return dist_matrix


def build_location_lookup(depots, orders) -> Dict:
    """สร้าง lookup table สำหรับพิกัด"""
    locations = {}
    for depot in depots.values():
        locations[depot.name] = (depot.lat, depot.lng)
        locations[depot.depot_id] = (depot.lat, depot.lng)
    for order in orders:
        locations[order.customer_name] = (order.lat, order.lng)
    return locations


# =====================================================
# SECTION 8: MAIN LOADING FUNCTION
# =====================================================
def load_all_data(excel_file, progress=None):
    if hasattr(excel_file, 'seek'):
        excel_file.seek(0)
    xls = pd.ExcelFile(excel_file)

    depots = load_depots(xls)
    vehicles = load_vehicles(xls)
    customer_master = load_customer_master(xls)

    # ⭐ C1 FIX: รับ skipped_orders จาก load_orders
    orders, raw_order_lines, skipped_orders = load_orders(xls, depots, customer_master)

    dist_matrix = load_distance_matrix(xls, depots, orders)
    locations = build_location_lookup(depots, orders)
    priority_config = load_priority_config(xls)

    return VRPData(
        depots=depots,
        vehicles=vehicles,
        orders=orders,
        dist_matrix=dist_matrix,
        locations=locations,
        priority_config=priority_config,
        raw_order_lines=raw_order_lines,
        skipped_orders=skipped_orders,  # ← C1: ส่งต่อ
    )


# =====================================================
# SECTION 9: ROUTE METRICS CALCULATION
# =====================================================
def calculate_route_metrics(route: Route, depots: Dict[str, Depot],
                            dist_matrix: Dict, locations: Dict,
                            priority_config: Dict = None):
    """
    คำนวณ metrics ของ route รวม pickup phase
    ✅ Logic 100% เหมือน Colab Part B — B2 [1]
    ✅ B1 FIX: เพิ่ม apply_break_time()
    ใช้ distance matrix (ไม่ใช่ haversine ตรงๆ)
    """
    if route.is_empty():
        route.pickup_distance = 0
        route.delivery_distance = 0
        route.return_distance = 0
        route.total_distance = 0
        return

    vehicle = route.vehicle
    default_end = get_default_end_depot(depots)

    # ── Phase 1: Pickup ──
    pickup_distance = 0
    pickup_time = 0
    if len(route.pickup_sequence) > 1:
        for i in range(len(route.pickup_sequence) - 1):
            from_depot = route.pickup_sequence[i]
            to_depot = route.pickup_sequence[i + 1]
            dist = get_distance(from_depot.name, to_depot.name,
                                dist_matrix, locations)
            pickup_distance += dist
            travel_time = (dist / 50) * 60
            pickup_time += travel_time + SECOND_DEPOT_LOADING_TIME

    route.pickup_distance = pickup_distance
    current_time = vehicle.start_time
    if hasattr(current_time, 'hour'):
        current_time = current_time.hour * 60 + current_time.minute
    current_time += pickup_time

    # ── Phase 2: Delivery ──
    delivery_distance = 0
    if route.pickup_sequence:
        current_name = route.pickup_sequence[-1].name
    elif default_end:
        current_name = default_end.name
    else:
        current_name = list(depots.values())[0].name if depots else ""

    for stop in route.stops:
        order = stop.order
        dist = get_distance(current_name, order.customer_name,
                            dist_matrix, locations)
        delivery_distance += dist
        stop.distance_from_prev = dist

        speed = order.zone_speed if order.zone_speed > 0 else DEFAULT_SPEED_KMH
        travel_time = (dist / speed) * 60

        # ⭐ B1 FIX: ใช้ apply_break_time() แทนการบวก travel_time ตรงๆ
        arrival_time = apply_break_time(current_time, travel_time, vehicle)

        if arrival_time < order.time_open:
            stop.wait_time = int(order.time_open - arrival_time)
            arrival_time = order.time_open
        else:
            stop.wait_time = 0

        # Check late (against TW Close)
        stop.is_late = arrival_time > order.time_close

        # Also check against priority deadline if config provided
        if priority_config:
            deadline = get_order_deadline(order, priority_config)
            if arrival_time > deadline and deadline != order.time_close:
                stop.is_late = True

        stop.arrival_time = int(arrival_time)
        service_time = order.service_time + order.unload_time
        stop.departure_time = int(arrival_time + service_time)
        current_time = stop.departure_time
        current_name = order.customer_name

    route.delivery_distance = delivery_distance

    # ── Phase 3: Return ──
    if default_end:
        return_distance = get_distance(current_name, default_end.name,
                                       dist_matrix, locations)
    else:
        return_distance = 0
    route.return_distance = return_distance
    route.total_distance = pickup_distance + delivery_distance

    # ── Cumulative distances ──
    cumulative = 0
    for stop in route.stops:
        cumulative += stop.distance_from_prev
        stop.cumulative_distance = cumulative


def calculate_stop_times(route: Route, depots: Dict[str, Depot],
                         dist_matrix: Dict, locations: Dict,
                         priority_config: Dict = None):
    """
    Alias — เรียก calculate_route_metrics เพื่อ backward compatibility
    """
    calculate_route_metrics(route, depots, dist_matrix, locations,
                            priority_config)
    
# =====================================================
# SECTION 10: CONSTRUCTIVE HEURISTIC
# (Progressive Relaxation 4 รอบ — Multi-Depot)
# ✅ Logic 100% เหมือน Colab Part B v3.1 [1]
# ✅ B1 FIX: เพิ่ม apply_break_time()
# ✅ B3 FIX: เพิ่ม vehicle end time check
# =====================================================

# ----- Relaxation Round Configuration -----
RELAXATION_ROUNDS = [
    {
        'name': 'Strict capacity + Strict time + Hard priority only',
        'time_flex': 0,
        'allow_extra_drops': False,
        'depot_penalty_weight': 1.0,
    },
    {
        'name': 'Strict capacity + Strict time + Relaxed depot penalty',
        'time_flex': 0,
        'allow_extra_drops': False,
        'depot_penalty_weight': 0.5,
    },
    {
        'name': 'Allow extra drops + Strict time',
        'time_flex': 0,
        'allow_extra_drops': True,
        'depot_penalty_weight': 0.3,
    },
    {
        'name': 'Allow extra drops + Relaxed time (+2hr)',
        'time_flex': 120,
        'allow_extra_drops': True,
        'depot_penalty_weight': 0.0,
    },
]


# ── B1: Helper — can_insert_order ──
def can_insert_order(route: Route, order: Order, vehicle: Vehicle,
                     allow_extra_drops: bool = False) -> Tuple[bool, str]:
    """
    ตรวจสอบว่าสามารถเพิ่ม order เข้า route ได้หรือไม่
    ✅ v3.4: เพิ่ม HARD max_drops limit
    """
    new_weight = route.total_weight + order.weight_kg
    new_volume = route.total_volume + order.volume_cbm
    new_drops = route.num_stops + 1

    if new_weight > vehicle.max_weight_kg:
        return False, "WEIGHT_EXCEEDED"

    if new_volume > vehicle.max_volume_cbm:
        return False, "VOLUME_EXCEEDED"

    max_drops_limit = getattr(vehicle, 'max_drops', 10)

    # HARD LIMIT: ห้ามเกิน max_drops ไม่ว่า allow_extra_drops จะเป็นอะไร
    if new_drops > max_drops_limit:
        return False, "MAX_DROPS_EXCEEDED"

    # SOFT LIMIT: ถ้าไม่ allow extra drops → ห้ามเกิน capacity_drop
    if not allow_extra_drops:
        capacity_drop = getattr(vehicle, 'capacity_drop', 7)
        if new_drops > capacity_drop:
            return False, "CAPACITY_DROP_EXCEEDED"

    return True, "OK"


# ── B1.1: Calculate Arrival Time ──
def calculate_arrival_time_at_position(
    route: Route, order: Order, position: int,
    depots: Dict, dist_matrix: Dict, locations: Dict
) -> int:
    """
    คำนวณ arrival time ถ้า insert order ที่ position นี้
    ✅ เหมือน Colab Part B [1] — ใช้ distance matrix
    ✅ B1 FIX: เพิ่ม apply_break_time()
    """
    vehicle = route.vehicle
    default_depot = get_default_end_depot(depots)

    if position == 0:
        # First stop — start from last depot in pickup sequence
        if route.pickup_sequence:
            prev_name = route.pickup_sequence[-1].name
        else:
            depot = depots.get(order.plant, default_depot)
            prev_name = depot.name if depot else ""

        # Time after pickup phase
        pickup_time = 0
        if len(route.pickup_sequence) > 1:
            for i in range(len(route.pickup_sequence) - 1):
                from_d = route.pickup_sequence[i]
                to_d = route.pickup_sequence[i + 1]
                dist = get_distance(from_d.name, to_d.name,
                                    dist_matrix, locations)
                pickup_time += (dist / 50) * 60 + SECOND_DEPOT_LOADING_TIME

        start_time = vehicle.start_time
        if hasattr(start_time, 'hour'):
            start_time = start_time.hour * 60 + start_time.minute
        current_time = start_time + pickup_time
    else:
        prev_stop = route.stops[position - 1]
        prev_name = prev_stop.order.customer_name
        current_time = prev_stop.departure_time

    # Calculate travel time to order using distance matrix
    dist = get_distance(prev_name, order.customer_name,
                        dist_matrix, locations)
    speed = order.zone_speed if order.zone_speed > 0 else DEFAULT_SPEED_KMH
    travel_time = (dist / speed) * 60

    # ⭐ B1 FIX: ใช้ apply_break_time()
    arrival_time = apply_break_time(current_time, travel_time, vehicle, enabled=False)

    # Wait if early
    if arrival_time < order.time_open:
        arrival_time = order.time_open

    return int(arrival_time)


# ── B1.2: Check Feasibility ──
def check_insertion_feasibility(
    route: Route, order: Order, position: int,
    depots: Dict, dist_matrix: Dict, locations: Dict,
    priority_config: Dict,
    time_flex: int = 0,
    allow_extra_drops: bool = False
) -> Tuple[bool, int, str]:
    """
    ตรวจสอบว่าสามารถ insert order ที่ position นี้ได้หรือไม่
    ✅ เหมือน Colab Part B v3.1 [1]
    ✅ B3 FIX: เพิ่ม vehicle end time check
    """
    vehicle = route.vehicle

    # 1. Capacity check
    can_fit, reason = can_insert_order(route, order, vehicle,
                                       allow_extra_drops)
    if not can_fit:
        return False, 0, reason

    # 2. Calculate arrival time (ใช้ distance matrix + break time)
    arrival_time = calculate_arrival_time_at_position(
        route, order, position, depots, dist_matrix, locations
    )

    # 3. Hard Constraint เฉพาะ P1/P2 (hard: True) เท่านั้น
    is_hard = is_hard_priority(order, priority_config)
    if is_hard and not can_meet_deadline(order, arrival_time,
                                         priority_config):
        deadline = get_order_deadline(order, priority_config)
        return (False, arrival_time,
                f"HARD_DEADLINE_MISSED "
                f"({minutes_to_time_str(arrival_time)} > "
                f"{minutes_to_time_str(deadline)})")

    # 4. Check TW Close (with time_flex for relaxed rounds)
    effective_close = order.time_close + time_flex
    if arrival_time > effective_close:
        return False, arrival_time, "TW_CLOSE_EXCEEDED"

    # 5. ⭐ B3 FIX: Vehicle end time check (soft limit)
    service_time = order.service_time + order.unload_time
    departure_time = arrival_time + service_time
    vehicle_end = getattr(vehicle, 'end_time', 1020)
    if hasattr(vehicle_end, 'hour'):
        vehicle_end = vehicle_end.hour * 60 + vehicle_end.minute

    if departure_time > vehicle_end + MAX_OVERTIME_MINUTES:
        return False, arrival_time, "VEHICLE_OVERTIME_EXCEEDED"

    return True, arrival_time, "OK"


# ── B1.3: Calculate Insertion Cost ──
def _calc_insertion_cost_full(
    route, order, position, depots, locations,
    dist_matrix, arrival_time=0, priority_config=None
) -> float:
    if priority_config is None:
        priority_config = DEFAULT_PRIORITY_CONFIG

    vehicle = route.vehicle
    default_depot = get_default_end_depot(depots)

    # ─── Get previous/next location (เดิม) ───
    if position == 0:
        if route.pickup_sequence:
            prev_name = route.pickup_sequence[-1].name
        else:
            depot = depots.get(order.plant, default_depot)
            prev_name = depot.name if depot else ""
    else:
        prev_name = route.stops[position - 1].order.customer_name

    if position < len(route.stops):
        next_name = route.stops[position].order.customer_name
    else:
        next_name = None

    # ─── Distance calculations (เดิม) ───
    dist_to_order = get_distance(
        prev_name, order.customer_name, dist_matrix, locations
    )
    if next_name:
        dist_from_order = get_distance(
            order.customer_name, next_name, dist_matrix, locations
        )
        old_dist = get_distance(
            prev_name, next_name, dist_matrix, locations
        )
    else:
        dist_from_order = 0
        old_dist = 0

    delivery_cost = (
        (dist_to_order + dist_from_order - old_dist)
        * vehicle.variable_cost
    )

    # ─── Multi-Depot penalty (เดิม) ───
    pickup_cost = 0
    if order.plant not in route.orders_by_depot:
        new_depot = depots.get(order.plant)
        if new_depot and route.pickup_sequence:
            last_depot = route.pickup_sequence[-1]
            extra_dist = get_distance(
                last_depot.name, new_depot.name,
                dist_matrix, locations
            )
            pickup_cost = extra_dist * vehicle.variable_cost
            pickup_cost += 30 * vehicle.variable_cost

    # ─── Soft Priority late penalty (เดิม) ───
    late_penalty = 0
    if priority_config and arrival_time > 0:
        deadline = get_order_deadline(order, priority_config)
        if (arrival_time > deadline and
                not is_hard_priority(order, priority_config)):
            lateness = arrival_time - deadline
            late_penalty = lateness * 5

    # ─── Geographic Scatter Penalty (เดิม) ───
    scatter_penalty = 0
    if route.stops and len(route.stops) >= 2:
        avg_lat = (
            sum(s.order.lat for s in route.stops)
            / len(route.stops)
        )
        avg_lng = (
            sum(s.order.lng for s in route.stops)
            / len(route.stops)
        )
        dist_to_centroid = haversine_distance(
            order.lat, order.lng, avg_lat, avg_lng
        )
        if dist_to_centroid > 50:
            scatter_penalty = (dist_to_centroid - 50) * 3

    return (delivery_cost + pickup_cost
            + late_penalty + scatter_penalty)


# ── B3.1: Try Insert Order ──
def _try_insert_order(
    order: 'Order', routes: List['Route'],
    depots: Dict[str, 'Depot'], dist_matrix: Dict,
    locations: Dict, priority_config: Dict,
    round_config: Dict
) -> Tuple[bool, str]:
    time_flex = round_config['time_flex']
    allow_extra_drops = round_config['allow_extra_drops']
    depot_penalty_weight = round_config['depot_penalty_weight']

    best_route = None
    best_position = None
    best_cost = float('inf')
    last_reason = "No feasible route"

    for route in routes:
        can_fit, reason = can_insert_order(
            route, order, route.vehicle, allow_extra_drops
        )
        if not can_fit:
            last_reason = reason
            continue

        # ─── Depot consolidation bonus (เดิม) ───
        depot_bonus = 0
        if order.plant in route.orders_by_depot:
            depot_bonus = -100 * depot_penalty_weight
        elif route.num_stops == 0:
            depot_bonus = 0
        else:
            depot_bonus = 50 * depot_penalty_weight

        # ─── Zone compatibility bonus (เดิม) ───
        zone_bonus = 0
        if route.stops:
            first_order_zone = route.stops[0].order.zone
            if order.zone == first_order_zone:
                zone_bonus = -50

        # ─── Priority bonus (เดิม) ───
        priority_bonus = 0
        if route.stops:
            avg_priority = (
                sum(s.order.priority for s in route.stops)
                / len(route.stops)
            )
            if abs(order.priority - avg_priority) <= 1:
                priority_bonus = -30

        # ─── Geographic Proximity Bonus (เดิม) ───
        proximity_bonus = 0
        if route.stops:
            avg_lat = sum(s.order.lat for s in route.stops) / len(route.stops)
            avg_lng = sum(s.order.lng for s in route.stops) / len(route.stops)
            dist_to_centroid = haversine_distance(
                order.lat, order.lng, avg_lat, avg_lng
            )
            if dist_to_centroid < 80:
                proximity_bonus = -150 * (1 - dist_to_centroid / 80)
            route_provinces = set(
                s.order.province for s in route.stops
            )
            if order.province in route_provinces:
                proximity_bonus -= 80

        # Try each position
        for pos in range(len(route.stops) + 1):
            feasible, arrival_time, check_reason = (
                check_insertion_feasibility(
                    route, order, pos, depots, dist_matrix,
                    locations, priority_config,
                    time_flex=time_flex,
                    allow_extra_drops=allow_extra_drops
                )
            )
            if not feasible:
                last_reason = check_reason
                continue

            cost = _calc_insertion_cost_full(
                route, order, pos, depots, locations,
                dist_matrix,
                arrival_time=arrival_time,
                priority_config=priority_config
            )

            total_cost = (cost + depot_bonus + zone_bonus
                         + priority_bonus + proximity_bonus)

            if total_cost < best_cost:
                best_cost = total_cost
                best_route = route
                best_position = pos

    if best_route is not None:
        depot = depots.get(order.plant)
        best_route.add_order(order, best_position, depot)
        return True, "OK"
    else:
        # ✅ บันทึกสาเหตุลง order เพื่อแสดงผลใน UI
        order.unassign_reason = last_reason
        return False, last_reason


# ── B3: Main Constructive Heuristic ──
def build_initial_solution_multi_depot(
    orders: List[Order], vehicles: List[Vehicle],
    depots: Dict[str, Depot], dist_matrix: Dict,
    locations: Dict, priority_config: Dict = None,
    progress: VRPProgress = None
) -> Tuple[List[Route], List[Order]]:
    """
    Progressive Relaxation Constructive Heuristic
    ✅ Logic 100% เหมือน Colab Part B v3.1 [1]
    """
    if priority_config is None:
        priority_config = DEFAULT_PRIORITY_CONFIG

    # ==========================================
    # Step 1: Sort Orders by Priority
    # ==========================================
    sorted_orders = sort_orders_by_priority(orders, priority_config)
    if progress:
        progress.report("construct",
                        f"🏗️ เรียง {len(orders)} ออเดอร์ตาม Priority...",
                        5)

    # ==========================================
    # Step 2: Initialize Routes for ALL vehicles
    # ==========================================
    routes = [Route(vehicle=v) for v in vehicles]
    if progress:
        progress.report("construct",
                        f"🚛 สร้าง {len(routes)} เส้นทางสำหรับรถทุกคัน",
                        10)

    # ==========================================
    # Step 3: Progressive Relaxation (4 Rounds)
    # ==========================================
    remaining_orders = list(sorted_orders)

    for round_idx, round_config in enumerate(RELAXATION_ROUNDS):
        if not remaining_orders:
            if progress:
                progress.report(
                    "construct",
                    f"✅ จัดครบก่อนถึง Round {round_idx + 1}!",
                    75
                )
            break

        round_num = round_idx + 1
        if progress:
            pct = 10 + round_idx * 15  # 10, 25, 40, 55
            progress.report(
                "construct",
                f"🏗️ Round {round_num}: {round_config['name']} "
                f"| เหลือ {len(remaining_orders)} ออเดอร์",
                pct
            )

        newly_assigned = []
        still_remaining = []

        for order in remaining_orders:
            success, reason = _try_insert_order(
                order, routes, depots, dist_matrix, locations,
                priority_config, round_config
            )
            if success:
                newly_assigned.append(order)
            else:
                still_remaining.append(order)

        remaining_orders = still_remaining

        if progress:
            total_assigned = sum(r.num_stops for r in routes)
            total_orders = len(orders)
            pct_assigned = (total_assigned / total_orders * 100
                            if total_orders > 0 else 0)
            progress.report(
                "construct",
                f"✅ Round {round_num}: +{len(newly_assigned)} | "
                f"รวม {total_assigned}/{total_orders} "
                f"({pct_assigned:.1f}%)",
                10 + round_idx * 15 + 10
            )

    # Final unassigned — ใส่ reason default ถ้ายังไม่มี
    unassigned = remaining_orders
    for o in unassigned:
        if not getattr(o, 'unassign_reason', ''):
            o.unassign_reason = "NO_FEASIBLE_ROUTE — ไม่มีรถที่ใส่ได้ (ผ่าน 4 รอบแล้ว)"

    # ==========================================
    # Step 4: Remove Empty Routes
    # ==========================================
    routes = [r for r in routes if not r.is_empty()]

    # ==========================================
    # Step 5: Calculate Metrics
    # ==========================================
    for route in routes:
        calculate_route_metrics(route, depots, dist_matrix, locations,
                                priority_config)

    if progress:
        assigned_count = sum(r.num_stops for r in routes)
        total_count = len(orders)
        progress.report(
            "construct",
            f"✅ จัดได้ {assigned_count}/{total_count} ออเดอร์ "
            f"ใช้ {len(routes)} คัน",
            100,
            {'assigned': assigned_count, 'total': total_count,
             'unassigned': len(unassigned)}
        )

    return routes, unassigned


# =====================================================
# SECTION 11: ALNS CONFIGURATION
# =====================================================
class ALNSConfig:
    """ALNS Parameters — With Priority Support"""

    # Stopping Criteria
    MAX_ITERATIONS = 999999
    NO_IMPROVEMENT_LIMIT = 700
    TIME_LIMIT_SECONDS = 180

    # Destroy Parameters
    MIN_DESTROY_RATIO = 0.15
    MAX_DESTROY_RATIO = 0.40

    # Simulated Annealing
    INITIAL_TEMPERATURE = 2000
    COOLING_RATE = 0.9995
    MIN_TEMPERATURE = 0.1

    # Reheating — ⭐ A2 FIX: เปลี่ยนเป็น interval-based
    REHEAT_ENABLED = True
    REHEAT_INTERVAL = 200       # ← reheat ทุก 200 no-imp iterations
    REHEAT_TEMPERATURE = 500
    MAX_REHEATS = 3             # ← ลดจาก 5 เป็น 3

    # Adaptive Weights
    WEIGHT_DECAY = 0.85
    SCORE_RESET_INTERVAL = 100
    INITIAL_WEIGHT = 1.0
    MIN_WEIGHT = 0.2

    # Scoring System
    SCORE_BEST = 15
    SCORE_BETTER = 5
    SCORE_ACCEPTED = 2
    SCORE_REJECTED = 0

    # Internal Penalties (สำหรับ Optimization)
    PENALTY_UNASSIGNED = 50000
    PENALTY_LATE = 2000
    PENALTY_OVERTIME = 3000
    PENALTY_EXTRA_DROP = 250
    PENALTY_ZONE_MISMATCH = 100
    PENALTY_PRIORITY_MISS = 50000
    PENALTY_PRIORITY_1_MISS = 100000
    PENALTY_PRIORITY_2_MISS = 75000
    PENALTY_PRIORITY_3_MISS = 25000
    PENALTY_MULTI_DEPOT = 500
    BONUS_SAME_DEPOT = -200

    # Display Costs (THB)
    DISPLAY_COST_UNASSIGNED = 500
    DISPLAY_COST_LATE = 100
    DISPLAY_COST_OVERTIME = 200

    # Display Settings
    DISPLAY_INTERVAL = 100
    SHOW_OPERATOR_STATS = True


def get_priority_penalty(priority: int) -> int:
    """Get penalty for missing priority deadline"""
    if priority == 1:
        return ALNSConfig.PENALTY_PRIORITY_1_MISS
    elif priority == 2:
        return ALNSConfig.PENALTY_PRIORITY_2_MISS
    elif priority == 3:
        return ALNSConfig.PENALTY_PRIORITY_3_MISS
    else:
        return ALNSConfig.PENALTY_LATE


# =====================================================
# SECTION 12: ALNS SOLUTION
# =====================================================
class ALNSSolution:
    """Solution class with Priority Cost Calculation"""

    def __init__(self, routes: List[Route] = None, unassigned: List[Order] = None):
        self.routes = routes if routes else []
        self.unassigned = unassigned if unassigned else []
        self._cost = None
        self._display_cost = None
        self._breakdown = None
        self._display_breakdown = None
        self._priority_config = None

    def set_priority_config(self, config: Dict):
        self._priority_config = config
        self.invalidate_cost()

    def copy(self) -> 'ALNSSolution':
        """Deep copy solution — v3.0 Single Source of Truth"""
        new_sol = ALNSSolution()
        new_sol.routes = []

        for route in self.routes:
            new_route = Route(vehicle=route.vehicle)
            new_route.stops = []
            for s in route.stops:
                new_stop = RouteStop(
                    order=s.order,
                    arrival_time=s.arrival_time,
                    departure_time=s.departure_time,
                    distance_from_prev=s.distance_from_prev,
                    cumulative_distance=s.cumulative_distance,
                    wait_time=getattr(s, 'wait_time', 0),
                    is_late=getattr(s, 'is_late', False)
                )
                new_route.stops.append(new_stop)

            if hasattr(route, 'pickup_sequence') and route.pickup_sequence:
                new_route.pickup_sequence = list(route.pickup_sequence)
            else:
                new_route.pickup_sequence = []

            new_route.recalc_weight_volume()
            new_route.total_distance = getattr(route, 'total_distance', 0)
            new_route.pickup_distance = getattr(route, 'pickup_distance', 0)
            new_route.delivery_distance = getattr(route, 'delivery_distance', 0)
            new_route.return_distance = getattr(route, 'return_distance', 0)
            new_sol.routes.append(new_route)

        new_sol.unassigned = list(self.unassigned)
        new_sol._priority_config = self._priority_config
        new_sol._cost = None
        new_sol._display_cost = None
        new_sol._breakdown = None
        new_sol._display_breakdown = None
        return new_sol

    def invalidate_cost(self):
        self._cost = None
        self._display_cost = None
        self._breakdown = None
        self._display_breakdown = None

    @property
    def cost(self) -> float:
        if self._cost is None:
            self._calculate_costs()
        return self._cost

    @property
    def display_cost(self) -> float:
        if self._display_cost is None:
            self._calculate_costs()
        return self._display_cost

    @property
    def breakdown(self) -> dict:
        if self._breakdown is None:
            self._calculate_costs()
        return self._breakdown

    @property
    def display_breakdown(self) -> dict:
        if self._display_breakdown is None:
            self._calculate_costs()
        return self._display_breakdown

    def _calculate_costs(self):
        """
        Calculate both Internal Cost and Display Cost with Priority
        ✅ B2 FIX: ไม่คิด priority penalty ซ้ำซ้อนเมื่อ deadline == time_close
        """
        priority_config = self._priority_config or DEFAULT_PRIORITY_CONFIG

        bd = {
            'fixed_cost': 0, 'variable_cost': 0,
            'unassigned_penalty': 0, 'late_penalty': 0,
            'overtime_penalty': 0, 'extra_drop_penalty': 0,
            'zone_mismatch_penalty': 0, 'multi_depot_penalty': 0,
            'priority_penalty': 0, 'num_vehicles': 0,
            'num_unassigned': len(self.unassigned),
            'total_distance': 0, 'pickup_distance': 0,
            'delivery_distance': 0, 'num_late': 0,
            'num_priority_miss': 0, 'overtime_hours': 0,
            'extra_drops': 0, 'zone_mismatches': 0,
            'multi_depot_routes': 0, 'total_orders_assigned': 0
        }
        dbd = {
            'fixed_cost': 0, 'variable_cost': 0,
            'unassigned_cost': 0, 'late_cost': 0,
            'overtime_cost': 0, 'extra_drop_cost': 0,
            'num_vehicles': 0, 'num_unassigned': len(self.unassigned),
            'total_distance': 0, 'pickup_distance': 0,
            'delivery_distance': 0, 'num_late': 0,
            'num_priority_miss': 0, 'overtime_hours': 0,
            'extra_drops': 0, 'extra_drop_details': [],
            'total_orders_assigned': 0,
            'priority_breakdown': defaultdict(lambda: {'assigned': 0, 'missed': 0})
        }

        for route in self.routes:
            if route.is_empty():
                continue

            num_orders = route.num_stops
            v = route.vehicle

            bd['num_vehicles'] += 1
            dbd['num_vehicles'] += 1
            bd['total_orders_assigned'] += num_orders
            dbd['total_orders_assigned'] += num_orders

            # Fixed Cost
            fixed = getattr(v, 'fixed_cost', 1500)
            bd['fixed_cost'] += fixed
            dbd['fixed_cost'] += fixed

            # Variable Cost
            var_cost_per_km = getattr(v, 'variable_cost', 14)
            pickup_dist = getattr(route, 'pickup_distance', 0)
            delivery_dist = getattr(route, 'delivery_distance', 0) or route.total_distance
            billable_distance = pickup_dist + delivery_dist

            bd['total_distance'] += billable_distance
            dbd['total_distance'] += billable_distance
            bd['pickup_distance'] += pickup_dist
            dbd['pickup_distance'] += pickup_dist
            bd['delivery_distance'] += delivery_dist
            dbd['delivery_distance'] += delivery_dist

            bd['variable_cost'] += billable_distance * var_cost_per_km
            dbd['variable_cost'] += billable_distance * var_cost_per_km

            # Extra Drops (v3.4: แยก capacity_drop vs max_drops)
            capacity_drop = getattr(v, 'capacity_drop', 7)
            max_drops_limit = getattr(v, 'max_drops', 10)
            extra_drop_charge = getattr(v, 'extra_drop_charge', 150)

            if num_orders > max_drops_limit:
                over_max = num_orders - max_drops_limit
                bd['extra_drop_penalty'] += over_max * ALNSConfig.PENALTY_UNASSIGNED
                dbd['extra_drop_cost'] += over_max * extra_drop_charge * 10

            if num_orders > capacity_drop:
                extra = min(num_orders, max_drops_limit) - capacity_drop
                if extra > 0:
                    bd['extra_drops'] += extra
                    dbd['extra_drops'] += extra
                    bd['extra_drop_penalty'] += extra * ALNSConfig.PENALTY_EXTRA_DROP
                    extra_cost = extra * extra_drop_charge
                    dbd['extra_drop_cost'] += extra_cost
                    dbd['extra_drop_details'].append({
                        'vehicle_id': v.vehicle_id,
                        'actual_drops': num_orders,
                        'capacity_drop': capacity_drop,
                        'max_drops': max_drops_limit,
                        'extra_drops': extra,
                        'charge_per_drop': extra_drop_charge,
                        'penalty': extra_cost
                    })

            # Late Delivery & Priority Check
            for stop in route.stops:
                order = stop.order
                arrival = getattr(stop, 'arrival_time', 0)
                priority = getattr(order, 'priority', 4)

                dbd['priority_breakdown'][priority]['assigned'] += 1

                time_close = getattr(order, 'time_close', 1020)

                # Late check (against TW Close)
                if arrival > time_close:
                    bd['num_late'] += 1
                    dbd['num_late'] += 1
                    bd['late_penalty'] += ALNSConfig.PENALTY_LATE
                    dbd['late_cost'] += ALNSConfig.DISPLAY_COST_LATE

                # ⭐ B2 FIX: Priority miss — เฉพาะเมื่อ deadline ≠ time_close
                deadline = get_order_deadline(order, priority_config)
                if arrival > deadline and deadline != time_close:
                    bd['num_priority_miss'] += 1
                    dbd['num_priority_miss'] += 1
                    dbd['priority_breakdown'][priority]['missed'] += 1
                    bd['priority_penalty'] += get_priority_penalty(priority)

            # Multi-Depot Penalty
            num_depots = len(route.orders_by_depot)
            if num_depots > 1:
                bd['multi_depot_routes'] += 1
                bd['multi_depot_penalty'] += ALNSConfig.PENALTY_MULTI_DEPOT * (num_depots - 1)
            elif num_depots == 1:
                bd['multi_depot_penalty'] += ALNSConfig.BONUS_SAME_DEPOT

            # Overtime
            if route.stops:
                last_stop = route.stops[-1]
                last_departure = getattr(last_stop, 'departure_time', 0)
                return_dist = getattr(route, 'return_distance', 0)
                return_time = (return_dist / 35) * 60 if return_dist > 0 else 30
                end_time = last_departure + return_time

                vehicle_end = getattr(v, 'end_time', 1020)
                if hasattr(vehicle_end, 'hour'):
                    vehicle_end = vehicle_end.hour * 60 + vehicle_end.minute

                if end_time > vehicle_end:
                    ot_minutes = end_time - vehicle_end
                    ot_hours = ot_minutes / 60
                    bd['overtime_hours'] += ot_hours
                    dbd['overtime_hours'] += ot_hours
                    bd['overtime_penalty'] += ot_hours * ALNSConfig.PENALTY_OVERTIME
                    dbd['overtime_cost'] += ot_hours * ALNSConfig.DISPLAY_COST_OVERTIME

        # Unassigned Orders (with Priority weight)
        base_unassigned_penalty = 0
        for order in self.unassigned:
            priority = getattr(order, 'priority', 4)
            if priority <= 2:
                base_unassigned_penalty += ALNSConfig.PENALTY_UNASSIGNED * 2
            else:
                base_unassigned_penalty += ALNSConfig.PENALTY_UNASSIGNED

        bd['unassigned_penalty'] = base_unassigned_penalty
        dbd['unassigned_cost'] = len(self.unassigned) * ALNSConfig.DISPLAY_COST_UNASSIGNED

        # Total
        self._cost = (
            bd['fixed_cost'] + bd['variable_cost'] +
            bd['unassigned_penalty'] + bd['late_penalty'] +
            bd['overtime_penalty'] + bd['extra_drop_penalty'] +
            bd['zone_mismatch_penalty'] + bd['multi_depot_penalty'] +
            bd['priority_penalty']
        )
        self._display_cost = (
            dbd['fixed_cost'] + dbd['variable_cost'] +
            dbd['unassigned_cost'] + dbd['late_cost'] +
            dbd['overtime_cost'] + dbd['extra_drop_cost']
        )
        self._breakdown = bd
        self._display_breakdown = dbd

    def get_summary(self) -> dict:
        dbd = self.display_breakdown
        bd = self.breakdown
        total_orders = dbd['total_orders_assigned'] + dbd['num_unassigned']
        return {
            'total_cost': self.display_cost,
            'internal_score': self.cost,
            'fixed_cost': dbd['fixed_cost'],
            'variable_cost': dbd['variable_cost'],
            'num_vehicles': dbd['num_vehicles'],
            'total_distance': dbd['total_distance'],
            'pickup_distance': dbd['pickup_distance'],
            'delivery_distance': dbd['delivery_distance'],
            'total_orders': total_orders,
            'assigned_orders': dbd['total_orders_assigned'],
            'unassigned_orders': dbd['num_unassigned'],
            'num_late': dbd['num_late'],
            'num_priority_miss': dbd['num_priority_miss'],
            'overtime_hours': dbd['overtime_hours'],
            'extra_drops': dbd['extra_drops'],
            'multi_depot_routes': bd.get('multi_depot_routes', 0),
            'single_depot_routes': dbd['num_vehicles'] - bd.get('multi_depot_routes', 0)
        }


# =====================================================
# SECTION 13: DESTROY OPERATORS (10 ตัว)
# =====================================================
class DestroyOperators:
    """
    Destroy Operators — v3.0 ใช้ Route methods [1]
    ✅ D1 FIX: worst_removal ไม่ข้าม 1-stop route
    """

    @staticmethod
    def _get_orders_from_route(route: Route) -> List[Order]:
        return route.orders

    @staticmethod
    def _remove_order_from_route(route: Route, order: Order):
        route.remove_order(order)
        return True

    # ── (1) Random Removal ──
    @staticmethod
    def random_removal(solution: ALNSSolution, num_to_remove: int,
                       dist_matrix: Dict, **kwargs) -> List[Order]:
        removed = []
        all_orders = []
        for route in solution.routes:
            for order in DestroyOperators._get_orders_from_route(route):
                all_orders.append((route, order))

        if not all_orders:
            return removed

        num_to_remove = min(num_to_remove, len(all_orders))
        to_remove = random.sample(all_orders, num_to_remove)

        for route, order in to_remove:
            if route.has_order(order):
                DestroyOperators._remove_order_from_route(route, order)
                removed.append(order)

        solution.invalidate_cost()
        return removed

    # ── (2) Worst Removal ──
    @staticmethod
    def worst_removal(solution: ALNSSolution, num_to_remove: int,
                      dist_matrix: Dict, **kwargs) -> List[Order]:
        priority_config = kwargs.get('priority_config', DEFAULT_PRIORITY_CONFIG)
        removed = []
        order_costs = []

        for route in solution.routes:
            # ⭐ D1 FIX: ข้ามเฉพาะ route ว่าง (ไม่ข้าม 1-stop route)
            if route.num_stops < 1:
                continue

            for i, stop in enumerate(route.stops):
                order = stop.order
                cost = 0
                cost += getattr(stop, 'distance_from_prev', 0) * getattr(route.vehicle, 'variable_cost', 14)

                if getattr(stop, 'is_late', False):
                    cost += ALNSConfig.PENALTY_LATE

                arrival = getattr(stop, 'arrival_time', 0)
                deadline = get_order_deadline(order, priority_config)
                if arrival > deadline:
                    priority = getattr(order, 'priority', 4)
                    cost += get_priority_penalty(priority)

                if len(route.orders_by_depot) > 1:
                    if order.plant in route.orders_by_depot:
                        if len(route.orders_by_depot[order.plant]) == 1:
                            cost += ALNSConfig.PENALTY_MULTI_DEPOT

                order_costs.append((cost, route, order))

        order_costs.sort(key=lambda x: -x[0])

        count = 0
        for cost, route, order in order_costs:
            if count >= num_to_remove:
                break
            if route.has_order(order):
                DestroyOperators._remove_order_from_route(route, order)
                removed.append(order)
                count += 1

        solution.invalidate_cost()
        return removed

    # ── (3) Route Removal ──
    @staticmethod
    def route_removal(solution: ALNSSolution, num_to_remove: int,
                      dist_matrix: Dict, **kwargs) -> List[Order]:
        removed = []
        route_utils = []

        for route in solution.routes:
            orders = DestroyOperators._get_orders_from_route(route)
            if not orders:
                continue
            weight_util = route.total_weight / route.vehicle.max_weight_kg if route.vehicle.max_weight_kg > 0 else 0
            route_utils.append((weight_util, len(orders), route))

        if not route_utils:
            return removed

        route_utils.sort(key=lambda x: (x[0], x[1]))
        num_routes = min(2, len(route_utils))

        for i in range(num_routes):
            _, _, route = route_utils[i]
            for order in DestroyOperators._get_orders_from_route(route).copy():
                removed.append(order)
            route.clear_all()

        solution.invalidate_cost()
        return removed

    # ── (4) Related Removal ──
    @staticmethod
    def related_removal(solution, num_to_remove, dist_matrix, **kwargs):
        removed = []
        all_orders = []
        for route in solution.routes:
            for order in DestroyOperators._get_orders_from_route(route):
                all_orders.append((route, order))

        if not all_orders:
            return removed

        seed_route, seed_order = random.choice(all_orders)

        relatedness = []
        for route, order in all_orders:
            if order == seed_order:
                continue

            dist = haversine_distance(
                seed_order.lat, seed_order.lng,
                order.lat, order.lng
            )
            zone_bonus = 50 if (
                getattr(seed_order, 'zone', '') ==
                getattr(order, 'zone', '') and
                getattr(seed_order, 'zone', '')
            ) else 0
            depot_bonus = 40 if seed_order.plant == order.plant else 0
            priority_bonus = 30 if (
                getattr(seed_order, 'priority', 4) ==
                getattr(order, 'priority', 4)
            ) else 0

            province_bonus = 60 if (
                seed_order.province and
                order.province and
                seed_order.province == order.province
            ) else 0

            score = (dist - zone_bonus - depot_bonus
                    - priority_bonus - province_bonus)
            relatedness.append((score, route, order))

        relatedness.sort(key=lambda x: x[0])

        if seed_route.has_order(seed_order):
            DestroyOperators._remove_order_from_route(
                seed_route, seed_order
            )
            removed.append(seed_order)

        count = 1
        for score, route, order in relatedness:
            if count >= num_to_remove:
                break
            if route.has_order(order):
                DestroyOperators._remove_order_from_route(route, order)
                removed.append(order)
                count += 1

        solution.invalidate_cost()
        return removed

    # ── (5) Time-based Removal ──
    @staticmethod
    def time_based_removal(solution: ALNSSolution, num_to_remove: int,
                           dist_matrix: Dict, **kwargs) -> List[Order]:
        priority_config = kwargs.get('priority_config', DEFAULT_PRIORITY_CONFIG)
        removed = []
        late_orders = []

        for route in solution.routes:
            for stop in route.stops:
                order = stop.order
                arrival = getattr(stop, 'arrival_time', 0)
                deadline = get_order_deadline(order, priority_config)
                if arrival > deadline:
                    lateness = arrival - deadline
                    priority = getattr(order, 'priority', 4)
                    urgency = lateness * (6 - priority)
                    late_orders.append((urgency, route, order))

        if not late_orders:
            return DestroyOperators.random_removal(solution, num_to_remove, dist_matrix)

        late_orders.sort(key=lambda x: -x[0])
        num_to_remove = min(num_to_remove, len(late_orders))

        for i in range(num_to_remove):
            _, route, order = late_orders[i]
            if route.has_order(order):
                DestroyOperators._remove_order_from_route(route, order)
                removed.append(order)

        solution.invalidate_cost()
        return removed

    # ── (6) Large Shake Removal ──
    @staticmethod
    def large_shake_removal(solution: ALNSSolution, num_to_remove: int,
                            dist_matrix: Dict, **kwargs) -> List[Order]:
        removed = []
        all_orders = []
        for route in solution.routes:
            for order in DestroyOperators._get_orders_from_route(route):
                all_orders.append((route, order))

        if not all_orders:
            return removed

        shake_ratio = random.uniform(0.4, 0.6)
        num_to_remove = max(num_to_remove, int(len(all_orders) * shake_ratio))
        num_to_remove = min(num_to_remove, len(all_orders) - 1)

        to_remove = random.sample(all_orders, num_to_remove)
        for route, order in to_remove:
            if route.has_order(order):
                DestroyOperators._remove_order_from_route(route, order)
                removed.append(order)

        solution.invalidate_cost()
        return removed

    # ── (7) Cluster Removal ──
    @staticmethod
    def cluster_removal(solution: ALNSSolution, num_to_remove: int,
                        dist_matrix: Dict, **kwargs) -> List[Order]:
        removed = []
        zone_orders = {}
        for route in solution.routes:
            for order in DestroyOperators._get_orders_from_route(route):
                zone = getattr(order, 'zone', 'OTHER')
                if zone not in zone_orders:
                    zone_orders[zone] = []
                zone_orders[zone].append((route, order))

        if not zone_orders:
            return removed

        zones = list(zone_orders.keys())
        weights = [len(zone_orders[z]) for z in zones]
        total = sum(weights)
        if total == 0:
            return removed

        r = random.random() * total
        cumulative = 0
        selected_zone = zones[0]
        for i, z in enumerate(zones):
            cumulative += weights[i]
            if cumulative >= r:
                selected_zone = z
                break

        orders_in_zone = zone_orders[selected_zone]
        num_to_remove = min(num_to_remove, len(orders_in_zone))
        to_remove = random.sample(orders_in_zone, num_to_remove)

        for route, order in to_remove:
            if route.has_order(order):
                DestroyOperators._remove_order_from_route(route, order)
                removed.append(order)

        solution.invalidate_cost()
        return removed

    # ── (8) Depot-based Removal ──
    @staticmethod
    def depot_based_removal(solution: ALNSSolution, num_to_remove: int,
                            dist_matrix: Dict, **kwargs) -> List[Order]:
        depot_costs = defaultdict(lambda: {'cost': 0, 'orders': []})

        for route in solution.routes:
            orders = DestroyOperators._get_orders_from_route(route)
            if not orders:
                continue
            cost_per_order = route.total_distance * getattr(route.vehicle, 'variable_cost', 14) / len(orders)
            for order in orders:
                depot_costs[order.plant]['cost'] += cost_per_order
                depot_costs[order.plant]['orders'].append((route, order))

        if not depot_costs:
            return DestroyOperators.random_removal(solution, num_to_remove, dist_matrix)

        worst_depot = max(depot_costs.keys(),
                         key=lambda d: depot_costs[d]['cost'] / max(1, len(depot_costs[d]['orders'])))

        orders_to_remove = depot_costs[worst_depot]['orders']
        num_to_remove = min(num_to_remove, len(orders_to_remove))
        to_remove = random.sample(orders_to_remove, num_to_remove)

        removed = []
        for route, order in to_remove:
            if route.has_order(order):
                DestroyOperators._remove_order_from_route(route, order)
                removed.append(order)

        solution.invalidate_cost()
        return removed

    # ── (9) Cross-depot Removal ──
    @staticmethod
    def cross_depot_removal(solution: ALNSSolution, num_to_remove: int,
                            dist_matrix: Dict, **kwargs) -> List[Order]:
        removed = []
        multi_depot_routes = [r for r in solution.routes if len(r.orders_by_depot) > 1]

        if not multi_depot_routes:
            return DestroyOperators.random_removal(solution, num_to_remove, dist_matrix)

        for route in multi_depot_routes:
            if len(removed) >= num_to_remove:
                break
            depot_counts = {d: len(orders) for d, orders in route.orders_by_depot.items()}
            minority_depot = min(depot_counts.keys(), key=lambda d: depot_counts[d])

            orders = DestroyOperators._get_orders_from_route(route)
            for order in orders.copy():
                if order.plant == minority_depot and len(removed) < num_to_remove:
                    DestroyOperators._remove_order_from_route(route, order)
                    removed.append(order)

        solution.invalidate_cost()
        return removed

    # ── (10) Priority-based Removal ──
    @staticmethod
    def priority_based_removal(solution: ALNSSolution, num_to_remove: int,
                               dist_matrix: Dict, **kwargs) -> List[Order]:
        priority_config = kwargs.get('priority_config', DEFAULT_PRIORITY_CONFIG)
        removed = []
        priority_miss_orders = []

        for route in solution.routes:
            for stop in route.stops:
                order = stop.order
                arrival = getattr(stop, 'arrival_time', 0)
                deadline = get_order_deadline(order, priority_config)
                if arrival > deadline:
                    priority = getattr(order, 'priority', 4)
                    urgency = (6 - priority) * 1000 + (arrival - deadline)
                    priority_miss_orders.append((urgency, route, order))

        if not priority_miss_orders:
            return DestroyOperators.random_removal(solution, num_to_remove, dist_matrix)

        priority_miss_orders.sort(key=lambda x: -x[0])
        num_to_remove = min(num_to_remove, len(priority_miss_orders))

        for i in range(num_to_remove):
            _, route, order = priority_miss_orders[i]
            if route.has_order(order):
                DestroyOperators._remove_order_from_route(route, order)
                removed.append(order)

        solution.invalidate_cost()
        return removed


# =====================================================
# SECTION 14: REPAIR OPERATORS (3 ตัว)
# =====================================================
class RepairOperators:
    """
    Repair Operators — v3.2 Single Source of Truth
    ✅ B1 FIX: เพิ่ม apply_break_time() ในทุก method ที่คำนวณ arrival
    """

    @staticmethod
    def _insert_order_at_position(route: Route, order: Order,
                                  position: int,
                                  depots: Dict[str, Depot],
                                  dist_matrix: Dict,
                                  locations: Dict):
        """
        Insert order เข้า route ที่ position + คำนวณ times ใหม่
        ✅ B1 FIX: ใช้ apply_break_time()
        """
        vehicle = route.vehicle
        default_depot = get_default_end_depot(depots)

        # --- หา previous location ---
        if position == 0:
            if route.pickup_sequence:
                prev_name = route.pickup_sequence[-1].name
            else:
                depot = depots.get(order.plant, default_depot)
                prev_name = depot.name if depot else ""

            prev_time = getattr(vehicle, 'start_time', 480)
            if hasattr(prev_time, 'hour'):
                prev_time = prev_time.hour * 60 + prev_time.minute

            # เพิ่ม pickup phase time
            if len(route.pickup_sequence) > 1:
                for k in range(len(route.pickup_sequence) - 1):
                    from_d = route.pickup_sequence[k]
                    to_d = route.pickup_sequence[k + 1]
                    d = get_distance(from_d.name, to_d.name,
                                     dist_matrix, locations)
                    prev_time += (d / 50) * 60 + SECOND_DEPOT_LOADING_TIME
            prev_cumulative = 0
        else:
            prev_stop = route.stops[position - 1]
            prev_name = prev_stop.order.customer_name
            prev_time = prev_stop.departure_time
            prev_cumulative = prev_stop.cumulative_distance

        # --- คำนวณ distance + time ---
        dist = get_distance(prev_name, order.customer_name,
                            dist_matrix, locations)
        speed = order.zone_speed if order.zone_speed > 0 else DEFAULT_SPEED_KMH
        travel_time = (dist / speed) * 60

        # ⭐ B1 FIX: ใช้ apply_break_time()
        arrival = apply_break_time(prev_time, travel_time, vehicle, enabled=False)

        time_open = getattr(order, 'time_open', 480)
        time_close = getattr(order, 'time_close', 1020)

        wait_time = 0
        if arrival < time_open:
            wait_time = int(time_open - arrival)
            arrival = time_open

        service = order.service_time + order.unload_time
        departure = arrival + service
        cumulative = prev_cumulative + dist

        stop = RouteStop(
            order=order,
            arrival_time=int(arrival),
            departure_time=int(departure),
            distance_from_prev=dist,
            cumulative_distance=cumulative,
            wait_time=wait_time,
            is_late=(arrival > time_close)
        )

        # --- ใช้ route.add_stop() เพื่อ invalidate cache อัตโนมัติ ---
        depot_obj = depots.get(order.plant)
        route.add_stop(stop, position, depot_obj)

        # --- recalculate times สำหรับ stops หลัง position ---
        RepairOperators._recalculate_times_from_position(
            route, position + 1, depots, dist_matrix, locations
        )

    @staticmethod
    def _recalculate_times_from_position(route: Route, start_position: int,
                                         depots: Dict[str, Depot],
                                         dist_matrix: Dict,
                                         locations: Dict):
        """
        Recalculate arrival/departure times จาก start_position เป็นต้นไป
        ✅ B1 FIX: ใช้ apply_break_time()
        """
        vehicle = route.vehicle

        for i in range(start_position, len(route.stops)):
            prev_stop = route.stops[i - 1]
            prev_name = prev_stop.order.customer_name
            prev_time = prev_stop.departure_time
            prev_cumulative = prev_stop.cumulative_distance

            stop = route.stops[i]
            order = stop.order

            dist = get_distance(prev_name, order.customer_name,
                                dist_matrix, locations)
            speed = order.zone_speed if order.zone_speed > 0 else DEFAULT_SPEED_KMH
            travel_time = (dist / speed) * 60

            # ⭐ B1 FIX: ใช้ apply_break_time()
            arrival = apply_break_time(prev_time, travel_time, vehicle, enabled=False)

            time_open = getattr(order, 'time_open', 480)
            time_close = getattr(order, 'time_close', 1020)

            wait_time = 0
            if arrival < time_open:
                wait_time = int(time_open - arrival)
                arrival = time_open

            service = order.service_time + order.unload_time
            departure = arrival + service

            stop.distance_from_prev = dist
            stop.cumulative_distance = prev_cumulative + dist
            stop.arrival_time = int(arrival)
            stop.departure_time = int(departure)
            stop.is_late = (arrival > time_close)
            stop.wait_time = wait_time

        if route.stops:
            route.total_distance = route.stops[-1].cumulative_distance
        else:
            route.total_distance = 0

    @staticmethod
    def _add_empty_routes(solution: ALNSSolution,
                          all_vehicles: List[Vehicle]):
        used_vehicle_ids = set(
            r.vehicle.vehicle_id for r in solution.routes
        )
        for v in all_vehicles:
            if v.vehicle_id not in used_vehicle_ids:
                empty_route = Route(vehicle=v)
                solution.routes.append(empty_route)

    @staticmethod
    def _remove_empty_routes(solution: ALNSSolution):
        solution.routes = [
            r for r in solution.routes if not r.is_empty()
        ]

    @staticmethod
    def _find_best_insertion(
        order, routes, depots, dist_matrix, locations,
        priority_config,
        allow_extra_drops=True,
        time_flex=120,
        depot_penalty_weight=0.3,
        prefer_earliest_arrival=False
    ):
        best_route = None
        best_position = None
        best_cost = float('inf')
        best_arrival = float('inf')

        priority = getattr(order, 'priority', 4)

        for route in routes:
            can_fit, reason = can_insert_order(
                route, order, route.vehicle, allow_extra_drops
            )
            if not can_fit:
                continue

            # ─── Depot bonus (เดิม) ───
            depot_bonus = 0
            if order.plant in route.orders_by_depot:
                depot_bonus = -100 * depot_penalty_weight
            elif route.num_stops == 0:
                depot_bonus = 0
            else:
                depot_bonus = 50 * depot_penalty_weight

            # ─── Zone bonus (เดิม) ───
            zone_bonus = 0
            if route.stops:
                first_order_zone = route.stops[0].order.zone
                if order.zone == first_order_zone:
                    zone_bonus = -50

            # ─── Priority bonus (เดิม) ───
            priority_bonus = 0
            if route.stops:
                avg_priority = (
                    sum(s.order.priority for s in route.stops)
                    / len(route.stops)
                )
                if abs(order.priority - avg_priority) <= 1:
                    priority_bonus = -30

            # ─── Geographic Proximity Bonus (เดิม) ───
            proximity_bonus = 0
            if route.stops:
                avg_lat = (
                    sum(s.order.lat for s in route.stops)
                    / len(route.stops)
                )
                avg_lng = (
                    sum(s.order.lng for s in route.stops)
                    / len(route.stops)
                )
                dist_to_centroid = haversine_distance(
                    order.lat, order.lng, avg_lat, avg_lng
                )
                if dist_to_centroid < 80:
                    proximity_bonus = -150 * (1 - dist_to_centroid / 80)
                route_provinces = set(
                    s.order.province for s in route.stops
                )
                if order.province in route_provinces:
                    proximity_bonus -= 80

            for pos in range(len(route.stops) + 1):
                feasible, arrival_time, check_reason = (
                    check_insertion_feasibility(
                        route, order, pos, depots,
                        dist_matrix, locations,
                        priority_config,
                        time_flex=time_flex,
                        allow_extra_drops=allow_extra_drops
                    )
                )
                if not feasible:
                    continue

                cost = _calc_insertion_cost_full(
                    route, order, pos, depots, locations,
                    dist_matrix,
                    arrival_time=arrival_time,
                    priority_config=priority_config
                )

                if route.is_empty():
                    cost += getattr(
                        route.vehicle, 'fixed_cost', 1500
                    )

                total_cost = (cost + depot_bonus + zone_bonus
                             + priority_bonus + proximity_bonus)

                if prefer_earliest_arrival and priority <= 2:
                    if (arrival_time < best_arrival or
                            (arrival_time == best_arrival
                             and total_cost < best_cost)):
                        best_cost = total_cost
                        best_route = route
                        best_position = pos
                        best_arrival = arrival_time
                else:
                    if total_cost < best_cost:
                        best_cost = total_cost
                        best_route = route
                        best_position = pos
                        best_arrival = arrival_time

        return best_route, best_position, best_cost, best_arrival

    # ══════════════════════════════════════════════
    # (1) Greedy Insertion
    # ══════════════════════════════════════════════
    @staticmethod
    def greedy_insertion(solution: ALNSSolution,
                         removed_orders: List[Order],
                         dist_matrix: Dict,
                         all_vehicles: List[Vehicle],
                         depots: Dict[str, Depot] = None,
                         priority_config: Dict = None,
                         locations: Dict = None):
        if priority_config is None:
            priority_config = DEFAULT_PRIORITY_CONFIG
        if depots is None:
            depots = {}
        if locations is None:
            locations = {}

        unassigned = []
        to_insert = removed_orders + solution.unassigned
        solution.unassigned = []

        to_insert.sort(
            key=lambda o: (getattr(o, 'priority', 4), -o.weight_kg)
        )

        RepairOperators._add_empty_routes(solution, all_vehicles)

        for order in to_insert:
            best_route, best_pos, best_cost, best_arrival = (
                RepairOperators._find_best_insertion(
                    order=order,
                    routes=solution.routes,
                    depots=depots,
                    dist_matrix=dist_matrix,
                    locations=locations,
                    priority_config=priority_config,
                    allow_extra_drops=True,
                    time_flex=120,
                    depot_penalty_weight=0.3,
                    prefer_earliest_arrival=False
                )
            )
            if best_route is not None:
                RepairOperators._insert_order_at_position(
                    best_route, order, best_pos,
                    depots, dist_matrix, locations
                )
            else:
                unassigned.append(order)

        RepairOperators._remove_empty_routes(solution)
        # ✅ ใส่ reason สำหรับ order ที่ ALNS insert ไม่ได้
        for o in unassigned:
            if not getattr(o, 'unassign_reason', ''):
                o.unassign_reason = "ALNS_NO_FIT — ALNS หาตำแหน่งที่เหมาะสมไม่ได้"
        solution.unassigned = unassigned
        solution.invalidate_cost()

    # ══════════════════════════════════════════════
    # (2) Regret-2 Insertion
    # ══════════════════════════════════════════════
    @staticmethod
    def regret_insertion(solution: ALNSSolution,
                         removed_orders: List[Order],
                         dist_matrix: Dict,
                         all_vehicles: List[Vehicle],
                         depots: Dict[str, Depot] = None,
                         priority_config: Dict = None,
                         locations: Dict = None):
        if priority_config is None:
            priority_config = DEFAULT_PRIORITY_CONFIG
        if depots is None:
            depots = {}
        if locations is None:
            locations = {}

        unassigned = []
        to_insert = removed_orders + solution.unassigned
        solution.unassigned = []

        RepairOperators._add_empty_routes(solution, all_vehicles)

        while to_insert:
            best_order = None
            best_route = None
            best_position = None
            best_regret = -float('inf')

            for order in to_insert:
                priority = getattr(order, 'priority', 4)

                insertion_options = []
                for route in solution.routes:
                    can_fit, _ = can_insert_order(
                        route, order, route.vehicle,
                        allow_extra_drops=True
                    )
                    if not can_fit:
                        continue

                    route_best_cost = float('inf')
                    route_best_pos = None

                    for pos in range(len(route.stops) + 1):
                        feasible, arrival_time, _ = (
                            check_insertion_feasibility(
                                route, order, pos, depots,
                                dist_matrix, locations,
                                priority_config,
                                time_flex=120,
                                allow_extra_drops=True
                            )
                        )
                        if not feasible:
                            continue

                        cost = _calc_insertion_cost_full(
                            route, order, pos, depots,
                            locations, dist_matrix,
                            arrival_time=arrival_time,
                            priority_config=priority_config
                        )
                        if route.is_empty():
                            cost += getattr(
                                route.vehicle, 'fixed_cost', 1500
                            )

                        bonus = 0
                        if order.plant in route.orders_by_depot:
                            bonus -= 30
                        if route.stops:
                            if (order.zone ==
                                    route.stops[0].order.zone):
                                bonus -= 50
                        total = cost + bonus

                        if total < route_best_cost:
                            route_best_cost = total
                            route_best_pos = pos

                    if route_best_pos is not None:
                        insertion_options.append(
                            (route_best_cost, route, route_best_pos)
                        )

                if not insertion_options:
                    continue

                insertion_options.sort(key=lambda x: x[0])
                best_cost = insertion_options[0][0]
                second_best = (
                    insertion_options[1][0]
                    if len(insertion_options) > 1
                    else best_cost + 1000
                )
                regret = second_best - best_cost
                regret *= (6 - priority)

                if regret > best_regret:
                    best_regret = regret
                    best_order = order
                    best_route = insertion_options[0][1]
                    best_position = insertion_options[0][2]

            if best_order is not None:
                RepairOperators._insert_order_at_position(
                    best_route, best_order, best_position,
                    depots, dist_matrix, locations
                )
                to_insert.remove(best_order)
            else:
                unassigned.extend(to_insert)
                break

        RepairOperators._remove_empty_routes(solution)
        # ✅ ใส่ reason สำหรับ order ที่ ALNS insert ไม่ได้
        for o in unassigned:
            if not getattr(o, 'unassign_reason', ''):
                o.unassign_reason = "ALNS_NO_FIT — ALNS หาตำแหน่งที่เหมาะสมไม่ได้"
        solution.unassigned = unassigned
        solution.invalidate_cost()

    # ══════════════════════════════════════════════
    # (3) Priority-First Insertion
    # ══════════════════════════════════════════════
    @staticmethod
    def priority_first_insertion(solution: ALNSSolution,
                                  removed_orders: List[Order],
                                  dist_matrix: Dict,
                                  all_vehicles: List[Vehicle],
                                  depots: Dict[str, Depot] = None,
                                  priority_config: Dict = None,
                                  locations: Dict = None):
        if priority_config is None:
            priority_config = DEFAULT_PRIORITY_CONFIG
        if depots is None:
            depots = {}
        if locations is None:
            locations = {}

        unassigned = []
        to_insert = removed_orders + solution.unassigned
        solution.unassigned = []

        def sort_key(order):
            p = getattr(order, 'priority', 4)
            d = get_order_deadline(order, priority_config)
            return (p, d, -order.weight_kg)

        to_insert.sort(key=sort_key)

        RepairOperators._add_empty_routes(solution, all_vehicles)

        for order in to_insert:
            priority = getattr(order, 'priority', 4)
            best_route, best_pos, best_cost, best_arrival = (
                RepairOperators._find_best_insertion(
                    order=order,
                    routes=solution.routes,
                    depots=depots,
                    dist_matrix=dist_matrix,
                    locations=locations,
                    priority_config=priority_config,
                    allow_extra_drops=True,
                    time_flex=120,
                    depot_penalty_weight=0.3,
                    prefer_earliest_arrival=(priority <= 2)
                )
            )
            if best_route is not None:
                RepairOperators._insert_order_at_position(
                    best_route, order, best_pos,
                    depots, dist_matrix, locations
                )
            else:
                unassigned.append(order)

        RepairOperators._remove_empty_routes(solution)
        # ✅ ใส่ reason สำหรับ order ที่ ALNS insert ไม่ได้
        for o in unassigned:
            if not getattr(o, 'unassign_reason', ''):
                o.unassign_reason = "ALNS_NO_FIT — ALNS หาตำแหน่งที่เหมาะสมไม่ได้"
        solution.unassigned = unassigned
        solution.invalidate_cost()

# =====================================================
# SECTION 15: LOCAL SEARCH OPERATORS
# =====================================================
class LocalSearchOperators:
    """
    Local Search — v3.2 Single Source of Truth
    ✅ B1 FIX: เพิ่ม apply_break_time() ใน _rebuild_stops_times
    ✅ C3 FIX: เพิ่ม _invalidate_cache() ใน inter_route_relocate
    """

    @staticmethod
    def _get_start_name(route: Route,
                        depots: Dict[str, Depot]) -> str:
        if (hasattr(route, 'pickup_sequence') and
                route.pickup_sequence):
            return route.pickup_sequence[-1].name
        default_depot = get_default_end_depot(depots)
        if default_depot:
            return default_depot.name
        if depots:
            return list(depots.values())[0].name
        return ""

    @staticmethod
    def _calc_route_distance(route: Route,
                             dist_matrix: Dict,
                             locations: Dict,
                             depots: Dict[str, Depot]) -> float:
        if not route.stops:
            return 0
        total_dist = 0
        prev_name = LocalSearchOperators._get_start_name(
            route, depots
        )
        for stop in route.stops:
            order = stop.order
            dist = get_distance(
                prev_name, order.customer_name,
                dist_matrix, locations
            )
            total_dist += dist
            prev_name = order.customer_name
        return total_dist

    @staticmethod
    def _rebuild_stops_times(route: Route,
                             dist_matrix: Dict,
                             locations: Dict,
                             depots: Dict[str, Depot]):
        """
        Rebuild arrival/departure/cumulative สำหรับทุก stop
        ✅ B1 FIX: ใช้ apply_break_time()
        """
        if not route.stops:
            route.total_distance = 0
            return

        vehicle = route.vehicle
        prev_name = LocalSearchOperators._get_start_name(
            route, depots
        )

        start_time = getattr(vehicle, 'start_time', 480)
        if hasattr(start_time, 'hour'):
            start_time = start_time.hour * 60 + start_time.minute
        current_time = start_time

        # เพิ่ม pickup phase time
        if (hasattr(route, 'pickup_sequence') and
                len(route.pickup_sequence) > 1):
            for k in range(len(route.pickup_sequence) - 1):
                from_d = route.pickup_sequence[k]
                to_d = route.pickup_sequence[k + 1]
                d = get_distance(
                    from_d.name, to_d.name,
                    dist_matrix, locations
                )
                current_time += (d / 50) * 60 + SECOND_DEPOT_LOADING_TIME

        cumulative_dist = 0
        for stop in route.stops:
            order = stop.order
            dist = get_distance(
                prev_name, order.customer_name,
                dist_matrix, locations
            )
            speed = (order.zone_speed
                     if order.zone_speed > 0
                     else DEFAULT_SPEED_KMH)
            travel_time = (dist / speed) * 60

            # ⭐ B1 FIX: ใช้ apply_break_time()
            arrival = apply_break_time(current_time, travel_time, vehicle)

            time_open = getattr(order, 'time_open', 480)
            time_close = getattr(order, 'time_close', 1020)

            wait_time = 0
            if arrival < time_open:
                wait_time = int(time_open - arrival)
                arrival = time_open

            service = (order.service_time + order.unload_time)
            departure = arrival + service
            cumulative_dist += dist

            stop.distance_from_prev = dist
            stop.cumulative_distance = cumulative_dist
            stop.arrival_time = int(arrival)
            stop.departure_time = int(departure)
            stop.is_late = (arrival > time_close)
            stop.wait_time = wait_time

            current_time = departure
            prev_name = order.customer_name

        route.total_distance = cumulative_dist

    # ══════════════════════════════════════════════
    # (1) Intra-route 2-opt
    # ══════════════════════════════════════════════
    @staticmethod
    def intra_route_2opt(solution: ALNSSolution,
                         dist_matrix: Dict,
                         locations: Dict,
                         depots: Dict[str, Depot]) -> int:
        total_improvements = 0

        for route in solution.routes:
            if len(route.stops) < 3:
                continue

            improved = True
            while improved:
                improved = False
                for i in range(len(route.stops) - 1):
                    for j in range(i + 2, len(route.stops)):
                        dist_before = (
                            LocalSearchOperators._calc_route_distance(
                                route, dist_matrix,
                                locations, depots
                            )
                        )
                        # Reverse segment [i+1 .. j]
                        route.stops[i+1:j+1] = (
                            route.stops[i+1:j+1][::-1]
                        )
                        dist_after = (
                            LocalSearchOperators._calc_route_distance(
                                route, dist_matrix,
                                locations, depots
                            )
                        )

                        if dist_after < dist_before - 0.1:
                            LocalSearchOperators._rebuild_stops_times(
                                route, dist_matrix,
                                locations, depots
                            )
                            route._invalidate_cache()
                            improved = True
                            total_improvements += 1
                        else:
                            # Revert
                            route.stops[i+1:j+1] = (
                                route.stops[i+1:j+1][::-1]
                            )

        if total_improvements > 0:
            solution.invalidate_cost()
        return total_improvements

    # ══════════════════════════════════════════════
    # (2) Inter-route Relocate
    # ══════════════════════════════════════════════
    @staticmethod
    def inter_route_relocate(solution: ALNSSolution,
                             dist_matrix: Dict,
                             locations: Dict,
                             depots: Dict[str, Depot],
                             max_iterations: int = 50) -> int:
        """
        Relocate order จาก route หนึ่ง → route อื่น
        ✅ C3 FIX: เพิ่ม _invalidate_cache() หลัง manual insert/pop
        """
        total_improvements = 0
        improved = True

        while improved and total_improvements < max_iterations:
            improved = False
            best_saving = 0
            best_move = None

            routes = [r for r in solution.routes if r.stops]

            for route_from in routes:
                if route_from.num_stops <= 1:
                    continue

                for idx, stop in enumerate(route_from.stops):
                    order = stop.order

                    for route_to in routes:
                        if route_to is route_from:
                            continue

                        # ── Capacity check ครบ 3 ตัว ──
                        new_weight = (route_to.total_weight +
                                      order.weight_kg)
                        if new_weight > route_to.vehicle.max_weight_kg:
                            continue
                        new_volume = (route_to.total_volume +
                                      order.volume_cbm)
                        if new_volume > route_to.vehicle.max_volume_cbm:
                            continue
                        max_drops = getattr(
                            route_to.vehicle, 'max_drops', 10
                        )
                        if route_to.num_stops >= max_drops:
                            continue

                        # ── Distance before ──
                        dist_before = (
                            LocalSearchOperators._calc_route_distance(
                                route_from, dist_matrix,
                                locations, depots
                            ) +
                            LocalSearchOperators._calc_route_distance(
                                route_to, dist_matrix,
                                locations, depots
                            )
                        )

                        # ── หา best position ใน route_to ──
                        best_pos = 0
                        best_insert_dist = float('inf')

                        for pos in range(len(route_to.stops) + 1):
                            # ⭐ C3 FIX: invalidate cache หลัง manual insert/pop
                            route_to.stops.insert(pos, stop)
                            route_to._invalidate_cache()
                            insert_dist = (
                                LocalSearchOperators._calc_route_distance(
                                    route_to, dist_matrix,
                                    locations, depots
                                )
                            )
                            route_to.stops.pop(pos)
                            route_to._invalidate_cache()

                            if insert_dist < best_insert_dist:
                                best_insert_dist = insert_dist
                                best_pos = pos

                        # ── Distance after remove from route_from ──
                        removed_stop = route_from.stops.pop(idx)
                        route_from._invalidate_cache()
                        dist_after_from = (
                            LocalSearchOperators._calc_route_distance(
                                route_from, dist_matrix,
                                locations, depots
                            )
                        )
                        route_from.stops.insert(idx, removed_stop)
                        route_from._invalidate_cache()

                        # ── Total saving ──
                        total_saving = dist_before - (
                            dist_after_from + best_insert_dist
                        )

                        if total_saving > best_saving + 0.1:
                            best_saving = total_saving
                            best_move = (
                                route_from, idx, stop,
                                route_to, best_pos, order
                            )

            # ── Execute best move ──
            if best_move and best_saving > 0.5:
                (route_from, idx, stop,
                 route_to, pos, order) = best_move

                route_from.remove_order(order)

                depot_obj = depots.get(order.plant)
                route_to.add_stop(stop, pos, depot_obj)

                LocalSearchOperators._rebuild_stops_times(
                    route_from, dist_matrix, locations, depots
                )
                LocalSearchOperators._rebuild_stops_times(
                    route_to, dist_matrix, locations, depots
                )

                improved = True
                total_improvements += 1

        if total_improvements > 0:
            solution.invalidate_cost()
        return total_improvements

    # ══════════════════════════════════════════════
    # Run All
    # ══════════════════════════════════════════════
    @staticmethod
    def run_all(solution: ALNSSolution,
                dist_matrix: Dict,
                locations: Dict,
                depots: Dict[str, Depot]) -> int:
        total = 0
        total += LocalSearchOperators.intra_route_2opt(
            solution, dist_matrix, locations, depots
        )
        total += LocalSearchOperators.inter_route_relocate(
            solution, dist_matrix, locations, depots
        )
        return total


# =====================================================
# SECTION 16: ALNS SOLVER
# =====================================================
class ALNSSolver:
    """
    ALNS Solver with Priority Support
    ✅ A1 FIX: ไม่ reset no_improvement_count ตอน reheat
    ✅ A2 FIX: ใช้ interval-based reheat
    ✅ A3 FIX: _should_stop() ไม่หยุดถ้ายัง reheat ได้
    """

    def __init__(self, initial_routes: List[Route], unassigned_orders: List[Order],
                 all_orders: List[Order], all_vehicles: List[Vehicle],
                 dist_matrix: Dict, depots: Dict[str, Depot] = None,
                 locations: Dict = None, priority_config: Dict = None,
                 progress: VRPProgress = None):
        """Initialize ALNS Solver"""
        self.priority_config = priority_config or DEFAULT_PRIORITY_CONFIG
        self.progress = progress or VRPProgress()

        # Solution
        self.current = ALNSSolution(
            routes=[r for r in initial_routes],
            unassigned=unassigned_orders.copy()
        )
        self.current.set_priority_config(self.priority_config)
        self.best = self.current.copy()

        # Data
        self.all_orders = all_orders
        self.all_vehicles = all_vehicles
        self.dist_matrix = dist_matrix
        self.depots = depots or {}
        self.locations = locations or {}

        # ── Define Operators (10 destroy + 3 repair) ──
        self.destroy_operators = [
            ("Random Removal", DestroyOperators.random_removal),
            ("Worst Removal", DestroyOperators.worst_removal),
            ("Route Removal", DestroyOperators.route_removal),
            ("Related Removal", DestroyOperators.related_removal),
            ("Time-based Removal", DestroyOperators.time_based_removal),
            ("Large Shake", DestroyOperators.large_shake_removal),
            ("Cluster Removal", DestroyOperators.cluster_removal),
            ("Depot-based Removal", DestroyOperators.depot_based_removal),
            ("Cross-depot Removal", DestroyOperators.cross_depot_removal),
            ("Priority-based Removal", DestroyOperators.priority_based_removal),
        ]
        self.repair_operators = [
            ("Greedy Insertion", RepairOperators.greedy_insertion),
            ("Regret-2 Insertion", RepairOperators.regret_insertion),
            ("Priority-First Insertion", RepairOperators.priority_first_insertion),
        ]

        # ── Adaptive Weights ──
        num_destroy = len(self.destroy_operators)
        num_repair = len(self.repair_operators)
        self.destroy_weights = [ALNSConfig.INITIAL_WEIGHT] * num_destroy
        self.repair_weights = [ALNSConfig.INITIAL_WEIGHT] * num_repair
        self.destroy_scores = [0.0] * num_destroy
        self.repair_scores = [0.0] * num_repair
        self.destroy_counts = [0] * num_destroy
        self.repair_counts = [0] * num_repair

        # ── State ──
        self.temperature = ALNSConfig.INITIAL_TEMPERATURE
        self.iteration = 0
        self.best_iteration = 0
        self.no_improvement_count = 0
        self.reheat_count = 0
        self.start_time = None

        # ── History ──
        self.cost_history = []
        self.best_cost_history = []
        self.temperature_history = []

    def _should_stop(self) -> bool:
        """
        ✅ A3 FIX: ถ้ายัง reheat ได้ → อย่าเพิ่งหยุด
        """
        if self.iteration >= ALNSConfig.MAX_ITERATIONS:
            return True

        if self.no_improvement_count >= ALNSConfig.NO_IMPROVEMENT_LIMIT:
            # ⭐ A3 FIX: ถ้ายัง reheat ได้ → อย่าเพิ่งหยุด
            if (ALNSConfig.REHEAT_ENABLED and
                self.reheat_count < ALNSConfig.MAX_REHEATS):
                return False
            return True

        if self.start_time:
            elapsed = time.time() - self.start_time
            if elapsed >= ALNSConfig.TIME_LIMIT_SECONDS:
                return True
        return False

    def _select_operator(self, weights: List[float]) -> int:
        total = sum(weights)
        if total == 0:
            return random.randint(0, len(weights) - 1)
        r = random.random() * total
        cumulative = 0
        for i, w in enumerate(weights):
            cumulative += w
            if cumulative >= r:
                return i
        return len(weights) - 1

    def _get_num_to_remove(self) -> int:
        total_orders = sum(r.num_stops for r in self.current.routes)
        if total_orders == 0:
            return 1
        min_remove = max(1, int(total_orders * ALNSConfig.MIN_DESTROY_RATIO))
        max_remove = max(2, int(total_orders * ALNSConfig.MAX_DESTROY_RATIO))
        return random.randint(min_remove, max_remove)

    def _accept_solution(self, new_cost: float, current_cost: float) -> bool:
        if new_cost < current_cost:
            return True
        if self.temperature <= 0:
            return False
        delta = new_cost - current_cost
        probability = math.exp(-delta / self.temperature)
        return random.random() < probability

    def _update_weights(self):
        for i in range(len(self.destroy_weights)):
            if self.destroy_counts[i] > 0:
                avg_score = self.destroy_scores[i] / self.destroy_counts[i]
                self.destroy_weights[i] = (
                    ALNSConfig.WEIGHT_DECAY * self.destroy_weights[i] +
                    (1 - ALNSConfig.WEIGHT_DECAY) * avg_score
                )
                self.destroy_weights[i] = max(ALNSConfig.MIN_WEIGHT,
                                                self.destroy_weights[i])

        for i in range(len(self.repair_weights)):
            if self.repair_counts[i] > 0:
                avg_score = self.repair_scores[i] / self.repair_counts[i]
                self.repair_weights[i] = (
                    ALNSConfig.WEIGHT_DECAY * self.repair_weights[i] +
                    (1 - ALNSConfig.WEIGHT_DECAY) * avg_score
                )
                self.repair_weights[i] = max(ALNSConfig.MIN_WEIGHT,
                                              self.repair_weights[i])

    def _reset_scores(self):
        self.destroy_scores = [0.0] * len(self.destroy_operators)
        self.repair_scores = [0.0] * len(self.repair_operators)
        self.destroy_counts = [0] * len(self.destroy_operators)
        self.repair_counts = [0] * len(self.repair_operators)

    def _report_progress(self, accepted: bool, score: int):
        elapsed = time.time() - self.start_time if self.start_time else 0
        pct = min(95, int((elapsed / ALNSConfig.TIME_LIMIT_SECONDS) * 95))
        num_vehicles = sum(1 for r in self.best.routes if not r.is_empty())

        self.progress.report(
            "alns",
            f"🔄 Iter {self.iteration}: "
            f"Best={self.best.display_cost:,.0f} THB | "
            f"Current={self.current.display_cost:,.0f} THB | "
            f"T={self.temperature:.1f} | "
            f"V={num_vehicles} | "
            f"No-imp={self.no_improvement_count}",
            pct,
            {
                'iteration': self.iteration,
                'best_cost': self.best.display_cost,
                'current_cost': self.current.display_cost,
                'temperature': self.temperature,
                'no_improvement': self.no_improvement_count,
                'elapsed': elapsed,
                'accepted': accepted,
                'score': score,
                'num_vehicles': num_vehicles,
            }
        )

    def _report_final_results(self):
        elapsed = time.time() - self.start_time if self.start_time else 0
        summary = self.best.get_summary()
        bd = self.best.display_breakdown

        self.progress.report(
            "alns",
            f"✅ ALNS เสร็จสิ้น! "
            f"ต้นทุน={self.best.display_cost:,.0f} THB | "
            f"รถ={summary['num_vehicles']} คัน | "
            f"จัดได้={summary['assigned_orders']}/{summary['total_orders']} | "
            f"เวลา={elapsed:.1f}s | "
            f"Iterations={self.iteration}",
            100,
            {
                'summary': summary,
                'display_breakdown': dict(bd) if bd else {},
                'elapsed': elapsed,
                'iterations': self.iteration,
                'best_iteration': self.best_iteration,
            }
        )

    def solve(self) -> 'ALNSSolution':
        """
        Main ALNS loop
        ✅ A1 FIX: ไม่ reset no_improvement_count ตอน reheat
        ✅ A2 FIX: ใช้ interval-based reheat
        """
        self.start_time = time.time()

        initial_summary = self.current.get_summary()
        self.progress.report(
            "alns",
            f"📊 Initial: Cost={self.current.display_cost:,.0f} THB | "
            f"V={initial_summary['num_vehicles']} | "
            f"Assigned={initial_summary['assigned_orders']}"
            f"/{initial_summary['total_orders']}",
            5,
            {'initial_summary': initial_summary}
        )

        # ── Main Loop ──
        while not self._should_stop():
            self.iteration += 1

            # 1) Copy
            candidate = self.current.copy()

            # 2) Select operators
            destroy_idx = self._select_operator(self.destroy_weights)
            repair_idx = self._select_operator(self.repair_weights)
            destroy_name, destroy_func = self.destroy_operators[destroy_idx]
            repair_name, repair_func = self.repair_operators[repair_idx]

            # 3) Destroy
            num_to_remove = self._get_num_to_remove()
            try:
                removed = destroy_func(
                    candidate, num_to_remove, self.dist_matrix,
                    priority_config=self.priority_config
                )
            except Exception:
                continue

            # 4) Repair
            try:
                repair_func(
                    candidate, removed, self.dist_matrix,
                    self.all_vehicles, self.depots,
                    priority_config=self.priority_config,
                    locations=self.locations
                )
            except Exception:
                continue

            # 5) Evaluate
            new_cost = candidate.cost
            current_cost = self.current.cost

            # 6) Accept/Reject
            score = ALNSConfig.SCORE_REJECTED
            accepted = False

            if new_cost < self.best.cost:
                score = ALNSConfig.SCORE_BEST
                self.best = candidate.copy()
                self.current = candidate
                self.best_iteration = self.iteration
                self.no_improvement_count = 0
                accepted = True
            elif new_cost < current_cost:
                score = ALNSConfig.SCORE_BETTER
                self.current = candidate
                self.no_improvement_count = 0
                accepted = True
            else:
                delta = new_cost - current_cost
                if self.temperature > 0:
                    acceptance_prob = math.exp(
                        -delta / self.temperature
                    )
                    if random.random() < acceptance_prob:
                        score = ALNSConfig.SCORE_ACCEPTED
                        self.current = candidate
                        accepted = True
                if not accepted:
                    self.no_improvement_count += 1

            # 7) ⭐ A1+A2 FIX: Reheating — interval-based, ไม่ reset no_improvement_count
            if (ALNSConfig.REHEAT_ENABLED and
                self.reheat_count < ALNSConfig.MAX_REHEATS and
                self.no_improvement_count > 0 and
                self.no_improvement_count % ALNSConfig.REHEAT_INTERVAL == 0):

                self.temperature = ALNSConfig.REHEAT_TEMPERATURE
                self.reheat_count += 1
                # ⭐ A1 FIX: ไม่ reset no_improvement_count!

                self.progress.report(
                    "alns",
                    f"🔥 REHEAT #{self.reheat_count} "
                    f"(no_imp={self.no_improvement_count}) "
                    f"→ T={self.temperature}",
                    min(90, int(
                        (time.time() - self.start_time) /
                        ALNSConfig.TIME_LIMIT_SECONDS * 95
                    ))
                )

                # Local Search หลัง reheat
                ls_improvements = LocalSearchOperators.run_all(
                    self.current, self.dist_matrix,
                    self.locations, self.depots
                )
                if self.current.cost < self.best.cost:
                    self.best = self.current.copy()
                    self.best_iteration = self.iteration
                    self.no_improvement_count = 0  # ← reset เฉพาะตอน BEST จริงๆ

            # 8) Update scores
            self.destroy_scores[destroy_idx] += score
            self.repair_scores[repair_idx] += score
            self.destroy_counts[destroy_idx] += 1
            self.repair_counts[repair_idx] += 1

            # 9) Cool down
            self.temperature = max(
                ALNSConfig.MIN_TEMPERATURE,
                self.temperature * ALNSConfig.COOLING_RATE
            )

            # 10) Update weights periodically
            if self.iteration % ALNSConfig.SCORE_RESET_INTERVAL == 0:
                self._update_weights()
                self._reset_scores()

            # 11) Record history
            self.cost_history.append(self.current.cost)
            self.best_cost_history.append(self.best.cost)
            self.temperature_history.append(self.temperature)

            # 12) Report progress
            if self.iteration % ALNSConfig.DISPLAY_INTERVAL == 0:
                self._report_progress(accepted, score)

        # ── Final Local Search ──
        self.progress.report("alns", "🔧 Final Local Search...", 95)
        final_ls = LocalSearchOperators.run_all(
            self.best, self.dist_matrix,
            self.locations, self.depots
        )

        self._report_final_results()
        return self.best

    def get_operator_stats(self) -> Dict:
        stats = {
            'destroy': [],
            'repair': [],
            'history': {
                'cost': self.cost_history,
                'best_cost': self.best_cost_history,
                'temperature': self.temperature_history,
            },
            'iterations': self.iteration,
            'best_iteration': self.best_iteration,
            'reheat_count': self.reheat_count,
            'elapsed': (time.time() - self.start_time) if self.start_time else 0
        }

        for i, (name, _) in enumerate(self.destroy_operators):
            stats['destroy'].append({
                'name': name,
                'weight': self.destroy_weights[i],
                'count': self.destroy_counts[i],
                'score': self.destroy_scores[i]
            })

        for i, (name, _) in enumerate(self.repair_operators):
            stats['repair'].append({
                'name': name,
                'weight': self.repair_weights[i],
                'count': self.repair_counts[i],
                'score': self.repair_scores[i]
            })

        return stats


# =====================================================
# SECTION 17: POST-PROCESSING
# =====================================================
def post_process_solution(solution: ALNSSolution, depots: Dict[str, Depot],
                          dist_matrix: Dict, locations: Dict,
                          progress: VRPProgress = None) -> ALNSSolution:
    """
    ปรับปรุง solution หลัง ALNS:
    1. ลบ route ว่าง
    2. คำนวณ metrics ใหม่ทุก route
    3. เรียง route ตามจำนวน stop
    ✅ C2 FIX: ลบ print() ทั้งหมด → ใช้ logger
    """
    if progress:
        progress.report("post", "🔧 Post-processing...", 0)

    # ลบ route ว่าง
    solution.routes = [r for r in solution.routes if not r.is_empty()]

    # คำนวณ metrics + times ใหม่ทุก route
    for i, route in enumerate(solution.routes):
        calculate_stop_times(route, depots, dist_matrix, locations)
        calculate_route_metrics(route, depots, dist_matrix, locations)

    # เรียง route: มากไปน้อย (จำนวน stop)
    solution.routes.sort(key=lambda r: -r.num_stops)

    # Invalidate cost เพื่อคำนวณใหม่
    solution.invalidate_cost()

    if progress:
        summary = solution.get_summary()
        progress.report(
            "post",
            f"✅ Post-processing เสร็จ: "
            f"{summary['num_vehicles']} คัน, "
            f"{summary['assigned_orders']} ออเดอร์",
            100
        )

    # ✅ C2 FIX: ใช้ logger แทน print
    total_pickup = 0
    total_delivery = 0
    total_return = 0
    total_combined = 0

    for i, route in enumerate(solution.routes):
        if route.is_empty():
            continue
        pd_ = getattr(route, 'pickup_distance', 0)
        dd = getattr(route, 'delivery_distance', 0)
        rd = getattr(route, 'return_distance', 0)
        td = getattr(route, 'total_distance', 0)
        total_pickup += pd_
        total_delivery += dd
        total_return += rd
        total_combined += td

        logger.debug(
            f"R{i+1} {route.vehicle.vehicle_id}: "
            f"pickup={pd_:.1f} delivery={dd:.1f} "
            f"return={rd:.1f} total={td:.1f} "
            f"sum={pd_+dd+rd:.1f}"
        )

    logger.debug(f"DISTANCE VERIFICATION:")
    logger.debug(f"  Total Pickup:   {total_pickup:,.1f} km")
    logger.debug(f"  Total Delivery: {total_delivery:,.1f} km")
    logger.debug(f"  Total Return:   {total_return:,.1f} km")
    logger.debug(f"  Total (P+D):    {total_combined:,.1f} km")
    logger.debug(f"  Total (P+D+R):  {total_pickup+total_delivery+total_return:,.1f} km")

    summary = solution.get_summary()
    logger.debug(f"  Summary shows:  {summary['total_distance']:,.1f} km")
    logger.debug(f"  Breakdown P:    {summary.get('pickup_distance',0):,.1f} km")
    logger.debug(f"  Breakdown D:    {summary.get('delivery_distance',0):,.1f} km")

    return solution


# =====================================================
# SECTION 18: MAIN PIPELINE
# =====================================================
def run_pipeline(excel_file, progress: VRPProgress = None):
    """
    Main pipeline
    ✅ C1 FIX: ใช้ vrp_data.skipped_orders แทน global
    ✅ C2 FIX: ลบ print() ทั้งหมด → ใช้ logger
    """
    if progress is None:
        progress = VRPProgress()

    pipeline_start = time.time()
    filename = getattr(excel_file, 'name', 'unknown.xlsx')

    # ══════════════════════════════════════════════
    # PHASE 0: VALIDATE INPUT
    # ══════════════════════════════════════════════
    progress.report('validate', f'🔍 ตรวจสอบไฟล์ {filename}...', 0)

    validation_warnings = []
    try:
        if hasattr(excel_file, 'seek'):
            excel_file.seek(0)

        is_valid, errors, warnings = validate_input(excel_file)

        if not is_valid:
            return VRPResult(
                is_valid=False,
                errors=errors,
                warnings=warnings
            )

        validation_warnings = warnings
        if warnings:
            progress.report(
                'validate',
                f'⚠️ พบ {len(warnings)} คำเตือน (ยังรันต่อได้)',
                100,
                {'warnings': warnings}
            )
        else:
            progress.report('validate', '✅ ข้อมูลถูกต้อง', 100)
    except Exception:
        progress.report(
            'validate',
            '⚠️ ข้ามการตรวจสอบ — ดำเนินการต่อ',
            100
        )

    # ══════════════════════════════════════════════
    # PHASE 1: LOAD
    # ══════════════════════════════════════════════
    progress.report('load', f'📁 กำลังโหลดไฟล์ {filename}...', 0)

    try:
        if hasattr(excel_file, 'seek'):
            excel_file.seek(0)
        vrp_data = load_all_data(excel_file, progress)
    except Exception as e:
        return VRPResult(
            is_valid=False,
            errors=[f"ไม่สามารถโหลดข้อมูลได้: {str(e)}"]
        )

    depots = vrp_data.depots
    vehicles = vrp_data.vehicles
    orders = vrp_data.orders
    dist_matrix = vrp_data.dist_matrix
    locations = vrp_data.locations
    priority_config = vrp_data.priority_config

    if not orders:
        return VRPResult(
            is_valid=False,
            errors=["ไม่พบข้อมูลออเดอร์"]
        )
    if not vehicles:
        return VRPResult(
            is_valid=False,
            errors=["ไม่พบข้อมูลรถ"]
        )

    # ── Loading Summary ──
    load_elapsed = time.time() - pipeline_start
    raw_lines = (
        len(vrp_data.raw_order_lines)
        if vrp_data.raw_order_lines is not None
        and not vrp_data.raw_order_lines.empty
        else 0
    )

    progress.report(
        'load',
        f'✅ โหลดสำเร็จ: {len(orders)} ออเดอร์, '
        f'{len(vehicles)} รถ, {len(depots)} คลัง',
        100,
        {
            'filename': filename,
            'depots': len(depots),
            'vehicles': len(vehicles),
            'orders': len(orders),
            'dist_matrix_entries': sum(
                len(v) for v in dist_matrix.values()
            ),
            'priority_levels': len(priority_config),
            'raw_order_lines': raw_lines,
            'load_time': load_elapsed,
        }
    )

    # ✅ C2 FIX: ใช้ logger แทน print สำหรับ debug
    logger.debug("=" * 70)
    logger.debug("DATA LOADING VERIFICATION")
    logger.debug("=" * 70)

    logger.debug("VEHICLE DETAILS:")
    for v in vehicles:
        logger.debug(
            f"  {v.vehicle_id} | type={getattr(v, 'vehicle_type', 'N/A')} | "
            f"maxKG={getattr(v, 'max_weight_kg', 'N/A')} | "
            f"capDrop={getattr(v, 'capacity_drop', 'N/A')} | "
            f"maxDrop={getattr(v, 'max_drops', 'N/A')} | "
            f"extraDrop={getattr(v, 'extra_drop_charge', 'N/A')} | "
            f"fixCost={getattr(v, 'fixed_cost', 'N/A')} | "
            f"varCost={getattr(v, 'variable_cost', 'N/A')}"
        )

    logger.debug(f"DISTANCE MATRIX: {len(dist_matrix)} outer keys")
    all_values = []
    for inner in dist_matrix.values():
        if isinstance(inner, dict):
            all_values.extend(inner.values())
    positives = [val for val in all_values if val > 0]
    if positives:
        logger.debug(
            f"  Min: {min(positives):.1f} km, "
            f"Max: {max(positives):.1f} km, "
            f"Avg: {sum(positives)/len(positives):.1f} km"
        )

    logger.debug(
        f"ORDERS: {len(orders)} total, "
        f"{sum(o.weight_kg for o in orders):,.0f} kg"
    )
    logger.debug(f"  Plants: {set(o.plant for o in orders)}")
    pri_dist = {}
    for o in orders:
        p = getattr(o, 'priority', 'N/A')
        pri_dist[p] = pri_dist.get(p, 0) + 1
    logger.debug(f"  Priorities: {pri_dist}")

    logger.debug(f"DEPOTS:")
    for name, depot in depots.items():
        logger.debug(f"  {name}: lat={depot.lat}, lng={depot.lng}")
    logger.debug("=" * 70)

    # ══════════════════════════════════════════════
    # PHASE 2: CONSTRUCTIVE
    # ══════════════════════════════════════════════
    progress.report(
        'construct', '🏗️ สร้าง Solution เริ่มต้น...', 0
    )

    initial_routes, unassigned = build_initial_solution_multi_depot(
        orders, vehicles, depots, dist_matrix,
        locations, priority_config, progress
    )

    # ⭐ C1 FIX: ใช้ vrp_data.skipped_orders แทน global
    if vrp_data.skipped_orders:
        skipped_count = len(vrp_data.skipped_orders)
        for o in vrp_data.skipped_orders:
            if o not in unassigned:
                unassigned.append(o)
        progress.report(
            'construct',
            f'⚠️ เพิ่ม {skipped_count} ออเดอร์ '
            f'(Plant ไม่ตรง) เข้า Unassigned',
            80
        )

    progress.report('construct', '✅ Solution เริ่มต้นเสร็จ', 100)

    # ══════════════════════════════════════════════
    # PHASE 3: ALNS
    # ══════════════════════════════════════════════
    progress.report(
        'alns', '🔄 กำลังรัน ALNS Optimization...', 0
    )

    solver = ALNSSolver(
        initial_routes=initial_routes,
        unassigned_orders=unassigned,
        all_orders=orders,
        all_vehicles=vehicles,
        dist_matrix=dist_matrix,
        depots=depots,
        locations=locations,
        priority_config=priority_config,
        progress=progress,
    )
    best_solution = solver.solve()

    progress.report('alns', '✅ ALNS เสร็จสิ้น', 100)

    # ══════════════════════════════════════════════
    # PHASE 4: POST PROCESS
    # ══════════════════════════════════════════════
    progress.report('post', '🔧 Post-processing...', 0)
    best_solution = post_process_solution(
        best_solution, depots, dist_matrix, locations, progress
    )

    elapsed = time.time() - pipeline_start

    # ✅ C2 FIX: ใช้ logger แทน print สำหรับ distance verification
    logger.debug("=" * 70)
    logger.debug("DISTANCE VERIFICATION (post-process)")
    logger.debug("=" * 70)

    total_pickup = 0
    total_delivery = 0
    total_return = 0
    total_combined = 0

    for i, route in enumerate(best_solution.routes):
        if route.is_empty():
            continue
        pd_ = getattr(route, 'pickup_distance', 0)
        dd = getattr(route, 'delivery_distance', 0)
        rd = getattr(route, 'return_distance', 0)
        td = getattr(route, 'total_distance', 0)
        last_cum = (
            route.stops[-1].cumulative_distance
            if route.stops else 0
        )

        total_pickup += pd_
        total_delivery += dd
        total_return += rd
        total_combined += td

        mismatch = ""
        if abs(td - (pd_ + dd)) > 0.1:
            mismatch = " ⚠️ total ≠ pickup+delivery"
        if abs(dd - last_cum) > 0.1:
            mismatch += " ⚠️ delivery ≠ cumulative"

        logger.debug(
            f"R{i+1:2d} {route.vehicle.vehicle_id:4s}: "
            f"pickup={pd_:7.1f}  delivery={dd:7.1f}  "
            f"return={rd:7.1f}  total={td:7.1f}  "
            f"cumulative={last_cum:7.1f}{mismatch}"
        )

    logger.debug(
        f"SUM: pickup={total_pickup:7.1f}  "
        f"delivery={total_delivery:7.1f}  "
        f"return={total_return:7.1f}  "
        f"total={total_combined:7.1f}"
    )
    logger.debug(f"P+D:   {total_pickup + total_delivery:,.1f} km")
    logger.debug(
        f"P+D+R: "
        f"{total_pickup + total_delivery + total_return:,.1f} km"
    )

    summary = best_solution.get_summary()
    logger.debug(
        f"Summary reports: {summary['total_distance']:,.1f} km"
    )
    logger.debug(
        f"Summary pickup:  "
        f"{summary.get('pickup_distance', 'N/A')}"
    )
    logger.debug(
        f"Summary delivery: "
        f"{summary.get('delivery_distance', 'N/A')}"
    )

    logger.debug("=" * 70)
    logger.debug("SOLUTION VERIFICATION")
    logger.debug("=" * 70)
    logger.debug(
        f"Assigned:    "
        f"{summary.get('assigned_orders','?')}/"
        f"{summary.get('total_orders','?')}"
    )
    logger.debug(f"Vehicles:    {summary.get('num_vehicles','?')}")
    logger.debug(
        f"Distance:    {summary.get('total_distance',0):,.1f} km"
    )
    logger.debug(
        f"Total Cost:  {summary.get('total_cost',0):,.0f} THB"
    )
    logger.debug(f"Late:        {summary.get('num_late',0)}")
    logger.debug(
        f"Priority Miss: {summary.get('num_priority_miss',0)}"
    )
    logger.debug(
        f"OT Hours:    {summary.get('overtime_hours',0):.1f}"
    )
    logger.debug(
        f"Extra Drops: {summary.get('extra_drops',0)}"
    )
    logger.debug(
        f"Unassigned:  {summary.get('unassigned_orders',0)}"
    )
    logger.debug("=" * 70)

    # ══════════════════════════════════════════════
    # PHASE 5: BUILD RESULT
    # ══════════════════════════════════════════════
    progress.report('done', '✅ เสร็จสมบูรณ์!', 100)

    operator_stats = solver.get_operator_stats()

    result = VRPResult(
        solution=best_solution,
        data=vrp_data,
        operator_stats=operator_stats,
        elapsed_time=elapsed,
        is_valid=True,
        errors=[],
        warnings=validation_warnings,
    )

    return result

