from flask import Flask, jsonify, render_template, request, redirect, url_for, flash, Response, session
import requests, sqlite3, threading, time, json, csv, io, sys, asyncio, re, struct, secrets, logging, bisect
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from functools import wraps
from logging.handlers import RotatingFileHandler
from zoneinfo import ZoneInfo
from pathlib import Path
import aiohttp
from esios_api import PVPCData
from pymodbus.client import ModbusTcpClient
from werkzeug.security import generate_password_hash, check_password_hash

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "shelly.db"
CONFIG_PATH = BASE_DIR / "config.json"
LOG_PATH = BASE_DIR / "app.log"
PORT = 5000
POLL_SECONDS = 5
GAP_SECONDS = 300
DEFAULT_ZONE = "principal"
PVPC_TZ = ZoneInfo("Europe/Madrid")
ZONE_KEY_RE = re.compile(r"^[a-z0-9_]{1,30}$")
MODELS = {
    "pro_em": "Shelly Pro EM (EM1)",
    "pm_mini_gen3": "Shelly PM Mini Gen3 (PM1)",
    "pm710_modbus": "Schneider PM710 (Modbus TCP)",
}

DEFAULT_ZONES = {
    "principal": {"name": "Principal · Shelly Pro EM-50", "model": "pro_em", "ip": "192.168.4.175", "fixed_costs": True},
    "acuario": {"name": "Acuario · Shelly PM Mini Gen 3", "model": "pm_mini_gen3", "ip": "192.168.4.203"},
    "schneider": {"name": "Schneider PowerLogic PM710", "model": "pm710_modbus", "ip": "192.168.100.206", "port": 502, "slave_id": 1},
}
ZONE_DEFAULTS = {
    "name": "", "model": "pro_em", "ip": "", "port": 502, "slave_id": 1,
    "voltage": "ll",          # PM710: "ll" = media fase-fase (1014), "ln" = media fase-neutro (1016)
    "fixed_costs": False,     # la factura de esta zona incluye término de potencia, alquiler y fijos
    "parent": "",             # zona en la que está incluido este medidor (para el reparto)
    "enabled": True,
    "power_alert_w": 0.0,     # 0 = sin aviso de consumo alto
    "power_alert_minutes": 2,
}
DEFAULTS = {
    "esios_token": "",
    "power_kw_p1": 4.6,
    "power_kw_p2": 4.6,
    "power_price_p1_eur_kw_day": 0.0,
    "power_price_p2_eur_kw_day": 0.0,
    "meter_rent_eur_day": 0.0266,
    "electricity_tax_pct": 5.11269632,
    "vat_pct": 21.0,
    "other_fixed_monthly_eur": 0.0,
    "extra_discount_pct": 0.0,
    "raw_retention_days": 0,  # 0 = conservar todas las lecturas de 5 s
    "admin_password_hash": "",
    "secret_key": "",
    "telegram_token": "",
    "telegram_chat_id": "",
    "offline_alert_minutes": 10,
    "cheap_hours_enabled": False,
    "cheap_hours_count": 3,
    "cheap_hours_time": "20:45",
}

app = Flask(__name__)
log = logging.getLogger("consumo")
config_lock = threading.Lock()
ZONES = {}
ZONE_ERRORS = {}
ZONE_STATE = {}   # zone -> texto mientras se prepara el resumen horario
LAST_OK = {}      # zone -> epoch de la última lectura correcta
POLLERS = {}      # zone -> Thread
STARTED_AT = time.time()

# ---------------------------------------------------------------- configuración

def normalize_zones(zones):
    out = {}
    for key, z in (zones or {}).items():
        out[key] = {**ZONE_DEFAULTS, **z}
    if DEFAULT_ZONE not in out:
        out = {DEFAULT_ZONE: {**ZONE_DEFAULTS, **DEFAULT_ZONES[DEFAULT_ZONE]}, **out}
    return out

def load_config():
    cfg = dict(DEFAULTS)
    if CONFIG_PATH.exists():
        try:
            cfg.update(json.loads(CONFIG_PATH.read_text(encoding="utf-8")))
        except Exception as e:
            log.warning("No se pudo leer config.json: %s", e)
    cfg["zones"] = normalize_zones(cfg.get("zones") or DEFAULT_ZONES)
    return cfg

def save_config(cfg):
    with config_lock:
        tmp = CONFIG_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(CONFIG_PATH)

def reload_zones():
    zones = load_config()["zones"]
    ZONES.clear()
    ZONES.update(zones)

def zone_connection(dev):
    if dev["model"] == "pm710_modbus":
        return f"{dev['ip']}:{dev['port']} · Modbus TCP · Slave ID {dev['slave_id']}"
    return dev["ip"]

# ---------------------------------------------------------------- base de datos

ZONE_SCHEMA = """
CREATE TABLE IF NOT EXISTS measurements (
  id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL, epoch REAL NOT NULL,
  voltage REAL, current REAL, act_power REAL, aprt_power REAL, pf REAL, freq REAL,
  total_kwh REAL NOT NULL, returned_kwh REAL NOT NULL, delta_kwh REAL NOT NULL DEFAULT 0,
  energy_cost_eur REAL NOT NULL DEFAULT 0, pvpc_eur_mwh REAL);
CREATE INDEX IF NOT EXISTS idx_measurements_epoch ON measurements(epoch);
CREATE TABLE IF NOT EXISTS hourly (
  epoch INTEGER PRIMARY KEY, label TEXT NOT NULL,
  kwh REAL NOT NULL DEFAULT 0, cost_eur REAL NOT NULL DEFAULT 0, priced_kwh REAL NOT NULL DEFAULT 0,
  power_sum REAL NOT NULL DEFAULT 0, power_n INTEGER NOT NULL DEFAULT 0, power_max REAL, power_max_at TEXT);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
"""
SHARED_SCHEMA = """
CREATE TABLE IF NOT EXISTS pvpc_prices (epoch INTEGER PRIMARY KEY, price_eur_mwh REAL NOT NULL, fetched_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT, epoch REAL NOT NULL, zone TEXT, kind TEXT NOT NULL,
  message TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 0, resolved_epoch REAL);
"""

def db_path_for_zone(zone=None):
    if zone in (None, "", DEFAULT_ZONE):
        return DB_PATH
    if not ZONE_KEY_RE.match(zone):
        raise ValueError("Zona desconocida")
    return BASE_DIR / f"{zone}.db"

@contextmanager
def db(zone=None, rows=False):
    c = sqlite3.connect(db_path_for_zone(zone), timeout=30)
    if rows:
        c.row_factory = sqlite3.Row
    try:
        with c:
            yield c
    finally:
        c.close()

def init_db(zone):
    with db(zone) as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.executescript(ZONE_SCHEMA)
        cols = {r[1] for r in c.execute("PRAGMA table_info(measurements)")}
        for name, typ in [("energy_cost_eur", "REAL NOT NULL DEFAULT 0"), ("pvpc_eur_mwh", "REAL")]:
            if name not in cols:
                c.execute(f"ALTER TABLE measurements ADD COLUMN {name} {typ}")
        if zone == DEFAULT_ZONE:
            c.executescript(SHARED_SCHEMA)

def get_meta(key, zone=DEFAULT_ZONE):
    with db(zone) as c:
        row = c.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else None

def set_meta(key, value, zone=DEFAULT_ZONE):
    with db(zone) as c:
        c.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", (key, str(value)))

def local_stamp(epoch):
    return datetime.fromtimestamp(epoch).strftime("%Y-%m-%d %H:%M:%S")

def hour_label(hour_epoch):
    return datetime.fromtimestamp(hour_epoch).strftime("%Y-%m-%d %H")

def day_start(dt):
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)

# ---------------------------------------------------------------- precios PVPC

class PriceIndex:
    """In-memory view of pvpc_prices for fast lookups. A price is valid for at most one hour."""
    def __init__(self, rows):
        rows = sorted(rows)
        self.epochs = [r[0] for r in rows]
        self.prices = [r[1] for r in rows]

    @classmethod
    def load(cls, start=None, end=None):
        with db() as c:
            if start is None:
                rows = c.execute("SELECT epoch,price_eur_mwh FROM pvpc_prices").fetchall()
            else:
                rows = c.execute("SELECT epoch,price_eur_mwh FROM pvpc_prices WHERE epoch>=? AND epoch<?",
                                 (int(start) - 3600, int(end) + 3600)).fetchall()
        return cls(rows)

    def at(self, epoch):
        i = bisect.bisect_right(self.epochs, epoch) - 1
        if i >= 0 and epoch - self.epochs[i] < 3600:
            return self.prices[i]
        return None

    def hour_average(self, hour_epoch):
        lo = bisect.bisect_left(self.epochs, hour_epoch)
        hi = bisect.bisect_left(self.epochs, hour_epoch + 3600)
        values = self.prices[lo:hi]
        if values:
            return sum(values) / len(values)
        return self.at(hour_epoch)

def price_for_epoch(epoch):
    with db() as c:
        row = c.execute("SELECT epoch,price_eur_mwh FROM pvpc_prices WHERE epoch<=? ORDER BY epoch DESC LIMIT 1",
                        (int(epoch),)).fetchone()
    return float(row[1]) if row and epoch - row[0] < 3600 else None

def fetch_pvpc(now_utc=None):
    """
    Descarga el PVPC 2.0TD con `esios_api` (la biblioteca de Home Assistant) y guarda
    todos los precios que devuelve, con su marca de tiempo UTC: el día de `now_utc` y,
    a partir de las 20:00, también el día siguiente. Los precios llegan en €/kWh y se
    guardan en €/MWh. Pasando una fecha pasada descarga ese día (relleno de huecos).
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    token = str(load_config().get("esios_token", "")).strip()

    async def _get_prices():
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=35)) as s:
            api = PVPCData(session=s, tariff="2.0TD", local_timezone=PVPC_TZ,
                           api_token=token or None, sensor_keys=("PVPC",))
            data = await api.async_update_all(current_data=None, now=now_utc)
            return dict(data.sensors.get("PVPC") or {})

    try:
        prices = asyncio.run(_get_prices())
    except Exception as exc:
        raise RuntimeError(f"esios_api (biblioteca de Home Assistant): {exc}") from exc
    rows = [(int(ts.timestamp()), float(p) * 1000.0, time.time()) for ts, p in prices.items() if p is not None]
    if not rows:
        raise RuntimeError("ESIOS no ha devuelto precios para "
                           f"{now_utc.astimezone(PVPC_TZ):%d/%m/%Y}; revisa la conexión y, si hace falta, el token.")
    with db() as c:
        c.executemany("INSERT OR REPLACE INTO pvpc_prices(epoch,price_eur_mwh,fetched_at) VALUES(?,?,?)", rows)
    return len(rows)

FAILED_PRICE_DAYS = {}

def backfill_prices(max_days=10):
    """Descarga precios de días pasados con consumo registrado pero sin precio."""
    days = set()
    for zone in list(ZONES):
        try:
            with db(zone) as c:
                days.update(r[0] for r in c.execute(
                    "SELECT DISTINCT substr(label,1,10) FROM hourly WHERE priced_kwh < kwh - 1e-9"))
        except sqlite3.Error:
            continue
    today = datetime.now().date().isoformat()
    pending = [d for d in sorted(days, reverse=True)
               if d <= today and FAILED_PRICE_DAYS.get(d) != today][:max_days]
    for d in pending:
        noon = datetime.strptime(d, "%Y-%m-%d").replace(hour=12, tzinfo=PVPC_TZ)
        try:
            fetch_pvpc(noon.astimezone(timezone.utc))
            FAILED_PRICE_DAYS.pop(d, None)
        except Exception as e:
            FAILED_PRICE_DAYS[d] = today  # no reintentar hasta mañana
            log.warning("Sin precios PVPC para %s: %s", d, e)
        time.sleep(1)

def reprice_zone(zone, prices=None):
    """Asigna coste a la energía que se registró sin precio, cuando el precio ya está disponible."""
    with db(zone) as c:
        pending = c.execute("SELECT epoch,kwh,priced_kwh FROM hourly WHERE priced_kwh < kwh - 1e-9").fetchall()
        if not pending:
            return 0
        prices = prices or PriceIndex.load()
        fixed = 0
        for hour, kwh, priced in pending:
            price = prices.hour_average(hour)
            if price is None:
                continue
            c.execute("UPDATE hourly SET cost_eur=cost_eur+?, priced_kwh=kwh WHERE epoch=?",
                      ((kwh - priced) * price / 1000.0, hour))
            c.execute("""UPDATE measurements SET pvpc_eur_mwh=?, energy_cost_eur=delta_kwh*?/1000.0
                         WHERE epoch>=? AND epoch<? AND pvpc_eur_mwh IS NULL""", (price, price, hour, hour + 3600))
            fixed += 1
    return fixed

def refresh_prices():
    try:
        n = fetch_pvpc()
        log.info("PVPC actualizado: %d precios", n)
    except Exception as e:
        log.warning("Aviso PVPC: %s", e)
    backfill_prices()
    prices = PriceIndex.load()
    for zone in list(ZONES):
        try:
            reprice_zone(zone, prices)
        except sqlite3.Error as e:
            log.warning("No se pudo recalcular el coste de %s: %s", zone, e)

def hourly_ready(zone):
    try:
        return bool(get_meta("hourly_built", zone))
    except sqlite3.Error:
        return False

def price_refresh_loop():
    # The first run waits (up to 10 min) for the hourly summaries, so it can fill in missing past prices.
    deadline = time.time() + 600
    while time.time() < deadline and not all(hourly_ready(z) for z, d in list(ZONES.items()) if d["enabled"]):
        time.sleep(2)
    while True:
        refresh_prices()
        # Los precios de mañana se publican hacia las 20:15: en esa franja se mira más a menudo.
        hour = datetime.now(PVPC_TZ).hour
        time.sleep(900 if 20 <= hour <= 22 else 3600)

# ---------------------------------------------------------------- lectura de medidores

def rpc(method, params, ip):
    r = requests.get(f"http://{ip}/rpc/{method}", params=params or {}, timeout=4)
    r.raise_for_status()
    return r.json()

def _float32(registers, offset):
    """Decode PM710 IEEE-754 float from two consecutive Modbus registers."""
    return float(struct.unpack(">f", struct.pack(">HH", registers[offset], registers[offset + 1]))[0])

def read_zone(dev):
    """Lee un Shelly o un Schneider PM710 y normaliza sus medidas."""
    ip = dev["ip"]
    if dev["model"] == "pm710_modbus":
        # PM710 direct floating-point map. PDU addresses are one less than
        # the register numbers in the Schneider documentation. Read-only FC03.
        client = ModbusTcpClient(host=ip, port=int(dev.get("port", 502)), timeout=3)
        try:
            if not client.connect():
                raise ConnectionError(f"No se pudo conectar por Modbus TCP a {ip}:{dev.get('port', 502)}")
            response = client.read_holding_registers(address=999, count=22, slave=int(dev.get("slave_id", 1)))
            if response.isError():
                raise RuntimeError(f"Modbus FC03 devolvió error al leer registros PM710 1000–1021: {response}")
            regs = response.registers
            if len(regs) < 22:
                raise RuntimeError(f"Respuesta Modbus incompleta: {len(regs)} registros; se esperaban 22")
            energy_kwh = _float32(regs, 0)      # 1000/1001: total real energy, kWh
            power_kw = _float32(regs, 6)        # 1006/1007: total real power, kW
            pf = _float32(regs, 12)             # 1012/1013: total power factor
            # 1014/1015: average line-line voltage; 1016/1017: average line-neutral voltage
            voltage = _float32(regs, 16 if dev.get("voltage") == "ln" else 14)
            current = _float32(regs, 18)        # 1018/1019: average current
            freq = _float32(regs, 20)           # 1020/1021: frequency, Hz
            values = (energy_kwh, power_kw, voltage, current, freq)
            if not all(map(lambda v: abs(v) < 1e9, values)) or energy_kwh < 0 or voltage < 0 or current < 0 or freq < 0:
                raise RuntimeError(f"Valores PM710 decodificados no válidos: energía={energy_kwh}, potencia={power_kw}, tensión={voltage}, corriente={current}, frecuencia={freq}. Revisar orden de palabras Modbus.")
            status = {"voltage": voltage, "current": current, "act_power": power_kw * 1000.0,
                      "aprt_power": None, "pf": pf, "freq": freq}
            return status, energy_kwh, 0.0
        finally:
            client.close()
    if dev["model"] == "pro_em":
        status = rpc("EM1.GetStatus", {"id": 0}, ip)
        energy = rpc("EM1Data.GetStatus", {"id": 0}, ip)
        total = float(energy["total_act_energy"]) / 1000.0  # Wh -> kWh
        returned = float(energy.get("total_act_ret_energy", 0)) / 1000.0
        return status, total, returned

    # Shelly PM Mini Gen3 is a power meter without a relay: its component is PM1.
    status = rpc("PM1.GetStatus", {"id": 0}, ip)
    total_wh = (status.get("aenergy") or {}).get("total")
    if total_wh is None:
        # Compatibility fallback for firmware variants with a different energy field.
        total_wh = status.get("total_act_energy", 0)
    returned_wh = (status.get("ret_aenergy") or {}).get("total", 0)
    normalized = {
        "voltage": status.get("voltage"),
        "current": status.get("current"),
        "act_power": status.get("apower", status.get("act_power")),
        # PM Mini Gen3 PM1.GetStatus does not report apparent power or power factor.
        "aprt_power": status.get("aprt_power"),
        "pf": status.get("pf"),
        "freq": status.get("freq"),
    }
    return normalized, float(total_wh or 0) / 1000.0, float(returned_wh or 0) / 1000.0

# ---------------------------------------------------------------- resumen horario

def split_hours(t0, t1):
    """Split [t0, t1) at hour boundaries: [(hour_epoch, start, end), ...]."""
    if t1 <= t0:
        return [(int(t1 // 3600 * 3600), t1, t1)]
    out, t = [], t0
    while t < t1:
        hour = int(t // 3600 * 3600)
        end = min(t1, hour + 3600)
        out.append((hour, t, end))
        t = end
    return out

def spread_energy(t0, t1, kwh, price_at):
    """
    Reparte el consumo medido entre dos lecturas de forma proporcional al tiempo.
    Si la aplicación estuvo parada, el consumo del hueco se distribuye entre sus
    horas en lugar de cargarse entero en la hora de la primera lectura posterior.
    Devuelve [(hour_epoch, kwh, coste_eur, kwh_con_precio)].
    """
    parts = []
    span = t1 - t0
    for hour, a, b in split_hours(t0, t1):
        part = kwh * (b - a) / span if span > 0 else kwh
        price = price_at((a + b) / 2)
        parts.append((hour, part, part * price / 1000.0 if price is not None else 0.0, part if price is not None else 0.0))
    return parts

UPSERT_ENERGY = """INSERT INTO hourly(epoch,label,kwh,cost_eur,priced_kwh) VALUES(?,?,?,?,?)
    ON CONFLICT(epoch) DO UPDATE SET kwh=kwh+excluded.kwh, cost_eur=cost_eur+excluded.cost_eur,
    priced_kwh=priced_kwh+excluded.priced_kwh"""
UPSERT_POWER = """INSERT INTO hourly(epoch,label,power_sum,power_n,power_max,power_max_at) VALUES(?,?,?,1,?,?)
    ON CONFLICT(epoch) DO UPDATE SET power_sum=power_sum+excluded.power_sum, power_n=power_n+1,
    power_max_at=CASE WHEN power_max IS NULL OR excluded.power_max>power_max THEN excluded.power_max_at ELSE power_max_at END,
    power_max=CASE WHEN power_max IS NULL OR excluded.power_max>power_max THEN excluded.power_max ELSE power_max END"""

def record_reading(zone, status, total, returned, now=None, new_device=False):
    """Guarda una lectura. Con new_device=True no se cuenta consumo desde la lectura anterior (otro contador)."""
    now = now or time.time()
    stamp = local_stamp(now)
    price_now = price_for_epoch(now)
    with db(zone) as c:
        prev = c.execute("SELECT epoch,total_kwh FROM measurements ORDER BY id DESC LIMIT 1").fetchone()
        # A lower counter means the meter was reset: nothing is counted for that interval.
        delta = total - float(prev[1]) if prev and total >= float(prev[1]) and not new_device else 0.0
        parts = spread_energy(prev[0], now, delta, price_for_epoch) if prev and delta > 0 else []
        c.executemany(UPSERT_ENERGY, [(h, hour_label(h), k, e, p) for h, k, e, p in parts])
        power = status.get("act_power")
        if power is not None:
            hour = int(now // 3600 * 3600)
            c.execute(UPSERT_POWER, (hour, hour_label(hour), float(power), float(power), stamp))
        c.execute("""INSERT INTO measurements(timestamp,epoch,voltage,current,act_power,aprt_power,pf,freq,total_kwh,returned_kwh,delta_kwh,energy_cost_eur,pvpc_eur_mwh)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (stamp, now, status.get("voltage"), status.get("current"), power, status.get("aprt_power"),
             status.get("pf"), status.get("freq"), total, returned, delta, sum(p[2] for p in parts), price_now))

def build_hourly(zone):
    """Genera el resumen horario a partir de las lecturas ya guardadas (solo la primera vez)."""
    if get_meta("hourly_built", zone):
        return
    ZONE_STATE[zone] = "Preparando el resumen horario del histórico…"
    started = time.time()
    prices = PriceIndex.load()
    hours = {}
    prev_epoch, n = None, 0
    with db(zone) as c:
        for epoch, delta, power, stamp in c.execute(
                "SELECT epoch,delta_kwh,act_power,timestamp FROM measurements ORDER BY epoch"):
            n += 1
            if prev_epoch is not None and delta and delta > 0:
                for h, k, e, p in spread_energy(prev_epoch, epoch, delta, prices.at):
                    acc = hours.setdefault(h, [0.0, 0.0, 0.0, 0.0, 0, None, None])
                    acc[0] += k; acc[1] += e; acc[2] += p
            if power is not None:
                h = int(epoch // 3600 * 3600)
                acc = hours.setdefault(h, [0.0, 0.0, 0.0, 0.0, 0, None, None])
                acc[3] += power; acc[4] += 1
                if acc[5] is None or power > acc[5]:
                    acc[5], acc[6] = power, stamp
            prev_epoch = epoch
        c.execute("DELETE FROM hourly")
        c.executemany("""INSERT INTO hourly(epoch,label,kwh,cost_eur,priced_kwh,power_sum,power_n,power_max,power_max_at)
                         VALUES(?,?,?,?,?,?,?,?,?)""", [(h, hour_label(h), *v) for h, v in hours.items()])
        c.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('hourly_built',?)", (local_stamp(time.time()),))
    log.info("Resumen horario de %s: %d lecturas en %d horas (%.1f s)", zone, n, len(hours), time.time() - started)
    ZONE_STATE.pop(zone, None)

def device_signature(dev):
    return f"{dev['model']}|{dev['ip']}|{dev['port']}|{dev['slave_id']}" if dev["model"] == "pm710_modbus" else f"{dev['model']}|{dev['ip']}"

def poller(zone):
    """Lee una zona cada POLL_SECONDS. Termina sola si la zona se borra o se desactiva."""
    try:
        init_db(zone)
        build_hourly(zone)
    except Exception as e:
        ZONE_STATE.pop(zone, None)
        ZONE_ERRORS[zone] = f"No se pudo preparar la base de datos: {e}"
        log.exception("Error preparando %s", zone)
        return
    known_device = get_meta("device", zone)
    while True:
        dev = ZONES.get(zone)
        if dev is None or not dev["enabled"]:
            ZONE_ERRORS.pop(zone, None)
            return
        started = time.time()
        device = device_signature(dev)
        try:
            status, total, returned = read_zone(dev)
            # If the zone now points to another meter, its counter is unrelated to the previous one.
            record_reading(zone, status, total, returned, new_device=known_device not in (None, device))
            if known_device != device:
                set_meta("device", device, zone)
                known_device = device
            ZONE_ERRORS[zone] = ""
            LAST_OK[zone] = time.time()
        except Exception as e:
            ZONE_ERRORS[zone] = f"{type(e).__name__}: {e}"
            log.warning("Error zona %s (%s): %s", zone, dev["ip"], e)
        time.sleep(max(0.5, POLL_SECONDS - (time.time() - started)))

def start_pollers():
    for zone, dev in list(ZONES.items()):
        thread = POLLERS.get(zone)
        if dev["enabled"] and not (thread and thread.is_alive()):
            ZONE_ERRORS.setdefault(zone, "Esperando primera lectura")
            POLLERS[zone] = threading.Thread(target=poller, args=(zone,), daemon=True, name=f"poller-{zone}")
            POLLERS[zone].start()

def maintenance_loop():
    """Borra las lecturas de 5 s más antiguas que el periodo configurado (el resumen horario se conserva)."""
    while True:
        time.sleep(3600)
        days = int(load_config().get("raw_retention_days") or 0)
        if days <= 0:
            continue
        limit = time.time() - days * 86400
        for zone in list(ZONES):
            try:
                if not get_meta("hourly_built", zone):
                    continue
                with db(zone) as c:
                    n = c.execute("""DELETE FROM measurements WHERE epoch<?
                                     AND id < (SELECT MAX(id) FROM measurements)""", (limit,)).rowcount
                if n:
                    log.info("Borradas %d lecturas de %s anteriores a %s", n, zone, local_stamp(limit))
            except sqlite3.Error as e:
                log.warning("Limpieza de %s: %s", zone, e)

# ---------------------------------------------------------------- avisos

def notify(text):
    cfg = load_config()
    token, chat = cfg.get("telegram_token", "").strip(), str(cfg.get("telegram_chat_id", "")).strip()
    if not token or not chat:
        return False
    r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      json={"chat_id": chat, "text": text}, timeout=10)
    r.raise_for_status()
    return True

def safe_notify(text):
    try:
        notify(text)
    except Exception as e:
        log.warning("No se pudo enviar el aviso por Telegram: %s", e)

def raise_alert(zone, kind, message, active=True):
    with db() as c:
        if active and c.execute("SELECT 1 FROM alerts WHERE zone IS ? AND kind=? AND active=1", (zone, kind)).fetchone():
            return False
        c.execute("INSERT INTO alerts(epoch,zone,kind,message,active) VALUES(?,?,?,?,?)",
                  (time.time(), zone, kind, message, 1 if active else 0))
    safe_notify(message)
    return True

def resolve_alert(zone, kind, message):
    with db() as c:
        n = c.execute("UPDATE alerts SET active=0, resolved_epoch=? WHERE zone IS ? AND kind=? AND active=1",
                      (time.time(), zone, kind)).rowcount
    if n:
        safe_notify(message)

def check_zone_alerts(cfg, now):
    offline_min = float(cfg.get("offline_alert_minutes") or 0)
    for zone, dev in list(ZONES.items()):
        if not dev["enabled"] or zone in ZONE_STATE:
            continue
        name = dev["name"] or zone
        last = LAST_OK.get(zone, STARTED_AT)
        if offline_min > 0 and now - last > offline_min * 60:
            raise_alert(zone, "sin_conexion", f"⚠️ {name}: sin lecturas desde hace {int((now - last) // 60)} min. "
                        f"Último error: {(ZONE_ERRORS.get(zone) or '—')[:160]}")
        elif zone in LAST_OK and now - LAST_OK[zone] < 60:
            resolve_alert(zone, "sin_conexion", f"✅ {name}: vuelve a responder.")
        limit_w = float(dev.get("power_alert_w") or 0)
        if limit_w > 0:
            minutes = max(1.0, float(dev.get("power_alert_minutes") or 1))
            with db(zone) as c:
                low, n = c.execute("SELECT MIN(act_power), COUNT(*) FROM measurements WHERE epoch>=?",
                                   (now - minutes * 60,)).fetchone()
            # Only when the whole window is above the limit, so short peaks don't trigger it.
            if n and n >= max(2, minutes * 60 / POLL_SECONDS * 0.5) and low is not None and low > limit_w:
                raise_alert(zone, "consumo_alto", f"⚡ {name}: consumo por encima de {limit_w:.0f} W "
                            f"durante {minutes:g} min (mínimo {low:.0f} W).")
            elif n and low is not None and low <= limit_w:
                resolve_alert(zone, "consumo_alto", f"✅ {name}: el consumo vuelve a estar por debajo de {limit_w:.0f} W.")

def cheapest_hours(day, count):
    """Las `count` horas más baratas del día local `day` (date), o None si aún no hay precios."""
    start = datetime(day.year, day.month, day.day).astimezone().timestamp()
    end = (datetime(day.year, day.month, day.day) + timedelta(days=1)).astimezone().timestamp()
    with db() as c:
        rows = c.execute("SELECT epoch,price_eur_mwh FROM pvpc_prices WHERE epoch>=? AND epoch<?", (start, end)).fetchall()
    by_hour = {}
    for epoch, price in rows:
        by_hour.setdefault(int(epoch // 3600 * 3600), []).append(price)
    if len(by_hour) < 23:
        return None
    hours = sorted((sum(v) / len(v), h) for h, v in by_hour.items())[:count]
    return [(h, p) for p, h in sorted(hours, key=lambda x: x[1])]

def check_cheap_hours(cfg, now):
    if not cfg.get("cheap_hours_enabled"):
        return
    local = datetime.fromtimestamp(now)
    try:
        hh, mm = map(int, str(cfg.get("cheap_hours_time", "20:45")).split(":"))
    except ValueError:
        hh, mm = 20, 45
    if (local.hour, local.minute) < (hh, mm):
        return
    tomorrow = (local + timedelta(days=1)).date()
    if get_meta("cheap_hours_sent") == tomorrow.isoformat():
        return
    hours = cheapest_hours(tomorrow, max(1, int(cfg.get("cheap_hours_count") or 3)))
    if not hours:
        return
    text = f"💡 Horas más baratas mañana {tomorrow:%d/%m}: " + ", ".join(
        f"{datetime.fromtimestamp(h):%H}h ({p / 1000:.3f} €/kWh)".replace(".", ",") for h, p in hours)
    raise_alert(None, "horas_baratas", text, active=False)
    set_meta("cheap_hours_sent", tomorrow.isoformat())

def alert_loop():
    time.sleep(30)
    while True:
        now = time.time()
        cfg = load_config()
        for check in (check_zone_alerts, check_cheap_hours):
            try:
                check(cfg, now)
            except Exception:
                log.exception("Error comprobando avisos")
        time.sleep(30)

# ---------------------------------------------------------------- factura

def bill_for(zone, energy_eur, cfg, localnow):
    """Estimación de factura del mes en curso. El término fijo solo cuenta en las zonas que lo pagan."""
    fixed = bool(ZONES.get(zone, {}).get("fixed_costs"))
    days = localnow.day
    month_days = (localnow.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    power_cost = meter_cost = other_fixed = 0.0
    if fixed:
        power_cost = (float(cfg["power_kw_p1"]) * float(cfg["power_price_p1_eur_kw_day"]) +
                      float(cfg["power_kw_p2"]) * float(cfg["power_price_p2_eur_kw_day"])) * days
        meter_cost = float(cfg["meter_rent_eur_day"]) * days
        other_fixed = float(cfg["other_fixed_monthly_eur"]) * days / month_days.day
    discount = energy_eur * float(cfg["extra_discount_pct"]) / 100.0
    taxable = max(0.0, energy_eur - discount) + power_cost + other_fixed
    # El impuesto eléctrico no grava el alquiler del contador; el IVA sí.
    elec_tax = taxable * float(cfg["electricity_tax_pct"]) / 100.0
    vat = (taxable + elec_tax + meter_cost) * float(cfg["vat_pct"]) / 100.0
    return {"fixed_costs": fixed, "power_cost": power_cost, "meter_cost": meter_cost, "other_fixed": other_fixed,
            "discount": discount, "taxable_base": taxable, "electricity_tax": elec_tax, "vat": vat,
            "month_total_estimate": taxable + elec_tax + meter_cost + vat}

# ---------------------------------------------------------------- web: seguridad

def admin_required(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        if load_config().get("admin_password_hash") and not session.get("admin"):
            return redirect(url_for("entrar", next=request.path))
        return view(*args, **kwargs)
    return wrapper

@app.context_processor
def inject_globals():
    cfg = load_config()
    return {"has_password": bool(cfg.get("admin_password_hash")), "is_admin": bool(session.get("admin"))}

@app.route("/entrar", methods=["GET", "POST"])
def entrar():
    cfg = load_config()
    if request.method == "POST":
        if cfg.get("admin_password_hash") and check_password_hash(cfg["admin_password_hash"], request.form.get("password", "")):
            session.permanent = True
            session["admin"] = True
            target = request.args.get("next", "")
            return redirect(target if target.startswith("/") and not target.startswith("//") else url_for("ajustes"))
        time.sleep(1)
        flash("Contraseña incorrecta.")
    return render_template("entrar.html")

@app.route("/salir")
def salir():
    session.pop("admin", None)
    return redirect(url_for("home"))

# ---------------------------------------------------------------- web: monitor

@app.route("/")
def home():
    return render_template("index.html", zones=ZONES, default_zone=DEFAULT_ZONE)

def zone_arg():
    zone = request.args.get("zona", DEFAULT_ZONE)
    if zone not in ZONES:
        raise ValueError("Zona desconocida")
    return zone

@app.route("/api/estado")
def api_estado():
    try:
        zone = zone_arg()
    except ValueError as e:
        return jsonify(error=str(e)), 400
    now = time.time()
    localnow = datetime.now().astimezone()
    daystart = day_start(localnow).timestamp()
    monthstart = day_start(localnow).replace(day=1).timestamp()
    with db(zone, rows=True) as c:
        last = c.execute("SELECT * FROM measurements ORDER BY id DESC LIMIT 1").fetchone()
        today = c.execute("SELECT COALESCE(SUM(kwh),0) k,COALESCE(SUM(cost_eur),0) e FROM hourly WHERE epoch>=?", (daystart,)).fetchone()
        month = c.execute("SELECT COALESCE(SUM(kwh),0) k,COALESCE(SUM(cost_eur),0) e FROM hourly WHERE epoch>=?", (monthstart,)).fetchone()
        recent = c.execute("SELECT timestamp,act_power,total_kwh,delta_kwh,energy_cost_eur,pvpc_eur_mwh FROM measurements ORDER BY id DESC LIMIT 20").fetchall()
        power_hist = c.execute("SELECT timestamp,act_power,pvpc_eur_mwh FROM measurements ORDER BY id DESC LIMIT 180").fetchall()
        daily_rows = c.execute("""SELECT substr(label,1,10) day, SUM(kwh) kwh FROM hourly WHERE epoch>=?
            GROUP BY day ORDER BY day""", (day_start(localnow - timedelta(days=13)).timestamp(),)).fetchall()
        hourly_rows = c.execute("SELECT substr(label,12,2) hour, SUM(kwh) kwh FROM hourly WHERE epoch>=? GROUP BY hour ORDER BY hour", (daystart,)).fetchall()
        cost_hist = c.execute("SELECT label, SUM(cost_eur) OVER (ORDER BY epoch) cost FROM hourly WHERE epoch>=? ORDER BY epoch", (monthstart,)).fetchall()
    with db() as c:
        count = c.execute("SELECT COUNT(*) FROM pvpc_prices").fetchone()[0]
    cfg = load_config()
    bill = bill_for(zone, float(month["e"]), cfg, localnow)
    dev = ZONES[zone]
    latest = dict(last) if last else None
    payer = next((z["name"] or k for k, z in ZONES.items() if z.get("fixed_costs")), "")
    return jsonify(ok=bool(last) and zone in LAST_OK and now - LAST_OK[zone] < 60, zone=zone, zone_name=dev["name"],
        ip=zone_connection(dev), error=ZONE_STATE.get(zone) or ZONE_ERRORS.get(zone, ""), latest=latest,
        total_kwh=latest["total_kwh"] if latest else 0, returned_kwh=latest["returned_kwh"] if latest else 0,
        today_kwh=float(today["k"]), today_energy_eur=float(today["e"]), month_kwh=float(month["k"]),
        month_energy_eur=float(month["e"]), price_now=price_for_epoch(now), price_count=int(count),
        fixed_costs_zone=payer, **bill,
        cost_history=[{"time": r["label"][8:10] + " " + r["label"][11:13] + "h", "cost": r["cost"]} for r in cost_hist],
        power_pvpc_history=[{"time": r["timestamp"][-8:-3], "power": r["act_power"], "price": r["pvpc_eur_mwh"]} for r in reversed(power_hist)],
        daily_consumption=[{"day": r["day"], "kwh": float(r["kwh"])} for r in daily_rows],
        hourly_consumption=[{"hour": r["hour"] + ":00", "kwh": float(r["kwh"])} for r in hourly_rows],
        recent=[dict(r) for r in reversed(recent)],
        alerts=active_alerts())

def active_alerts(limit=10):
    with db(rows=True) as c:
        rows = c.execute("""SELECT epoch,zone,kind,message,active FROM alerts
            WHERE active=1 OR epoch>=? ORDER BY active DESC, epoch DESC LIMIT ?""", (time.time() - 86400, limit)).fetchall()
    return [{"time": local_stamp(r["epoch"])[:16], "zone": r["zone"], "kind": r["kind"], "message": r["message"],
             "active": bool(r["active"])} for r in rows]

@app.route("/api/avisos")
def api_avisos():
    with db(rows=True) as c:
        rows = c.execute("SELECT * FROM alerts ORDER BY epoch DESC LIMIT 200").fetchall()
    return jsonify([{**dict(r), "time": local_stamp(r["epoch"]),
                     "resolved": local_stamp(r["resolved_epoch"]) if r["resolved_epoch"] else None} for r in rows])

# ---------------------------------------------------------------- web: histórico

HISTORY_GROUPS = {"hora": 13, "dia": 10, "mes": 7}

def history_range(args):
    """Parse zona/desde/hasta/agrupar into local epochs. Raises ValueError with a user-facing message."""
    zone = args.get("zona", DEFAULT_ZONE)
    if zone not in ZONES:
        raise ValueError("Zona desconocida")
    try:
        start = datetime.strptime(args.get("desde", ""), "%Y-%m-%d").astimezone()
        end = (datetime.strptime(args.get("hasta", ""), "%Y-%m-%d") + timedelta(days=1)).astimezone()
    except ValueError:
        raise ValueError("Fechas no válidas: usa el formato AAAA-MM-DD")
    if end <= start:
        raise ValueError("La fecha final debe ser igual o posterior a la inicial")
    group = args.get("agrupar", "auto")
    if group == "auto":
        days = (end - start).days
        group = "hora" if days <= 3 else ("dia" if days <= 92 else "mes")
    if group not in HISTORY_GROUPS:
        raise ValueError("Agrupación no válida")
    return zone, start.timestamp(), end.timestamp(), group

def avg_price(cost, priced_kwh):
    return cost / priced_kwh if priced_kwh else None

def history_groups(c, start, end, group):
    width = HISTORY_GROUPS[group]
    rows = c.execute(f"""SELECT substr(label,1,{width}) lbl, SUM(kwh) kwh, SUM(cost_eur) cost, SUM(priced_kwh) priced,
        SUM(power_sum)/NULLIF(SUM(power_n),0) avg_power, MAX(power_max) max_power, SUM(power_n) readings
        FROM hourly WHERE epoch>=? AND epoch<? GROUP BY lbl ORDER BY lbl""", (start, end)).fetchall()
    return [{"label": r["lbl"] + ":00" if group == "hora" else r["lbl"], "kwh": float(r["kwh"]),
             "cost_eur": float(r["cost"]), "avg_price_eur_kwh": avg_price(r["cost"], r["priced"]),
             "avg_power": r["avg_power"], "max_power": r["max_power"], "readings": r["readings"]} for r in rows]

def period_totals(c, start, end):
    t = c.execute("""SELECT COALESCE(SUM(kwh),0) kwh, COALESCE(SUM(cost_eur),0) cost, SUM(priced_kwh) priced,
        SUM(power_sum)/NULLIF(SUM(power_n),0) avg_power, COALESCE(SUM(power_n),0) readings
        FROM hourly WHERE epoch>=? AND epoch<?""", (start, end)).fetchone()
    peak = c.execute("""SELECT power_max,power_max_at FROM hourly WHERE epoch>=? AND epoch<? AND power_max IS NOT NULL
        ORDER BY power_max DESC LIMIT 1""", (start, end)).fetchone()
    return {"kwh": float(t["kwh"]), "cost_eur": float(t["cost"]), "avg_price_eur_kwh": avg_price(t["cost"], t["priced"]),
            "avg_power": t["avg_power"], "readings": t["readings"],
            "max_power": peak["power_max"] if peak else None, "max_power_at": peak["power_max_at"] if peak else None}

def zone_breakdown(start, end):
    """Consumo de cada zona en el periodo; para las zonas que contienen otras, añade el resto no medido aparte."""
    out = {}
    for zone, dev in ZONES.items():
        try:
            with db(zone) as c:
                kwh, cost = c.execute("SELECT COALESCE(SUM(kwh),0), COALESCE(SUM(cost_eur),0) FROM hourly WHERE epoch>=? AND epoch<?",
                                      (start, end)).fetchone()
        except sqlite3.Error:
            kwh, cost = 0.0, 0.0
        out[zone] = {"zone": zone, "name": dev["name"] or zone, "kwh": kwh, "cost_eur": cost, "parent": dev.get("parent") or ""}
    rows = []
    for zone, z in out.items():
        if z["parent"]:
            continue
        rows.append(z)
        children = [o for o in out.values() if o["parent"] == zone]
        rows.extend(children)
        if children:
            rows.append({"zone": f"{zone}__resto", "name": f"Resto de {z['name']}", "parent": zone, "rest": True,
                         "kwh": max(0.0, z["kwh"] - sum(ch["kwh"] for ch in children)),
                         "cost_eur": max(0.0, z["cost_eur"] - sum(ch["cost_eur"] for ch in children))})
    return rows

@app.route("/historico")
def historico():
    return render_template("historico.html", zones=ZONES)

@app.route("/api/historico/rango")
def api_historico_rango():
    try:
        zone = zone_arg()
    except ValueError as e:
        return jsonify(error=str(e)), 400
    with db(zone) as c:
        first, last = c.execute("SELECT MIN(label),MAX(label) FROM hourly").fetchone()
        count = c.execute("SELECT COALESCE(SUM(power_n),0) FROM hourly").fetchone()[0]
        raw_first = c.execute("SELECT MIN(timestamp) FROM measurements").fetchone()[0]
    return jsonify(zone=zone, first=first and first + ":00", last=last and last + ":00", count=count, raw_first=raw_first)

@app.route("/api/historico")
def api_historico():
    try:
        zone, start, end, group = history_range(request.args)
    except ValueError as e:
        return jsonify(error=str(e)), 400
    # About 400 points in the power chart whatever the length of the period.
    bucket = max(POLL_SECONDS, int((end - start) / 400))
    prev_start = start - (end - start)
    with db(zone, rows=True) as c:
        groups = history_groups(c, start, end, group)
        totals = period_totals(c, start, end)
        previous = period_totals(c, prev_start, start)
        power = []
        if bucket < 3600:
            power = c.execute("""SELECT MIN(timestamp) time, AVG(act_power) avg, MAX(act_power) max FROM measurements
                WHERE epoch>=? AND epoch<? GROUP BY CAST(epoch/? AS INTEGER) ORDER BY time""", (start, end, bucket)).fetchall()
        if not power:
            # Periodo largo o lecturas de 5 s ya borradas: se usa el resumen horario.
            bucket = max(3600, bucket // 3600 * 3600)
            power = c.execute("""SELECT MIN(label)||':00:00' time, SUM(power_sum)/NULLIF(SUM(power_n),0) avg, MAX(power_max) max
                FROM hourly WHERE epoch>=? AND epoch<? AND power_n>0 GROUP BY CAST(epoch/? AS INTEGER) ORDER BY epoch""",
                (start, end, bucket)).fetchall()
        gaps = c.execute("""SELECT prev_ts, timestamp, epoch-prev_epoch secs, delta_kwh FROM (
              SELECT timestamp, epoch, delta_kwh, LAG(epoch) OVER (ORDER BY epoch) prev_epoch, LAG(timestamp) OVER (ORDER BY epoch) prev_ts
              FROM measurements WHERE epoch>=? AND epoch<?) WHERE epoch-prev_epoch>? ORDER BY epoch""", (start, end, GAP_SECONDS)).fetchall()
        raw_first = c.execute("SELECT MIN(epoch) FROM measurements").fetchone()[0]
    short = bucket < 86400
    return jsonify(zone=zone, group=group, groups=groups, power_bucket_seconds=bucket, totals=totals,
        previous={**previous, "from": local_stamp(prev_start)[:10], "to": local_stamp(start - 1)[:10]},
        zones=zone_breakdown(start, end),
        gaps_complete=raw_first is None or raw_first <= start + GAP_SECONDS,
        power=[{"time": (r["time"][5:16] if short else r["time"][:10]), "avg": r["avg"], "max": r["max"]} for r in power],
        gaps=[{"from": g["prev_ts"], "to": g["timestamp"], "seconds": g["secs"], "kwh_after": g["delta_kwh"]} for g in gaps])

@app.route("/exportar.csv")
def export_csv():
    zone = request.args.get("zona", DEFAULT_ZONE)
    if zone not in ZONES:
        return "Zona desconocida", 400
    # Optional period (desde/hasta) and, with agrupar, totals per hour/day/month instead of raw readings.
    start, end, group, suffix = 0, float("inf"), None, ""
    if request.args.get("desde") or request.args.get("hasta"):
        try:
            zone, start, end, group = history_range(request.args)
        except ValueError as e:
            return str(e), 400
        suffix = f"_{request.args['desde']}_{request.args['hasta']}"
        if "agrupar" not in request.args:
            group = None
    out = io.StringIO(); w = csv.writer(out)
    with db(zone, rows=True) as c:
        if group:
            w.writerow(["periodo", "consumo_kWh", "coste_energia_EUR", "precio_medio_EUR_kWh", "potencia_media_W", "potencia_max_W", "lecturas"])
            w.writerows([[g["label"], g["kwh"], g["cost_eur"], g["avg_price_eur_kwh"], g["avg_power"], g["max_power"], g["readings"]]
                         for g in history_groups(c, start, end, group)])
            suffix += f"_por_{group}"
        else:
            rows = c.execute("SELECT timestamp,voltage,current,act_power,aprt_power,pf,freq,total_kwh,returned_kwh,delta_kwh,pvpc_eur_mwh,energy_cost_eur FROM measurements WHERE epoch>=? AND epoch<? ORDER BY id", (start, end)).fetchall()
            w.writerow(["fecha_hora", "tension_V", "corriente_A", "potencia_W", "potencia_aparente_VA", "factor_potencia", "frecuencia_Hz", "energia_consumida_total_kWh", "energia_devuelta_total_kWh", "delta_consumo_kWh", "PVPC_EUR_MWh", "coste_energia_EUR"])
            w.writerows([tuple(r) for r in rows])
    return Response("﻿" + out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=shelly_pvpc_historico_{zone}{suffix}.csv"})

# ---------------------------------------------------------------- web: ajustes

TARIFF_FIELDS = ["power_kw_p1", "power_kw_p2", "power_price_p1_eur_kw_day", "power_price_p2_eur_kw_day",
                 "meter_rent_eur_day", "other_fixed_monthly_eur", "electricity_tax_pct", "vat_pct", "extra_discount_pct"]

def parse_zones(form, old_zones):
    """Lee la tabla de equipos del formulario. Lanza ValueError con un mensaje para el usuario."""
    zones = {}
    for i in range(int(form.get("zone_count", 0))):
        f = lambda name, default="": form.get(f"z{i}_{name}", default).strip()
        key = f("key").lower()
        if not key:
            continue
        if f("delete"):
            if key == DEFAULT_ZONE:
                raise ValueError("La zona principal no se puede borrar (guarda los precios PVPC).")
            continue
        if not ZONE_KEY_RE.match(key):
            raise ValueError(f"Identificador de zona no válido: «{key}». Usa minúsculas, números y _.")
        if key in zones:
            raise ValueError(f"La zona «{key}» está repetida.")
        model = f("model")
        if model not in MODELS:
            raise ValueError(f"Modelo no válido en la zona «{key}».")
        if not f("ip"):
            raise ValueError(f"Falta la IP de la zona «{key}».")
        try:
            zone = {**ZONE_DEFAULTS, **old_zones.get(key, {}),
                    "name": f("name") or key, "model": model, "ip": f("ip"),
                    "port": int(f("port", "502") or 502), "slave_id": int(f("slave_id", "1") or 1),
                    "voltage": "ln" if f("voltage") == "ln" else "ll",
                    "fixed_costs": bool(f("fixed_costs")), "enabled": bool(f("enabled")), "parent": f("parent"),
                    "power_alert_w": max(0.0, float(f("power_alert_w", "0") or 0)),
                    "power_alert_minutes": max(1.0, float(f("power_alert_minutes", "2") or 2))}
        except ValueError:
            raise ValueError(f"Hay un número no válido en la zona «{key}».")
        zones[key] = zone
    if DEFAULT_ZONE not in zones:
        raise ValueError("Falta la zona principal.")
    for key, z in zones.items():
        if z["parent"] and (z["parent"] not in zones or z["parent"] == key):
            z["parent"] = ""
    # Only one level: a zone that contains others can't be inside another one.
    for z in zones.values():
        if z["parent"] and zones[z["parent"]]["parent"]:
            raise ValueError(f"«{zones[z['parent']]['name']}» está dentro de otra zona y no puede contener a «{z['name']}».")
    return zones

@app.route("/ajustes", methods=["GET", "POST"])
@admin_required
def ajustes():
    cfg = load_config()
    if request.method == "POST":
        form = request.form
        try:
            for key in TARIFF_FIELDS:
                cfg[key] = float(form.get(key, DEFAULTS[key]))
            new_token = form.get("esios_token", "").strip()
            if form.get("esios_token_delete"):
                cfg["esios_token"] = ""
            elif new_token:
                cfg["esios_token"] = new_token
            cfg["zones"] = parse_zones(form, cfg["zones"])
            cfg["raw_retention_days"] = max(0, int(form.get("raw_retention_days", 0) or 0))
            new_tg = form.get("telegram_token", "").strip()
            if form.get("telegram_token_delete"):
                cfg["telegram_token"] = ""
            elif new_tg:
                cfg["telegram_token"] = new_tg
            cfg["telegram_chat_id"] = form.get("telegram_chat_id", "").strip()
            cfg["offline_alert_minutes"] = max(0.0, float(form.get("offline_alert_minutes", 10) or 0))
            cfg["cheap_hours_enabled"] = bool(form.get("cheap_hours_enabled"))
            cfg["cheap_hours_count"] = min(12, max(1, int(form.get("cheap_hours_count", 3) or 3)))
            hh_mm = form.get("cheap_hours_time", "20:45").strip()
            if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", hh_mm):
                raise ValueError("La hora del aviso de horas baratas debe tener el formato HH:MM.")
            cfg["cheap_hours_time"] = hh_mm
            password, repeat = form.get("new_password", ""), form.get("new_password_repeat", "")
            if password or repeat:
                if password != repeat:
                    raise ValueError("Las dos contraseñas no coinciden.")
                if len(password) < 6:
                    raise ValueError("La contraseña debe tener al menos 6 caracteres.")
                cfg["admin_password_hash"] = generate_password_hash(password)
                session["admin"] = True
            elif form.get("remove_password"):
                cfg["admin_password_hash"] = ""
            save_config(cfg)
            reload_zones()
            start_pollers()
        except (ValueError, TypeError) as e:
            flash(f"No se han guardado los ajustes: {e}")
            return redirect(url_for("ajustes"))
        try:
            n = fetch_pvpc()
            flash(f"Ajustes guardados. Precios horarios cargados: {n}.")
        except Exception as e:
            flash(f"Ajustes guardados, pero no se pudieron descargar precios PVPC: {e}")
        return redirect(url_for("ajustes"))
    return render_template("ajustes.html", cfg=cfg, zones=cfg["zones"], models=MODELS, default_zone=DEFAULT_ZONE,
                           zone_errors=ZONE_ERRORS)

@app.route("/ajustes/probar-telegram", methods=["POST"])
@admin_required
def probar_telegram():
    try:
        if notify("✅ Prueba de avisos del monitor de consumo eléctrico."):
            flash("Mensaje de prueba enviado por Telegram.")
        else:
            flash("Configura y guarda el token del bot y el chat ID antes de probar.")
    except Exception as e:
        flash(f"No se pudo enviar el mensaje de prueba: {e}")
    return redirect(url_for("ajustes") + "#avisos")

# ---------------------------------------------------------------- arranque

def setup_logging():
    # Con pythonw (arranque automático) no hay consola: todo va a app.log.
    if sys.stdout is None or sys.stderr is None:
        stream = open(LOG_PATH.with_suffix(".console.log"), "a", encoding="utf-8", buffering=1)
        sys.stdout = sys.stdout or stream
        sys.stderr = sys.stderr or stream
    handlers = [RotatingFileHandler(LOG_PATH, maxBytes=2_000_000, backupCount=3, encoding="utf-8"),
                logging.StreamHandler()]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=handlers)

def setup_app():
    cfg = load_config()
    if not cfg.get("secret_key"):
        cfg["secret_key"] = secrets.token_hex(32)
        save_config(cfg)
    app.secret_key = cfg["secret_key"]
    app.permanent_session_lifetime = timedelta(days=30)
    reload_zones()
    init_db(DEFAULT_ZONE)
    for zone in ZONES:
        init_db(zone)

if __name__ == "__main__":
    setup_logging()
    setup_app()
    start_pollers()
    for target in (price_refresh_loop, maintenance_loop, alert_loop):
        threading.Thread(target=target, daemon=True, name=target.__name__).start()
    log.info("Web local: http://127.0.0.1:%d", PORT)
    log.info("Web en la red: http://<IP-DE-ESTE-ORDENADOR>:%d", PORT)
    log.info("Zonas: %s", ", ".join(f"{z}={zone_connection(d)} ({d['name']})" for z, d in ZONES.items()))
    app.run(host="0.0.0.0", port=PORT, debug=False)
