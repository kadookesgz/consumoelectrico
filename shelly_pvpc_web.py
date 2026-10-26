from flask import Flask, jsonify, render_template_string, request, redirect, url_for, flash, Response
import requests, sqlite3, threading, time, json, csv, io, os, asyncio, re, struct
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import aiohttp
from esios_api import PVPCData
from pathlib import Path
from pymodbus.client import ModbusTcpClient

ZONES = {
    "principal": {"name": "Principal · Shelly Pro EM-50", "ip": "192.168.4.175", "model": "pro_em"},
    "acuario": {"name": "Acuario · Shelly PM Mini Gen 3", "ip": "192.168.4.203", "model": "pm_mini_gen3"},
    "schneider": {"name": "Schneider PowerLogic PM710", "ip": "192.168.100.206", "port": 502, "slave_id": 1, "model": "pm710_modbus"},
}
DEFAULT_ZONE = "principal"
IP = ZONES[DEFAULT_ZONE]["ip"]
PORT = 5000
POLL_SECONDS = 5
DB_PATH = Path(__file__).with_name("shelly.db")
CONFIG_PATH = Path(__file__).with_name("config.json")
ESIOS_URL = "https://api.esios.ree.es/indicators/1001"
PENINSULA_GEO_ID = 8741
app = Flask(__name__)
app.secret_key = "local-shelly-pvpc-change-me"
lock = threading.Lock()
latest_error = ""
ZONE_ERRORS = {zone: "Esperando primera lectura" for zone in ZONES}

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
    "extra_discount_pct": 0.0
}

def load_config():
    cfg = dict(DEFAULTS)
    if CONFIG_PATH.exists():
        try:
            cfg.update(json.loads(CONFIG_PATH.read_text(encoding="utf-8")))
        except Exception:
            pass
    return cfg

def save_config(cfg):
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")

def db_path_for_zone(zone=None):
    if zone in (None, "", DEFAULT_ZONE):
        return DB_PATH
    if zone not in ZONES:
        raise ValueError("Zona desconocida")
    return Path(__file__).with_name(f"{zone}.db")

def connect(zone=None):
    return sqlite3.connect(db_path_for_zone(zone), timeout=10)

def init_db():
    with connect() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS measurements (
          id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL, epoch REAL NOT NULL,
          voltage REAL, current REAL, act_power REAL, aprt_power REAL, pf REAL, freq REAL,
          total_kwh REAL NOT NULL, returned_kwh REAL NOT NULL, delta_kwh REAL NOT NULL DEFAULT 0,
          energy_cost_eur REAL NOT NULL DEFAULT 0, pvpc_eur_mwh REAL)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_measurements_epoch ON measurements(epoch)")
        cols = {r[1] for r in c.execute("PRAGMA table_info(measurements)")}
        for name, typ in [("energy_cost_eur", "REAL NOT NULL DEFAULT 0"), ("pvpc_eur_mwh", "REAL")]:
            if name not in cols:
                c.execute(f"ALTER TABLE measurements ADD COLUMN {name} {typ}")
        c.execute("""CREATE TABLE IF NOT EXISTS pvpc_prices (
          epoch INTEGER PRIMARY KEY, price_eur_mwh REAL NOT NULL, fetched_at REAL NOT NULL)""")

def init_db_for_zone(zone):
    """Create the standard tables in a zone-specific database."""
    with connect(zone) as c:
        c.execute("""CREATE TABLE IF NOT EXISTS measurements (
          id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL, epoch REAL NOT NULL,
          voltage REAL, current REAL, act_power REAL, aprt_power REAL, pf REAL, freq REAL,
          total_kwh REAL NOT NULL, returned_kwh REAL NOT NULL, delta_kwh REAL NOT NULL DEFAULT 0,
          energy_cost_eur REAL NOT NULL DEFAULT 0, pvpc_eur_mwh REAL)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_measurements_epoch ON measurements(epoch)")
        c.execute("""CREATE TABLE IF NOT EXISTS pvpc_prices (
          epoch INTEGER PRIMARY KEY, price_eur_mwh REAL NOT NULL, fetched_at REAL NOT NULL)""")


def rpc(method, params=None, ip=IP):
    r = requests.get(f"http://{ip}/rpc/{method}", params=params or {}, timeout=4)
    r.raise_for_status()
    return r.json()

def fetch_pvpc(start, end):
    """
    Usa la biblioteca `esios_api` declarada por Home Assistant para PVPC.
    La aplicación almacena el precio en €/MWh, por lo que convierte el valor
    de la biblioteca (€/kWh) multiplicándolo por 1000.
    """
    token = str(load_config().get("esios_token", "")).strip()

    async def _get_data():
        timeout = aiohttp.ClientTimeout(total=35)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            api = PVPCData(
                session=session,
                tariff="2.0TD",
                local_timezone=ZoneInfo("Europe/Madrid"),
                api_token=token or None,
                sensor_keys=("PVPC",),
            )
            now_utc = datetime.now(timezone.utc)
            data = await api.async_update_all(current_data=None, now=now_utc)
            return api, data

    try:
        api, data = asyncio.run(_get_data())
        attrs = getattr(api, "sensor_attributes", {}).get("PVPC", {}) or {}
        local_now = datetime.now().astimezone()
        start_epoch, end_epoch = start.timestamp(), end.timestamp()
        prices_by_epoch = {}

        for key, value in attrs.items():
            # Current-day hourly attributes documented by esios_api.
            match = re.fullmatch(r"price_(\d{2})h", str(key))
            if not match:
                continue
            try:
                hour = int(match.group(1))
                price_eur_kwh = float(value)
                hour_dt = local_now.replace(
                    hour=hour, minute=0, second=0, microsecond=0
                )
                epoch = int(hour_dt.timestamp())
                if start_epoch <= epoch <= end_epoch:
                    prices_by_epoch[epoch] = price_eur_kwh * 1000.0
            except (TypeError, ValueError):
                continue

        # Include the active hourly price if the library populated a state but
        # did not expose all hour attributes.
        current_price = getattr(api, "states", {}).get("PVPC")
        if current_price is not None:
            try:
                price_eur_kwh = float(current_price)
                hour_dt = local_now.replace(minute=0, second=0, microsecond=0)
                epoch = int(hour_dt.timestamp())
                if start_epoch <= epoch <= end_epoch:
                    prices_by_epoch[epoch] = price_eur_kwh * 1000.0
            except (TypeError, ValueError):
                pass

        if not prices_by_epoch:
            # Expose helpful data shape in the error without leaking token.
            keys = sorted(map(str, attrs.keys()))[:25]
            raise RuntimeError(
                "esios_api no ha devuelto precios horarios utilizables. "
                f"Atributos recibidos: {keys or 'ninguno'}; "
                "revisa conectividad y, si ESIOS lo exige, configura el token."
            )

        with connect() as c:
            c.executemany(
                "INSERT OR REPLACE INTO pvpc_prices(epoch,price_eur_mwh,fetched_at) VALUES(?,?,?)",
                [(epoch, price, time.time()) for epoch, price in prices_by_epoch.items()]
            )
        return len(prices_by_epoch)
    except Exception as exc:
        raise RuntimeError(
            "esios_api (biblioteca de Home Assistant): " + str(exc)
        ) from exc

def ensure_prices():
    now = datetime.now().astimezone()
    # Fetch from yesterday through tomorrow to cover recent history and today's schedule.
    try:
        with connect() as c:
            newest = c.execute("SELECT MAX(epoch) FROM pvpc_prices").fetchone()[0]
            oldest = c.execute("SELECT MIN(epoch) FROM pvpc_prices").fetchone()[0]
        if newest is None or newest < now.timestamp() + 18*3600:
            fetch_pvpc(now - timedelta(days=2), now + timedelta(days=2))
        elif oldest is None or oldest > now.timestamp() - 24*3600:
            fetch_pvpc(now - timedelta(days=2), now + timedelta(days=1))
    except Exception as e:
        print("Aviso PVPC:", e)

def price_for_epoch(epoch):
    with connect() as c:
        row = c.execute("SELECT price_eur_mwh FROM pvpc_prices WHERE epoch<=? ORDER BY epoch DESC LIMIT 1", (int(epoch),)).fetchone()
    return float(row[0]) if row else None

def _float32(registers, offset):
    """Decode PM710 IEEE-754 float from two consecutive Modbus registers."""
    return float(struct.unpack(">f", struct.pack(">HH", registers[offset], registers[offset + 1]))[0])


def read_zone(zone):
    """Lee un Shelly o un Schneider PM710 y normaliza sus medidas."""
    dev = ZONES[zone]
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
                raise RuntimeError(f"Modbus FC03 devolvió error al leer registros PM710 1000–1020: {response}")
            regs = response.registers
            if len(regs) < 22:
                raise RuntimeError(f"Respuesta Modbus incompleta: {len(regs)} registros; se esperaban 22")
            energy_kwh = _float32(regs, 0)      # 1000/1001: total real energy, kWh
            power_kw = _float32(regs, 6)        # 1006/1007: total real power, kW
            pf = _float32(regs, 12)             # 1012/1013: total power factor
            voltage = _float32(regs, 14)       # 1014/1015: average line-line voltage
            current = _float32(regs, 18)       # 1018/1019: average current
            freq = _float32(regs, 20)          # 1020/1021: frequency, Hz
            values = (energy_kwh, power_kw, voltage, current, freq)
            if not all(map(lambda v: abs(v) < 1e9, values)) or energy_kwh < 0 or voltage < 0 or current < 0 or freq < 0:
                raise RuntimeError(f"Valores PM710 decodificados no válidos: energía={energy_kwh}, potencia={power_kw}, tensión={voltage}, corriente={current}, frecuencia={freq}. Revisar orden de palabras Modbus.")
            status = {"voltage": voltage, "current": current, "act_power": power_kw * 1000.0,
                      "aprt_power": None, "pf": pf, "freq": freq}
            return status, energy_kwh, 0.0
        finally:
            client.close()
    if dev["model"] == "pro_em":
        status = rpc("EM1.GetStatus", {"id": 0}, ip=ip)
        energy = rpc("EM1Data.GetStatus", {"id": 0}, ip=ip)
        total = float(energy["total_act_energy"]) / 1000.0  # Wh -> kWh
        returned = float(energy.get("total_act_ret_energy", 0)) / 1000.0
        return status, total, returned

    # Shelly PM Mini Gen3 is a power meter without a relay: its component is PM1.
    status = rpc("PM1.GetStatus", {"id": 0}, ip=ip)
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

def poller(zone):
    """Polls a single zone into its own SQLite database."""
    while True:
        now = datetime.now().astimezone()
        epoch = now.timestamp()
        stamp = now.strftime("%Y-%m-%d %H:%M:%S")
        try:
            status, total, returned = read_zone(zone)
            ZONE_ERRORS[zone] = ""
            price = price_for_epoch(epoch)
            with connect(zone) as c:
                prev = c.execute(
                    "SELECT epoch,total_kwh,energy_cost_eur FROM measurements ORDER BY id DESC LIMIT 1"
                ).fetchone()
                delta = max(0.0, total - float(prev[1])) if prev else 0.0
                if prev and total < float(prev[1]):
                    delta = 0.0
                cost_delta = delta * (price / 1000.0) if price is not None else 0.0
                c.execute("""INSERT INTO measurements(timestamp,epoch,voltage,current,act_power,aprt_power,pf,freq,total_kwh,returned_kwh,delta_kwh,energy_cost_eur,pvpc_eur_mwh)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (stamp, epoch, status.get("voltage"), status.get("current"), status.get("act_power"),
                     status.get("aprt_power"), status.get("pf"), status.get("freq"), total, returned,
                     delta, cost_delta, price))
        except Exception as e:
            ZONE_ERRORS[zone] = f"{type(e).__name__}: {e}"
            print(f"{stamp} | Error zona {zone} ({ZONES[zone]['ip']}): {e}")
        time.sleep(POLL_SECONDS)

def price_refresh_loop():
    while True:
        ensure_prices()
        time.sleep(3600)

def bill_summary():
    cfg = load_config()
    now = time.time()
    day_start = datetime.now().astimezone().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    month_start_dt = datetime.now().astimezone().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_start = month_start_dt.timestamp()
    with connect() as c:
        # Costs and consumption summed by sample deltas in chosen periods
        today = c.execute("SELECT COALESCE(SUM(delta_kwh),0),COALESCE(SUM(energy_cost_eur),0) FROM measurements WHERE epoch>=?", (day_start,)).fetchone()
        month = c.execute("SELECT COALESCE(SUM(delta_kwh),0),COALESCE(SUM(energy_cost_eur),0) FROM measurements WHERE epoch>=?", (month_start,)).fetchone()
        allrow = c.execute("SELECT COALESCE(SUM(delta_kwh),0),COALESCE(SUM(energy_cost_eur),0) FROM measurements").fetchone()
        latest = c.execute("SELECT * FROM measurements ORDER BY id DESC LIMIT 1").fetchone()
        recent = c.execute("SELECT timestamp,act_power,total_kwh,delta_kwh,energy_cost_eur,pvpc_eur_mwh FROM measurements ORDER BY id DESC LIMIT 1000").fetchall()
        price_now = c.execute("SELECT price_eur_mwh FROM pvpc_prices WHERE epoch<=? ORDER BY epoch DESC LIMIT 1", (int(now),)).fetchone()
        price_count = c.execute("SELECT COUNT(*) FROM pvpc_prices").fetchone()[0]
    # Full estimated invoice: measured energy cost + power term + meter rent + configurable extras; taxes applied.
    # PVPC indicator 1001 already includes energy, access tolls and charges; it excludes taxes.
    power_days_month = datetime.now().day
    power_cost = (float(cfg["power_kw_p1"]) * float(cfg["power_price_p1_eur_kw_day"]) +
                  float(cfg["power_kw_p2"]) * float(cfg["power_price_p2_eur_kw_day"])) * power_days_month
    meter_cost = float(cfg["meter_rent_eur_day"]) * power_days_month
    other_fixed = float(cfg["other_fixed_monthly_eur"]) * power_days_month / datetime.now().replace(day=28).day
    energy_base = float(month[1])
    discount = energy_base * float(cfg["extra_discount_pct"]) / 100.0
    taxable_base = max(0.0, energy_base - discount) + power_cost + meter_cost + other_fixed
    electricity_tax = taxable_base * float(cfg["electricity_tax_pct"]) / 100.0
    vat_base = taxable_base + electricity_tax
    vat = vat_base * float(cfg["vat_pct"]) / 100.0
    month_total = vat_base + vat
    return {
        "today_kwh": float(today[0]), "today_energy_eur": float(today[1]),
        "month_kwh": float(month[0]), "month_energy_eur": energy_base,
        "all_kwh_logged": float(allrow[0]), "all_energy_eur_logged": float(allrow[1]),
        "power_cost_month_to_date": power_cost, "meter_cost_month_to_date": meter_cost,
        "other_fixed": other_fixed, "discount": discount, "taxable_base": taxable_base,
        "electricity_tax": electricity_tax, "vat": vat, "month_total_estimate": month_total,
        "price_now_eur_mwh": float(price_now[0]) if price_now else None,
        "price_count": int(price_count), "config": cfg,
        "latest": dict(zip(["id","timestamp","epoch","voltage","current","act_power","aprt_power","pf","freq","total_kwh","returned_kwh","delta_kwh","energy_cost_eur","pvpc_eur_mwh"], latest)) if latest else None,
        "recent": [{"timestamp": r[0], "act_power": r[1], "total_kwh": r[2], "delta_kwh": r[3], "energy_cost_eur": r[4], "pvpc_eur_mwh": r[5]} for r in reversed(recent[-20:])]
    }

PAGE = r"""<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Shelly Pro EM-50 · PVPC</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script><style>
*{box-sizing:border-box}body{margin:0;background:#0b1220;color:#edf4ff;font:15px system-ui,Arial}main{max-width:1200px;margin:auto;padding:22px}header{display:flex;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:20px}h1{margin:0;font-size:clamp(24px,4vw,34px)}h2{font-size:18px}p,.muted{color:#9aabc4}.grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}.card{background:#121d30;border:1px solid #25334b;border-radius:16px;padding:18px}.label{color:#9aabc4;font-size:13px}.value{font-size:clamp(24px,3vw,34px);font-weight:700;margin-top:8px}.unit{font-size:14px;color:#9aabc4}.small{font-size:12px;color:#9aabc4;margin-top:8px}.full{grid-column:1/-1}.chart{height:280px}table{width:100%;border-collapse:collapse}td,th{text-align:left;padding:8px;border-bottom:1px solid #25334b;white-space:nowrap}th{color:#9aabc4}a{color:#54d6a0}.status{border:1px solid #25334b;border-radius:25px;padding:8px 12px;color:#9aabc4}button{background:#23644f;border:0;color:white;border-radius:9px;padding:10px 14px;cursor:pointer}.warn{color:#ffd479}.row{display:flex;gap:12px;flex-wrap:wrap;align-items:center}
@media(max-width:760px){main{padding:12px}.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.value{font-size:25px}}@media(max-width:430px){.grid{grid-template-columns:1fr}.full{grid-column:auto}}
</style></head><body><main>
<header><div><h1 id="pageTitle">Consumo eléctrico · Zona principal</h1><p>Lecturas de Shelly + PVPC horario + estimación de factura</p><div class="row"><label for="zoneSelect">Zona:</label><select id="zoneSelect" onchange="changeZone()" style="padding:9px;border-radius:8px;background:#121d30;color:#edf4ff;border:1px solid #344761"><option value="principal">Principal · Shelly Pro EM-50</option><option value="acuario">Acuario · Shelly PM Mini Gen 3</option><option value="schneider">Schneider PowerLogic PM710</option></select><span class="small" id="deviceIp">IP: 192.168.4.175</span></div></div><div class="status" id="status">Cargando…</div></header>
<section class="grid">
<div class="card"><div class="label">Potencia actual</div><div class="value" id="power">— W</div><div class="small" id="updated"></div></div>
<div class="card"><div class="label">Contador real consumido</div><div class="value" id="total">— kWh</div><div class="small">Contador interno del Shelly</div></div>
<div class="card"><div class="label">PVPC hora actual</div><div class="value" id="price">— €/kWh</div><div class="small">Indicador PVPC de REE/ESIOS, antes de impuestos</div></div>
<div class="card"><div class="label">Consumo de hoy</div><div class="value" id="today">— kWh</div><div class="small">Registrado por esta aplicación</div></div>
<div class="card"><div class="label">Coste energía hoy</div><div class="value" id="todaycost">— €</div><div class="small">Solo energía registrada y con precio disponible</div></div>
<div class="card"><div class="label">Consumo mes actual</div><div class="value" id="monthkwh">— kWh</div></div>
<div class="card"><div class="label">Coste energía mes</div><div class="value" id="monthenergy">— €</div><div class="small">Sin impuestos ni término fijo</div></div>
<div class="card"><div class="label">Factura estimada del mes</div><div class="value" id="bill">— €</div><div class="small">Incluye término fijo e impuestos configurados</div></div>
<div class="card"><div class="label">Energía devuelta total</div><div class="value" id="returned">— kWh</div></div>
<div class="card full"><h2>Potencia actual y precio PVPC</h2><div class="chart"><canvas id="powerPvpcChart"></canvas></div><p class="small">Potencia en W y PVPC en €/kWh, con escalas independientes. Histórico de las últimas lecturas guardadas.</p></div>
<div class="card"><h2>Consumo por día</h2><div class="chart"><canvas id="dailyChart"></canvas></div><p class="small">Consumo diario en kWh. Días con datos guardados en esta zona.</p></div>
<div class="card"><h2>Consumo por hora · hoy</h2><div class="chart"><canvas id="hourlyChart"></canvas></div><p class="small">Suma del consumo registrado en cada hora del día actual.</p></div>
<div class="card full"><h2>Coste estimado acumulado del mes</h2><div class="chart"><canvas id="costChart"></canvas></div><p class="small">El coste horario se acumula cuando la aplicación está funcionando y el precio horario está disponible.</p></div>
<div class="card full"><h2>Desglose estimado de factura</h2><div style="overflow:auto"><table><tbody id="billrows"></tbody></table></div></div>
<div class="card full"><h2>Últimas lecturas</h2><p><a id="exportLink" href="/exportar.csv?zona=principal">Descargar CSV de esta zona</a> · <a id="historyLink" href="/historico?zona=principal">Consultar histórico</a> · <a href="/ajustes">Configurar tarifa y token PVPC</a></p><div style="overflow:auto"><table><thead><tr><th>Fecha/hora</th><th>W</th><th>Contador kWh</th><th>Delta kWh</th><th>Precio €/MWh</th><th>Coste energía €</th></tr></thead><tbody id="rows"></tbody></table></div></div>
</section><p class="small">Fuente de precios: <a href="https://www.esios.ree.es/" target="_blank" rel="noreferrer">Red Eléctrica de España · ESIOS</a>. El indicador PVPC incluye energía, peajes y cargos, pero no impuestos. La factura completa es aproximada y depende de los datos configurados y de que el histórico esté completo.</p>
</main>
<script>
const f=(v,n=3)=>v==null?'—':Number(v).toLocaleString('es-ES',{minimumFractionDigits:0,maximumFractionDigits:n});
const eur=v=>v==null?'—':f(v,2)+' €';
let currentZone='principal';function changeZone(){currentZone=document.getElementById('zoneSelect').value;document.getElementById('pageTitle').textContent=currentZone==='acuario'?'Consumo eléctrico · Acuario':(currentZone==='schneider'?'Consumo eléctrico · Schneider PM710':'Consumo eléctrico · Zona principal');document.getElementById('deviceIp').textContent='Conexión: '+(currentZone==='acuario'?'192.168.4.203':(currentZone==='schneider'?'192.168.100.206:502 · Modbus TCP · Slave ID 1':'192.168.4.175'));document.getElementById('exportLink').href='/exportar.csv?zona='+encodeURIComponent(currentZone);document.getElementById('historyLink').href='/historico?zona='+encodeURIComponent(currentZone);update();}
const chartOptions={responsive:true,maintainAspectRatio:false,animation:false,interaction:{mode:'index',intersect:false},plugins:{legend:{labels:{color:'#edf4ff'}}},scales:{x:{ticks:{color:'#9aabc4',maxTicksLimit:10},grid:{color:'#25334b'}},y:{beginAtZero:true,ticks:{color:'#9aabc4'},grid:{color:'#25334b'}}}};
const powerPvpcChart=new Chart(document.getElementById('powerPvpcChart'),{type:'line',data:{labels:[],datasets:[
{label:'Potencia (W)',data:[],yAxisID:'yPower',borderColor:'#54d6a0',backgroundColor:'#54d6a022',fill:false,tension:.2,pointRadius:0},
{label:'PVPC (€/kWh)',data:[],yAxisID:'yPrice',borderColor:'#ffd479',backgroundColor:'#ffd47922',fill:false,tension:.2,pointRadius:0}
]},options:{...chartOptions,scales:{x:chartOptions.scales.x,yPower:{type:'linear',position:'left',beginAtZero:true,title:{display:true,text:'Potencia (W)',color:'#54d6a0'},ticks:{color:'#9aabc4'},grid:{color:'#25334b'}},yPrice:{type:'linear',position:'right',beginAtZero:true,title:{display:true,text:'PVPC (€/kWh)',color:'#ffd479'},ticks:{color:'#9aabc4'},grid:{drawOnChartArea:false}}}}});
const dailyChart=new Chart(document.getElementById('dailyChart'),{type:'bar',data:{labels:[],datasets:[{label:'Consumo (kWh)',data:[],backgroundColor:'#54d6a0'}]},options:chartOptions});
const hourlyChart=new Chart(document.getElementById('hourlyChart'),{type:'bar',data:{labels:[],datasets:[{label:'Consumo (kWh)',data:[],backgroundColor:'#6da9ff'}]},options:chartOptions});
const costChart=new Chart(document.getElementById('costChart'),{type:'line',data:{labels:[],datasets:[{label:'Coste de energía acumulado (€)',data:[],borderColor:'#54d6a0',backgroundColor:'#54d6a022',fill:true,tension:.2,pointRadius:0}]},options:chartOptions});
async function update(){try{const j=await(await fetch('/api/estado?zona='+encodeURIComponent(currentZone))).json();document.getElementById('status').textContent=j.ok?'Conectado · '+j.ip:(j.error?'Error · '+j.error:'Sin lectura todavía · '+j.ip);let d=j.latest||{};document.getElementById('power').textContent=f(d.act_power,1)+' W';document.getElementById('total').textContent=f(j.total_kwh,3)+' kWh';document.getElementById('returned').textContent=f(j.returned_kwh,3)+' kWh';document.getElementById('price').textContent=j.price_now==null?'Sin precio':f(j.price_now/1000,5)+' €/kWh';document.getElementById('updated').textContent=d.timestamp?'Actualizado '+d.timestamp:'';document.getElementById('today').textContent=f(j.today_kwh,3)+' kWh';document.getElementById('todaycost').textContent=eur(j.today_energy_eur);document.getElementById('monthkwh').textContent=f(j.month_kwh,3)+' kWh';document.getElementById('monthenergy').textContent=eur(j.month_energy_eur);document.getElementById('bill').textContent=eur(j.month_total_estimate);document.getElementById('billrows').innerHTML=[['Energía PVPC registrada',j.month_energy_eur],['Término potencia (estimado)',j.power_cost],['Alquiler contador',j.meter_cost],['Otros costes fijos',j.other_fixed],['Descuento configurado',-j.discount],['Base antes de impuesto eléctrico',j.taxable_base],['Impuesto electricidad',j.electricity_tax],['IVA',j.vat],['TOTAL ESTIMADO',j.month_total_estimate]].map(r=>'<tr><td>'+r[0]+'</td><td>'+eur(r[1])+'</td></tr>').join('');document.getElementById('rows').innerHTML=j.recent.slice().reverse().map(x=>'<tr>'+[x.timestamp,f(x.act_power,1),f(x.total_kwh,3),f(x.delta_kwh,5),f(x.pvpc_eur_mwh,2),eur(x.energy_cost_eur)].map(v=>'<td>'+v+'</td>').join('')+'</tr>').join('');powerPvpcChart.data.labels=j.power_pvpc_history.map(x=>x.time);powerPvpcChart.data.datasets[0].data=j.power_pvpc_history.map(x=>x.power);powerPvpcChart.data.datasets[1].data=j.power_pvpc_history.map(x=>x.price==null?null:x.price/1000);powerPvpcChart.update();
dailyChart.data.labels=j.daily_consumption.map(x=>x.day);dailyChart.data.datasets[0].data=j.daily_consumption.map(x=>x.kwh);dailyChart.update();
hourlyChart.data.labels=j.hourly_consumption.map(x=>x.hour);hourlyChart.data.datasets[0].data=j.hourly_consumption.map(x=>x.kwh);hourlyChart.update();
costChart.data.labels=j.cost_history.map(x=>x.time);costChart.data.datasets[0].data=j.cost_history.map(x=>x.cost);costChart.update();if(j.price_count===0)document.getElementById('status').textContent='Esperando precios PVPC';}catch(e){document.getElementById('status').textContent='Error de conexión web';}}
update();setInterval(update,5000);
</script></body></html>"""

SETTINGS = r"""<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Ajustes PVPC</title><style>
body{margin:0;background:#0b1220;color:#edf4ff;font:15px system-ui,Arial}.wrap{max-width:800px;margin:25px auto;padding:18px}.card{background:#121d30;border:1px solid #25334b;border-radius:16px;padding:22px}h1{margin-top:0}label{display:block;color:#b5c4d9;font-size:13px;margin:14px 0 5px}input{width:100%;padding:11px;border-radius:8px;border:1px solid #344761;background:#0b1220;color:white}button{margin-top:18px;padding:11px 16px;border:0;border-radius:8px;background:#23644f;color:white;cursor:pointer}a{color:#54d6a0}.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.hint{color:#9aabc4;font-size:13px;line-height:1.5}.msg{color:#ffd479}@media(max-width:600px){.grid{grid-template-columns:1fr}}
</style></head><body><div class="wrap"><div class="card"><h1>Ajustes de tarifa PVPC</h1><p class="hint">Los precios horarios se consultan en REE/ESIOS mediante el indicador 1001 para Península (geo_id 8741). El token personal de ESIOS es opcional para el PVPC básico; se guarda localmente en config.json. El token se guarda localmente en config.json.</p>{% with messages = get_flashed_messages() %}{% if messages %}<p class="msg">{{ messages[0] }}</p>{% endif %}{% endwith %}
<form method="post">
<label>Token personal ESIOS</label><input name="esios_token" type="password" value="{{ cfg.esios_token }}" autocomplete="off" placeholder="Pega aquí tu token de ESIOS">
<div class="grid">
<div><label>Potencia contratada P1 (kW)</label><input name="power_kw_p1" type="number" step="0.01" value="{{ cfg.power_kw_p1 }}"></div>
<div><label>Potencia contratada P2 (kW)</label><input name="power_kw_p2" type="number" step="0.01" value="{{ cfg.power_kw_p2 }}"></div>
<div><label>Precio potencia P1 (€/kW/día, según factura)</label><input name="power_price_p1_eur_kw_day" type="number" step="0.000001" value="{{ cfg.power_price_p1_eur_kw_day }}"></div>
<div><label>Precio potencia P2 (€/kW/día, según factura)</label><input name="power_price_p2_eur_kw_day" type="number" step="0.000001" value="{{ cfg.power_price_p2_eur_kw_day }}"></div>
<div><label>Alquiler contador (€/día)</label><input name="meter_rent_eur_day" type="number" step="0.0001" value="{{ cfg.meter_rent_eur_day }}"></div>
<div><label>Otros costes fijos (€/mes)</label><input name="other_fixed_monthly_eur" type="number" step="0.01" value="{{ cfg.other_fixed_monthly_eur }}"></div>
<div><label>Impuesto electricidad (%)</label><input name="electricity_tax_pct" type="number" step="0.000001" value="{{ cfg.electricity_tax_pct }}"></div>
<div><label>IVA (%)</label><input name="vat_pct" type="number" step="0.01" value="{{ cfg.vat_pct }}"></div>
<div><label>Descuento sobre energía (%)</label><input name="extra_discount_pct" type="number" step="0.01" value="{{ cfg.extra_discount_pct }}"></div>
</div><p class="hint">Los precios de potencia, alquiler e impuestos son editables: comprueba tu factura y ajusta los valores a los que te aplique tu comercializadora. El término de energía PVPC del indicador 1001 incluye peajes/cargos y coste de energía, pero excluye impuestos.</p><button type="submit">Guardar ajustes y actualizar precios</button></form><p><a href="/">← Volver al monitor</a></p></div></div></body></html>"""

HISTORY = r"""<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Histórico de consumo</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script><style>
*{box-sizing:border-box}body{margin:0;background:#0b1220;color:#edf4ff;font:15px system-ui,Arial}main{max-width:1200px;margin:auto;padding:22px}header{margin-bottom:16px}h1{margin:0 0 6px;font-size:clamp(24px,4vw,34px)}h2{font-size:18px;margin-top:0}p,.muted{color:#9aabc4}.grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}.card{background:#121d30;border:1px solid #25334b;border-radius:16px;padding:18px}.label{color:#9aabc4;font-size:13px}.value{font-size:clamp(22px,3vw,30px);font-weight:700;margin-top:8px}.small{font-size:12px;color:#9aabc4;margin-top:8px}.full{grid-column:1/-1}.chart{height:300px}table{width:100%;border-collapse:collapse}td,th{text-align:left;padding:7px 8px;border-bottom:1px solid #25334b;white-space:nowrap}th{color:#9aabc4}a{color:#54d6a0}button{background:#23644f;border:0;color:white;border-radius:9px;padding:9px 13px;cursor:pointer}button.alt{background:#1d2a40;border:1px solid #344761}select,input{padding:9px;border-radius:8px;background:#0b1220;color:#edf4ff;border:1px solid #344761}.row{display:flex;gap:10px;flex-wrap:wrap;align-items:end}.row label{display:flex;flex-direction:column;gap:4px;font-size:13px;color:#9aabc4}.warn{color:#ffd479}
@media(max-width:760px){main{padding:12px}.grid{grid-template-columns:repeat(2,minmax(0,1fr))}}@media(max-width:430px){.grid{grid-template-columns:minmax(0,1fr)}.full{grid-column:auto}}.card{min-width:0}
</style></head><body><main>
<header><h1>Histórico de consumo</h1><p><a href="/">← Volver al monitor</a> · <span id="available">Cargando…</span></p></header>
<section class="card" style="margin-bottom:14px"><div class="row">
<label>Zona<select id="zona">{% for key, z in zones.items() %}<option value="{{ key }}">{{ z.name }}</option>{% endfor %}</select></label>
<label>Desde<input type="date" id="desde"></label><label>Hasta<input type="date" id="hasta"></label>
<label>Agrupar por<select id="agrupar"><option value="auto">Automático</option><option value="hora">Hora</option><option value="dia">Día</option><option value="mes">Mes</option></select></label>
<button onclick="load()">Consultar</button></div>
<div class="row" style="margin-top:10px"><button class="alt" onclick="preset('hoy')">Hoy</button><button class="alt" onclick="preset('ayer')">Ayer</button><button class="alt" onclick="preset('7d')">Últimos 7 días</button><button class="alt" onclick="preset('30d')">Últimos 30 días</button><button class="alt" onclick="preset('mes')">Este mes</button><button class="alt" onclick="preset('mesant')">Mes anterior</button><button class="alt" onclick="preset('todo')">Todo</button></div></section>
<section class="grid">
<div class="card"><div class="label">Consumo del periodo</div><div class="value" id="kwh">—</div></div>
<div class="card"><div class="label">Coste de energía</div><div class="value" id="cost">—</div><div class="small">PVPC sin impuestos ni término fijo</div></div>
<div class="card"><div class="label">Precio medio pagado</div><div class="value" id="avgprice">—</div><div class="small">Coste / consumo con precio disponible</div></div>
<div class="card"><div class="label">Potencia máxima</div><div class="value" id="maxpower">—</div><div class="small" id="maxpowerat"></div></div>
<div class="card"><div class="label">Potencia media</div><div class="value" id="avgpower">—</div></div>
<div class="card"><div class="label">Lecturas guardadas</div><div class="value" id="readings">—</div><div class="small" id="coverage"></div></div>
<div class="card full"><h2>Consumo y precio PVPC</h2><div class="chart"><canvas id="energyChart"></canvas></div></div>
<div class="card full"><h2>Coste de energía</h2><div class="chart"><canvas id="costChart"></canvas></div></div>
<div class="card full"><h2>Potencia</h2><div class="chart"><canvas id="powerChart"></canvas></div><p class="small" id="powerNote"></p></div>
<div class="card full"><h2>Detalle</h2><p><a id="csvAgg" href="#">Descargar este detalle en CSV</a> · <a id="csvRaw" href="#">Descargar lecturas originales del periodo</a></p><div style="overflow:auto;max-height:520px"><table><thead><tr><th>Periodo</th><th>Consumo kWh</th><th>Coste €</th><th>Precio medio €/kWh</th><th>Potencia media W</th><th>Potencia máx. W</th><th>Lecturas</th></tr></thead><tbody id="rows"></tbody></table></div></div>
<div class="card full"><h2>Huecos sin datos</h2><p class="small">Periodos de más de 5 minutos sin lecturas (la aplicación estaba parada o el medidor no respondía). El consumo de un hueco se registra todo junto en la primera lectura posterior.</p><div style="overflow:auto"><table><thead><tr><th>Desde</th><th>Hasta</th><th>Duración</th><th>Consumo acumulado kWh</th></tr></thead><tbody id="gaps"></tbody></table></div></div>
</section></main>
<script>
const $=id=>document.getElementById(id);
const f=(v,n=3)=>v==null?'—':Number(v).toLocaleString('es-ES',{minimumFractionDigits:0,maximumFractionDigits:n});
const eur=v=>v==null?'—':f(v,2)+' €';
const iso=d=>d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');
const dur=s=>{s=Math.round(s);const d=Math.floor(s/86400),h=Math.floor(s%86400/3600),m=Math.floor(s%3600/60);return (d?d+' d ':'')+(h?h+' h ':'')+m+' min';};
let info=null;
function preset(p){const t=new Date();let a=new Date(t),b=new Date(t);
 if(p==='ayer'){a.setDate(t.getDate()-1);b=new Date(a);}
 else if(p==='7d')a.setDate(t.getDate()-6);
 else if(p==='30d')a.setDate(t.getDate()-29);
 else if(p==='mes')a=new Date(t.getFullYear(),t.getMonth(),1);
 else if(p==='mesant'){a=new Date(t.getFullYear(),t.getMonth()-1,1);b=new Date(t.getFullYear(),t.getMonth(),0);}
 else if(p==='todo'&&info&&info.first){a=new Date(info.first.slice(0,10)+'T00:00');b=new Date(info.last.slice(0,10)+'T00:00');}
 $('desde').value=iso(a);$('hasta').value=iso(b);load();}
const opts={responsive:true,maintainAspectRatio:false,animation:false,interaction:{mode:'index',intersect:false},plugins:{legend:{labels:{color:'#edf4ff'}}},scales:{x:{ticks:{color:'#9aabc4',maxTicksLimit:14},grid:{color:'#25334b'}},y:{beginAtZero:true,ticks:{color:'#9aabc4'},grid:{color:'#25334b'}}}};
const energyChart=new Chart($('energyChart'),{data:{labels:[],datasets:[
 {type:'bar',label:'Consumo (kWh)',data:[],yAxisID:'y',backgroundColor:'#54d6a0'},
 {type:'line',label:'PVPC medio (€/kWh)',data:[],yAxisID:'yPrice',borderColor:'#ffd479',backgroundColor:'#ffd479',pointRadius:0,tension:.2,spanGaps:true}]},
 options:{...opts,scales:{x:opts.scales.x,y:{...opts.scales.y,title:{display:true,text:'kWh',color:'#54d6a0'}},yPrice:{position:'right',beginAtZero:true,title:{display:true,text:'€/kWh',color:'#ffd479'},ticks:{color:'#9aabc4'},grid:{drawOnChartArea:false}}}}});
const costChart=new Chart($('costChart'),{type:'bar',data:{labels:[],datasets:[{label:'Coste (€)',data:[],backgroundColor:'#6da9ff'}]},options:opts});
const powerChart=new Chart($('powerChart'),{type:'line',data:{labels:[],datasets:[
 {label:'Potencia media (W)',data:[],borderColor:'#54d6a0',backgroundColor:'#54d6a022',fill:true,pointRadius:0,tension:.2},
 {label:'Potencia máxima (W)',data:[],borderColor:'#ff8a8a',pointRadius:0,tension:.2,borderWidth:1}]},options:opts});
async function loadInfo(){const r=await fetch('/api/historico/rango?zona='+encodeURIComponent($('zona').value));info=await r.json();
 $('available').textContent=info.first?('Datos de esta zona desde '+info.first+' hasta '+info.last+' · '+f(info.count,0)+' lecturas'):'Esta zona todavía no tiene lecturas guardadas';}
async function load(){const q=new URLSearchParams({zona:$('zona').value,desde:$('desde').value,hasta:$('hasta').value,agrupar:$('agrupar').value});
 history.replaceState(null,'','/historico?'+q);
 $('csvAgg').href='/exportar.csv?'+q;const raw=new URLSearchParams(q);raw.delete('agrupar');$('csvRaw').href='/exportar.csv?'+raw;
 const r=await fetch('/api/historico?'+q);const j=await r.json();if(!r.ok){$('available').innerHTML='<span class="warn">'+(j.error||'Error')+'</span>';return;}
 const t=j.totals;$('kwh').textContent=f(t.kwh,3)+' kWh';$('cost').textContent=eur(t.cost_eur);$('avgprice').textContent=t.avg_price_eur_kwh==null?'—':f(t.avg_price_eur_kwh,5)+' €/kWh';
 $('maxpower').textContent=t.max_power==null?'—':f(t.max_power,1)+' W';$('maxpowerat').textContent=t.max_power_at?'el '+t.max_power_at:'';$('avgpower').textContent=t.avg_power==null?'—':f(t.avg_power,1)+' W';
 $('readings').textContent=f(t.readings,0);$('coverage').textContent=t.readings?('Primera '+t.first+' · última '+t.last+(j.gaps.length?' · '+j.gaps.length+' huecos':'')):'Sin lecturas en este periodo';
 const lab=j.groups.map(g=>g.label);energyChart.data.labels=lab;energyChart.data.datasets[0].data=j.groups.map(g=>g.kwh);energyChart.data.datasets[1].data=j.groups.map(g=>g.avg_price_eur_kwh);energyChart.update();
 costChart.data.labels=lab;costChart.data.datasets[0].data=j.groups.map(g=>g.cost_eur);costChart.update();
 powerChart.data.labels=j.power.map(p=>p.time);powerChart.data.datasets[0].data=j.power.map(p=>p.avg);powerChart.data.datasets[1].data=j.power.map(p=>p.max);powerChart.update();
 $('powerNote').textContent='Cada punto resume '+dur(j.power_bucket_seconds)+' de lecturas.';
 $('rows').innerHTML=j.groups.map(g=>'<tr>'+[g.label,f(g.kwh,3),eur(g.cost_eur),g.avg_price_eur_kwh==null?'—':f(g.avg_price_eur_kwh,5),f(g.avg_power,1),f(g.max_power,1),f(g.readings,0)].map(v=>'<td>'+v+'</td>').join('')+'</tr>').join('')||'<tr><td colspan="7">Sin datos</td></tr>';
 $('gaps').innerHTML=j.gaps.map(g=>'<tr><td>'+g.from+'</td><td>'+g.to+'</td><td>'+dur(g.seconds)+'</td><td>'+f(g.kwh_after,3)+'</td></tr>').join('')||'<tr><td colspan="4">Sin huecos en este periodo</td></tr>';}
$('zona').onchange=async()=>{await loadInfo();load();};
(async()=>{const p=new URLSearchParams(location.search);if(p.get('zona'))$('zona').value=p.get('zona');if(p.get('agrupar'))$('agrupar').value=p.get('agrupar');
 await loadInfo();if(p.get('desde')&&p.get('hasta')){$('desde').value=p.get('desde');$('hasta').value=p.get('hasta');load();}else preset('7d');})();
</script></body></html>"""

HISTORY_GROUPS = {"hora": (13, "%Y-%m-%d %H"), "dia": (10, "%Y-%m-%d"), "mes": (7, "%Y-%m")}
GAP_SECONDS = 300

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

def history_groups(c, start, end, group):
    width = HISTORY_GROUPS[group][0]
    rows = c.execute(f"""SELECT substr(timestamp,1,{width}) label, COALESCE(SUM(delta_kwh),0) kwh,
        COALESCE(SUM(energy_cost_eur),0) cost, SUM(CASE WHEN pvpc_eur_mwh IS NOT NULL THEN delta_kwh ELSE 0 END) priced_kwh,
        AVG(pvpc_eur_mwh) price, AVG(act_power) avg_power, MAX(act_power) max_power, COUNT(*) readings
        FROM measurements WHERE epoch>=? AND epoch<? GROUP BY label ORDER BY label""", (start, end)).fetchall()
    out = []
    for r in rows:
        # Energy-weighted price when there was consumption; plain hourly average otherwise.
        price = r["cost"] / r["priced_kwh"] if r["priced_kwh"] else (r["price"] / 1000.0 if r["price"] is not None else None)
        label = r["label"] + ":00" if group == "hora" else r["label"]
        out.append({"label": label, "kwh": float(r["kwh"]), "cost_eur": float(r["cost"]), "avg_price_eur_kwh": price,
                    "avg_power": r["avg_power"], "max_power": r["max_power"], "readings": r["readings"]})
    return out

@app.route("/historico")
def historico():
    return render_template_string(HISTORY, zones=ZONES)

@app.route("/api/historico/rango")
def api_historico_rango():
    zone = request.args.get("zona", DEFAULT_ZONE)
    if zone not in ZONES:
        return jsonify(error="Zona desconocida"), 400
    with connect(zone) as c:
        first, last, count = c.execute("SELECT MIN(timestamp),MAX(timestamp),COUNT(*) FROM measurements").fetchone()
    return jsonify(zone=zone, first=first, last=last, count=count)

@app.route("/api/historico")
def api_historico():
    try:
        zone, start, end, group = history_range(request.args)
    except ValueError as e:
        return jsonify(error=str(e)), 400
    # About 400 points in the power chart whatever the length of the period.
    bucket = max(POLL_SECONDS, int((end - start) / 400))
    with connect(zone) as c:
        c.row_factory = sqlite3.Row
        groups = history_groups(c, start, end, group)
        t = c.execute("""SELECT COALESCE(SUM(delta_kwh),0) kwh, COALESCE(SUM(energy_cost_eur),0) cost,
            SUM(CASE WHEN pvpc_eur_mwh IS NOT NULL THEN delta_kwh ELSE 0 END) priced_kwh, AVG(act_power) avg_power,
            COUNT(*) readings, MIN(timestamp) first, MAX(timestamp) last
            FROM measurements WHERE epoch>=? AND epoch<?""", (start, end)).fetchone()
        peak = c.execute("SELECT act_power,timestamp FROM measurements WHERE epoch>=? AND epoch<? ORDER BY act_power DESC LIMIT 1", (start, end)).fetchone()
        power = c.execute("""SELECT MIN(timestamp) time, AVG(act_power) avg, MAX(act_power) max FROM measurements
            WHERE epoch>=? AND epoch<? GROUP BY CAST(epoch/? AS INTEGER) ORDER BY time""", (start, end, bucket)).fetchall()
        gaps = c.execute("""SELECT prev_ts, timestamp, epoch-prev_epoch secs, delta_kwh FROM (
              SELECT timestamp, epoch, delta_kwh, LAG(epoch) OVER (ORDER BY epoch) prev_epoch, LAG(timestamp) OVER (ORDER BY epoch) prev_ts
              FROM measurements WHERE epoch>=? AND epoch<?) WHERE epoch-prev_epoch>? ORDER BY epoch""", (start, end, GAP_SECONDS)).fetchall()
    short = bucket < 86400
    return jsonify(zone=zone, group=group, groups=groups, power_bucket_seconds=bucket,
        totals={"kwh": float(t["kwh"]), "cost_eur": float(t["cost"]),
                "avg_price_eur_kwh": t["cost"] / t["priced_kwh"] if t["priced_kwh"] else None,
                "avg_power": t["avg_power"], "readings": t["readings"], "first": t["first"], "last": t["last"],
                "max_power": peak["act_power"] if peak else None, "max_power_at": peak["timestamp"] if peak else None},
        power=[{"time": (r["time"][5:16] if short else r["time"][:10]), "avg": r["avg"], "max": r["max"]} for r in power],
        gaps=[{"from": g["prev_ts"], "to": g["timestamp"], "seconds": g["secs"], "kwh_after": g["delta_kwh"]} for g in gaps])

@app.route("/")
def home():
    return render_template_string(PAGE)

@app.route("/ajustes", methods=["GET","POST"])
def ajustes():
    cfg = load_config()
    if request.method == "POST":
        try:
            cfg["esios_token"] = request.form.get("esios_token","").strip()
            for key in ["power_kw_p1","power_kw_p2","power_price_p1_eur_kw_day","power_price_p2_eur_kw_day","meter_rent_eur_day","other_fixed_monthly_eur","electricity_tax_pct","vat_pct","extra_discount_pct"]:
                cfg[key] = float(request.form.get(key, DEFAULTS[key]))
            save_config(cfg)
            try:
                n = fetch_pvpc(datetime.now().astimezone()-timedelta(days=2), datetime.now().astimezone()+timedelta(days=2))
                flash(f"Ajustes guardados. Precios horarios cargados: {n}.")
            except Exception as e:
                flash(f"Ajustes guardados, pero no se pudieron descargar precios PVPC: {e}")
        except Exception as e:
            flash(f"No se pudieron guardar los ajustes: {e}")
        return redirect(url_for("ajustes"))
    return render_template_string(SETTINGS, cfg=cfg)

@app.route("/api/estado")
def api_estado():
    zone = request.args.get("zona", DEFAULT_ZONE)
    if zone not in ZONES:
        return jsonify(error="Zona desconocida"), 400
    now = time.time()
    localnow = datetime.now().astimezone()
    daystart = localnow.replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
    monthstart = localnow.replace(day=1,hour=0,minute=0,second=0,microsecond=0).timestamp()
    with connect(zone) as c:
        c.row_factory = sqlite3.Row
        last = c.execute("SELECT * FROM measurements ORDER BY id DESC LIMIT 1").fetchone()
        today = c.execute("SELECT COALESCE(SUM(delta_kwh),0) k,COALESCE(SUM(energy_cost_eur),0) e FROM measurements WHERE epoch>=?",(daystart,)).fetchone()
        month = c.execute("SELECT COALESCE(SUM(delta_kwh),0) k,COALESCE(SUM(energy_cost_eur),0) e FROM measurements WHERE epoch>=?",(monthstart,)).fetchone()
        recent = c.execute("SELECT timestamp,act_power,total_kwh,delta_kwh,energy_cost_eur,pvpc_eur_mwh FROM measurements ORDER BY id DESC LIMIT 20").fetchall()
        power_hist = c.execute("SELECT timestamp,act_power,pvpc_eur_mwh FROM measurements ORDER BY id DESC LIMIT 180").fetchall()
        daily_rows = c.execute("""SELECT substr(timestamp,1,10) day, COALESCE(SUM(delta_kwh),0) kwh
            FROM measurements WHERE delta_kwh>=0 GROUP BY substr(timestamp,1,10) ORDER BY day DESC LIMIT 14""").fetchall()
        hourly_rows = c.execute("""SELECT substr(timestamp,12,2) hour, COALESCE(SUM(delta_kwh),0) kwh
            FROM measurements WHERE epoch>=? AND delta_kwh>=0 GROUP BY substr(timestamp,12,2) ORDER BY hour""",(daystart,)).fetchall()
        cost_hist = c.execute("SELECT timestamp, SUM(energy_cost_eur) OVER (ORDER BY id) AS cost FROM measurements WHERE epoch>=? ORDER BY epoch",(monthstart,)).fetchall()
    # Shared PVPC table is kept in the original database.
    with connect() as c:
        count = c.execute("SELECT COUNT(*) FROM pvpc_prices").fetchone()[0]
        price_now = c.execute("SELECT price_eur_mwh FROM pvpc_prices WHERE epoch<=? ORDER BY epoch DESC LIMIT 1",(int(now),)).fetchone()
    cfg = load_config()
    days_elapsed = localnow.day
    power_cost = (cfg["power_kw_p1"]*cfg["power_price_p1_eur_kw_day"] + cfg["power_kw_p2"]*cfg["power_price_p2_eur_kw_day"]) * days_elapsed
    meter_cost = cfg["meter_rent_eur_day"] * days_elapsed
    month_days = (localnow.replace(day=28)+timedelta(days=4)).replace(day=1).day
    other_fixed = cfg["other_fixed_monthly_eur"] * days_elapsed / month_days
    energy_base = float(month["e"])
    discount = energy_base * cfg["extra_discount_pct"] / 100.0
    taxable = max(0,energy_base-discount) + power_cost + meter_cost + other_fixed
    elec_tax = taxable * cfg["electricity_tax_pct"] / 100.0
    vat = (taxable + elec_tax) * cfg["vat_pct"] / 100.0
    bill = taxable + elec_tax + vat
    latest = dict(last) if last else None
    return jsonify(ok=bool(last),zone=zone,zone_name=ZONES[zone]["name"],ip=ZONES[zone]["ip"],error=ZONE_ERRORS.get(zone, ""),latest=latest,
        total_kwh=latest["total_kwh"] if latest else 0, returned_kwh=latest["returned_kwh"] if latest else 0,
        today_kwh=float(today["k"]),today_energy_eur=float(today["e"]),month_kwh=float(month["k"]),
        month_energy_eur=energy_base,price_now=float(price_now[0]) if price_now else None,
        price_count=int(count),power_cost=power_cost,meter_cost=meter_cost,other_fixed=other_fixed,discount=discount,
        taxable_base=taxable,electricity_tax=elec_tax,vat=vat,month_total_estimate=bill,
        cost_history=[{"time":r["timestamp"][-8:-3],"cost":r["cost"]} for r in cost_hist[::max(1,len(cost_hist)//200)]],
        power_pvpc_history=[{"time":r["timestamp"][-8:-3],"power":r["act_power"],"price":r["pvpc_eur_mwh"]} for r in reversed(power_hist)],
        daily_consumption=[{"day":r["day"],"kwh":float(r["kwh"])} for r in reversed(daily_rows)],
        hourly_consumption=[{"hour":r["hour"]+":00","kwh":float(r["kwh"])} for r in hourly_rows],
        recent=[dict(r) for r in reversed(recent)])

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
    out=io.StringIO(); w=csv.writer(out)
    with connect(zone) as c:
        if group:
            c.row_factory = sqlite3.Row
            w.writerow(["periodo","consumo_kWh","coste_energia_EUR","precio_medio_EUR_kWh","potencia_media_W","potencia_max_W","lecturas"])
            w.writerows([[g["label"],g["kwh"],g["cost_eur"],g["avg_price_eur_kwh"],g["avg_power"],g["max_power"],g["readings"]] for g in history_groups(c, start, end, group)])
            suffix += f"_por_{group}"
        else:
            rows=c.execute("SELECT timestamp,voltage,current,act_power,aprt_power,pf,freq,total_kwh,returned_kwh,delta_kwh,pvpc_eur_mwh,energy_cost_eur FROM measurements WHERE epoch>=? AND epoch<? ORDER BY id",(start,end)).fetchall()
            w.writerow(["fecha_hora","tension_V","corriente_A","potencia_W","potencia_aparente_VA","factor_potencia","frecuencia_Hz","energia_consumida_total_kWh","energia_devuelta_total_kWh","delta_consumo_kWh","PVPC_EUR_MWh","coste_energia_EUR"])
            w.writerows(rows)
    return Response("\ufeff"+out.getvalue(),mimetype="text/csv",headers={"Content-Disposition":f"attachment; filename=shelly_pvpc_historico_{zone}{suffix}.csv"})

if __name__=="__main__":
    init_db()
    for zone in ZONES:
        if zone != DEFAULT_ZONE:
            init_db_for_zone(zone)
        threading.Thread(target=poller, args=(zone,), daemon=True).start()
    threading.Thread(target=price_refresh_loop,daemon=True).start()
    print(f"Web local: http://127.0.0.1:{PORT}")
    print(f"Web en la red: http://<IP-DE-ESTE-ORDENADOR>:{PORT}")
    print("Zonas: " + ", ".join(f"{z}={d['ip']} ({d['name']})" for z,d in ZONES.items()))
    app.run(host="0.0.0.0",port=PORT,debug=False)
