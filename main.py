import os
import requests
import sqlite3
from fastapi import FastAPI
from fastapi.responses import JSONResponse, FileResponse
from datetime import datetime
from dateutil import parser 
import pytz 

# ==================================
# CONFIGURACIÓN
# ==================================
API_KEY = os.getenv("AIRLABS_API_KEY", "TU_CLAVE_DE_AIRLABS_AQUI") 
AIRPORT_IATA = "MAD"
DB_PATH = "barajas.db"
MADRID_TZ = pytz.timezone('Europe/Madrid')

# --- CONFIGURACIÓN DE AHORRO (NIGHT SAVER) ---
# El script solo consumirá llamadas a la API en este rango horario (Hora Madrid).
# Rango: 06:00 AM hasta 23:59 PM.
# Las horas de madrugada (00:00 - 06:00) se ignoran para ahorrar cuota, 
# confiando en que la llamada de las 06:00 recuperará los pocos vuelos nocturnos.
START_HOUR = 6  
END_HOUR = 24   

app = FastAPI()

# ==================================
# UTILIDADES
# ==================================
def is_operating_hour():
    """
    Devuelve True si estamos en horario operativo para gastar API.
    Funciona para rangos dentro del mismo día (ej: 06 a 24).
    """
    current_hour = datetime.now(MADRID_TZ).hour
    # Nota: END_HOUR=24 permite que funcione hasta las 23:59
    return START_HOUR <= current_hour < END_HOUR

def convert_to_madrid(utc_time_str):
    """Convierte UTC string a Madrid string para guardar hora local."""
    if not utc_time_str: return None
    try:
        dt_utc = parser.parse(utc_time_str).replace(tzinfo=pytz.utc)
        return dt_utc.astimezone(MADRID_TZ).strftime("%Y-%m-%d %H:%M")
    except: return utc_time_str

def calculate_delay(actual, scheduled):
    """Calcula la diferencia en minutos entre hora real y programada."""
    if not actual or not scheduled: return None
    try:
        act_dt = parser.parse(actual).replace(tzinfo=None)
        sch_dt = parser.parse(scheduled).replace(tzinfo=None)
        return int((act_dt - sch_dt).total_seconds() / 60)
    except: return None

# ==================================
# API AIRLABS (CON PROTECCIÓN DE HORARIO)
# ==================================
def airlabs_request(endpoint, params):
    # 1. VERIFICACIÓN DE HORARIO (Ahorro de API)
    if not is_operating_hour():
        print(f"💤 Modo Noche ({datetime.now(MADRID_TZ).strftime('%H:%M')}). Ahorrando llamada.")
        return [] # Retorna lista vacía sin tocar la API

    # 2. PETICIÓN REAL
    url = f"https://airlabs.co/api/v9/{endpoint}"
    params = dict(params)
    params["api_key"] = API_KEY
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status() 
        data = r.json()
        if "error" in data:
            print(f"Error API: {data['error']}")
            return []
        return data.get("response", [])
    except Exception as e:
        print(f"Error Conexión: {e}")
        return []

def get_all_landed():
    return airlabs_request("schedules", {"arr_iata": AIRPORT_IATA, "status": "landed"})

def get_all_active_departures():
    return airlabs_request("schedules", {"dep_iata": AIRPORT_IATA, "status": "active"})

# ==================================
# GUARDADO EN BASE DE DATOS
# ==================================
def save_arrivals(records):
    if not records: return 0
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Tabla Arrivals
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS arrivals (
            timestamp TEXT, flight_iata TEXT, airline_iata TEXT, dep_iata TEXT,
            arr_iata TEXT, arr_sch_time TEXT, arr_time TEXT, status TEXT,
            delay_minutes INTEGER, arr_terminal TEXT, arr_gate TEXT, arr_baggage TEXT,        
            duration INTEGER, dep_delayed INTEGER, arr_delayed INTEGER, aircraft_icao TEXT,
            PRIMARY KEY (flight_iata, arr_time) 
        )
    """)
    
    ts = datetime.now(MADRID_TZ).strftime("%Y-%m-%d %H:%M:%S")
    initial = conn.total_changes
    
    for r in records:
        flight = r.get("flight_iata")
        # Lógica de tiempos
        raw_sch = r.get("arr_time_sch")
        raw_time = r.get("arr_estimated") or r.get("arr_actual") or r.get("arr_time")
        
        # Fallback si falta la programada
        if not raw_sch:
             fallback = r.get("arr_time")
             if fallback and fallback != raw_time: raw_sch = fallback

        if not flight or not raw_time: continue

        # Conversión y cálculo
        sch = convert_to_madrid(raw_sch)
        real = convert_to_madrid(raw_time)
        delay = calculate_delay(real, sch)
        
        try:
            cursor.execute("INSERT OR IGNORE INTO arrivals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                (ts, flight, r.get("airline_iata"), r.get("dep_iata"), r.get("arr_iata"), 
                 sch, real, r.get("status"), delay, r.get("arr_terminal"), r.get("arr_gate"), 
                 r.get("arr_baggage"), r.get("duration"), r.get("dep_delayed"), 
                 r.get("arr_delayed"), r.get("aircraft_icao")))
        except: pass

    conn.commit()
    inserted = conn.total_changes - initial
    conn.close()
    return inserted

def save_departures(records):
    if not records: return 0
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Tabla Departures
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS departures (
            timestamp TEXT, flight_iata TEXT, airline_iata TEXT, dep_iata TEXT,
            arr_iata TEXT, dep_sch_time TEXT, dep_time TEXT, status TEXT,
            delay_minutes INTEGER, dep_terminal TEXT, dep_gate TEXT, duration INTEGER, 
            dep_delayed INTEGER, arr_delayed INTEGER, aircraft_icao TEXT,
            PRIMARY KEY (flight_iata, dep_sch_time)
        )
    """)
    
    ts = datetime.now(MADRID_TZ).strftime("%Y-%m-%d %H:%M:%S")
    initial = conn.total_changes
    
    for r in records:
        flight = r.get("flight_iata")
        # Lógica de tiempos
        raw_sch = r.get("dep_time_sch")
        raw_time = r.get("dep_estimated") or r.get("dep_actual") or r.get("dep_time")

        if not raw_sch:
            fallback = r.get("dep_time")
            if fallback and fallback != raw_time: raw_sch = fallback

        if not flight or not raw_sch: continue 
        
        # Conversión y cálculo
        sch = convert_to_madrid(raw_sch)
        real = convert_to_madrid(raw_time)
        delay = calculate_delay(real, sch)
            
        try:
            cursor.execute("INSERT OR IGNORE INTO departures VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                (ts, flight, r.get("airline_iata"), r.get("dep_iata"), r.get("arr_iata"), 
                 sch, real, r.get("status"), delay, r.get("dep_terminal"), r.get("dep_gate"), 
                 r.get("duration"), r.get("dep_delayed"), r.get("arr_delayed"), r.get("aircraft_icao")))
        except: pass

    conn.commit()
    inserted = conn.total_changes - initial
    conn.close()
    return inserted

# ==================================
# ENDPOINTS
# ==================================

@app.get("/")
def home():
    return {"msg": "Recolector Barajas V7 (Night Saver: 06h-24h)"}

@app.get("/ping")
def ping_service():
    """Endpoint ligero para despertar el servidor sin gastar API externa."""
    now = datetime.now(MADRID_TZ).strftime("%Y-%m-%d %H:%M:%S")
    return JSONResponse(content={"status": "alive", "time": now}, status_code=200)

@app.get("/recolectar")
def recolectar():
    """
    Ejecuta la recolección SOLO si estamos en horario operativo.
    Devuelve siempre 200 OK para no alertar al Cron, incluso si no hizo nada.
    """
    res = {}
    
    # Intentamos ejecutar (las funciones internas chequean el horario)
    try:
        landed = get_all_landed()
        res["llegadas"] = save_arrivals(landed)
    except Exception as e: res["err_arr"] = str(e)

    try:
        active = get_all_active_departures()
        res["salidas"] = save_departures(active)
    except Exception as e: res["err_dep"] = str(e)
    
    # Información extra para debug
    if not is_operating_hour():
        res["status"] = "NIGHT_MODE"
        res["info"] = "Fuera de horario (06-24). No se hicieron llamadas a Airlabs."
    else:
        res["status"] = "ACTIVE"
        
    return JSONResponse(res)

@app.get("/descargarDB")
def descargar_db():
    if os.path.exists(DB_PATH):
        return FileResponse(DB_PATH, filename="barajas.db", media_type="application/octet-stream")
    else:
        return JSONResponse(content={"error": "Base de datos no encontrada"}, status_code=404)
