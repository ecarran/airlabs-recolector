import os
import requests
import sqlite3
from fastapi import FastAPI
from fastapi.responses import JSONResponse, FileResponse
from datetime import datetime
from dateutil import parser 
import pytz 

# ==================================
# CONSTANTES
# ==================================
# Se lee la clave de la variable de entorno AIRLABS_API_KEY.
API_KEY = os.getenv("AIRLABS_API_KEY", "TU_CLAVE_DE_AIRLABS_AQUI") 
AIRPORT_IATA = "MAD"
DB_PATH = "barajas.db"
MADRID_TZ = pytz.timezone('Europe/Madrid')

app = FastAPI()

# ==================================
# LÓGICA DE AIRLABS (Funciones Puras)
# ==================================

def airlabs_request(endpoint, params):
    """Realiza una petición a la API de Airlabs con manejo de errores HTTP."""
    url = f"https://airlabs.co/api/v9/{endpoint}"
    params = dict(params)
    params["api_key"] = API_KEY

    print(f"Haciendo petición a {url} con status={params.get('status')}...")
    try:
        if API_KEY == "TU_CLAVE_DE_AIRLABS_AQUI":
             raise RuntimeError("API Key no configurada. Por favor, define AIRLABS_API_KEY en Render.")
             
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status() 
        data = r.json()

        if "error" in data:
            raise RuntimeError(f"Error de API: {data['error']}")
        
        response = data.get("response")
        if not response:
             print(f"  ⚠ La API devolvió una lista de vuelos vacía para status: {params.get('status')}.")
        
        return response
    
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error en la petición HTTP: {e}")

def get_all_landed():
    """Obtiene los últimos 100 vuelos aterrizados en MAD (1 llamada)."""
    return airlabs_request(
        "schedules",
        {"arr_iata": AIRPORT_IATA, "status": "landed"}
    )

def get_all_active_departures():
    """Obtiene vuelos activos saliendo de MAD (1 llamada)."""
    return airlabs_request(
        "schedules",
        {"dep_iata": AIRPORT_IATA, "status": "active"}
    )

def calculate_delay(actual_time_str, scheduled_time_str):
    """Calcula la diferencia en minutos entre el tiempo real y el programado/estimado."""
    if not actual_time_str or not scheduled_time_str:
        return None
    
    try:
        actual_dt = parser.parse(actual_time_str).replace(tzinfo=None)
        scheduled_dt = parser.parse(scheduled_time_str).replace(tzinfo=None)
        delay_seconds = (actual_dt - scheduled_dt).total_seconds()
        return int(delay_seconds / 60)
    except Exception:
        return None

# ==================================
# GUARDADO DE DATOS (Recolección y guardado)
# ==================================

def save_arrivals(records):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # DDL: Se añade el campo aircraft_icao (modelo del avión)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS arrivals (
            timestamp TEXT, flight_iata TEXT, airline_iata TEXT, dep_iata TEXT,
            arr_iata TEXT, arr_sch_time TEXT, arr_time TEXT, status TEXT,
            delay_minutes INTEGER, 
            arr_terminal TEXT,       
            arr_gate TEXT,           
            arr_baggage TEXT,        
            duration INTEGER,        
            dep_delayed INTEGER,     
            arr_delayed INTEGER,     
            aircraft_icao TEXT,      -- ¡NUEVO CAMPO!
            PRIMARY KEY (flight_iata, arr_time) 
        )
    """)
    
    timestamp_recolection = datetime.now(MADRID_TZ).strftime("%Y-%m-%d %H:%M:%S")
    initial_changes = conn.total_changes
    
    for r in records:
        flight_iata = r.get("flight_iata")
        arr_time = r.get("arr_time") 
        arr_sch_time = r.get("arr_time_sch")
        if not arr_sch_time:
            arr_sch_time = r.get("arr_estimated")
        
        if not flight_iata or not arr_time:
            continue
            
        delay = calculate_delay(arr_time, arr_sch_time)
        
        # Extracción de todos los datos complementarios
        arr_terminal = r.get("arr_terminal")
        arr_gate = r.get("arr_gate")
        arr_baggage = r.get("arr_baggage")
        duration = r.get("duration")
        dep_delayed = r.get("dep_delayed")
        arr_delayed = r.get("arr_delayed")
        aircraft_icao = r.get("aircraft_icao") # ¡Extracción del nuevo campo!

            
        try:
            # DML: Se insertan 16 valores (9 originales + 7 nuevos)
            cursor.execute("""
                INSERT OR IGNORE INTO arrivals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                timestamp_recolection, flight_iata, r.get("airline_iata"), r.get("dep_iata"),
                r.get("arr_iata"), arr_sch_time, arr_time, r.get("status"), delay,
                arr_terminal, arr_gate, arr_baggage, duration, dep_delayed, arr_delayed, 
                aircraft_icao # ¡Inserción del nuevo campo!
            ))
        except Exception as e:
            print(f"Error al insertar llegada {flight_iata}: {e}")

    conn.commit()
    rows_inserted = conn.total_changes - initial_changes
    conn.close()
    return rows_inserted

def save_departures(records):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # DDL: Se añade el campo aircraft_icao (modelo del avión)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS departures (
            timestamp TEXT, flight_iata TEXT, airline_iata TEXT, dep_iata TEXT,
            arr_iata TEXT, dep_sch_time TEXT, dep_time TEXT, status TEXT,
            delay_minutes INTEGER,
            dep_terminal TEXT,       
            dep_gate TEXT,           
            duration INTEGER,        
            dep_delayed INTEGER,     
            arr_delayed INTEGER,     
            aircraft_icao TEXT,      -- ¡NUEVO CAMPO!
            PRIMARY KEY (flight_iata, dep_sch_time)
        )
    """)

    timestamp_recolection = datetime.now(MADRID_TZ).strftime("%Y-%m-%d %H:%M:%S")
    initial_changes = conn.total_changes
    
    for r in records:
        flight_iata = r.get("flight_iata")
        dep_time = r.get("dep_time") 
        dep_sch_time = r.get("dep_time_sch")
        if not dep_sch_time:
            dep_sch_time = r.get("dep_estimated")
            
        if not flight_iata or not dep_sch_time:
            continue 
            
        delay = calculate_delay(dep_time, dep_sch_time)
        
        # Extracción de todos los datos complementarios
        dep_terminal = r.get("dep_terminal")
        dep_gate = r.get("dep_gate")
        duration = r.get("duration")
        dep_delayed = r.get("dep_delayed")
        arr_delayed = r.get("arr_delayed")
        aircraft_icao = r.get("aircraft_icao") # ¡Extracción del nuevo campo!
            
        try:
            # DML: Se insertan 15 valores (9 originales + 6 complementarios)
            cursor.execute("""
                INSERT OR IGNORE INTO departures VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                timestamp_recolection, flight_iata, r.get("airline_iata"), r.get("dep_iata"),
                r.get("arr_iata"), dep_sch_time, dep_time, r.get("status"), delay,
                dep_terminal, dep_gate, duration, dep_delayed, arr_delayed,
                aircraft_icao # ¡Inserción del nuevo campo!
            ))
        except Exception as e:
            print(f"Error al insertar despegue/activo {flight_iata}: {e}")

    conn.commit()
    rows_inserted = conn.total_changes - initial_changes
    conn.close()
    return rows_inserted

# ==================================
# ENDPOINTS
# ==================================

@app.get("/")
def home():
    """Página de inicio básica."""
    return {"message": "Recolector de Vuelos de Barajas activo. Use /recolectar o /descargarDB."}


@app.get("/ping")
def ping_service():
    """Endpoint simple para mantener el servicio activo."""
    now = datetime.now(MADRID_TZ).strftime("%Y-%m-%d %H:%M:%S")
    return JSONResponse(content={"status": "alive", "timestamp_madrid": now, "message": "Service is awake."}, status_code=200)


@app.get("/recolectar")
def recolectar():
    """
    Ejecuta la recolección de datos y los guarda en barajas.db.
    Solo llama a 'landed' y 'active' (2 llamadas a API).
    """
    total_inserted = 0
    results = {}
    
    # 1. COLECCIÓN DE LLEGADAS (landed)
    try:
        all_landed = get_all_landed()
        inserted_arrivals = save_arrivals(all_landed) if all_landed else 0
        results["nuevos_registros_llegadas"] = inserted_arrivals
        total_inserted += inserted_arrivals
    except RuntimeError as e:
        results["error_llegadas"] = f"Error en recolección de llegadas: {e}"

    # 2. COLECCIÓN DE SALIDAS ACTIVAS (active)
    try:
        all_active = get_all_active_departures()
        inserted_departures = save_departures(all_active) if all_active else 0
        results["nuevos_registros_salidas_activas"] = inserted_departures
        total_inserted += inserted_departures
    except RuntimeError as e:
        results["error_salidas_activas"] = f"Error en recolección de salidas activas: {e}"
    
    if total_inserted > 0:
        results["mensaje"] = f"Recolección completada con éxito. Total de nuevos registros: {total_inserted}."
        status_code = 200
    else:
        results["mensaje"] = "Recolección completada. No se insertaron registros nuevos."
        status_code = 500 if "error_llegadas" in results or "error_salidas_activas" in results else 200
        
    return JSONResponse(content=results, status_code=status_code)

@app.get("/descargarDB")
def descargar_db():
    """Permite descargar el archivo de base de datos SQLite."""
    if os.path.exists(DB_PATH):
        return FileResponse(DB_PATH, filename="barajas.db", media_type="application/octet-stream")
    else:
        return JSONResponse(content={"error": "Base de datos no encontrada"}, status_code=404)
