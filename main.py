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
# Usamos una variable de entorno para la clave API (MEJOR PRÁCTICA DE SEGURIDAD)
API_KEY = os.getenv("AIRLABS_API_KEY", "TU_CLAVE_AQUI") 
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

    print(f"Haciendo petición a {url}...")
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status() 
        data = r.json()

        if "error" in data:
            raise RuntimeError(f"Error de API: {data['error']}")
        
        return data["response"]
    
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error en la petición HTTP: {e}")

def get_all_landed():
    """Obtiene los últimos 100 vuelos aterrizados en MAD."""
    return airlabs_request(
        "schedules",
        {"arr_iata": AIRPORT_IATA, "status": "landed"}
    )

def get_all_departed():
    """Obtiene los últimos 100 vuelos despegados de MAD."""
    return airlabs_request(
        "schedules",
        {"dep_iata": AIRPORT_IATA, "status": "departed"}
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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS arrivals (
            timestamp TEXT, flight_iata TEXT, airline_iata TEXT, dep_iata TEXT,
            arr_iata TEXT, arr_sch_time TEXT, arr_time TEXT, status TEXT,
            delay_minutes INTEGER, PRIMARY KEY (flight_iata, arr_time) 
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
            
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO arrivals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                timestamp_recolection, flight_iata, r.get("airline_iata"), r.get("dep_iata"),
                r.get("arr_iata"), arr_sch_time, arr_time, r.get("status"), delay))
        except Exception as e:
            print(f"Error al insertar llegada {flight_iata}: {e}")

    conn.commit()
    rows_inserted = conn.total_changes - initial_changes
    conn.close()
    return rows_inserted

def save_departures(records):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS departures (
            timestamp TEXT, flight_iata TEXT, airline_iata TEXT, dep_iata TEXT,
            arr_iata TEXT, dep_sch_time TEXT, dep_time TEXT, status TEXT,
            delay_minutes INTEGER, PRIMARY KEY (flight_iata, dep_time)
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
            
        if not flight_iata or not dep_time:
            continue 
            
        delay = calculate_delay(dep_time, dep_sch_time)
            
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO departures VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                timestamp_recolection, flight_iata, r.get("airline_iata"), r.get("dep_iata"),
                r.get("arr_iata"), dep_sch_time, dep_time, r.get("status"), delay))
        except Exception as e:
            print(f"Error al insertar despegue {flight_iata}: {e}")

    conn.commit()
    rows_inserted = conn.total_changes - initial_changes
    conn.close()
    return rows_inserted

# ==================================
# ENDPOINTS
# ==================================

@app.get("/recolectar")
def recolectar():
    """Ejecuta la recolección de datos y los guarda en barajas.db."""
    
    # 1. COLECCIÓN DE LLEGADAS
    try:
        all_landed = get_all_landed()
        inserted_arrivals = save_arrivals(all_landed) if all_landed else 0
    except RuntimeError as e:
        return JSONResponse(content={"error": f"Error en recolección de llegadas: {e}"}, status_code=500)

    # 2. COLECCIÓN DE DESPEGUES
    try:
        all_departed = get_all_departed()
        inserted_departures = save_departures(all_departed) if all_departed else 0
    except RuntimeError as e:
        return JSONResponse(content={"error": f"Error en recolección de despegues: {e}"}, status_code=500)
    
    return {
        "mensaje": "Recolección completada con éxito.",
        "nuevos_registros_llegadas": inserted_arrivals,
        "nuevos_registros_despegues": inserted_departures
    }

@app.get("/descargarDB")
def descargar_db():
    """Permite descargar el archivo de base de datos SQLite."""
    if os.path.exists(DB_PATH):
        # Usamos FileResponse de FastAPI para servir el archivo
        return FileResponse(DB_PATH, filename="barajas.db", media_type="application/octet-stream")
    else:
        return JSONResponse(content={"error": "Base de datos no encontrada"}, status_code=404)