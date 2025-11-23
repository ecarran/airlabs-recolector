import requests
import sqlite3
from datetime import datetime
from dateutil import parser 
import pytz 
import os
from flask import Flask, send_file

# ==================================
# CONSTANTES
# ==================================
API_KEY = os.environ.get("AIRLABS_API_KEY", "90e110b8-cdc9-4e81-886d-b2dfa3112feb") # USAR VARIABLE DE ENTORNO
AIRPORT_IATA = "MAD"
DB_FILE = "barajas.db"
MADRID_TZ = pytz.timezone('Europe/Madrid')

# Inicializar Flask
app = Flask(__name__)

# ==================================
# LÓGICA DE AIRLABS (MISMO CÓDIGO)
# ==================================

def airlabs_request(endpoint, params):
    """Realiza una petición a la API de Airlabs con manejo de errores HTTP."""
    url = f"https://airlabs.co/api/v9/{endpoint}"
    params = dict(params)
    params["api_key"] = API_KEY
    # ... (el resto de la función airlabs_request, get_all_landed, get_all_departed, calculate_delay)
    # Lo he omitido aquí por brevedad, pero debe ir en este archivo 'app.py'
    # Copia TODAS las funciones de 'recolector.py' aquí.

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
# GUARDADO DE LLEGADAS/DESPEGUES (Mismo código, sólo se añade la creación de DB)
# ==================================

def save_arrivals(records):
    # ... (Copia aquí la función save_arrivals COMPLETA del código v4.0 FINAL)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS arrivals (
            timestamp TEXT, flight_iata TEXT, airline_iata TEXT, dep_iata TEXT,
            arr_iata TEXT, arr_sch_time TEXT, arr_time TEXT, status TEXT,
            delay_minutes INTEGER, PRIMARY KEY (flight_iata, arr_time) 
        )
    """)
    # ... (resto de la función save_arrivals)
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
            cur.execute("INSERT OR IGNORE INTO arrivals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                timestamp_recolection, flight_iata, r.get("airline_iata"), r.get("dep_iata"),
                r.get("arr_iata"), arr_sch_time, arr_time, r.get("status"), delay))
        except Exception as e:
            print(f"Error al insertar llegada {flight_iata}: {e}")
    conn.commit()
    rows_inserted = conn.total_changes - initial_changes
    conn.close()
    return f"Registros nuevos insertados en arrivals: {rows_inserted}"

def save_departures(records):
    # ... (Copia aquí la función save_departures COMPLETA del código v4.0 FINAL)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS departures (
            timestamp TEXT, flight_iata TEXT, airline_iata TEXT, dep_iata TEXT,
            arr_iata TEXT, dep_sch_time TEXT, dep_time TEXT, status TEXT,
            delay_minutes INTEGER, PRIMARY KEY (flight_iata, dep_time)
        )
    """)
    # ... (resto de la función save_departures)
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
            cur.execute("INSERT OR IGNORE INTO departures VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                timestamp_recolection, flight_iata, r.get("airline_iata"), r.get("dep_iata"),
                r.get("arr_iata"), dep_sch_time, dep_time, r.get("status"), delay))
        except Exception as e:
            print(f"Error al insertar salida {flight_iata}: {e}")
    conn.commit()
    rows_inserted = conn.total_changes - initial_changes
    conn.close()
    return f"Registros nuevos insertados en departures: {rows_inserted}"


# ==================================
# ENDPOINTS
# ==================================

@app.route('/recolectar', methods=['GET'])
def recolectar_data():
    """
    Endpoint que ejecuta la lógica completa de recolección de AirLabs.
    """
    print("--- INICIANDO TAREA DE RECOLECCIÓN ---")
    
    # 1. COLECCIÓN DE LLEGADAS
    try:
        all_landed = get_all_landed()
        res_arrivals = save_arrivals(all_landed) if all_landed else "Lista de llegadas vacía."
    except RuntimeError as e:
        res_arrivals = f"ERROR LLEGADAS: {e}"

    # 2. COLECCIÓN DE DESPEGUES
    try:
        all_departed = get_all_departed()
        res_departures = save_departures(all_departed) if all_departed else "Lista de despegues vacía."
    except RuntimeError as e:
        res_departures = f"ERROR DESPEGUES: {e}"

    # Respuesta consolidada
    response_msg = (
        f"Tarea de recolección completada: "
        f"LLEGADAS: {res_arrivals}. "
        f"DESPEGUES: {res_departures}."
    )
    print(response_msg)
    return response_msg, 200


@app.route('/descargarDB', methods=['GET'])
def descargar_db():
    """
    Endpoint para descargar el archivo de base de datos SQLite.
    """
    if os.path.exists(DB_FILE):
        return send_file(DB_FILE, as_attachment=True)
    else:
        return f"Error: El archivo {DB_FILE} no existe.", 404

if __name__ == '__main__':
    # Esto solo es para pruebas locales, Render usará Gunicorn
    app.run(host='0.0.0.0', port=5000)