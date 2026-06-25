
"""
API de Tasa de Cambio del BCV (versión producción)
==================================================
Lee las tasas oficiales del Banco Central de Venezuela, las guarda en una
base de datos PostgreSQL (histórico) y las expone mediante una API REST con
CORS habilitado.

Variables de entorno necesarias:
    DATABASE_URL   -> cadena de conexión de PostgreSQL (la da Render).
    CORS_ORIGINS   -> orígenes permitidos, separados por coma.
                      Ej: "https://miapp.com,https://www.miapp.com"
                      Si no se define, se permite cualquier origen ("*").

Cómo ejecutar localmente:
    pip install -r requirements.txt
    export DATABASE_URL="postgresql://usuario:clave@localhost:5432/bcv"
    uvicorn main:app --reload

En Render (Start Command):
    uvicorn main:app --host 0.0.0.0 --port $PORT

Documentación interactiva: /docs
"""

import os
import logging
import warnings
from contextlib import contextmanager, asynccontextmanager
from datetime import date, datetime

import httpx
import urllib3
import psycopg2
import psycopg2.extras
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# El sitio del BCV tiene una cadena de certificado SSL incompleta, por eso
# desactivamos la verificación. Silenciamos también la advertencia asociada.
warnings.simplefilter("ignore", urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bcv_api")

DATABASE_URL = os.environ.get("DATABASE_URL")
URL_BCV = "https://www.bcv.org.ve/"

# Mapeo: código de moneda -> id del <div> en la página del BCV
MONEDAS = {
    "usd": "dolar",
    "eur": "euro",
    "cny": "yuan",
    "try": "lira",
    "rub": "rublo",
}


# ---------------------------------------------------------------------------
# Base de datos (PostgreSQL)
# ---------------------------------------------------------------------------
@contextmanager
def get_conn():
    """Abre una conexión a PostgreSQL, hace commit/rollback y la cierra."""
    if not DATABASE_URL:
        raise RuntimeError("La variable de entorno DATABASE_URL no está definida.")
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def get_cursor():
    """Cursor que devuelve filas como diccionarios."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur


def init_db():
    """Crea la tabla de tasas si no existe."""
    with get_cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tasas (
                id           SERIAL PRIMARY KEY,
                fecha        DATE NOT NULL,            -- fecha valor
                moneda       TEXT NOT NULL,            -- usd, eur, cny, ...
                valor        NUMERIC(18, 8) NOT NULL,  -- tasa en bolívares
                capturado_en TIMESTAMP NOT NULL,       -- timestamp de captura
                UNIQUE(fecha, moneda)                  -- 1 valor por moneda/día
            )
            """
        )
    log.info("Base de datos PostgreSQL lista.")


def guardar_tasas(fecha: str, tasas: dict[str, float]):
    """Inserta o actualiza las tasas de un día (UPSERT por fecha + moneda)."""
    capturado = datetime.now()
    with get_cursor() as cur:
        for moneda, valor in tasas.items():
            cur.execute(
                """
                INSERT INTO tasas (fecha, moneda, valor, capturado_en)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (fecha, moneda)
                DO UPDATE SET valor = EXCLUDED.valor,
                              capturado_en = EXCLUDED.capturado_en
                """,
                (fecha, moneda, valor, capturado),
            )
    log.info("Guardadas %d tasas para la fecha %s", len(tasas), fecha)


# ---------------------------------------------------------------------------
# Scraping del BCV
# ---------------------------------------------------------------------------
def _parse_decimal(texto: str) -> float:
    """Convierte el formato venezolano '36.123,50' -> 36123.50."""
    limpio = texto.strip().replace(".", "").replace(",", ".")
    return float(limpio)


def obtener_tasas_bcv() -> tuple[str, dict[str, float]]:
    """
    Descarga la página del BCV y extrae las tasas.
    Devuelve (fecha_valor, {moneda: tasa}).
    Lanza una excepción si no logra obtener ninguna tasa.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )
    }
    resp = httpx.get(URL_BCV, headers=headers, verify=False, timeout=30.0)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    tasas: dict[str, float] = {}
    for codigo, div_id in MONEDAS.items():
        div = soup.find(id=div_id)
        if not div:
            continue
        strong = div.find("strong")
        if strong and strong.text.strip():
            try:
                tasas[codigo] = _parse_decimal(strong.text)
            except ValueError:
                log.warning("No se pudo parsear la tasa de %s: %r", codigo, strong.text)

    if not tasas:
        raise RuntimeError("No se encontró ninguna tasa en la página del BCV.")

    # Fecha valor publicada por el BCV (si está disponible)
    fecha_span = soup.find("span", class_="date-display-single")
    if fecha_span and fecha_span.get("content"):
        fecha_valor = fecha_span["content"][:10]
    else:
        fecha_valor = date.today().isoformat()

    return fecha_valor, tasas


def actualizar_tasas() -> dict:
    """Obtiene las tasas del BCV y las guarda. Usada por el scheduler y el endpoint manual."""
    log.info("Consultando tasas del BCV...")
    try:
        fecha, tasas = obtener_tasas_bcv()
        guardar_tasas(fecha, tasas)
        return {"fecha": fecha, "tasas": tasas}
    except Exception as exc:  # noqa: BLE001
        # No tumbamos el servicio: registramos el error y conservamos el histórico.
        log.error("Fallo al actualizar tasas: %s", exc)
        raise


# ---------------------------------------------------------------------------
# Programador de tareas (1 actualización diaria)
# ---------------------------------------------------------------------------
scheduler = BackgroundScheduler(timezone="America/Caracas")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Al arrancar: inicializa la BD, intenta una primera carga y programa la tarea diaria."""
    init_db()
    try:
        actualizar_tasas()
    except Exception:
        log.warning("No se pudo cargar la tasa inicial. Se reintentará en el horario programado.")

    # NOTA: en planes gratuitos que "duermen" el servicio, este scheduler puede no
    # ejecutarse. Para fiabilidad, configura un cron externo (ej. cron-job.org) que
    # llame al endpoint POST /actualizar una vez al día.
    scheduler.add_job(
        actualizar_tasas,
        CronTrigger(hour=17, minute=0),
        id="actualizacion_diaria",
        replace_existing=True,
    )
    scheduler.start()
    log.info("Scheduler iniciado: actualización diaria a las 17:00 (America/Caracas).")

    yield  # --- la aplicación corre aquí ---

    scheduler.shutdown()
    log.info("Scheduler detenido.")


# ---------------------------------------------------------------------------
# Aplicación FastAPI
# ---------------------------------------------------------------------------
app = FastAPI(
    title="API Tasa de Cambio BCV",
    description="Tasas oficiales del Banco Central de Venezuela con histórico.",
    version="2.0.0",
    lifespan=lifespan,
)

# --- CORS -----------------------------------------------------------------
# Define CORS_ORIGINS con los dominios de tu app separados por coma.
# Si no se define, se permite cualquier origen (útil para pruebas).
_origins_env = os.environ.get("CORS_ORIGINS", "*").strip()
if _origins_env == "*":
    _allow_origins = ["*"]
    _allow_credentials = False  # "*" y credenciales no pueden combinarse.
else:
    _allow_origins = [o.strip() for o in _origins_env.split(",") if o.strip()]
    _allow_credentials = True

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allow_origins,
    allow_credentials=_allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/salud", tags=["Sistema"])
def salud():
    """Verifica que el servicio esté activo."""
    return {"estado": "ok", "hora": datetime.now().isoformat(timespec="seconds")}


@app.get("/tasas", tags=["Tasas"])
def tasas_actuales():
    """Devuelve la última tasa registrada de cada moneda."""
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT t.moneda, t.valor, t.fecha, t.capturado_en
            FROM tasas t
            INNER JOIN (
                SELECT moneda, MAX(fecha) AS max_fecha
                FROM tasas GROUP BY moneda
            ) ult ON t.moneda = ult.moneda AND t.fecha = ult.max_fecha
            ORDER BY t.moneda
            """
        )
        filas = cur.fetchall()

    if not filas:
        raise HTTPException(status_code=404, detail="Aún no hay tasas registradas.")

    return {
        "tasas": {
            f["moneda"]: {
                "valor": float(f["valor"]),
                "fecha": f["fecha"].isoformat(),
                "capturado_en": f["capturado_en"].isoformat(timespec="seconds"),
            }
            for f in filas
        }
    }


@app.get("/tasas/{moneda}", tags=["Tasas"])
def tasa_por_moneda(moneda: str):
    """Devuelve la última tasa de una moneda específica (usd, eur, cny, try, rub)."""
    moneda = moneda.lower()
    if moneda not in MONEDAS:
        raise HTTPException(
            status_code=400,
            detail=f"Moneda no válida. Opciones: {', '.join(MONEDAS)}",
        )

    with get_cursor() as cur:
        cur.execute(
            "SELECT moneda, valor, fecha, capturado_en FROM tasas "
            "WHERE moneda = %s ORDER BY fecha DESC LIMIT 1",
            (moneda,),
        )
        fila = cur.fetchone()

    if not fila:
        raise HTTPException(status_code=404, detail=f"Sin datos para {moneda}.")

    return {
        "moneda": fila["moneda"],
        "valor": float(fila["valor"]),
        "fecha": fila["fecha"].isoformat(),
        "capturado_en": fila["capturado_en"].isoformat(timespec="seconds"),
    }


@app.get("/historico/{moneda}", tags=["Histórico"])
def historico(
    moneda: str,
    desde: str | None = Query(None, description="Fecha inicial YYYY-MM-DD"),
    hasta: str | None = Query(None, description="Fecha final YYYY-MM-DD"),
    limite: int = Query(30, ge=1, le=1000, description="Máximo de registros"),
):
    """Devuelve el histórico de una moneda, opcionalmente filtrado por rango de fechas."""
    moneda = moneda.lower()
    if moneda not in MONEDAS:
        raise HTTPException(
            status_code=400,
            detail=f"Moneda no válida. Opciones: {', '.join(MONEDAS)}",
        )

    consulta = "SELECT fecha, valor, capturado_en FROM tasas WHERE moneda = %s"
    params: list = [moneda]
    if desde:
        consulta += " AND fecha >= %s"
        params.append(desde)
    if hasta:
        consulta += " AND fecha <= %s"
        params.append(hasta)
    consulta += " ORDER BY fecha DESC LIMIT %s"
    params.append(limite)

    with get_cursor() as cur:
        cur.execute(consulta, params)
        filas = cur.fetchall()

    return {
        "moneda": moneda,
        "total": len(filas),
        "historico": [
            {
                "fecha": f["fecha"].isoformat(),
                "valor": float(f["valor"]),
                "capturado_en": f["capturado_en"].isoformat(timespec="seconds"),
            }
            for f in filas
        ],
    }


@app.post("/actualizar", tags=["Sistema"])
def forzar_actualizacion():
    """Fuerza una actualización inmediata de las tasas desde el BCV."""
    try:
        resultado = actualizar_tasas()
        return {"mensaje": "Tasas actualizadas correctamente", **resultado}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"No se pudo actualizar: {exc}")