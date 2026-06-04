import os
from datetime import datetime
from typing import Optional, Dict, Any
from bson import ObjectId

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import uvicorn

# ──────────────────────────────────────────────────────────────────────────────
# 1. CONFIGURACIÓN DE CREDENCIALES Y VARIABLES DE ENTORNO
# ──────────────────────────────────────────────────────────────────────────────
_MONGO_USER   = os.getenv("MONGO_USER",        "juan_admin")
_MONGO_PASS   = os.getenv("MONGO_PASS",        "excelencia_2026")
_MONGO_HOST   = os.getenv("MONGO_HOST",        "localhost:27017")
_MONGO_DB     = os.getenv("MONGO_DB",          "smart_fleet_db")
_MONGO_AUTH   = os.getenv("MONGO_AUTH_SOURCE", "admin")
_MONGO_URI    = os.getenv("MONGO_URI", "")
_COLLECTION   = "analytics_reports"

def _build_uri(host_port: str) -> str:
    """Construye un URI de conexión autenticado para el host especificado."""
    return f"mongodb://{_MONGO_USER}:{_MONGO_PASS}@{host_port}/?authSource={_MONGO_AUTH}"

# ──────────────────────────────────────────────────────────────────────────────
# 2. CAPA DE ACCESO A DATOS DEFENSIVA (FALLBACK MULTI-HOST)
# ──────────────────────────────────────────────────────────────────────────────
def _get_mongo_client() -> MongoClient:
    """
    Establece y retorna un cliente PyMongo usando una estrategia de resolución resiliente.
    """
    primary_uri = _MONGO_URI if _MONGO_URI else _build_uri(_MONGO_HOST)
    channels = [
        ("Canal 1: Host de Entorno", primary_uri),
        ("Canal 2: Docker DNS (mongo_fleet)", _build_uri("mongo_fleet:27017")),
        ("Canal 3: Loopback Host (localhost)", _build_uri("localhost:27017")),
        ("Canal 4: Loopback IP (127.0.0.1)", _build_uri("127.0.0.1:27017"))
    ]

    for name, uri in channels:
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=1000, connect=True)
            # Handshake real para validar conectividad activa
            client.admin.command("ping")
            return client
        except (PyMongoError, Exception):
            continue

    raise ConnectionError("No se pudo establecer conexión con MongoDB en ninguno de los canales configurados.")

# ──────────────────────────────────────────────────────────────────────────────
# 3. SERIALIZADOR DE DOCUMENTOS BSON A JSON
# ──────────────────────────────────────────────────────────────────────────────
def serialize_mongo_document(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Transforma de manera recursiva tipos BSON (ObjectId, datetime) a strings serializables.
    """
    if doc is None:
        return None
    
    serialized = {}
    for key, val in doc.items():
        if isinstance(val, ObjectId):
            serialized[key] = str(val)
        elif isinstance(val, datetime):
            serialized[key] = val.isoformat()
        elif isinstance(val, dict):
            serialized[key] = serialize_mongo_document(val)
        elif isinstance(val, list):
            serialized[key] = [
                serialize_mongo_document(item) if isinstance(item, dict) else item 
                for item in val
            ]
        else:
            serialized[key] = val
    return serialized

# ──────────────────────────────────────────────────────────────────────────────
# 4. INICIALIZACIÓN DE FASTAPI Y MIDDLEWARES
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="SmartFleet Pro REST API",
    description="Servicio de API REST desacoplado para la exposición de KPIs de telemetría de flota.",
    version="1.0.0"
)

# Configuración de CORS defensiva global para permitir consumo local (Next.js, Live Server, etc.)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────────────────────────────────────
# 5. ENDPOINTS DE LA API
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/api/v1/fleet/analytics", response_model=Dict[str, Any])
def get_fleet_analytics():
    """
    Retorna el reporte analítico consolidado más reciente generado por el pipeline de Spark.
    """
    try:
        with _get_mongo_client() as client:
            col = client[_MONGO_DB][_COLLECTION]
            # Búsqueda directa del reporte absoluto más reciente
            report = col.find_one({}, sort=[("_id", -1)])
            
            if not report:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No se encontró ningún reporte analítico en la base de datos."
                )
                
            serialized_report = serialize_mongo_document(report)
            return serialized_report
            
    except ConnectionError as conn_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(conn_err)
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor al procesar la solicitud: {str(exc)}"
        )

# ──────────────────────────────────────────────────────────────────────────────
# ENTRYPOINT DE EJECUCIÓN NATIVA
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("app_api:app", host="127.0.0.1", port=8000, reload=True)
