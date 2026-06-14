import os
import json
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any
from bson import ObjectId

from fastapi import FastAPI, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from sse_starlette.sse import EventSourceResponse
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
# 2. SERIALIZADOR DE DOCUMENTOS BSON A JSON
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
# 3. INICIALIZACIÓN DE FASTAPI Y MIDDLEWARES
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="SmartFleet Pro REST API",
    description="Servicio de API REST desacoplado para la exposición de KPIs de telemetría de flota.",
    version="1.0.0"
)

# Configuración de CORS segura para permitir consumo universal desde cualquier origen de Streamlit/React
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────────────────────────────────────
# 4. EVENTOS DE CICLO DE VIDA (POOL ASÍNCRONO DE MONGO)
# ──────────────────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_db_client():
    """
    Inicializa el cliente asíncrono MotorClient con la estrategia de resolución multi-canal.
    """
    primary_uri = _MONGO_URI if _MONGO_URI else _build_uri(_MONGO_HOST)
    channels = [
        ("Canal 1: Host de Entorno", primary_uri),
        ("Canal 2: Docker DNS (mongo_fleet)", _build_uri("mongo_fleet:27017")),
        ("Canal 3: Loopback Host (localhost)", _build_uri("localhost:27017")),
        ("Canal 4: Loopback IP (127.0.0.1)", _build_uri("127.0.0.1:27017"))
    ]

    last_error = None
    for name, uri in channels:
        try:
            client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=1500)
            # Handshake asíncrono real para validar conectividad activa
            await client.admin.command("ping")
            app.mongodb_client = client
            return
        except Exception as e:
            last_error = e
            continue

    raise ConnectionError(f"No se pudo establecer conexión con MongoDB en ningún canal: {last_error}")

@app.on_event("shutdown")
async def shutdown_db_client():
    """
    Libera el pool de conexiones del cliente asíncrono.
    """
    if hasattr(app, "mongodb_client") and app.mongodb_client:
        app.mongodb_client.close()

# ──────────────────────────────────────────────────────────────────────────────
# 5. ENDPOINTS DE LA API
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/api/v1/fleet/analytics", response_model=Dict[str, Any])
async def get_fleet_analytics():
    """
    Retorna el reporte analítico consolidado más reciente generado por el pipeline de Spark.
    """
    try:
        db = app.mongodb_client[_MONGO_DB]
        col = db[_COLLECTION]
        # Búsqueda asíncrona directa del reporte absoluto más reciente
        report = await col.find_one({}, sort=[("_id", -1)])
        
        if not report:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No se encontró ningún reporte analítico en la base de datos."
            )
            
        serialized_report = serialize_mongo_document(report)
        return serialized_report
        
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor al procesar la solicitud: {str(exc)}"
        )

@app.get("/api/v1/fleet/analytics/history", response_model=list)
async def get_fleet_analytics_history(limit: int = 50):
    """
    Retorna el histórico de los reportes analíticos más recientes.
    """
    try:
        db = app.mongodb_client[_MONGO_DB]
        col = db[_COLLECTION]
        cursor = col.find({}, sort=[("_id", -1)]).limit(limit)
        reports = await cursor.to_list(length=limit)
        serialized_reports = [serialize_mongo_document(r) for r in reports]
        return serialized_reports
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor al procesar el histórico: {str(exc)}"
        )

@app.get("/api/v1/fleet/analytics/stream")
async def get_fleet_analytics_stream(request: Request):
    """
    Expone un Change Stream en caliente para la transmisión en tiempo real de nuevos reportes
    utilizando Server-Sent Events (SSE).
    """
    async def event_generator():
        db = app.mongodb_client[_MONGO_DB]
        col = db[_COLLECTION]
        
        # Filtro estricto para capturar únicamente inserciones de nuevos reportes
        pipeline = [{"$match": {"operationType": "insert"}}]
        
        try:
            async with col.watch(pipeline) as stream:
                async for change in stream:
                    # Control defensivo: romper el bucle si el cliente se desconecta
                    if await request.is_disconnected():
                        break
                    
                    full_doc = change.get("fullDocument")
                    if full_doc:
                        serialized = serialize_mongo_document(full_doc)
                        yield {
                            "event": "pipeline_update",
                            "data": json.dumps(serialized)
                        }
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            yield {
                "event": "error",
                "data": json.dumps({"detail": f"Error en Change Stream: {str(exc)}"})
            }

    return EventSourceResponse(event_generator(), headers={"Cache-Control": "no-cache"})

# ──────────────────────────────────────────────────────────────────────────────
# ENTRYPOINT DE EJECUCIÓN NATIVA
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("app_api:app", host="127.0.0.1", port=8000, reload=True)

