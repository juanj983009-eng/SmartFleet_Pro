import os
import json
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any
from bson import ObjectId

from fastapi import FastAPI, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from motor.motor_asyncio import AsyncIOMotorClient
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
# Instancia global del cliente reactivo MotorClient (unificada)
_MONGO_CONNECTION_URI = "mongodb://juan_admin:excelencia_2026@mongo_fleet:27017/?authSource=admin"
mongo_client = AsyncIOMotorClient(_MONGO_CONNECTION_URI)

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
    Realiza un ping inicial sobre el pool global para verificar conectividad con MongoDB.
    """
    try:
        await mongo_client.admin.command("ping")
    except Exception as exc:
        raise ConnectionError(f"No se pudo establecer conexión con MongoDB usando el pool global: {exc}")

@app.on_event("shutdown")
async def shutdown_db_client():
    """
    Cierra el pool global de conexiones a MongoDB.
    """
    mongo_client.close()

# ──────────────────────────────────────────────────────────────────────────────
# 5. ENDPOINTS DE LA API
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/api/v1/fleet/analytics", response_model=Dict[str, Any])
async def get_fleet_analytics():
    """
    Retorna el reporte analítico consolidado más reciente generado por el pipeline de Spark.
    """
    try:
        db = mongo_client[_MONGO_DB]
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
        
    except HTTPException:
        raise
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
        db = mongo_client[_MONGO_DB]
        col = db[_COLLECTION]
        cursor = col.find({}, sort=[("_id", -1)]).limit(limit)
        reports = await cursor.to_list(length=limit)
        serialized_reports = [serialize_mongo_document(r) for r in reports]
        return serialized_reports
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor al procesar el histórico: {str(exc)}"
        )

@app.get("/api/v1/fleet/analytics/stream")
async def get_analytics_stream(request: Request):
    """
    Expone un flujo SSE continuo en caliente mediante consultas de baja latencia al Data Warehouse.
    """
    async def event_generator():
        db = mongo_client[_MONGO_DB]
        col = db[_COLLECTION]
        
        while True:
            try:
                # Control de desconexión activa (Keep-Alive Guard)
                if await request.is_disconnected():
                    break
                
                # Query reactivo no bloqueante para extraer el último reporte de telemetría de viaje
                report = await col.find_one(
                    {"id_viaje": "TRIP-MANUAL-TEST-99"},
                    sort=[("_id", -1)]
                )
                
                if report:
                    serialized = serialize_mongo_document(report)
                    yield f"event: pipeline_update\ndata: {json.dumps(serialized)}\n\n"
                else:
                    yield 'event: ping\ndata: {"status": "empty_collection"}\n\n'
            except asyncio.CancelledError:
                break
            except (ConnectionError, BrokenPipeError, ConnectionResetError):
                break
            except Exception as exc:
                # Captura defensiva de cualquier otro error para evitar pánicos en Uvicorn
                break
            
            # Frecuencia de refresco para fluidez georreferencial y liberación de event loop
            await asyncio.sleep(1)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

# ──────────────────────────────────────────────────────────────────────────────
# ENTRYPOINT DE EJECUCIÓN NATIVA
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("app_api:app", host="127.0.0.1", port=8000, reload=True)

