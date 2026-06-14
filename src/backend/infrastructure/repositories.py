import logging
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pyspark.sql import DataFrame

logger = logging.getLogger("SmartFleet_Repositories")


class MongoAnalyticsRepository:
    """
    Repositorio de persistencia NoSQL documental para reportes analíticos consolidados.
    """

    def __init__(self, uri: str, db_name: str) -> None:
        """
        Inicializa la conexión a MongoDB. Valida la conexión ejecutando un ping.
        Intenta URI principal y aplica fallback a localhost/127.0.0.1 si falla.
        """
        uris_to_try = [uri]
        if "mongo_fleet" in uri:
            uris_to_try.append(uri.replace("mongo_fleet", "localhost"))
            uris_to_try.append(uri.replace("mongo_fleet", "127.0.0.1"))

        last_error = None
        for current_uri in uris_to_try:
            try:
                self._client = MongoClient(current_uri, serverSelectionTimeoutMS=1500)
                self._client.admin.command("ping")
                self._db = self._client[db_name]
                self._collection = self._db["analytics_reports"]
                self.uri = current_uri
                self.db_name = db_name
                logger.info(f"[MongoAnalyticsRepository] Conexión establecida usando URI: {current_uri.split('@')[-1]}")
                return
            except PyMongoError as e:
                last_error = e
                continue

        logger.error(
            f"[MongoAnalyticsRepository] Fallaron todos los intentos de conexión a MongoDB: {last_error}",
            exc_info=True
        )
        raise last_error

    def save_batch_report(self, data_list: list) -> None:
        """
        Guarda o actualiza un lote de documentos utilizando operaciones atómicas UpdateOne.
        Utiliza el operador $push con el modificador $sort para mantener el histórico de telemetría ordenado.
        """
        if not data_list:
            return

        from pymongo import UpdateOne
        operations = []
        now_utc = datetime.utcnow()

        for document in data_list:
            document["timestamp_procesamiento"] = now_utc
            trip_id = document.get("id_viaje")
            if not trip_id:
                continue

            # Extraer puntos_telemetria para hacer el push
            points = document.get("puntos_telemetria", [])

            # Crear documento para $set (todos los campos excepto puntos_telemetria e id_viaje)
            set_doc = {k: v for k, v in document.items() if k not in ["puntos_telemetria", "id_viaje", "_id"]}
            set_doc["id_viaje"] = trip_id

            # Forzar métricas globales específicas a nivel de raíz para consistencia del Change Stream
            set_doc["velocidad_promedio_kmh"] = document.get("metricas_basicas", {}).get("velocidad_promedio_kmh")
            set_doc["velocidad_maxima_kmh"] = document.get("metricas_basicas", {}).get("velocidad_maxima_kmh")
            set_doc["global_risk_score"] = document.get("ia_predictiva", {}).get("score_riesgo_global")
            set_doc["timestamp_procesamiento"] = now_utc

            update_query = {
                "$set": set_doc
            }

            if points:
                structured_points = []
                for p in points:
                    ts_val = p.get("timestamp") or p.get("tiempo")
                    parsed_dt = None
                    if isinstance(ts_val, str):
                        ts_val_clean = ts_val.strip()
                        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
                            try:
                                parsed_dt = datetime.strptime(ts_val_clean, fmt)
                                break
                            except ValueError:
                                continue
                        if parsed_dt is None:
                            try:
                                from dateutil import parser
                                parsed_dt = parser.parse(ts_val_clean)
                            except Exception:
                                parsed_dt = datetime.utcnow()
                    elif isinstance(ts_val, datetime):
                        parsed_dt = ts_val
                    else:
                        parsed_dt = datetime.utcnow()

                    vel_val = p.get("velocidad") or p.get("velocidad_promedio_kmh") or p.get("velocidad_maxima_kmh") or 0.0
                    try:
                        vel_float = float(vel_val)
                    except (ValueError, TypeError):
                        vel_float = 0.0

                    structured_points.append({
                        "timestamp": parsed_dt,
                        "velocidad_promedio_kmh": vel_float,
                        "velocidad_maxima_kmh": vel_float
                    })

                update_query["$push"] = {
                    "puntos_telemetria": {
                        "$each": structured_points,
                        "$sort": {"timestamp": 1}
                    }
                }

            operations.append(
                UpdateOne(
                    {"id_viaje": trip_id},
                    update_query,
                    upsert=True
                )
            )

        if operations:
            try:
                result = self._collection.bulk_write(operations)
                logger.info(
                    f"[MongoAnalyticsRepository] Guardado exitoso con bulk_write: "
                    f"matched={result.matched_count}, upserted={result.upserted_count}, modified={result.modified_count}"
                )
            except PyMongoError as e:
                logger.error(
                    f"[MongoAnalyticsRepository] Error crítico al persistir lote en MongoDB vía bulk_write: {e}",
                    exc_info=True
                )
                raise

    def close(self) -> None:
        """
        Cierra de forma defensiva la conexión a PyMongo liberando sockets.
        """
        if hasattr(self, "_client") and self._client:
            try:
                self._client.close()
                logger.info("[MongoAnalyticsRepository] Conexión MongoDB cerrada defensivamente.")
            except Exception as e:
                logger.warning(f"[MongoAnalyticsRepository] Error cerrando cliente MongoDB: {e}")

    def __enter__(self) -> "MongoAnalyticsRepository":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class PostgresAuditRepository:
    """
    Repositorio transaccional relacional para el registro y auditoría de ejecuciones de pipelines.
    """

    def __init__(self, jdbc_url: str, properties: dict) -> None:
        """
        Inicializa las propiedades y dirección de conexión JDBC a PostgreSQL.
        """
        self._jdbc_url = jdbc_url
        self._properties = properties

    def log_execution_sync(self, df: DataFrame, table_name: str = "audit_execution_logs") -> None:
        """
        Escribe un DataFrame de Spark directamente en PostgreSQL usando JDBC en modo Append.
        Utiliza coalescencia para limitar el número de conexiones simultáneas.
        """
        try:
            # .coalesce(2) reduce el número de particiones y por ende las conexiones a BD
            (
                df.coalesce(2)
                .write
                .jdbc(
                    url=self._jdbc_url,
                    table=table_name,
                    mode="append",
                    properties=self._properties
                )
            )
            logger.info(f"[PostgresAuditRepository] Log de auditoría persistido en PostgreSQL, tabla: {table_name}.")
        except Exception as e:
            logger.error(
                f"[PostgresAuditRepository] Error crítico escribiendo log mediante JDBC a Postgres: {e}",
                exc_info=True
            )
            raise
