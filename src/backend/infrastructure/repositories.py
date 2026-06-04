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
        Inserta un lote de documentos en la colección 'analytics_reports'.
        Agrega un campo de auditoría 'timestamp_procesamiento' en formato UTC a cada registro.
        """
        if not data_list:
            return

        try:
            now_utc = datetime.utcnow()
            for document in data_list:
                document["timestamp_procesamiento"] = now_utc

            self._collection.insert_many(data_list)
            logger.info(f"[MongoAnalyticsRepository] Guardado exitoso de lote con {len(data_list)} reportes.")
        except PyMongoError as e:
            logger.error(
                f"[MongoAnalyticsRepository] Error crítico al persistir lote en MongoDB: {e}",
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
