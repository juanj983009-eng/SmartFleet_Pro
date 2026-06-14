import os
import sys

# ──────────────────────────────────────────────────────────────────────────────
# 0. CONFIGURACIÓN DE ENTORNOS LOCALES PARA SPARK / JVM (HADOOP & JAVA)
# ──────────────────────────────────────────────────────────────────────────────
# Intentar leer configuraciones prioritariamente desde las variables de entorno
_JAVA_HOME = os.getenv("JAVA_HOME")
_HADOOP_HOME = os.getenv("HADOOP_HOME")

# Mecanismo de fallback defensivo si el sistema operativo anfitrión es Windows
if not _JAVA_HOME and os.name == "nt":
    _JAVA_HOME = r"D:\Java17\openjdk-17.0.2_windows-x64_bin\jdk-17.0.2"
if not _HADOOP_HOME and os.name == "nt":
    _HADOOP_HOME = r"D:\Hadoop"

if _JAVA_HOME:
    os.environ["JAVA_HOME"] = _JAVA_HOME
if _HADOOP_HOME:
    os.environ["HADOOP_HOME"] = _HADOOP_HOME

# Construir PATH solo si las variables están definidas
_path_entries = []
if os.getenv("JAVA_HOME"):
    _path_entries.append(os.path.join(os.environ["JAVA_HOME"], "bin"))
if os.getenv("HADOOP_HOME"):
    _path_entries.append(os.path.join(os.environ["HADOOP_HOME"], "bin"))

if _path_entries:
    os.environ["PATH"] = os.pathsep.join(_path_entries) + os.pathsep + os.environ.get("PATH", "")

_JVM_FLAGS = (
    "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "-Dspark.hadoop.security.authentication=simple"
)

if "JAVA_TOOL_OPTIONS" in os.environ:
    if _JVM_FLAGS not in os.environ["JAVA_TOOL_OPTIONS"]:
        os.environ["JAVA_TOOL_OPTIONS"] += f" {_JVM_FLAGS}"
else:
    os.environ["JAVA_TOOL_OPTIONS"] = _JVM_FLAGS

os.environ["SPARK_SUBMIT_OPTS"] = _JVM_FLAGS
os.environ["SUBMIT_OPTS"] = _JVM_FLAGS

import logging
from config.database import DatabaseConfig
from infrastructure.spark_manager import SparkSessionBuilder
from infrastructure.repositories import MongoAnalyticsRepository, PostgresAuditRepository
from usecases.telemetry_etl import TelemetryETLUseCase

# Configuración profesional del sistema de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout
)
logger = logging.getLogger("SmartFleet_Main")


def main() -> None:
    """
    Composition Root del sistema SmartFleet. Inicializa las configuraciones de entorno,
    construye las conexiones de infraestructura y delega la ejecución al caso de uso.
    """
    spark = None
    mongo_repo = None
    try:
        logger.info("Iniciando arranque del pipeline analítico de SmartFleet Pro...")

        # 1. Validación estricta del entorno
        DatabaseConfig.validate_env()
        logger.info("[BOOTSTRAP] Entorno validado exitosamente (12-Factor App).")

        # 2. Inicialización de la configuración
        config = DatabaseConfig()

        # 3. Construcción del motor de procesamiento Spark
        logger.info("[BOOTSTRAP] Inicializando el Driver y contexto de Spark...")
        spark = SparkSessionBuilder.get_session(config)

        # 4. Inicialización de la persistencia (Infraestructura)
        logger.info("[BOOTSTRAP] Conectando repositorios de persistencia políglota...")
        mongo_repo = MongoAnalyticsRepository(
            uri=config.MONGO_URI,
            db_name=config.MONGO_DB
        )

        postgres_url = config.POSTGRES_URL
        # Fallback dinámico de host para PostgreSQL si se ejecuta fuera de la red de Docker
        if "postgres_fleet" in postgres_url:
            import socket
            try:
                with socket.create_connection(("postgres_fleet", 5432), timeout=0.8):
                    pass
            except Exception:
                postgres_url = postgres_url.replace("postgres_fleet", "localhost")

        postgres_properties = {
            "user": config.POSTGRES_USER,
            "password": config.POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
        postgres_repo = PostgresAuditRepository(
            jdbc_url=postgres_url,
            properties=postgres_properties
        )

        # 5. Composición de Capas e inyección de dependencias
        logger.info("[BOOTSTRAP] Inyectando dependencias al caso de uso...")
        etl_pipeline = TelemetryETLUseCase(
            spark=spark,
            mongo_repo=mongo_repo,
            postgres_repo=postgres_repo
        )

        # 6. Ejecución del flujo ETL
        etl_pipeline.execute_pipeline()

    except Exception as e:
        logger.fatal(
            f"[BOOTSTRAP FATAL] Error crítico en la inicialización o composición del sistema: {e}",
            exc_info=True
        )
        sys.exit(1)
    finally:
        logger.info("[TEARDOWN] Iniciando cierre defensivo de recursos de infraestructura...")
        if mongo_repo is not None:
            try:
                mongo_repo.close()
            except Exception as err:
                logger.warning(f"[TEARDOWN] Advertencia al cerrar MongoDB: {err}")
        if spark is not None:
            try:
                spark.stop()
                logger.info("[TEARDOWN] SparkSession detenida con éxito.")
            except Exception as err:
                logger.warning(f"[TEARDOWN] Advertencia al detener SparkSession: {err}")


if __name__ == "__main__":
    main()
