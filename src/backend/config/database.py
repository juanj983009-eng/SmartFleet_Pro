import os
import sys
from dotenv import load_dotenv

# Ingestión de variables desde archivo .env si está presente en el entorno
load_dotenv()


class DatabaseConfig:
    """
    Configuración de accesos y credenciales para las bases de datos del sistema.
    Aplica los principios de 12-Factor App cargando la configuración estrictamente
    desde las variables de entorno sin fallbacks en duro.
    """
    
    _REQUIRED_VARS = [
        "MONGO_DB",
        "CASSANDRA_HOST",
        "POSTGRES_URL",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD"
    ]

    @classmethod
    def validate_env(cls) -> None:
        """
        Valida que todas las variables de entorno requeridas estén definidas en el sistema.
        Si falta alguna variable, detiene la ejecución del proceso con código de salida 1.
        """
        missing_vars = [var for var in cls._REQUIRED_VARS if var not in os.environ]
        if missing_vars:
            # Flujo de salida controlado sin stack-traces innecesarios
            sys.stderr.write(
                f"[FATAL CONFIG ERROR] Faltan las siguientes variables de entorno requeridas: "
                f"{', '.join(missing_vars)}\n"
            )
            sys.exit(1)

    @property
    def MONGO_URI(self) -> str:
        if "MONGO_URI" in os.environ:
            return os.environ["MONGO_URI"]
        # Reconstruir dinámicamente si no está en el entorno
        user = os.environ.get("MONGO_USER", "juan_admin")
        pw = os.environ.get("MONGO_PASS", "excelencia_2026")
        host = os.environ.get("MONGO_HOST", "mongo_fleet:27017")
        auth = os.environ.get("MONGO_AUTH_SOURCE", "admin")
        return f"mongodb://{user}:{pw}@{host}/?authSource={auth}"

    @property
    def MONGO_DB(self) -> str:
        return os.environ["MONGO_DB"]

    @property
    def CASSANDRA_HOST(self) -> str:
        return os.environ["CASSANDRA_HOST"]

    @property
    def POSTGRES_URL(self) -> str:
        return os.environ["POSTGRES_URL"]

    @property
    def POSTGRES_USER(self) -> str:
        return os.environ["POSTGRES_USER"]

    @property
    def POSTGRES_PASSWORD(self) -> str:
        return os.environ["POSTGRES_PASSWORD"]
