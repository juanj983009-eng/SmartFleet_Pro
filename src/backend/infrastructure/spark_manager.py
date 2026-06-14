import os
from pyspark.sql import SparkSession
from config.database import DatabaseConfig


class SparkSessionBuilder:
    """
    Administrador de infraestructura para la sesión de Apache Spark.
    Implementa el patrón de diseño Singleton para reutilizar una única sesión Spark.
    """
    
    _session: SparkSession = None

    @classmethod
    def get_session(cls, config: DatabaseConfig) -> SparkSession:
        """
        Retorna la sesión de Spark global. Si no existe, la inicializa con
        paquetes nativos de base de datos, configuraciones de red e integraciones.
        """
        if cls._session is None:
            # Separación de host y puerto en caso de que CASSANDRA_HOST contenga ":"
            cassandra_host = config.CASSANDRA_HOST
            cassandra_port = 9042
            if ":" in cassandra_host:
                h, p = cassandra_host.split(":")
                cassandra_host = h
                cassandra_port = int(p)

            # Cadena de fallback interactiva de red local para Cassandra
            import socket
            def is_cassandra_reachable(h: str, p: int) -> bool:
                try:
                    with socket.create_connection((h, p), timeout=0.8):
                        return True
                except Exception:
                    return False

            resolved_cassandra_host = cassandra_host
            # Si el host especificado (p.ej. cassandra_fleet) no responde, probar fallback local
            if not is_cassandra_reachable(cassandra_host, cassandra_port):
                for fallback_host in ["localhost", "127.0.0.1"]:
                    if is_cassandra_reachable(fallback_host, cassandra_port):
                        resolved_cassandra_host = fallback_host
                        break

            cassandra_port_str = str(cassandra_port)

            # Opciones de compatibilidad JVM para Java 17 y superior (JPMS / modularidad fuerte)
            _JVM_OPENS = (
                "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
                "--add-opens=java.base/java.lang=ALL-UNNAMED"
            )

            cls._session = (
                SparkSession.builder
                .appName("SmartFleet_Pro_Analytics")
                .config(
                    "spark.jars.packages",
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                    "org.postgresql:postgresql:42.7.2,"
                    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
                )
                # Configuración de Cassandra con resolución dinámica / fallback
                .config("spark.cassandra.connection.host", resolved_cassandra_host)
                .config("spark.cassandra.connection.port", cassandra_port_str)
                # Optimizaciones de rendimiento de Spark
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.shuffle.partitions", "16")
                .config("spark.sql.session.timeZone", "America/Lima")
                # Configuraciones de compatibilidad JVM local
                .config("spark.driver.extraJavaOptions", _JVM_OPENS)
                .config("spark.executor.extraJavaOptions", _JVM_OPENS)
                .config("spark.hadoop.security.authentication", "simple")
                .getOrCreate()
            )
            
            # Silenciar logs INFO excesivos de Spark JVM
            cls._session.sparkContext.setLogLevel("WARN")

        return cls._session
