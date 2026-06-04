import logging
from datetime import datetime, timezone
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from domain.constants import TelemetryConstants, RiskScoreWeights
from engines.predictive_engine import PredictiveAnalyticsEngine
from infrastructure.repositories import MongoAnalyticsRepository, PostgresAuditRepository

logger = logging.getLogger("SmartFleet_TelemetryETL")


class TelemetryETLUseCase:
    """
    Caso de Uso de Aplicación que orquesta el pipeline ETL de telemetría vehicular.
    Encapsula la extracción de Cassandra, transformaciones y cálculos físicos distribuidos,
    llamadas al motor de scoring, y la persistencia políglota final.
    """

    def __init__(
        self,
        spark: SparkSession,
        mongo_repo: MongoAnalyticsRepository,
        postgres_repo: PostgresAuditRepository
    ) -> None:
        """
        Inyección de dependencias para infraestructura de bases de datos y motor de cómputo.
        """
        self._spark = spark
        self._mongo_repo = mongo_repo
        self._postgres_repo = postgres_repo

    def execute_pipeline(self) -> None:
        """
        Orquesta y ejecuta el ciclo de vida del pipeline ETL.
        """
        try:
            logger.info("[ETL START] Iniciando ejecución de extracción y cálculo analítico...")

            # EXTRACT: Retrieve telemetry records from Apache Cassandra keyspace
            df_cassandra_raw = (
                self._spark.read
                .format("org.apache.spark.sql.cassandra")
                .options(table="telemetria_gps", keyspace="fleet_telemetry")
                .load()
            )

            # Normalize schema to ensure consistent column mapping ('velocidad' -> 'velocidad_kmh')
            df_cassandra = df_cassandra_raw.withColumnRenamed("velocidad", "velocidad_kmh")

            # Ensure dataset is non-empty before initiating transformations
            if df_cassandra.isEmpty():
                logger.warning("[EXTRACT] Telemetría vacía en Cassandra. Finalizando proceso sin persistir.")
                return

            logger.info(f"[EXTRACT] Datos cargados con éxito desde Cassandra.")

            # TRANSFORM: Clean coordinates and compute physical velocity-time derivatives
            # Filter latitude and longitude using domain coordinate bounds
            df_filtered = df_cassandra.filter(
                F.col("latitud").between(TelemetryConstants.LATITUDE_MIN, TelemetryConstants.LATITUDE_MAX) &
                F.col("longitud").between(TelemetryConstants.LONGITUDE_MIN, TelemetryConstants.LONGITUDE_MAX)
            )

            # Performance Optimization: Repartition by partition key to minimize shuffle stages
            # aligned with Window partitionBy and subsequent groupBy in the Predictive Engine
            df_repartitioned = df_filtered.repartition("id_viaje")

            window_spec = Window.partitionBy("id_viaje").orderBy("tiempo")

            df_transformed = (
                df_repartitioned
                .withColumn("v_anterior", F.lag("velocidad_kmh", 1).over(window_spec).cast("double"))
                .withColumn("t_anterior", F.lag("tiempo", 1).over(window_spec))
                # Convert velocity delta from km/h to SI units (m/s) with double precision
                .withColumn(
                    "delta_v", 
                    (F.col("velocidad_kmh").cast("double") - F.col("v_anterior")) / F.lit(3.6).cast("double")
                )
                # Compute temporal delta with sub-second precision to calculate acceleration variance via Koenig-Huygens theorem without truncation errors
                .withColumn("delta_t", F.col("tiempo").cast("double") - F.col("t_anterior").cast("double"))
                # Calculate instant acceleration, safeguarding against zero-division errors for identical telemetry timestamps
                .withColumn(
                    "aceleracion",
                    F.when(F.col("delta_t") > 0.0, F.col("delta_v") / F.col("delta_t"))
                     .otherwise(F.lit(0.0).cast("double"))
                     .cast("double")
                )
                .fillna(0.0, subset=["aceleracion"])
            )

            logger.info("[TRANSFORM] Limpieza y cálculo de aceleración finalizados.")

            # SCORING: Evaluate global driver safety risks via the analytics engine
            df_scored = PredictiveAnalyticsEngine.calculate_risk_score(df_transformed)
            logger.info("[SCORING] Puntuaciones de riesgo e indicadores calculados exitosamente.")

            # LOAD: Persist execution logs in PostgreSQL and aggregated reports in MongoDB
            # Relational logging of ETL execution metadata via JDBC in PostgreSQL
            logger.info("[LOAD] Persistiendo logs analíticos consolidados en PostgreSQL...")
            self._postgres_repo.log_execution_sync(df_scored.drop("total_muestras"), table_name="audit_execution_logs")

            # Document-oriented storage of hierarchically aggregated analytics report in MongoDB
            logger.info("[LOAD] Persistiendo reportes estructurados en MongoDB Atlas...")
            # Collect records to driver memory to format raw Spark rows into MongoDB BSON documents
            rows = df_scored.collect()
            flat_data = [row.asDict(recursive=True) for row in rows]
            
            nested_data = []
            for item in flat_data:
                nested_doc = {
                    "proyecto": "SmartFleet_Pro",
                    "version_pipeline": "2.0.0",
                    "id_viaje": item.get("id_viaje"),
                    "fecha_analisis": datetime.now(timezone.utc),
                    
                    "metricas_basicas": {
                        "total_muestras": int(item.get("total_muestras", 0)),
                        "velocidad_promedio_kmh": round(float(item.get("velocidad_promedio_kmh", 0.0)), 2),
                        "velocidad_maxima_kmh": round(float(item.get("velocidad_maxima_kmh", 0.0)), 2),
                        "alertas_exceso_velocidad": int(item.get("alertas_exceso_velocidad", 0)),
                        "umbral_velocidad_kmh": TelemetryConstants.SPEED_LIMIT_KMH,
                    },
                    
                    "ia_predictiva": {
                        "aceleracion_varianza_kmhs2": round(float(item.get("aceleracion_varianza", 0.0)), 2),
                        "frenadas_bruscas_count": int(item.get("frenadas_bruscas_count", 0)),
                        "umbral_frenado_kmhs": -4.5,
                        "score_riesgo_global": round(float(item.get("score_riesgo_global", 0.0)), 2),
                        "ponderaciones_matriz": {
                            "exceso_velocidad": RiskScoreWeights.SPEEDING_WEIGHT,
                            "varianza_acel": RiskScoreWeights.VARIANCE_WEIGHT,
                            "frenadas_bruscas": RiskScoreWeights.HARD_BRAKING_WEIGHT,
                        }
                    },
                    
                    "arquitectura": {
                        "motor_procesamiento": f"PySpark {self._spark.version} (JVM Native)",
                        "patron_etl": "Window Functions — Partitioned by id_viaje",
                        "algoritmo_varianza": "Fórmula de Koenig-Huygens: E[X²] - (E[X])²",
                        "patron_persistencia": "Repository Pattern (PyMongo)",
                        "principios": "Clean Architecture / SOLID / 12-Factor App"
                    }
                }
                nested_data.append(nested_doc)
            
            if nested_data:
                self._mongo_repo.save_batch_report(nested_data)

            logger.info("[ETL END] Ejecución del pipeline ETL completada exitosamente.")

        except Exception as e:
            logger.error(f"[ETL ERROR] Error catastrófico durante la ejecución del pipeline: {e}", exc_info=True)
            raise
