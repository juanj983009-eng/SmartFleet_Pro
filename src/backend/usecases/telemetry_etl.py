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
            df_repartitioned = df_filtered.repartition("id_viaje")

            window_spec_base = Window.partitionBy("id_viaje").orderBy("tiempo")
            window_spec = (
                window_spec_base
                .rowsBetween(-10, 0)
            )

            df_transformed = (
                df_repartitioned
                .withColumn("v_anterior", F.lag("velocidad_kmh", 1).over(window_spec_base).cast("double"))
                .withColumn("t_anterior", F.lag("tiempo", 1).over(window_spec_base))
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
            self._postgres_repo.log_execution_sync(
                df_scored.drop("total_muestras", "ultimo_latitud", "ultimo_longitud", "puntos_telemetria"),
                table_name="audit_execution_logs"
            )

            # Document-oriented storage of hierarchically aggregated analytics report in MongoDB
            logger.info("[LOAD] Persistiendo reportes estructurados en MongoDB Atlas...")

            # Construct nested document structure in Spark to preserve database schema and support distributed writes
            df_mongo = df_scored.select(
                F.lit("SmartFleet_Pro").alias("proyecto"),
                F.lit("2.0.0").alias("version_pipeline"),
                F.col("id_viaje"),
                F.current_timestamp().alias("fecha_analisis"),
                F.current_timestamp().alias("timestamp_procesamiento"),
                
                # Global metrics at root level for Change Stream consistency
                F.round(F.col("velocidad_promedio_kmh").cast("double"), 2).alias("velocidad_promedio_kmh"),
                F.round(F.col("velocidad_maxima_kmh").cast("double"), 2).alias("velocidad_maxima_kmh"),
                F.round(F.col("score_riesgo_global").cast("double"), 2).alias("global_risk_score"),

                F.struct(
                    F.coalesce(F.col("ultimo_latitud").cast("double"), F.lit(-12.0464)).alias("latitud"),
                    F.coalesce(F.col("ultimo_longitud").cast("double"), F.lit(-77.0428)).alias("longitud")
                ).alias("posicion_actual"),

                F.struct(
                    F.col("total_muestras").cast("integer").alias("total_muestras"),
                    F.round(F.col("velocidad_promedio_kmh").cast("double"), 2).alias("velocidad_promedio_kmh"),
                    F.round(F.col("velocidad_maxima_kmh").cast("double"), 2).alias("velocidad_maxima_kmh"),
                    F.col("alertas_exceso_velocidad").cast("integer").alias("alertas_exceso_velocidad"),
                    F.lit(TelemetryConstants.SPEED_LIMIT_KMH).alias("umbral_velocidad_kmh")
                ).alias("metricas_basicas"),

                F.struct(
                    F.round(F.col("aceleracion_varianza").cast("double"), 2).alias("aceleracion_varianza_kmhs2"),
                    F.col("frenadas_bruscas_count").cast("integer").alias("frenadas_bruscas_count"),
                    F.lit(-4.5).alias("umbral_frenado_kmhs"),
                    F.round(F.col("score_riesgo_global").cast("double"), 2).alias("score_riesgo_global"),
                    F.struct(
                        F.lit(RiskScoreWeights.SPEEDING_WEIGHT).alias("exceso_velocidad"),
                        F.lit(RiskScoreWeights.VARIANCE_WEIGHT).alias("varianza_acel"),
                        F.lit(RiskScoreWeights.HARD_BRAKING_WEIGHT).alias("frenadas_bruscas")
                    ).alias("ponderaciones_matriz")
                ).alias("ia_predictiva"),

                F.struct(
                    F.lit(f"PySpark {self._spark.version} (JVM Native)").alias("motor_procesamiento"),
                    F.lit("Window Functions — Partitioned by id_viaje").alias("patron_etl"),
                    F.lit("Fórmula de Koenig-Huygens: E[X²] - (E[X])²").alias("algoritmo_varianza"),
                    F.lit("Native MongoDB Spark Connector").alias("patron_persistencia"),
                    F.lit("Clean Architecture / SOLID / 12-Factor App").alias("principios")
                ).alias("arquitectura"),

                F.slice(
                    F.col("puntos_telemetria"),
                    F.greatest(F.lit(1), F.size(F.col("puntos_telemetria")) - 99),
                    100
                ).alias("puntos_telemetria")
            )

            # Write to MongoDB using the native Spark MongoDB Connector in parallel
            (
                df_mongo.write
                .format("mongodb")
                .mode("append")
                .option("connection.uri", self._mongo_repo.uri)
                .option("database", self._mongo_repo.db_name)
                .option("collection", "analytics_reports")
                .option("operationType", "replace")
                .option("idFieldList", "id_viaje")
                .save()
            )

            logger.info("[ETL END] Ejecución del pipeline ETL completada exitosamente.")

        except Exception as e:
            logger.error(f"[ETL ERROR] Error catastrófico durante la ejecución del pipeline: {e}", exc_info=True)
            raise
