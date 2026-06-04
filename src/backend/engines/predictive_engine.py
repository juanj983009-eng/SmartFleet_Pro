from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from domain.constants import TelemetryConstants, RiskScoreWeights


class PredictiveAnalyticsEngine:
    """
    Motor analítico y de cálculo de métricas estadísticas predictivas de conducción.
    No almacena estado y procesa DataFrames de Spark con operaciones distribuidas optimizadas.
    """

    @staticmethod
    def calculate_risk_score(df: DataFrame) -> DataFrame:
        """
        Agrupa los datos por 'id_viaje' y calcula las métricas consolidadas,
        incluyendo la varianza de aceleración con Koenig-Huygens,
        excesos de velocidad, frenados bruscos y el score general de conducción.
        """
        # Expresiones de Koenig-Huygens para varianza en un solo paso con tipado double estricto
        e_x = F.avg(F.col("aceleracion").cast("double"))
        e_x2 = F.avg((F.col("aceleracion").cast("double") * F.col("aceleracion").cast("double")).cast("double"))
        formula_varianza = (e_x2 - F.pow(e_x, 2)).cast("double")

        # Agrupación y cálculo analítico distribuido
        df_agg = df.groupBy("id_viaje").agg(
            F.count("*").alias("total_muestras"),
            F.avg("velocidad_kmh").alias("velocidad_promedio_kmh"),
            F.max("velocidad_kmh").alias("velocidad_maxima_kmh"),
            
            # Conteo de excesos de velocidad
            F.sum(
                F.when(F.col("velocidad_kmh") > TelemetryConstants.SPEED_LIMIT_KMH, 1).otherwise(0)
            ).alias("alertas_exceso_velocidad"),
            
            # Varianza de aceleración forzando no negatividad por precisión de punto flotante
            F.greatest(F.lit(0.0), formula_varianza).alias("aceleracion_varianza"),
            
            # Conteo de frenadas bruscas (aceleración inferior a -4.5 m/s²)
            F.sum(
                F.when(F.col("aceleracion") < -4.5, 1).otherwise(0)
            ).alias("frenadas_bruscas_count")
        )

        # Escalado independiente de componentes (0 a 100 de penalización)
        score_exceso = F.least(F.col("alertas_exceso_velocidad") * 5.0, F.lit(100.0))
        score_varianza = F.least(F.col("aceleracion_varianza") * 2.0, F.lit(100.0))
        score_frenadas = F.least(F.col("frenadas_bruscas_count") * 10.0, F.lit(100.0))

        # Combinación lineal ponderada del riesgo crudo
        risk_score_raw = (
            score_varianza * RiskScoreWeights.VARIANCE_WEIGHT +
            score_exceso * RiskScoreWeights.SPEEDING_WEIGHT +
            score_frenadas * RiskScoreWeights.HARD_BRAKING_WEIGHT
        )

        # Acotación y cálculo del score de riesgo global (0.0 = sin incidentes)
        risk_score_clamped = F.greatest(F.lit(0.0), F.least(risk_score_raw, F.lit(100.0)))
        score_riesgo = F.round(risk_score_clamped, 2)

        # Proyección final del DataFrame de salida
        return df_agg.withColumn("score_riesgo_global", score_riesgo)
