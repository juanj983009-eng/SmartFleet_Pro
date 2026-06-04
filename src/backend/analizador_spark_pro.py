"""
================================================================================
  SmartFleet Pro - Analizador Analitico de Telemetria
  Version: 2.0.0 (Production-Grade)
  Arquitectura: Clean Architecture + SOLID
  Author: SmartFleet Pro Team
================================================================================

DIAGRAMA DE CAPAS (Dependency Rule: las capas externas dependen de las internas)

  │   Bootstrap: crea sesión Spark + inyecta dependencias       │
  └──────────────────────────┬──────────────────────────────────┘
                             │ invoca
  ┌──────────────────────────▼──────────────────────────────────┐
  │           CAPA DE APLICACIÓN  (Use Cases)                   │
  │   TelemetryETLUseCase — orquesta el pipeline ETL            │
  │   Recibe dependencias por inyección (DI), no las crea.      │
  └──────┬─────────────────────────────────────────┬────────────┘
         │ usa                                     │ usa
  ┌──────▼────────────────────┐   ┌───────────────▼────────────┐
  │  CAPA DE ANALÍTICA / IA   │   │  CAPA DE INFRAESTRUCTURA   │
  │  PredictiveAnalyticsEngine│   │  MongoAnalyticsRepository  │
  │  (sin dependencias ext.)  │   │  DatabaseConfig            │
  └──────┬────────────────────┘   └────────────────────────────┘
         │ usa
  ┌──────▼────────────────────┐
  │      CAPA DE DOMINIO      │
  │  TelemetryConstants       │
  │  RiskScoreWeights         │
  └───────────────────────────┘
"""

# ==============================================================================
# 0. PRE-IMPORT: INYECCIÓN DE FLAGS JVM PARA JAVA 24
# ==============================================================================
# RAZÓN CRÍTICA — Python de Windows Store corre en un AppContainer aislado que
# ignora las variables de entorno del sistema y del perfil de usuario. La única
# ventana de tiempo en la que se pueden forzar opciones sobre la JVM es ANTES
# de que el módulo 'pyspark' sea importado, porque su import-machinery arranca
# el proceso java.exe como subproceso durante la inicialización de py4j.
#
# Las tres variables cubren todos los vectores de lanzamiento conocidos:
#
#   JAVA_TOOL_OPTIONS   → Leída por la propia JVM (hotspot) en el arranque del
#                          proceso java.exe; tiene prioridad sobre cualquier
#                          argumento de línea de comandos. Es el mecanismo más
#                          robusto y portable entre distribuciones de Java.
#
#   SPARK_SUBMIT_OPTS   → Leída por el script spark-submit (y por el launcher
#                          interno de PySpark) para inyectar opciones al Driver
#                          antes de que SparkContext se construya.
#
#   SUBMIT_OPTS         → Variable legada usada por versiones anteriores de los
#                          scripts de arranque de Hadoop/Spark en Windows para
#                          pasar opciones adicionales al proceso java.exe.
#
# Flags inyectadas:
#   --add-opens=java.base/javax.security.auth=ALL-UNNAMED
#       → Hadoop 3.x accede reflexivamente a Subject.getSubject() que fue
#         eliminado del API público en Java 24. Sin esta apertura, el arranque
#         de Hadoop lanza InaccessibleObjectException y aborta la JVM.
#
#   --add-opens=java.base/java.lang=ALL-UNNAMED
#       → PySpark (vía Py4J) accede reflexivamente a Thread, ClassLoader y
#         String. JPMS lo bloquea por defecto desde Java 9; Java 24 lo hace
#         fatal (error duro, no solo warning).
#
#   -Dspark.hadoop.security.authentication=simple
#       → Propiedad de sistema Java que deshabilita el flujo JAAS/Kerberos en
#         Hadoop desde el nivel de la JVM, como capa de seguridad adicional
#         independiente de la configuración de SparkSession.

import os
import sys

_JVM_FLAGS = (
    "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "-Dspark.hadoop.security.authentication=simple"
)

# JAVA_TOOL_OPTIONS: vector principal — lo lee directamente el proceso java.exe
if "JAVA_TOOL_OPTIONS" in os.environ:
    # Anexamos sin duplicar si ya existe algún flag previo en el entorno
    if _JVM_FLAGS not in os.environ["JAVA_TOOL_OPTIONS"]:
        os.environ["JAVA_TOOL_OPTIONS"] += f" {_JVM_FLAGS}"
else:
    os.environ["JAVA_TOOL_OPTIONS"] = _JVM_FLAGS

# SPARK_SUBMIT_OPTS: vector secundario — lo consume el launcher de PySpark
os.environ["SPARK_SUBMIT_OPTS"] = _JVM_FLAGS

# SUBMIT_OPTS: vector legado de Hadoop/Spark en Windows
os.environ["SUBMIT_OPTS"] = _JVM_FLAGS


# ==============================================================================
# IMPORTS
# ==============================================================================
import logging
import traceback                          # [DEBUG] volcado de traza completa en consola
from datetime import datetime, timezone
from typing import Optional

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# PySpark se importa DESPUÉS de haber configurado todas las variables de entorno
# JVM. Cualquier import de pyspark antes de este punto podría arrancar la JVM
# con los flags incorrectos o sin ellos.
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ==============================================================================
# 1. CAPA DE INFRAESTRUCTURA — CONFIGURACIÓN DE ENTORNO
# ==============================================================================
# DECISIÓN ARQUITECTÓNICA: Configurar variables de entorno del sistema ANTES
# de cualquier import de PySpark. La JVM de Spark se inicializa al crear la
# SparkSession; si JAVA_HOME y HADOOP_HOME no están definidas en ese momento,
# el proceso falla. Hacerlo aquí —al nivel de módulo— garantiza que las rutas
# estén disponibles para cualquier hilo o subproceso generado posteriormente.
# En un entorno CI/CD estas variables vendrían de un secrets manager; en local
# Windows usamos defaults explícitos como fallback documentado.
_JAVA_HOME  = r"D:\Java17\openjdk-17.0.2_windows-x64_bin\jdk-17.0.2"
_HADOOP_HOME = r"D:\Hadoop"

os.environ.setdefault("JAVA_HOME",  _JAVA_HOME)
os.environ.setdefault("HADOOP_HOME", _HADOOP_HOME)

# Anteponemos los binarios de Java y Hadoop al PATH existente para garantizar
# que winutils.exe (requerido por Hadoop en Windows) sea encontrado por la JVM.
_path_entries = [
    os.path.join(os.environ["JAVA_HOME"],  "bin"),
    os.path.join(os.environ["HADOOP_HOME"], "bin"),
]
os.environ["PATH"] = os.pathsep.join(_path_entries) + os.pathsep + os.environ.get("PATH", "")


# ==============================================================================
# 1.1 CAPA DE INFRAESTRUCTURA — SISTEMA DE LOGGING PROFESIONAL
# ==============================================================================
# DECISIÓN ARQUITECTÓNICA: Un logger nombrado ('SmartFleet_Analytics') permite
# que sistemas externos (Datadog, ELK, Cloud Logging) filtren exclusivamente
# los eventos de esta aplicación sin capturar el ruido de PySpark o pymongo.
# El formato ISO 8601 es estándar de facto en entornos distribuidos.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("SmartFleet_Analytics")


# ==============================================================================
# 1.2 CAPA DE INFRAESTRUCTURA — CONFIGURACIÓN DE CREDENCIALES
# ==============================================================================
class DatabaseConfig:
    """
    Encapsula las credenciales y parámetros de conexión a MongoDB.

    DECISIÓN ARQUITECTÓNICA (12-Factor App / principio de sustitución de Liskov):
    Las credenciales se leen primero desde variables de entorno del sistema
    operativo. Esto permite sobreescribirlas sin modificar código (p.ej.
    inyectando secrets en Kubernetes o Docker). Los valores literales actúan
    únicamente como fallback para el entorno de desarrollo local; en producción
    siempre deben definirse las variables de entorno correspondientes.
    """
    MONGO_USER        : str = os.getenv("MONGO_USER",        "juan_admin")
    MONGO_PASS        : str = os.getenv("MONGO_PASS",        "excelencia_2026")
    MONGO_HOST        : str = os.getenv("MONGO_HOST",        "localhost:27017")
    MONGO_DB          : str = os.getenv("MONGO_DB",          "smart_fleet_db")
    MONGO_AUTH_SOURCE : str = os.getenv("MONGO_AUTH_SOURCE", "admin")
    # Timeout de conexión TCP en milisegundos (3 segundos). Evita colgarse
    # indefinidamente si MongoDB no está disponible en el entorno.
    MONGO_CONNECT_TIMEOUT_MS : int = int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "3000"))

    @classmethod
    def get_uri(cls) -> str:
        """Construye la URI de conexión MongoDB de forma segura."""
        return (
            f"mongodb://{cls.MONGO_USER}:{cls.MONGO_PASS}"
            f"@{cls.MONGO_HOST}/?authSource={cls.MONGO_AUTH_SOURCE}"
        )


# ==============================================================================
# 1.3 CAPA DE INFRAESTRUCTURA — PATRÓN REPOSITORY
# ==============================================================================
class MongoAnalyticsRepository:
    """
    Abstracción de persistencia sobre la colección 'reportes_analiticos'.

    DECISIÓN ARQUITECTÓNICA (Patrón Repository + SRP):
    Este objeto es el ÚNICO punto del sistema con conocimiento de PyMongo.
    La capa de aplicación opera contra la interfaz pública (save_report / close)
    sin saber si la implementación subyacente es MongoDB, PostgreSQL o un stub
    de tests. Esto facilita el reemplazo tecnológico sin tocar la lógica de
    negocio (Principio Abierto/Cerrado — OCP).

    La conexión se valida explícitamente en __init__ para detectar fallos de
    infraestructura ANTES de que el costoso procesamiento Spark comience. Así
    el pipeline falla rápido (fail-fast) con un mensaje claro, evitando que
    el usuario espere minutos de ETL para recibir un error en la fase LOAD.
    """

    _COLLECTION_NAME = "analytics_reports"

    def __init__(self, uri: str, db_name: str) -> None:
        self._client = MongoClient(
            uri,
            serverSelectionTimeoutMS=DatabaseConfig.MONGO_CONNECT_TIMEOUT_MS,
        )
        # Ping inmediato: verifica que el servidor MongoDB es alcanzable.
        # Lanza ServerSelectionTimeoutError si no responde.
        self._client.admin.command("ping")
        self._db         = self._client[db_name]
        self._collection = self._db[self._COLLECTION_NAME]
        logger.info(
            "[INFRA] Conexión MongoDB establecida → db='%s' colección='%s'",
            db_name, self._COLLECTION_NAME,
        )

    def save_report(self, report_data: dict) -> str:
        """
        Persiste un documento analítico y retorna su ObjectId como string.

        Args:
            report_data: Diccionario Python serializable a BSON.

        Returns:
            str: Representación string del ObjectId insertado.

        Raises:
            OperationFailure: Si MongoDB rechaza la operación (permisos, etc.).
        """
        result = self._collection.insert_one(report_data)
        return str(result.inserted_id)

    def close(self) -> None:
        """Libera el pool de conexiones TCP de PyMongo de forma ordenada."""
        if self._client:
            self._client.close()
            logger.info("[INFRA] Conexión MongoDB cerrada correctamente.")


# ==============================================================================
# 2. CAPA DE DOMINIO — CONSTANTES Y REGLAS DE NEGOCIO
# ==============================================================================
class TelemetryConstants:
    """
    Umbrales de negocio inmutables para la flota de vehículos.

    DECISIÓN ARQUITECTÓNICA (DRY + Single Source of Truth):
    Centralizar los umbrales en una clase de dominio garantiza que cualquier
    componente del sistema —hoy PySpark, mañana un microservicio de alertas—
    use exactamente la misma definición. Cambiar SPEED_LIMIT_KMH aquí impacta
    automáticamente todas las fases del pipeline.

    Los valores están anotados con tipos y comentados con la unidad de medida
    para evitar la ambigüedad clásica de "¿es mph o km/h?".
    """
    # Límite de velocidad máxima permitida por regulación de flota [km/h]
    SPEED_LIMIT_KMH: float = 80.0

    # Umbral de frenado brusco: variación de velocidad ≤ -12 km/h por segundo.
    # Un valor negativo representa desaceleración. Se usa con col("aceleracion") <=
    # para capturar todos los valores iguales o más negativos que este umbral.
    HARD_BRAKING_THRESHOLD: float = -12.0


class RiskScoreWeights:
    """
    Ponderaciones de la Matriz de Riesgo del Conductor.

    DECISIÓN ARQUITECTÓNICA (OCP + Cohesión alta):
    Separar los pesos en una clase propia permite ajustar la matriz de riesgo
    (p.ej. en respuesta a cambios regulatorios) sin alterar el algoritmo de
    cálculo en PredictiveAnalyticsEngine. La suma de pesos DEBE ser 1.0;
    la assertion a continuación lo garantiza en tiempo de carga del módulo.
    """
    SPEEDING_WEIGHT     : float = 0.30   # Penalización por exceso de velocidad
    VARIANCE_WEIGHT     : float = 0.30   # Conducción errática (alta varianza)
    HARD_BRAKING_WEIGHT : float = 0.40   # Frenadas bruscas (mayor peligro)

# Invariante de dominio: los pesos deben sumar exactamente 1.0
assert abs(
    RiskScoreWeights.SPEEDING_WEIGHT
    + RiskScoreWeights.VARIANCE_WEIGHT
    + RiskScoreWeights.HARD_BRAKING_WEIGHT
    - 1.0
) < 1e-9, "Los pesos de RiskScoreWeights deben sumar 1.0"


# ==============================================================================
# 3. CAPA DE ANALÍTICA / IA — MOTOR PREDICTIVO
# ==============================================================================
class PredictiveAnalyticsEngine:
    """
    Motor de scoring predictivo basado en telemetría de conducción.

    DECISIÓN ARQUITECTÓNICA (SRP + Método estático puro):
    Esta clase no guarda estado; únicamente implementa la función matemática
    de transformación (métricas brutas → score). Al ser un método estático
    puro, es trivialmente testeable (no requiere mocks) y no introduce
    acoplamiento con la infraestructura. Un futuro modelo ML podría reemplazar
    este scoring heurístico sin que el resto del pipeline cambie.
    """

    @staticmethod
    def calculate_risk_score(
        alertas_exceso      : int,
        aceleracion_varianza: float,
        frenadas_bruscas    : int,
    ) -> float:
        """
        Calcula el Global Risk Score normalizado en el rango [0.0, 100.0].

        Metodología:
            1. Cada componente se escala de forma independiente a [0, 100]
               usando heurísticas calibradas en datos históricos de flota.
            2. Los componentes escalados se combinan linealmente usando las
               ponderaciones de RiskScoreWeights.
            3. El resultado final se sujeta a [0.0, 100.0] para prevenir
               desbordamientos aritméticos ante datos extremos o atípicos.

        Args:
            alertas_exceso:       Número de muestras con velocidad > SPEED_LIMIT_KMH.
            aceleracion_varianza: Varianza estadística de la aceleración [km/h²/s²].
            frenadas_bruscas:     Conteo de eventos con aceleración ≤ HARD_BRAKING_THRESHOLD.

        Returns:
            float: Score de riesgo en [0.0, 100.0]. Mayor valor = mayor riesgo.
        """
        # Componente 1 — Exceso de velocidad
        # Cada alerta aporta 5 puntos de riesgo. Saturamos en 100 para que un
        # conductor con 30+ alertas no genere un score artificialmente negativo
        # al multiplicar por el peso.
        score_exceso   = min(alertas_exceso * 5.0, 100.0)

        # Componente 2 — Varianza de aceleración
        # Convertimos la varianza de (km/h/s)² a (m/s²)² dividiendo por 12.96 (3.6²)
        # para alinearla con el motor distribuido y prevenir saturación permanente a 100.0.
        varianza_normalizada = aceleracion_varianza / 12.96
        score_varianza = min(varianza_normalizada * 2.0, 100.0)

        # Componente 3 — Frenadas bruscas
        # 10 frenadas bruscas equivalen al máximo de riesgo en esta componente,
        # reflejando que este evento es el más peligroso de los tres.
        score_frenadas = min(frenadas_bruscas * 10.0, 100.0)

        # Composición lineal ponderada
        risk_score_raw = (
            score_exceso   * RiskScoreWeights.SPEEDING_WEIGHT
            + score_varianza * RiskScoreWeights.VARIANCE_WEIGHT
            + score_frenadas * RiskScoreWeights.HARD_BRAKING_WEIGHT
        )

        # Barrera de seguridad final: clamp [0, 100] para prevenir cualquier
        # desbordamiento provocado por entradas inesperadamente altas.
        risk_score_clamped = max(0.0, min(risk_score_raw, 100.0))

        return round(risk_score_clamped, 2)


# ==============================================================================
# 4. CAPA DE APLICACIÓN — CASO DE USO (Use Case / Interactor)
# ==============================================================================
class TelemetryETLUseCase:
    """
    Orquestador del pipeline ETL de telemetría vehicular.

    DECISIÓN ARQUITECTÓNICA (Inyección de Dependencias + ISP):
    El constructor recibe la sesión Spark y el repositorio como argumentos
    (Dependency Injection). Esto permite:
      a) Testar la lógica del pipeline con un SparkSession local y un
         FakeRepository sin necesidad de MongoDB real.
      b) Cumplir el Principio de Inversión de Dependencias (DIP): la capa de
         aplicación depende de abstracciones (la interfaz pública del repo),
         no de la implementación concreta de PyMongo.

    El método execute() está dividido en fases explícitas (EXTRACT, TRANSFORM,
    AI-ANALYSIS, LOAD) con bloques try-except independientes para que un fallo
    en la fase de transformación no deje el pipeline en un estado ambiguo y
    siempre se ejecute el bloque finally de teardown.
    """

    def __init__(
        self,
        spark     : SparkSession,
        repository: MongoAnalyticsRepository,
        path_csv  : str,
    ) -> None:
        """
        Args:
            spark:      Sesión Spark activa (creada externamente y pasada aquí).
            repository: Implementación del repositorio de persistencia analítica.
            path_csv:   Ruta absoluta al archivo CSV del Data Lake local.
        """
        self._spark      = spark
        self._repository = repository
        self._path_csv   = path_csv

    # --------------------------------------------------------------------------
    # MÉTODO PRINCIPAL: execute
    # --------------------------------------------------------------------------
    def execute(self) -> None:
        """
        Ejecuta el pipeline ETL de principio a fin de forma resiliente.

        Estructura del pipeline:
            [EXTRACT]     → Lectura del Data Lake CSV local.
            [TRANSFORM]   → Limpieza y enriquecimiento con Window Functions.
            [AI-ANALYSIS] → Cálculo del Risk Score predictivo.
            [LOAD]        → Persistencia del reporte jerárquico en MongoDB.
        """
        logger.info("=" * 70)
        logger.info("  INICIANDO PIPELINE ETL — SmartFleet Pro Analytics v2.0")
        logger.info("=" * 70)

        df_raw: Optional[DataFrame] = None

        try:
            # ==================================================================
            # FASE [EXTRACT] — Extracción del Data Lake Local
            # ==================================================================
            logger.info("[EXTRACT] [START] Fase 1 de 4: Extracción desde el Data Lake...")

            # Verificación de existencia del archivo ANTES de invocar Spark.
            # Un error de Spark por archivo ausente genera un stack-trace de 50
            # líneas ilegible; este check produce un mensaje de error accionable.
            if not os.path.isfile(self._path_csv):
                raise FileNotFoundError(
                    f"[EXTRACT] El archivo de Data Lake no existe en la ruta: '{self._path_csv}'"
                )

            # inferSchema=True delega el parsing de tipos a la JVM de Spark,
            # evitando el costoso .cast() manual posterior.
            # header=True indica que la primera fila contiene los nombres de columnas.
            # IMPORTANTE: NO usamos .collect() en ningún punto de esta fase;
            # el DataFrame permanece como un plan de ejecución lazy hasta que
            # se requiere una Action (count, first, etc.).
            df_raw = (
                self._spark.read
                .option("header",      "true")
                .option("inferSchema", "true")
                # Manejo robusto del formato de timestamp con zona horaria UTC
                # que usa el simulador GPS: '2026-05-06 14:00:39.165+0000'
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSZ")
                .csv(self._path_csv)
            )

            # .count() es la única Action en la fase EXTRACT.
            # Desencadena la lectura real del archivo y valida el esquema.
            total_registros = df_raw.count()

            if total_registros == 0:
                raise ValueError(
                    "[EXTRACT] El archivo CSV fue leído pero no contiene registros. "
                    "Verifique que el archivo no está vacío."
                )

            logger.info("[EXTRACT] [OK] Registros cargados en el DataFrame: %d", total_registros)
            logger.info("[EXTRACT] [OK] Esquema inferido: %s", df_raw.schema.simpleString())

            # ==================================================================
            # FASE [TRANSFORM] — Limpieza y Enriquecimiento Distribuido
            # ==================================================================
            logger.info("[TRANSFORM] [START] Fase 2 de 4: Transformaciones con Window Functions...")

            # ------------------------------------------------------------------
            # 2.1 Métricas estadísticas básicas de velocidad
            # ------------------------------------------------------------------
            # Usamos .first() sobre un DataFrame ya agregado (no sobre df_raw
            # completo) para minimizar los datos transferidos al Driver.
            # NUNCA usamos .collect()[0] porque .collect() transfiere TODOS los
            # registros al Driver; .first() transfiere exactamente 1 fila.
            stats_vel_row = df_raw.select(
                F.avg("velocidad_kmh").alias("promedio"),
                F.max("velocidad_kmh").alias("maxima"),
            ).first()

            # Extracción defensiva con fallback a 0.0 ante NULLs inesperados
            velocidad_promedio = (
                round(float(stats_vel_row["promedio"]), 2)
                if stats_vel_row and stats_vel_row["promedio"] is not None
                else 0.0
            )
            velocidad_maxima = (
                round(float(stats_vel_row["maxima"]), 2)
                if stats_vel_row and stats_vel_row["maxima"] is not None
                else 0.0
            )

            # Conteo de alertas de exceso de velocidad (Action sobre el plan lazy)
            alertas_exceso = df_raw.filter(
                F.col("velocidad_kmh") > TelemetryConstants.SPEED_LIMIT_KMH
            ).count()

            logger.info(
                "[TRANSFORM] [OK] Velocidad — promedio: %.2f km/h | máxima: %.2f km/h | alertas>%g km/h: %d",
                velocidad_promedio, velocidad_maxima,
                TelemetryConstants.SPEED_LIMIT_KMH, alertas_exceso,
            )

            # ------------------------------------------------------------------
            # 2.2 Conversión de timestamp y cálculo de delta_v / delta_t
            #     mediante Window Functions nativas de PySpark (ejecución en JVM)
            # ------------------------------------------------------------------
            # DECISIÓN ARQUITECTÓNICA (por qué Window Functions y no UDFs Python):
            # • Las Window Functions se ejecutan COMPLETAMENTE en la JVM de Spark,
            #   sin serialización Python↔JVM por fila. Esto las hace órdenes de
            #   magnitud más rápidas que cualquier UDF Python o bucle .collect().
            # • partitionBy("id_viaje") garantiza que el cálculo de lag respete
            #   los límites de cada viaje; el lag NO cruzará entre viajes distintos.
            # • orderBy("tiempo_dt") asegura el orden cronológico correcto para
            #   que delta_v = v(t) - v(t-1) tenga sentido físico.
            window_spec = (
                Window
                .partitionBy("id_viaje")
                .orderBy("tiempo_dt")
            )

            # Performance Optimization: Repartition by partition key to minimize shuffle stages
            # aligned with Window partitionBy and subsequent aggregation
            df_repartitioned = df_raw.repartition("id_viaje")

            df_enriched = (
                df_repartitioned
                # Convertimos la columna 'tiempo' (string) a TimestampType.
                # to_timestamp es nativa de la JVM; no invoca código Python por fila.
                .withColumn("tiempo_dt", F.to_timestamp(F.col("tiempo")))

                # lag(..., 1) retorna el valor de la fila ANTERIOR en la ventana.
                # La primera fila de cada partición retorna NULL (no hay fila previa).
                .withColumn("prev_velocidad", F.lag("velocidad_kmh", 1).over(window_spec).cast("double"))
                .withColumn("prev_tiempo_dt", F.lag("tiempo_dt",    1).over(window_spec))

                # delta_v = variación de velocidad entre dos muestras consecutivas [km/h] with double precision
                .withColumn(
                    "delta_v", 
                    (F.col("velocidad_kmh").cast("double") - F.col("prev_velocidad"))
                )

                # delta_t = diferencia de tiempo entre muestras consecutivas [segundos] with sub-second precision
                # Casting Timestamp to double returns seconds with decimals, preventing sub-second truncation
                .withColumn(
                    "delta_t",
                    F.col("tiempo_dt").cast("double") - F.col("prev_tiempo_dt").cast("double"),
                )

                # Aceleración = delta_v / delta_t [km/h por segundo]
                # La condición delta_t > 0 previene división por cero cuando dos
                # muestras tienen el mismo timestamp (datos duplicados o con baja
                # resolución temporal).
                .withColumn(
                    "aceleracion",
                    F.when(F.col("delta_t") > 0, F.col("delta_v") / F.col("delta_t"))
                     .otherwise(F.lit(0.0).cast("double"))
                     .cast("double"),
                )

                # fillna reemplaza NULLs residuales de la primera fila de cada
                # partición (donde lag no tiene fila anterior) con 0.0.
                .fillna({"aceleracion": 0.0, "delta_v": 0.0, "delta_t": 0.0})
            )

            # ==================================================================
            # FASE [AI-ANALYSIS] — Modelado Predictivo del Risk Score
            # ==================================================================
            logger.info("[AI-ANALYSIS] [START] Fase 3 de 4: Cálculo del Risk Score predictivo...")

            # ------------------------------------------------------------------
            # 3.1 Varianza de aceleración: E[X²] - (E[X])²
            # ------------------------------------------------------------------
            # La fórmula de varianza E[X²] - (E[X])² (también llamada "fórmula
            # de Koenig-Huygens") permite calcularla en un ÚNICO paseo por los
            # datos sin necesidad de calcular primero la media y hacer un segundo
            # paseo para las desviaciones cuadráticas. Esto es crítico en Big Data:
            # un segundo paseo sobre un dataset grande es muy costoso en I/O.
            # 
            # Ambas medidas se calculan en la MISMA Action (.first()), lo que
            # hace que Spark ejecute SOLO UN JOB para esta fase.
            acc_stats_row = df_enriched.select(
                F.avg(F.col("aceleracion") * F.col("aceleracion")).alias("e_x2"),
                F.avg(F.col("aceleracion")).alias("e_x"),
            ).first()

            e_x2  = float(acc_stats_row["e_x2"]) if acc_stats_row and acc_stats_row["e_x2"] is not None else 0.0
            e_x   = float(acc_stats_row["e_x"])  if acc_stats_row and acc_stats_row["e_x"]  is not None else 0.0

            # Varianza = E[X²] - (E[X])²
            # Por errores de punto flotante, la varianza podría resultar
            # marginalmente negativa (p.ej. -1e-15). max(0.0, ...) la sujeta a 0.
            varianza_aceleracion = round(max(0.0, e_x2 - e_x ** 2), 4)

            # ------------------------------------------------------------------
            # 3.2 Detección de frenadas bruscas
            # ------------------------------------------------------------------
            # Un evento de frenada brusca se define como aceleración <= -12 km/h/s
            # según la normativa interna de flota (TelemetryConstants).
            # El filtro opera sobre el DataFrame enriquecido en la JVM; solo se
            # transfiere el conteo (un Long) al Driver.
            frenadas_bruscas = df_enriched.filter(
                F.col("aceleracion") <= TelemetryConstants.HARD_BRAKING_THRESHOLD
            ).count()

            # ------------------------------------------------------------------
            # 3.3 Cálculo del Risk Score global
            # ------------------------------------------------------------------
            risk_score = PredictiveAnalyticsEngine.calculate_risk_score(
                alertas_exceso       = alertas_exceso,
                aceleracion_varianza = varianza_aceleracion,
                frenadas_bruscas     = frenadas_bruscas,
            )

            logger.info("[AI-ANALYSIS] [OK] E[X]  (media aceleración): %.6f km/h/s", e_x)
            logger.info("[AI-ANALYSIS] [OK] E[X²] (media cuad. acel.): %.6f",         e_x2)
            logger.info("[AI-ANALYSIS] [OK] Varianza de aceleración  : %.4f (km/h/s)²", varianza_aceleracion)
            logger.info("[AI-ANALYSIS] [OK] Frenadas bruscas detectadas: %d evento(s)", frenadas_bruscas)
            logger.info("[AI-ANALYSIS] [OK] Risk Score Global calculado: %.2f / 100", risk_score)

            # ==================================================================
            # FASE [LOAD] — Persistencia del Reporte Analítico en MongoDB
            # ==================================================================
            logger.info("[LOAD] [START] Fase 4 de 4: Persistiendo reporte en MongoDB Atlas...")

            # ------------------------------------------------------------------
            # Construcción del payload jerárquico y semántico
            # ------------------------------------------------------------------
            # DECISIÓN ARQUITECTÓNICA (Document-Oriented Design):
            # El documento se organiza en tres subdocumentos embebidos:
            #   • 'metricas_basicas'  → KPIs operacionales de velocidad.
            #   • 'ia_predictiva'     → Outputs del motor analítico.
            #   • 'arquitectura'      → Trazabilidad del entorno de procesamiento.
            # Esta estructura facilita queries MongoDB como:
            #   db.analytics_reports.find({"ia_predictiva.score_riesgo": {$gt: 70}})
            # sin necesitar joins (que no existen en MongoDB).
            reporte_payload = {
                # Metadatos del reporte
                "proyecto"       : "SmartFleet_Pro",
                "version_pipeline": "2.0.0",
                # datetime con zona horaria UTC para evitar ambigüedad en entornos
                # multi-región (MongoDB serializa a ISODate internamente).
                "fecha_analisis" : datetime.now(tz=timezone.utc),

                # Subdocumento 1: KPIs básicos de la telemetría GPS
                "metricas_basicas": {
                    "total_muestras"        : total_registros,
                    "velocidad_promedio_kmh" : velocidad_promedio,
                    "velocidad_maxima_kmh"   : velocidad_maxima,
                    "alertas_exceso_velocidad": alertas_exceso,
                    "umbral_velocidad_kmh"   : TelemetryConstants.SPEED_LIMIT_KMH,
                },

                # Subdocumento 2: Outputs del motor de IA predictiva
                "ia_predictiva": {
                    "aceleracion_varianza_kmhs2" : varianza_aceleracion,
                    "e_x_media_aceleracion"      : round(e_x,  6),
                    "e_x2_media_cuad_aceleracion": round(e_x2, 6),
                    "frenadas_bruscas_count"     : frenadas_bruscas,
                    "umbral_frenado_kmhs"        : TelemetryConstants.HARD_BRAKING_THRESHOLD,
                    "score_riesgo_global"        : risk_score,
                    "ponderaciones_matriz": {
                        "exceso_velocidad" : RiskScoreWeights.SPEEDING_WEIGHT,
                        "varianza_acel"    : RiskScoreWeights.VARIANCE_WEIGHT,
                        "frenadas_bruscas" : RiskScoreWeights.HARD_BRAKING_WEIGHT,
                    },
                },

                # Subdocumento 3: Trazabilidad y metadatos de arquitectura
                "arquitectura": {
                    "motor_procesamiento": f"PySpark {self._spark.version} (JVM Native)",
                    "patron_etl"         : "Window Functions — Partitioned by id_viaje",
                    "algoritmo_varianza" : "Fórmula de Koenig-Huygens: E[X²] - (E[X])²",
                    "patron_persistencia": "Repository Pattern (PyMongo)",
                    "principios"         : "Clean Architecture / SOLID / 12-Factor App",
                    "entorno_java"       : os.environ.get("JAVA_HOME", "N/A"),
                    "entorno_hadoop"     : os.environ.get("HADOOP_HOME", "N/A"),
                },
            }

            inserted_id = self._repository.save_report(reporte_payload)

            # Banner de resumen final
            logger.info("=" * 70)
            logger.info("  [SUCCESS] ETL COMPLETADO — REPORTE ALMACENADO EN MONGODB")
            logger.info("=" * 70)
            logger.info("  - ID Documento MongoDB    : %s", inserted_id)
            logger.info("  - Registros Procesados    : %d", total_registros)
            logger.info("  - Velocidad Promedio      : %.2f km/h", velocidad_promedio)
            logger.info("  - Alertas Exceso Velocidad: %d", alertas_exceso)
            logger.info("  - Frenadas Bruscas        : %d", frenadas_bruscas)
            logger.info("  - Varianza Aceleración    : %.4f (km/h/s)²", varianza_aceleracion)
            logger.info("  - Risk Score Global       : %.2f / 100", risk_score)
            logger.info("=" * 70)

        except FileNotFoundError as fnf_err:
            # Error accionable: el operador sabe exactamente qué ruta revisar.
            logger.error("[EXTRACT] [ERROR] Archivo no encontrado: %s", fnf_err)
            raise

        except (ConnectionFailure, OperationFailure) as mongo_err:
            # Separamos los errores de MongoDB de los errores de negocio para
            # facilitar el triage en sistemas de monitoreo (alertas de infra vs.
            # alertas de calidad de datos).
            logger.error("[LOAD] [ERROR] Error de persistencia MongoDB: %s", mongo_err, exc_info=True)
            raise

        except Exception as pipeline_err:
            # Captura genérica como último recurso; loguea el traceback completo
            # para facilitar el análisis post-mortem.
            logger.error(
                "[PIPELINE] [ERROR] Error crítico no esperado en el pipeline ETL: %s",
                pipeline_err,
                exc_info=True,
            )
            raise

        finally:
            # El bloque finally se ejecuta SIEMPRE (éxito o fallo), garantizando
            # que la conexión TCP de MongoDB y la JVM de Spark se liberen de forma
            # ordenada. Sin esto, el proceso Python quedaría colgado esperando
            # el GC o el timeout del pool de conexiones.
            logger.info("[TEARDOWN] Iniciando cierre ordenado de conexiones de infraestructura...")
            try:
                self._repository.close()
            except Exception as close_err:
                logger.warning("[TEARDOWN] Advertencia al cerrar MongoDB: %s", close_err)
            try:
                self._spark.stop()
                logger.info("[TEARDOWN] [OK] SparkSession detenida correctamente.")
            except Exception as spark_err:
                logger.warning("[TEARDOWN] Advertencia al detener SparkSession: %s", spark_err)


# ==============================================================================
# ENTRYPOINT — Bootstrap y composición de dependencias
# ==============================================================================
if __name__ == "__main__":
    """
    DECISIÓN ARQUITECTÓNICA (Composition Root):
    El bloque __main__ es el único lugar donde se instancian y 'cablean' las
    dependencias concretas. Ninguna otra capa conoce las clases concretas; todas
    trabajan contra las abstracciones. Esto es el patrón "Composition Root" y
    garantiza que el grafo de dependencias sea visible y gestionable en un solo
    punto del código.
    """
    logger.info("Arrancando SmartFleet Pro Analytics Pipeline...")

    # Ruta al Data Lake local.
    # Estrategia de resolución (en orden de prioridad):
    #   1. Variable de entorno SMARTFLEET_DATA_LAKE_PATH → permite override en
    #      CI/CD, Docker o cualquier orquestador sin tocar el código.
    #   2. Fallback relativo a __file__ → el CSV se busca en el mismo directorio
    #      del script, independientemente del CWD desde el que se ejecute.
    #      Esto elimina el FileNotFoundError por rutas absolutas quemadas.
    _SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    RUTA_DATA_LAKE_CSV = os.getenv(
        "SMARTFLEET_DATA_LAKE_PATH",
        os.path.join(_SCRIPT_DIR, "telemetria.csv"),
    )

    try:
        # ------------------------------------------------------------------
        # Inicialización de la SparkSession (infraestructura pesada)
        # ------------------------------------------------------------------
        # DECISIÓN ARQUITECTÓNICA: getOrCreate() permite reutilizar una sesión
        # existente si el script se ejecuta dentro de un entorno que ya tiene
        # una SparkSession activa (p.ej. un notebook Jupyter con PySpark kernel).
        # spark.sql.execution.arrow.pyspark.enabled se deja en 'false' de forma
        # explícita porque la serialización Arrow requiere pyarrow instalado y
        # genera errores crípticos si no está disponible. Para este pipeline
        # no se usa toPandas(), por lo que Arrow no aporta ningún beneficio.
        # ── Flags de compatibilidad con Java 24 ────────────────────────────────
        # Java 9+ introdujo el sistema de módulos (JPMS) que bloquea el acceso
        # reflexivo entre módulos no declarados. Java 24 refuerza aún más estas
        # restricciones y además eliminó 'Subject.getSubject(AccessControlContext)'
        # de javax.security.auth, API interna que Hadoop 3.x sigue invocando.
        #
        # --add-opens=java.base/javax.security.auth=ALL-UNNAMED
        #   → Abre el paquete de seguridad JAAS al classpath no nombrado de Hadoop
        #     para que pueda acceder a Subject sin lanzar InaccessibleObjectException.
        #
        # --add-opens=java.base/java.lang=ALL-UNNAMED
        #   → Abre java.lang para la reflexión interna de Spark (acceso a campos
        #     privados de Thread, ClassLoader y String que PySpark necesita en
        #     modo local sobre Windows).
        #
        # Ambas flags se inyectan tanto en el Driver (proceso Python/JVM principal)
        # como en el Executor (workers) para cubrir todos los puntos de entrada JVM.
        _JVM_OPENS = (
            "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
            "--add-opens=java.base/java.lang=ALL-UNNAMED"
        )
        spark_session = (
            SparkSession.builder
            .appName("SmartFleet_Pro_Analytics_v2")
            .config("spark.driver.memory",  "2g")
            .config("spark.executor.memory", "2g")
            # Deshabilitamos la UI de Spark en el Driver para reducir el overhead
            # de memoria en máquinas con recursos limitados (laptops de desarrollo).
            .config("spark.ui.enabled", "false")
            # Forzamos un único hilo de red para serializar correctamente en Windows
            # local mode sin los problemas de winutils.exe multi-hilo.
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            # ── Java 24: apertura de módulos internos de la JVM ───────────────
            # Necesario para que Hadoop pueda inspeccionar javax.security.auth
            # y java.lang sin que el encapsulamiento fuerte de JPMS aborte la JVM.
            .config("spark.driver.extraJavaOptions",   _JVM_OPENS)
            .config("spark.executor.extraJavaOptions", _JVM_OPENS)
            # ── Mitigación de autenticación Hadoop en entorno local ────────────
            # Hadoop en modo 'simple' omite la resolución JAAS/Kerberos y la
            # llamada a Subject.getSubject() que fue removida en Java 24.
            # En producción con Kerberos, cambiar a "kerberos".
            .config("spark.hadoop.security.authentication", "simple")
            .getOrCreate()
        )
        # Silenciamos los logs INFO verbosos de la JVM de Spark; solo mostramos
        # WARNING y superiores para mantener la salida legible.
        spark_session.sparkContext.setLogLevel("WARN")

        logger.info("SparkSession creada — versión Spark: %s", spark_session.version)

        # ------------------------------------------------------------------
        # Inicialización del repositorio de persistencia
        # ------------------------------------------------------------------
        mongo_repo = MongoAnalyticsRepository(
            uri     = DatabaseConfig.get_uri(),
            db_name = DatabaseConfig.MONGO_DB,
        )

        # ------------------------------------------------------------------
        # Instanciación y ejecución del Use Case (Composition Root)
        # ------------------------------------------------------------------
        pipeline = TelemetryETLUseCase(
            spark      = spark_session,
            repository = mongo_repo,
            path_csv   = RUTA_DATA_LAKE_CSV,
        )
        pipeline.execute()

    except Exception as e:                 # [DEBUG] captura explícita con binding
        # ──────────────────────────────────────────────────────────────────────
        # [DEBUG MODE] — BLOQUE TEMPORAL DE DIAGNÓSTICO LOCAL
        # Este bloque reemplaza el handler de producción silencioso para exponer
        # la excepción real con tipo, mensaje y traceback completo en la consola.
        # REVERTIR antes de desplegar a producción: restaurar 'except Exception'
        # con exc_info=False y eliminar las líneas print/traceback.
        # ──────────────────────────────────────────────────────────────────────
        print("\n" + "═" * 70, flush=True)
        print("  [DEBUG] FALLO EN LA INICIALIZACIÓN — TRAZA COMPLETA", flush=True)
        print("═" * 70, flush=True)
        print(f"  Tipo de excepción : {type(e).__name__}", flush=True)
        print(f"  Mensaje           : {e}", flush=True)
        print("─" * 70, flush=True)
        traceback.print_exc()              # Imprime la traza completa a stderr
        print("═" * 70 + "\n", flush=True)
        logger.fatal(
            "FALLO CATASTRÓFICO en la inicialización — tipo: %s | msg: %s",
            type(e).__name__, e,
            exc_info=True,                 # [DEBUG] exc_info=True para que el logger también vuelque la traza
        )
        sys.exit(1)