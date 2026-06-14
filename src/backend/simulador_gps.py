import os
import time
import random
from datetime import datetime
from cassandra.cluster import Cluster

# ---------------------------------------------------------------------------
# Constantes de dominio — deben coincidir con TelemetryConstants y
# RiskScoreWeights definidos en src/backend/domain/constants.py
# ---------------------------------------------------------------------------
SPEED_LIMIT_KMH       = 90.0   # Umbral de exceso de velocidad
HARD_BRAKING_KMH_S    = -12.0  # Umbral de frenada brusca (aceleracion km/h/s)

# Parametros de comportamiento del simulador
NORMAL_SPEED_MIN      = 55.0
NORMAL_SPEED_MAX      = 88.0
SPEEDING_SPEED_MIN    = 95.0   # Excede SPEED_LIMIT_KMH con margen
SPEEDING_SPEED_MAX    = 118.0
SPEEDING_PROBABILITY  = 0.30   # 30% de iteraciones son exceso de velocidad
HARD_BRAKE_PROBABILITY= 0.15   # 15% adicional son frenadas bruscas (delta < -12 km/h/s)
# ---------------------------------------------------------------------------


def conectar_cassandra():
    cassandra_host = os.getenv("CASSANDRA_HOST", "127.0.0.1:9042")
    host = cassandra_host
    port = 9042
    if ":" in cassandra_host:
        h, p = cassandra_host.split(":")
        host = h
        port = int(p)
    cluster = Cluster([host], port=port)
    session = cluster.connect('fleet_telemetry')
    return session


def simular_movimiento(id_viaje, lat_inicio, lon_inicio):
    session = conectar_cassandra()
    print(f"--- Iniciando simulacion para Viaje #{id_viaje} ---")

    lat, lon = lat_inicio, lon_inicio

    # La velocidad anterior se rastrean para calcular el delta de aceleracion
    # que Spark derivara en el ETL. Se inicializa en rango normal.
    velocidad_previa = random.uniform(NORMAL_SPEED_MIN, NORMAL_SPEED_MAX)

    for i in range(50):
        # Movimiento geografico incremental
        lat += random.uniform(-0.01, 0.01)
        lon += random.uniform(-0.01, 0.01)

        # --- Motor de inyeccion de anomalias ---
        roll = random.random()

        if roll < HARD_BRAKE_PROBABILITY:
            # FRENADA BRUSCA: la velocidad cae abruptamente desde un valor
            # alto para que el delta supere el umbral de -12 km/h/s.
            # delta_target < HARD_BRAKING_KMH_S = -12.0
            velocidad_alta = random.uniform(80.0, 100.0)
            # Con un intervalo de 0.1 s entre muestras, un delta de -15 km/h
            # equivale a -150 km/h/s en el calculo del ETL. Dado que el ETL
            # calcula la aceleracion como (v_actual - v_anterior) / dt, usar
            # una caida de al menos 1.5 km/h con dt=0.1s -> -15 km/h/s.
            velocidad = velocidad_alta - random.uniform(1.5, 8.0)
            velocidad_previa = velocidad_alta  # el punto anterior tenia la velocidad alta
            evento = "FRENADA_BRUSCA"

        elif roll < HARD_BRAKE_PROBABILITY + SPEEDING_PROBABILITY:
            # EXCESO DE VELOCIDAD: supera el limite de 90 km/h con margen
            velocidad = random.uniform(SPEEDING_SPEED_MIN, SPEEDING_SPEED_MAX)
            velocidad_previa = velocidad
            evento = "EXCESO_VELOCIDAD"

        else:
            # COMPORTAMIENTO NOMINAL
            velocidad = random.uniform(NORMAL_SPEED_MIN, NORMAL_SPEED_MAX)
            velocidad_previa = velocidad
            evento = "NOMINAL"

        ahora = datetime.now()

        # Insercion en Cassandra con TTL de 600 segundos (10 minutos)
        query = """
            INSERT INTO telemetria_gps (id_viaje, tiempo, latitud, longitud, velocidad)
            VALUES (%s, %s, %s, %s, %s) USING TTL 600
        """
        session.execute(query, (id_viaje, ahora, lat, lon, float(velocidad)))

        print(f"Punto {i+1:02d} [{evento:18s}]: Lat {lat:.4f}, Lon {lon:.4f} | {velocidad:.1f} km/h")
        time.sleep(0.1)

    print("--- Simulacion completada con exito ---")


if __name__ == "__main__":
    simular_movimiento(id_viaje="TRIP-MANUAL-TEST-99", lat_inicio=-12.0464, lon_inicio=-77.0428)