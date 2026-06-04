import time
import random
from datetime import datetime
from cassandra.cluster import Cluster

def conectar_cassandra():
    # Nos conectamos al contenedor de Docker
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('fleet_telemetry')
    return session

def simular_movimiento(id_viaje, lat_inicio, lon_inicio):
    session = conectar_cassandra()
    print(f"--- Iniciando simulación para Viaje #{id_viaje} ---")
    
    # Simularemos 50 puntos de recorrido
    lat, lon = lat_inicio, lon_inicio
    
    for i in range(50):
        # Simulamos un pequeño movimiento aleatorio
        lat += random.uniform(-0.01, 0.01)
        lon += random.uniform(-0.01, 0.01)
        velocidad = random.uniform(60.0, 90.0)
        ahora = datetime.now()

        # Query de inserción en Cassandra
        query = """
            INSERT INTO telemetria_gps (id_viaje, tiempo, latitud, longitud, velocidad)
            VALUES (%s, %s, %s, %s, %s)
        """
        session.execute(query, (id_viaje, ahora, lat, lon, velocidad))
        
        print(f"Punto {i+1}: Lat {lat:.4f}, Lon {lon:.4f} a {velocidad:.1f} km/h")
        time.sleep(0.1) # Pausa de medio segundo entre señales

    print("--- Simulación completada con éxito ---")

if __name__ == "__main__":
    # Usamos el id_viaje que creamos en Cassandra/Postgres
    simular_movimiento(id_viaje="V-UTP-2026", lat_inicio=-12.0464, lon_inicio=-77.0428)