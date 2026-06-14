CREATE KEYSPACE IF NOT EXISTS fleet_telemetry
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE fleet_telemetry;

CREATE TABLE IF NOT EXISTS telemetria_gps (
    id_viaje text,
    tiempo timestamp,
    latitud double,
    longitud double,
    velocidad double,
    PRIMARY KEY (id_viaje, tiempo)
) WITH default_time_to_live = 600;
