class TelemetryConstants:
    """
    Constantes y límites geográficos y de velocidad para la telemetría de la flota.
    """
    LATITUDE_MIN: float = -90.0
    LATITUDE_MAX: float = 90.0
    LONGITUDE_MIN: float = -180.0
    LONGITUDE_MAX: float = 180.0
    SPEED_LIMIT_KMH: float = 90.0


class RiskScoreWeights:
    """
    Ponderaciones de riesgo aplicadas al cálculo del score de conducción.
    La suma de todas las ponderaciones debe ser exactamente 1.0.
    """
    VARIANCE_WEIGHT: float = 0.40      # Peso de la varianza de aceleración
    SPEEDING_WEIGHT: float = 0.35      # Peso de alertas por exceso de velocidad
    HARD_BRAKING_WEIGHT: float = 0.25  # Peso de frenadas bruscas

# Invariante de dominio: validación de coherencia de pesos
assert abs(
    RiskScoreWeights.VARIANCE_WEIGHT
    + RiskScoreWeights.SPEEDING_WEIGHT
    + RiskScoreWeights.HARD_BRAKING_WEIGHT
    - 1.0
) < 1e-9, "Los pesos de RiskScoreWeights deben sumar exactamente 1.0"
