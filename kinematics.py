"""
Computes Davis tractive dynamics, kinematic integration, and speed advisory profiles.
"""
import math

class KinematicsEngine:
    A = 1.8   # Rolling resistance (N/kN)
    B = 0.01  # Mechanical resistance (N/kN per km/h)
    C = 0.0004 # Aerodynamic drag coefficient
    
    SERVICE_DECELERATION = 0.65  # m/s^2
    EMERGENCY_DECELERATION = 1.1 # m/s^2 (Kavach / TCAS benchmark)
    ACCELERATION = 0.45          # m/s^2

    @classmethod
    def calculate_davis_resistance(cls, mass_tons: float, speed_kmh: float) -> float:
        """Returns total track resistance in Newtons."""
        weight_kn = mass_tons * 9.81
        specific_res = cls.A + (cls.B * speed_kmh) + (cls.C * (speed_kmh ** 2))
        return specific_res * weight_kn

    @classmethod
    def compute_braking_distance(cls, speed_kmh: float, emergency: bool = False) -> float:
        """Calculates distance required to brake to a complete stop."""
        v_ms = speed_kmh / 3.6
        decel = cls.EMERGENCY_DECELERATION if emergency else cls.SERVICE_DECELERATION
        return (v_ms ** 2) / (2.0 * decel)

    @classmethod
    def calculate_optimal_target_speed(
        cls, current_dist_to_target_m: float, section_mps_kmh: float, target_speed_limit_kmh: float
    ) -> tuple[float, str]:
        """
        Dynamically recommends target speed (V_target) to coast or brake safely.
        """
        v_target_limit_ms = target_speed_limit_kmh / 3.6
        required_braking_dist = cls.compute_braking_distance(section_mps_kmh)

        if current_dist_to_target_m <= required_braking_dist * 1.1:
            # Within braking curve zone
            safe_v_ms = math.sqrt(max(0.0, (v_target_limit_ms ** 2) + 2 * cls.SERVICE_DECELERATION * current_dist_to_target_m))
            target_kmh = min(section_mps_kmh, safe_v_ms * 3.6)
            return round(target_kmh, 1), "SERVICE_BRAKE_ACTIVE"
        elif current_dist_to_target_m <= required_braking_dist * 2.0:
            return round(section_mps_kmh * 0.85, 1), "COASTING"
        
        return round(section_mps_kmh, 1), "MAX_SECTION_SPEED"