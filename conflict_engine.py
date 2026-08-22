"""
Predicts spatiotemporal conflicts and computes collision risk scores.
"""
from typing import Dict, Any

class ConflictRiskEngine:
    KAVACH_BUFFER_METERS = 300.0

    @classmethod
    def evaluate_risk(
        cls, 
        lead_train_dist_m: float, 
        lead_train_speed_kmh: float,
        trail_train_dist_m: float, 
        trail_train_speed_kmh: float,
        same_track: bool
    ) -> Dict[str, Any]:
        """
        Evaluates risk score (0.0 to 1.0) and time-to-conflict between two trains.
        """
        if not same_track:
            return {"risk_score": 0.0, "status": "NOMINAL_CLEAR", "tti_seconds": float('inf')}

        separation = abs(lead_train_dist_m - trail_train_dist_m)
        relative_speed_ms = (trail_train_speed_kmh - lead_train_speed_kmh) / 3.6

        # Safety envelope
        v_trail_ms = trail_train_speed_kmh / 3.6
        braking_envelope = (v_trail_ms ** 2) / (2 * 0.65) + cls.KAVACH_BUFFER_METERS

        if separation <= cls.KAVACH_BUFFER_METERS:
            return {"risk_score": 1.0, "status": "HARD_INTERLOCK_VIOLATION", "tti_seconds": 0.0}
        
        if separation < braking_envelope:
            risk = min(0.99, (braking_envelope - separation) / braking_envelope + 0.5)
            tti = separation / relative_speed_ms if relative_speed_ms > 0 else 999.0
            return {"risk_score": round(risk, 2), "status": "CRITICAL_CONFLICT", "tti_seconds": round(tti, 1)}
        
        if separation < braking_envelope * 1.5:
            return {"risk_score": 0.25, "status": "CAUTION_CONVERGING", "tti_seconds": 180.0}

        return {"risk_score": 0.0, "status": "NOMINAL_CLEAR", "tti_seconds": float('inf')}