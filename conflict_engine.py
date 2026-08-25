"""
conflict_engine.py — Spatiotemporal Conflict Predictor & Kavach-Style Safety Engine.
Predicts collision risk between trains on the same block section and
generates specific corrective speed commands for both trains.
"""
import math
from typing import Dict, Any, List

SERVICE_DECEL   = 0.65   # m/s²
EMERGENCY_DECEL = 1.1    # m/s²  (Kavach TCAS benchmark)
KAVACH_BUFFER_M = 300.0  # mandatory clearance buffer behind lead train


def _safe_trail_speed(separation_m: float) -> float:
    """Max speed (km/h) the trailing train can safely run given the current separation."""
    usable_sep = max(0.0, separation_m - KAVACH_BUFFER_M)
    v_ms       = math.sqrt(2 * SERVICE_DECEL * usable_sep)
    return round(v_ms * 3.6, 1)


class ConflictRiskEngine:
    KAVACH_BUFFER_METERS = KAVACH_BUFFER_M

    @classmethod
    def evaluate_risk(
        cls,
        lead_train_dist_m: float,
        lead_train_speed_kmh: float,
        trail_train_dist_m: float,
        trail_train_speed_kmh: float,
        same_track: bool,
        lead_id: str = "LEAD",
        trail_id: str = "TRAIL",
    ) -> Dict[str, Any]:
        """
        Evaluates collision risk and produces actionable commands for both trains.

        Returns dict with:
          risk_score      : 0.0 (safe) → 1.0 (imminent collision)
          status          : NOMINAL_CLEAR | CAUTION_CONVERGING | CRITICAL_CONFLICT | HARD_INTERLOCK_VIOLATION
          tti_seconds     : estimated time to collision (∞ if no threat)
          recommended_action: {lead_id: str, trail_id: str}
          consequence     : plain-English description of the hazard
        """
        if not same_track:
            return {
                "risk_score": 0.0,
                "status": "NOMINAL_CLEAR",
                "tti_seconds": 9999.0,
                "recommended_action": {},
                "consequence": "Trains on separate tracks — no conflict.",
            }

        separation   = abs(lead_train_dist_m - trail_train_dist_m)
        v_trail_ms   = trail_train_speed_kmh / 3.6
        v_lead_ms    = lead_train_speed_kmh  / 3.6
        rel_speed_ms = v_trail_ms - v_lead_ms   # positive → converging

        # Braking envelope for trail train
        braking_env = (v_trail_ms ** 2) / (2 * SERVICE_DECEL) + KAVACH_BUFFER_M

        # ── Hard interlock — inside Kavach buffer ─────────────────────────
        if separation <= KAVACH_BUFFER_M:
            return {
                "risk_score": 1.0,
                "status": "HARD_INTERLOCK_VIOLATION",
                "tti_seconds": 0.0,
                "recommended_action": {
                    lead_id:  f"Accelerate immediately to {min(lead_train_speed_kmh + 20, 130):.0f} km/h — clear block",
                    trail_id: "EMERGENCY BRAKE — apply full braking now. Speed must reach 0.",
                },
                "consequence": (
                    f"Trains are only {separation:.0f}m apart — inside Kavach safety envelope. "
                    f"Imminent collision if no action."
                ),
            }

        # ── Critical — inside braking envelope ───────────────────────────
        if separation <= braking_env:
            tti = (separation / rel_speed_ms) if rel_speed_ms > 0.5 else 999.0
            risk = round(min(0.99, (braking_env - separation) / braking_env + 0.5), 2)
            safe_spd = _safe_trail_speed(separation)
            return {
                "risk_score": risk,
                "status": "CRITICAL_CONFLICT",
                "tti_seconds": round(tti, 1),
                "recommended_action": {
                    lead_id:  (f"Maintain or increase speed above {lead_train_speed_kmh:.0f} km/h — "
                               f"clear block section immediately"),
                    trail_id: (f"Reduce to {safe_spd:.0f} km/h immediately — "
                               f"current separation {separation:.0f}m is within braking envelope"),
                },
                "consequence": (
                    f"Collision predicted in ~{tti:.0f}s at current speeds. "
                    f"{trail_id} must brake to ≤{safe_spd:.0f} km/h to maintain Kavach compliance."
                ),
            }

        # ── Caution — approaching braking zone ───────────────────────────
        if separation <= braking_env * 1.6:
            safe_spd = _safe_trail_speed(separation)
            return {
                "risk_score": 0.25,
                "status": "CAUTION_CONVERGING",
                "tti_seconds": 180.0,
                "recommended_action": {
                    lead_id:  "Maintain current speed.",
                    trail_id: (f"Reduce to {safe_spd:.0f} km/h — "
                               f"trains converging, approaching braking zone"),
                },
                "consequence": (
                    f"Trains converging on same section. Separation: {separation / 1000:.1f}km. "
                    f"Monitor speed difference."
                ),
            }

        # ── Nominal ───────────────────────────────────────────────────────
        return {
            "risk_score": 0.0,
            "status": "NOMINAL_CLEAR",
            "tti_seconds": float("inf"),
            "recommended_action": {},
            "consequence": f"Clear — {separation / 1000:.1f}km separation, safe headway maintained.",
        }

    @classmethod
    def evaluate_corridor(cls, corridor_trains: List[Dict]) -> List[Dict]:
        """
        Runs pairwise conflict evaluation for all consecutive trains on a corridor.
        Returns list of alert dicts (empty if all clear).
        """
        alerts = []
        if len(corridor_trains) < 2:
            return alerts

        # Sort by distance remaining (lead = smallest dist first)
        sorted_trains = sorted(corridor_trains, key=lambda t: float(t.get("dist_remaining", 0)))

        for i in range(len(sorted_trains) - 1):
            lead  = sorted_trains[i]
            trail = sorted_trains[i + 1]

            result = cls.evaluate_risk(
                lead_train_dist_m    = float(lead.get("dist_remaining", 0)),
                lead_train_speed_kmh = float(lead.get("current_speed", 0)),
                trail_train_dist_m   = float(trail.get("dist_remaining", 0)),
                trail_train_speed_kmh= float(trail.get("current_speed", 0)),
                same_track           = True,
                lead_id              = lead["id"],
                trail_id             = trail["id"],
            )

            if result["status"] != "NOMINAL_CLEAR":
                alerts.append({
                    "lead_id":   lead["id"],
                    "trail_id":  trail["id"],
                    "lead_name": lead.get("name", lead["id"]),
                    "trail_name":trail.get("name", trail["id"]),
                    **result,
                })

        return alerts