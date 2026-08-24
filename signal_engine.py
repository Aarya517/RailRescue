"""
signal_engine.py — 4-Aspect Color Light Signal Engine (Indian Railway Model).
Computes signal aspects per train based on block occupancy and Kavach headway rules.

IR Signal Aspects:
  CLEAR        — Green  : Proceed at full section speed
  ATTENTION    — Yellow : Next signal may be at Caution; be prepared to reduce speed
  CAUTION      — Double Yellow : Reduce speed; prepare to stop at next signal
  DANGER       — Red    : STOP — block ahead occupied within braking envelope
"""
from typing import List, Dict, Any

# ─── Signal aspect constants ───────────────────────────────────────────────────
CLEAR     = "CLEAR"
ATTENTION = "ATTENTION"
CAUTION   = "CAUTION"
DANGER    = "DANGER"
BERTHED   = "BERTHED"

ASPECT_COLOR = {
    CLEAR:     "#22c55e",  # green-500
    ATTENTION: "#eab308",  # yellow-500
    CAUTION:   "#f97316",  # orange-500
    DANGER:    "#ef4444",  # red-500
    BERTHED:   "#6b7280",  # gray-500
}
ASPECT_ICON = {
    CLEAR: "🟢", ATTENTION: "🟡", CAUTION: "🟠", DANGER: "🔴", BERTHED: "⬜",
}
# Max allowed speed at each aspect (km/h) — IR rule of thumb
ASPECT_MAX_SPEED = {
    CLEAR: None,   # full section MPS
    ATTENTION: None,  # MPS × 0.85 (computed dynamically)
    CAUTION:  45.0,
    DANGER:    0.0,
    BERTHED:   0.0,
}

KAVACH_BUFFER_M  = 300.0   # Kavach / TCAS mandatory buffer behind lead train (m)
SERVICE_DECEL    = 0.65    # m/s² — service braking deceleration


def _braking_dist(speed_kmh: float) -> float:
    """Returns service braking distance (m) from given speed."""
    v_ms = speed_kmh / 3.6
    return (v_ms ** 2) / (2 * SERVICE_DECEL) if v_ms > 0 else 0.0


class SignalEngine:
    """Computes 4-aspect signal states for all active trains."""

    @classmethod
    def compute_signals(cls, trains: List[Dict[str, Any]]) -> Dict[str, Dict]:
        """
        Groups trains by corridor, sorts closest-to-station first (lead train),
        then assigns signal aspect based on headway between consecutive trains.

        Returns: dict[train_id -> signal_info_dict]
        """
        results: Dict[str, Dict] = {}

        # Group by corridor
        corridors: Dict[str, List[Dict]] = {}
        for t in trains:
            c = t.get("corridor", "UNKNOWN")
            corridors.setdefault(c, []).append(t)

        for corridor_name, c_trains in corridors.items():
            # Sort ascending by dist_remaining → index 0 is lead (closest to station)
            c_trains.sort(key=lambda x: float(x.get("dist_remaining", 0)))

            for idx, train in enumerate(c_trains):
                tid          = train["id"]
                dist_rem     = float(train.get("dist_remaining", 0))
                speed_kmh    = float(train.get("current_speed", 0))
                mps_kmh      = float(train.get("mps", 100))

                # ── Berthed trains ──────────────────────────────────────────
                if dist_rem <= 0:
                    results[tid] = cls._make(
                        tid, BERTHED, dist_rem / 1000,
                        "Train berthed at platform", 0.0
                    )
                    continue

                brk = _braking_dist(speed_kmh)

                # ── Lead train (no train ahead) ─────────────────────────────
                if idx == 0:
                    if dist_rem <= brk * 1.2:
                        aspect = ATTENTION
                        reason = "Approaching platform — reduce to 30 km/h for entry"
                    else:
                        aspect = CLEAR
                        reason = "Block section ahead is clear — proceed at section speed"
                    results[tid] = cls._make(tid, aspect, dist_rem / 1000, reason, mps_kmh)
                    continue

                # ── Trailing trains — compute headway from next train ahead ─
                lead         = c_trains[idx - 1]
                lead_dist    = float(lead.get("dist_remaining", 0))
                headway_m    = dist_rem - lead_dist          # separation in metres
                safe_hw      = brk + KAVACH_BUFFER_M         # minimum safe headway

                if headway_m <= KAVACH_BUFFER_M:
                    aspect = DANGER
                    reason = (f"Train {lead['id']} is {headway_m:.0f}m ahead — "
                              f"KAVACH HARD INTERLOCK VIOLATION")
                elif headway_m <= safe_hw:
                    aspect = DANGER
                    reason = (f"Train {lead['id']} is {headway_m / 1000:.2f}km ahead — "
                              f"inside braking envelope ({brk:.0f}m needed)")
                elif headway_m <= safe_hw * 1.5:
                    aspect = CAUTION
                    reason = (f"Train {lead['id']} is {headway_m / 1000:.2f}km ahead — "
                              f"approaching braking zone, reduce speed now")
                elif headway_m <= safe_hw * 2.5:
                    aspect = ATTENTION
                    reason = (f"Train {lead['id']} is {headway_m / 1000:.2f}km ahead — "
                              f"maintain vigilance, next signal may be Caution")
                else:
                    aspect = CLEAR
                    reason = (f"Clear — {lead['id']} is {headway_m / 1000:.2f}km ahead, "
                              f"safe headway maintained")

                # Compute max speed allowed at this aspect
                if aspect == CLEAR:
                    max_spd = mps_kmh
                elif aspect == ATTENTION:
                    max_spd = round(mps_kmh * 0.85, 1)
                elif aspect == CAUTION:
                    max_spd = 45.0
                else:
                    max_spd = 0.0

                results[tid] = cls._make(tid, aspect, dist_rem / 1000, reason, max_spd)

        return results

    @staticmethod
    def _make(tid: str, aspect: str, km_from_station: float,
              reason: str, max_speed: float) -> Dict:
        return {
            "signal_aspect":         aspect,
            "signal_color":          ASPECT_COLOR[aspect],
            "signal_icon":           ASPECT_ICON[aspect],
            "signal_km_from_station": round(km_from_station, 2),
            "signal_reason":         reason,
            "signal_max_speed":      max_speed,
        }
