"""
speed_advisor.py — Zero-Delay Speed Optimizer & Priority-Aware Scheduler.

For each train approaching a station, computes:
  1. Current delay vs timetable (positive = late, negative = early)
  2. Required speed to arrive exactly on scheduled time
  3. Feasibility against Max Permissible Speed (MPS)
  4. Priority conflict resolution: lower-priority trains yield platform slots
     to higher-priority trains with a 3-minute headway buffer
"""
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# ── Delay status labels ───────────────────────────────────────────────────────
ON_TIME                = "ON_TIME"
RECOVERABLE            = "RECOVERABLE"
MARGINALLY_RECOVERABLE = "MARGINALLY_RECOVERABLE"
UNRECOVERABLE          = "UNRECOVERABLE"
ARRIVED                = "ARRIVED"
YIELDING               = "YIELDING"

DELAY_COLOR = {
    ON_TIME:                "#22c55e",
    RECOVERABLE:            "#3b82f6",
    MARGINALLY_RECOVERABLE: "#f97316",
    UNRECOVERABLE:          "#ef4444",
    ARRIVED:                "#6b7280",
    YIELDING:               "#a855f7",
}

PLATFORM_HEADWAY_SEC = 180   # 3-min minimum gap between trains on same platform


class SpeedAdvisor:
    """Computes speed advisories and delay status for all approaching trains."""

    @classmethod
    def compute_advisories(
        cls,
        trains: List[Dict[str, Any]],
        sim_seconds: float,
    ) -> Dict[str, Dict]:
        """
        Parameters
        ----------
        trains      : list of train state dicts from SimulationSession
        sim_seconds : elapsed simulation time in seconds (starts at 0 on load)

        Returns
        -------
        dict[train_id -> advisory_dict]
        """
        results: Dict[str, Dict] = {}

        # Sort by priority: tier ASC (1=highest), then scheduled arrival ASC
        active = [t for t in trains if float(t.get("dist_remaining", 0)) > 0]
        active.sort(key=lambda x: (
            x.get("tier", 5),
            x.get("scheduled_arrival_offset_sec", 99999)
        ))

        # Platform occupancy tracker: pf_num -> (predicted_arrival_sec, train_id)
        pf_occupancy: Dict[int, tuple] = {}

        for rank, train in enumerate(active, 1):
            tid         = train["id"]
            dist_rem    = float(train.get("dist_remaining", 0))
            cur_speed   = float(train.get("current_speed", 60))
            mps         = float(train.get("mps", 100))
            sched_sec   = float(train.get("scheduled_arrival_offset_sec", 300))
            alloc_pf    = int(train.get("allocated_pf", 1))

            if dist_rem <= 0:
                results[tid] = cls._make(ARRIVED, 0.0, 0.0, "At platform", rank, "Arrived")
                continue

            # Ensure current speed has a reasonable floor so division never blows up
            safe_speed_ms = max(cur_speed / 3.6, 0.5)

            # Predicted arrival: sim_seconds + travel time at current speed
            travel_secs   = dist_rem / safe_speed_ms
            pred_arrival  = sim_seconds + travel_secs

            # Delay = predicted - scheduled (positive = late)
            delay_sec  = pred_arrival - sched_sec
            delay_min  = delay_sec / 60.0

            # Time remaining to scheduled arrival
            time_to_sched = sched_sec - sim_seconds

            # Required speed to arrive exactly on time
            if time_to_sched > 5.0:
                req_kmh = (dist_rem / time_to_sched) * 3.6
            else:
                # Already past or within 5 seconds of schedule — run at MPS
                req_kmh = mps

            # ── Platform conflict check ────────────────────────────────────
            conflict_speed: Optional[float] = None
            conflict_with: str = ""
            if alloc_pf in pf_occupancy:
                prev_arr, prev_id = pf_occupancy[alloc_pf]
                gap = pred_arrival - prev_arr
                if abs(gap) < PLATFORM_HEADWAY_SEC:
                    # This train is lower priority — defer it by HEADWAY
                    safe_arr       = prev_arr + PLATFORM_HEADWAY_SEC
                    safe_time_sec  = safe_arr - sim_seconds
                    if safe_time_sec > 5.0:
                        conflict_ms    = dist_rem / safe_time_sec
                        conflict_speed = max(10.0, min(conflict_ms * 3.6, mps * 0.9))
                        delay_min      = (safe_arr - sched_sec) / 60.0
                        pred_arrival   = safe_arr
                    conflict_with = prev_id

            pf_occupancy[alloc_pf] = (pred_arrival, tid)

            # ── Build action text (ASCII-safe, no Unicode arrows) ──────────
            if conflict_speed is not None:
                advised = round(conflict_speed, 1)
                status  = YIELDING
                action  = (f"Hold at {advised:.0f} km/h — platform yield to "
                           f"priority train {conflict_with} (+{delay_min:.0f}min wait)")
            elif req_kmh <= 10.0:
                advised = 5.0
                status  = ON_TIME
                action  = "Brake — entering platform now"
            elif abs(delay_min) < 0.8 and req_kmh <= mps:
                advised = round(req_kmh, 1)
                status  = ON_TIME
                action  = f"Maintain {advised:.0f} km/h — on schedule"
            elif req_kmh <= mps:
                advised = round(req_kmh, 1)
                status  = RECOVERABLE
                action  = (f"Accelerate to {advised:.0f} km/h to recover "
                           f"+{delay_min:.1f} min delay — fully recoverable")
            elif req_kmh <= mps * 1.08:
                advised = round(mps, 1)
                residual = round(max(0, delay_min - (dist_rem / (mps / 3.6) - max(time_to_sched, 0)) / 60.0), 1)
                status  = MARGINALLY_RECOVERABLE
                action  = (f"Run at MPS {advised:.0f} km/h — "
                           f"~{residual} min residual delay expected")
            else:
                advised = round(mps, 1)
                residual = round(max(0, (pred_arrival - sched_sec) / 60.0), 1)
                status  = UNRECOVERABLE
                action  = (f"Run at MPS {advised:.0f} km/h — "
                           f"delay unrecoverable, ~{residual} min late")

            # Wall-clock predicted arrival time
            try:
                pred_wall = datetime.now() + timedelta(seconds=travel_secs)
                pred_str  = pred_wall.strftime("%H:%M:%S")
            except Exception:
                pred_str = "--:--"

            results[tid] = cls._make(status, round(delay_min, 1), advised, action, rank, pred_str)

        # Fill in ARRIVED for berthed/missing trains
        for t in trains:
            if t["id"] not in results:
                results[t["id"]] = cls._make(ARRIVED, 0.0, 0.0, "At platform", 999, "Arrived")

        return results

    @staticmethod
    def _make(status: str, delay_min: float, advised: float,
              action: str, rank: int, pred_str: str) -> Dict:
        return {
            "delay_status":          status,
            "delay_min":             delay_min,
            "delay_color":           DELAY_COLOR.get(status, "#6b7280"),
            "advised_speed":         advised,
            "required_speed":        advised,
            "action_text":           action,
            "priority_rank":         rank,
            "predicted_arrival_str": pred_str,
        }
