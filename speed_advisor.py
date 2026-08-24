"""
speed_advisor.py — Zero-Delay Speed Optimizer & Priority-Aware Scheduler.

For each train approaching a station, computes:
  1. Current delay vs timetable (positive = late, negative = early)
  2. Required speed to arrive exactly on scheduled time
  3. Feasibility against Max Permissible Speed (MPS)
  4. Priority conflict resolution: lower-priority trains yield platform slots
     to higher-priority trains with a 3-minute headway buffer
  5. Clear, human-readable operational directives for Controllers & Loco Pilots
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
    """Computes speed advisories, crossing directives, and delay status for all approaching trains."""

    @classmethod
    def compute_advisories(
        cls,
        trains: List[Dict[str, Any]],
        sim_seconds: float,
    ) -> Dict[str, Dict]:
        results: Dict[str, Dict] = {}

        # Sort by priority: tier ASC (1=highest), then scheduled arrival ASC
        active = [t for t in trains if float(t.get("dist_remaining", 0)) > 0]
        active.sort(key=lambda x: (
            x.get("tier", 5),
            x.get("scheduled_arrival_offset_sec", 99999)
        ))

        pf_occupancy: Dict[int, tuple] = {}

        for rank, train in enumerate(active, 1):
            tid         = train["id"]
            name        = train.get("name", f"Train {tid}")
            dist_rem    = float(train.get("dist_remaining", 0))
            cur_speed   = float(train.get("current_speed", 60))
            mps         = float(train.get("mps", 100))
            sched_sec   = float(train.get("scheduled_arrival_offset_sec", 300))
            alloc_pf    = int(train.get("allocated_pf", 1))

            if dist_rem <= 0:
                results[tid] = cls._make(
                    status=ARRIVED,
                    delay_min=0.0,
                    advised=0.0,
                    action="At platform — passenger boarding active",
                    rank=rank,
                    pred_str="Arrived",
                    cmd_title="🛑 BERTHED AT PLATFORM",
                    cmd_detail=f"Train {tid} has berthed at Platform {alloc_pf}. Doors released.",
                    crossing="Station throat section cleared."
                )
                continue

            safe_speed_ms = max(cur_speed / 3.6, 0.5)
            travel_secs   = dist_rem / safe_speed_ms
            pred_arrival  = sim_seconds + travel_secs

            delay_sec  = pred_arrival - sched_sec
            delay_min  = delay_sec / 60.0
            time_to_sched = sched_sec - sim_seconds

            if time_to_sched > 5.0:
                req_kmh = (dist_rem / time_to_sched) * 3.6
            else:
                req_kmh = mps

            # ── Platform & Precedence Crossing Conflict Check ───────────────
            conflict_speed: Optional[float] = None
            conflict_with: str = ""
            if alloc_pf in pf_occupancy:
                prev_arr, prev_id = pf_occupancy[alloc_pf]
                gap = pred_arrival - prev_arr
                if abs(gap) < PLATFORM_HEADWAY_SEC:
                    safe_arr       = prev_arr + PLATFORM_HEADWAY_SEC
                    safe_time_sec  = safe_arr - sim_seconds
                    if safe_time_sec > 5.0:
                        conflict_ms    = dist_rem / safe_time_sec
                        conflict_speed = max(15.0, min(conflict_ms * 3.6, mps * 0.9))
                        delay_min      = (safe_arr - sched_sec) / 60.0
                        pred_arrival   = safe_arr
                    conflict_with = prev_id

            pf_occupancy[alloc_pf] = (pred_arrival, tid)

            # ── Structured Directives & Action Commands ─────────────────────
            if conflict_speed is not None:
                advised = round(conflict_speed, 1)
                status  = YIELDING
                cmd_title = f"DECELERATE TO {advised:.0f} KM/H"
                action  = f"Hold at {advised:.0f} km/h — yield platform to priority train {conflict_with}"
                cmd_detail = (
                    f"Glide at {advised:.0f} km/h to prevent outer signal idling. "
                    f"Yielding Platform {alloc_pf} to higher-priority Train {conflict_with}."
                )
                crossing = f"Cross after Train {conflict_with} clears throat (Est. +{delay_min:.0f} min wait)."

            elif req_kmh <= 10.0 or dist_rem <= 250:
                advised = 5.0
                status  = ON_TIME
                cmd_title = "BRAKE FOR PLATFORM BERTHING"
                action  = f"Brake to stop — entering Platform {alloc_pf}"
                cmd_detail = f"Apply service brakes. Target stop point Platform {alloc_pf} buffer."
                crossing = f"Route locked to Platform {alloc_pf} track."

            elif abs(delay_min) < 0.8 and req_kmh <= mps:
                advised = round(req_kmh, 1)
                status  = ON_TIME
                cmd_title = f"MAINTAIN {advised:.0f} KM/H"
                action  = f"Maintain {advised:.0f} km/h — exactly on schedule"
                cmd_detail = f"Maintain section speed of {advised:.0f} km/h. Zero delay schedule locked."
                crossing = f"Granted direct non-stop crossing to Platform {alloc_pf}."

            elif req_kmh <= mps:
                advised = round(req_kmh, 1)
                status  = RECOVERABLE
                cmd_title = f"ACCELERATE TO {advised:.0f} KM/H"
                action  = f"Accelerate to {advised:.0f} km/h — recover +{delay_min:.1f} min delay"
                cmd_detail = (
                    f"Increase speed to {advised:.0f} km/h (within {mps:.0f} km/h MPS) "
                    f"to eliminate +{delay_min:.1f} min delay and arrive on time."
                )
                crossing = f"Cleared for high-speed approach to Platform {alloc_pf}."

            elif req_kmh <= mps * 1.08:
                advised = round(mps, 1)
                residual = round(max(0, delay_min - (dist_rem / (mps / 3.6) - max(time_to_sched, 0)) / 60.0), 1)
                status  = MARGINALLY_RECOVERABLE
                cmd_title = f"RUN AT MPS {advised:.0f} KM/H"
                action  = f"Run at MPS {advised:.0f} km/h — ~{residual} min residual delay"
                cmd_detail = f"Maximum track speed {advised:.0f} km/h authorized. Residual delay ~{residual} min."
                crossing = f"High priority green corridor to Platform {alloc_pf}."

            else:
                advised = round(mps, 1)
                residual = round(max(0, (pred_arrival - sched_sec) / 60.0), 1)
                status  = UNRECOVERABLE
                cmd_title = f"MAX SPEED {advised:.0f} KM/H"
                action  = f"Run at MPS {advised:.0f} km/h — ~{residual} min late"
                cmd_detail = f"Operate at maximum permissible speed {advised:.0f} km/h to minimize late arrival."
                crossing = f"Sequenced for Platform {alloc_pf} berth."

            try:
                pred_wall = datetime.now() + timedelta(seconds=travel_secs)
                pred_str  = pred_wall.strftime("%H:%M:%S")
            except Exception:
                pred_str = "--:--"

            results[tid] = cls._make(
                status=status,
                delay_min=round(delay_min, 1),
                advised=advised,
                action=action,
                rank=rank,
                pred_str=pred_str,
                cmd_title=cmd_title,
                cmd_detail=cmd_detail,
                crossing=crossing
            )

        # Berthed or missing trains
        for t in trains:
            if t["id"] not in results:
                results[t["id"]] = cls._make(
                    status=ARRIVED,
                    delay_min=0.0,
                    advised=0.0,
                    action="At platform",
                    rank=999,
                    pred_str="Arrived",
                    cmd_title="BERTHED",
                    cmd_detail="At platform",
                    crossing="Cleared"
                )

        return results

    @staticmethod
    def _make(status: str, delay_min: float, advised: float,
              action: str, rank: int, pred_str: str,
              cmd_title: str = "", cmd_detail: str = "", crossing: str = "") -> Dict:
        return {
            "delay_status":          status,
            "delay_min":             delay_min,
            "delay_color":           DELAY_COLOR.get(status, "#6b7280"),
            "advised_speed":         advised,
            "required_speed":        advised,
            "action_text":           action,
            "priority_rank":         rank,
            "predicted_arrival_str": pred_str,
            "cmd_title":             cmd_title or f"RUN AT {advised:.0f} KM/H",
            "cmd_detail":            cmd_detail or action,
            "crossing":              crossing or "Route locked.",
        }
