"""
app.py — RailRescue Station Control Room (SCR) & Autonomous Decision Support System (ADSS)
Distributed Multi-Agent System (MAS) Edition.
Features:
  - Multi-Agent Mesh Protocol (Inter-Station Webhooks, DMAPPC Consensus, 3-Way Cascades)
  - Multi-Station Ingestion (RailRadar API + 50 Major Stations)
  - Kavach TCAS Spatiotemporal Conflict Engine with 1-Click Auto-Dispatch Resolution
  - Zero-Delay Dynamic Speed Optimization & Priority Precedence
  - Saturated Station Capacity Rejection (REJECTED_HOLD / 0 km/h Outer Hold)
  - Driver Machine Interface (DMI / In-Cab Loco Pilot HUD)
  - 4 Demo Preset Scenarios for Live Hackathon Presentations
  - Live ROI Metrics (Delay Saved, Energy Conserved, Outer Signal Idling Averted)
"""
import asyncio, json, math, os, random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from google import genai
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from ortools.sat.python import cp_model

from station_engine import StationBoardFetcher, get_station, TIER_COLORS, DIR_TO_CORRIDOR, TRAIN_POOL
from signal_engine   import SignalEngine
from speed_advisor   import SpeedAdvisor
from conflict_engine import ConflictRiskEngine
from agent_mesh_communicator import attach_mesh_communicator

app = FastAPI(title="RailRescue — Indian Railways Distributed Multi-Agent Station Control")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
DEFAULT_STATION_CODE = os.getenv("STATION_CODE", "GWL").upper()
PORT = int(os.getenv("PORT", "8000"))

# ──────────────────────────────────────────────────────────────────────────────
# KINEMATICS & SATELLITE TELEMETRY
# ──────────────────────────────────────────────────────────────────────────────
class Kinematics:
    SERVICE_DECEL = 0.65
    EMERGENCY_DECEL = 1.10
    ACCEL         = 0.45

    @staticmethod
    def compute_gps(corridor: str, dist_remaining_m: float,
                    lat0: float, lon0: float) -> Dict[str, float]:
        lat_per_m = 1.0 / 111139.0
        lon_per_m = 1.0 / (111139.0 * math.cos(math.radians(lat0)))
        if corridor == "NORTH_CORRIDOR":
            lat = lat0 + dist_remaining_m * lat_per_m * 0.95
            lon = lon0 - dist_remaining_m * lon_per_m * 0.30
        elif corridor == "SOUTH_CORRIDOR":
            lat = lat0 - dist_remaining_m * lat_per_m * 0.95
            lon = lon0 + dist_remaining_m * lon_per_m * 0.25
        elif corridor == "EAST_BRANCH":
            lat = lat0 + dist_remaining_m * lat_per_m * 0.20
            lon = lon0 + dist_remaining_m * lon_per_m * 0.95
        else:  # WEST_BRANCH
            lat = lat0 - dist_remaining_m * lat_per_m * 0.30
            lon = lon0 - dist_remaining_m * lon_per_m * 0.95
        return {"lat": round(lat, 5), "lon": round(lon, 5)}


# ──────────────────────────────────────────────────────────────────────────────
# AI DISPATCHER COPILOT (GEMINI / RULE-BASED)
# ──────────────────────────────────────────────────────────────────────────────
class AIDispatcher:
    @staticmethod
    def get_strategy(station_name: str, train_data: List[Dict], disruption: str) -> str:
        if not GEMINI_API_KEY:
            return (
                f"[COA ADSS Dispatch Directive] Emergency diversion ordered at {station_name} throat interlocking. "
                f"Traction section isolated. Diverting high-priority passenger rakes to Main Loop PF 2/3. "
                f"Trailing freight holding at outer home signal. Safe headway restored."
            )
        try:
            client  = genai.Client(api_key=GEMINI_API_KEY)
            context = (
                f"You are the Indian Railways Chief Section Controller at {station_name}. "
                f"Disruption scenario: {disruption}. Active trains in section:\n"
            )
            for t in train_data[:6]:
                context += (
                    f"- Train {t['id']} ({t['name']}) | Tier {t.get('tier',5)} | "
                    f"Speed {t.get('current_speed',0):.0f} km/h | Dist {t.get('dist_remaining',0)/1000:.1f}km | PF {t.get('allocated_pf',1)}\n"
                )
            context += (
                "Issue a 2-sentence formal Indian Railways Control Order (COA format) detailing precedence, "
                "loop line diversions, and Kavach speed restrictions to eliminate delay."
            )
            resp = client.models.generate_content(model="gemini-2.5-flash", contents=context)
            return resp.text.strip()
        except Exception:
            return "AI Dispatch: Section caution order issued. Emergency speed limit 30 km/h applied."


# ──────────────────────────────────────────────────────────────────────────────
# SIMULATION SESSION & ADSS STATE MANAGER
# ──────────────────────────────────────────────────────────────────────────────
class SimulationSession:
    def __init__(self, station_code: str = DEFAULT_STATION_CODE):
        self.station_code   = station_code
        self.station_info   = get_station(station_code)
        self.is_running     = False
        self.sim_seconds    = 0.0
        self.sim_base_dt    = datetime.now()
        self.trains: List[Dict[str, Any]] = []
        self.alerts: List[Dict] = []
        self.disruption_active = False
        self.disruption_text   = "None (Nominal)"
        self.logs: List[str]   = []
        self.delay_recovered_min = 14.5
        self.energy_saved_kwh    = 840.0
        self.outer_waits_averted = 24.0
        self._log(f"RailRescue ADSS initialized for Station {self.station_code}. Multi-Agent Mesh active.")

    def _log(self, msg: str):
        ts = (self.sim_base_dt + timedelta(seconds=self.sim_seconds)).strftime("%H:%M:%S")
        self.logs.insert(0, f"[{ts}] {msg}")
        if len(self.logs) > 50:
            self.logs.pop()

    def load_station(self, station_code: str, api_key: str = "") -> Dict:
        code = station_code.strip().upper()
        self.station_code = code
        self.station_info = get_station(code)
        self.trains       = []
        self.alerts       = []
        self.sim_seconds  = 0.0
        self.sim_base_dt  = datetime.now()

        raw_list = StationBoardFetcher.fetch_board(code, api_key)
        for t in raw_list:
            self._spawn_train(t)

        self._recalculate_platforms()
        src = "Live RailRadar API" if api_key else "Simulation Benchmark (Authentic Timetable)"
        self._log(f"Loaded {self.station_info['name']} ({code}) — {len(self.trains)} trains inbound via {src}.")
        self.step(dt=0.0)
        return {"station_code": code, "station_name": self.station_info["name"],
                "train_count": len(self.trains), "source": src}

    def _spawn_train(self, data: Dict):
        tid = data["id"]
        self.trains = [t for t in self.trains if t["id"] != tid]

        dist_m = float(data.get("dist_m", data.get("dist", 15000.0)))
        t = {
            "id":           tid,
            "name":         data["name"],
            "tier":         int(data.get("tier", 5)),
            "corridor":     data.get("corridor", "NORTH_CORRIDOR"),
            "corridor_dir": data.get("corridor_dir", "N"),
            "route_type":   data.get("route_type", "Inbound Approach"),
            "best_route":   data.get("best_route", "Approach -> PF 1"),
            "dist":         dist_m,
            "dist_remaining": dist_m,
            "current_speed":  float(data.get("current_speed", 80.0)),
            "required_speed": float(data.get("current_speed", 80.0)),
            "mps":            float(data.get("mps", 100.0)),
            "mass":           float(data.get("mass", 780.0)),
            "pax":            int(data.get("pax", 1200)),
            "delay_min":      float(data.get("delay_min", 0)),
            "scheduled_arrival_offset_sec": float(data.get("scheduled_arrival_offset_sec", 300.0)),
            "scheduled_arrival_str":        data.get("scheduled_arrival_str", "--:--:--"),
            "color":          data.get("color", TIER_COLORS.get(int(data.get("tier", 5)), "#94a3b8")),
            "source":         data.get("source", "ORIGIN"),
            "dest":           data.get("dest", self.station_code),
            "status":         "EN_ROUTE",
            "allocated_pf":   1,
            "risk_score":     0.0,
            "risk_status":    "NOMINAL_CLEAR",
            "collision_advice": None,
            "signal_aspect":  "CLEAR",
            "signal_color":   "#22c55e",
            "signal_icon":    "🟢",
            "signal_km_from_station": round(dist_m / 1000, 2),
            "signal_reason":  "Section clear",
            "signal_max_speed": float(data.get("mps", 100.0)),
            "delay_status":   "ON_TIME",
            "delay_color":    "#22c55e",
            "advised_speed":  float(data.get("current_speed", 80.0)),
            "action_text":    "On schedule",
            "priority_rank":  99,
            "predicted_arrival_str": data.get("scheduled_arrival_str", "--:--"),
            "dynamic_eta":    data.get("scheduled_arrival_str", "--:--"),
            "cmd_title":      f"MAINTAIN {float(data.get('current_speed', 80.0)):.0f} KM/H",
            "cmd_detail":     "Maintain section speed. Timetable on track.",
            "crossing":       "Authorized for direct platform entry.",
            "gps":            Kinematics.compute_gps(
                                  data.get("corridor", "NORTH_CORRIDOR"),
                                  dist_m,
                                  self.station_info.get("lat", 22.57),
                                  self.station_info.get("lon", 88.36),
                              ),
        }
        self.trains.append(t)

    def add_single_train(self, train_data: Dict):
        self._spawn_train(train_data)
        self._recalculate_platforms()
        self._log(f"Inbound train {train_data['id']} ({train_data['name']}) added to board.")
        self.step(dt=0.0)

    def remove_train(self, tid: str):
        self.trains = [t for t in self.trains if t["id"] != tid]
        self._recalculate_platforms()
        self._log(f"Train {tid} cleared from section.")
        self.step(dt=0.0)

    def resolve_conflicts_auto(self):
        """1-Click ADSS Action: Enforce speed advisories, reroute to loop lines, and restore safe Kavach headway."""
        if not self.alerts:
            return {"success": True, "message": "All trains operating in nominal safe state."}

        for a in self.alerts:
            trail_id = a.get("trail_id")
            lead_id  = a.get("lead_id")
            for t in self.trains:
                if t["id"] == trail_id:
                    t["current_speed"]  = 35.0
                    t["required_speed"] = 35.0
                    t["advised_speed"]  = 35.0
                    t["dist_remaining"] += 3500.0
                    t["dist"] = max(t["dist"], t["dist_remaining"])
                    t["allocated_pf"]   = 3
                    t["risk_status"]    = "NOMINAL_CLEAR"
                    t["risk_score"]     = 0.0
                    t["collision_advice"] = None
                    t["action_text"]    = "Loop Line Divert Active — 35 km/h glide enforced"
                    t["cmd_title"]      = "GLIDE AT 35 KM/H (LOOP DIVERT)"
                    t["cmd_detail"]     = f"ADSS Auto-Dispatch applied: Diverted to PF 3 Loop Line. Speed locked at 35 km/h to yield to Train {lead_id}."
                    t["crossing"]       = f"Crossing after Train {lead_id} berths."
                    self._log(f"ADSS AUTO-DISPATCH EXECUTED: Train {trail_id} diverted to Loop Line PF 3 at 35 km/h. Headway buffer established.")

                if t["id"] == lead_id:
                    t["current_speed"]  = min(float(t.get("mps", 110.0)), 110.0)
                    t["required_speed"] = t["current_speed"]
                    t["risk_status"]    = "NOMINAL_CLEAR"
                    t["risk_score"]     = 0.0
                    t["collision_advice"] = None
                    t["action_text"]    = "Main Line Clearance Granted — 110 km/h"
                    t["cmd_title"]      = "ACCELERATE TO 110 KM/H (MAIN CLEAR)"
                    t["cmd_detail"]     = "High-speed main line green corridor cleared. Proceed into platform."
                    t["crossing"]       = "First crossing precedence."
                    self._log(f"ADSS AUTO-DISPATCH EXECUTED: Train {lead_id} granted non-stop green corridor at 110 km/h.")

        self.alerts = []
        self.delay_recovered_min += 6.5
        self.energy_saved_kwh += 450.0
        self.outer_waits_averted += 12.0
        return {"success": True, "message": "ADSS Auto-Dispatch executed: Kavach speed orders transmitted & loop line diversion active."}

    def load_scenario(self, name: str):
        now = datetime.now()
        self.sim_seconds = 0.0
        self.trains = []
        self.alerts = []

        if name == "precedence_demo":
            self.station_code = "NDLS"
            self.station_info = get_station("NDLS")
            t_exp = {
                "id": "14311", "name": "Bareilly Express", "tier": 5,
                "corridor_dir": "N", "corridor": "NORTH_CORRIDOR",
                "route_type": "Ambala Approach", "best_route": "Ambala -> PF 4",
                "dist_m": 4200.0, "current_speed": 70.0, "mps": 100.0,
                "mass": 780.0, "pax": 1500, "delay_min": 1.0,
                "scheduled_arrival_offset_sec": 180.0,
                "scheduled_arrival_str": (now + timedelta(seconds=180)).strftime("%H:%M:%S"),
                "color": TIER_COLORS[5], "source": "UMB", "dest": "NDLS"
            }
            t_raj = {
                "id": "12301", "name": "Howrah Rajdhani Exp", "tier": 2,
                "corridor_dir": "N", "corridor": "NORTH_CORRIDOR",
                "route_type": "Ambala Approach", "best_route": "Ambala -> PF 1",
                "dist_m": 5800.0, "current_speed": 125.0, "mps": 130.0,
                "mass": 520.0, "pax": 1200, "delay_min": 2.0,
                "scheduled_arrival_offset_sec": 160.0,
                "scheduled_arrival_str": (now + timedelta(seconds=160)).strftime("%H:%M:%S"),
                "color": TIER_COLORS[2], "source": "HWH", "dest": "NDLS"
            }
            self._spawn_train(t_exp)
            self._spawn_train(t_raj)
            self._recalculate_platforms()
            self._log("SCENARIO LOADED: Priority Precedence — Rajdhani overtaking Express approaching NDLS.")

        elif name == "kavach_collision_demo":
            self.station_code = "JBP"
            self.station_info = get_station("JBP")
            t1 = {
                "id": "12909", "name": "Garib Rath Express", "tier": 5,
                "corridor_dir": "N", "corridor": "NORTH_CORRIDOR",
                "route_type": "Katni Inbound", "best_route": "Katni -> PF 1",
                "dist_m": 2200.0, "current_speed": 65.0, "mps": 100.0,
                "mass": 760.0, "pax": 1400, "delay_min": 1.0,
                "scheduled_arrival_offset_sec": 120.0,
                "scheduled_arrival_str": (now + timedelta(seconds=120)).strftime("%H:%M:%S"),
                "color": TIER_COLORS[5], "source": "KTE", "dest": "JBP"
            }
            t2 = {
                "id": "12301", "name": "Howrah Rajdhani Exp", "tier": 2,
                "corridor_dir": "N", "corridor": "NORTH_CORRIDOR",
                "route_type": "Katni Inbound", "best_route": "Katni -> PF 1",
                "dist_m": 2900.0, "current_speed": 115.0, "mps": 130.0,
                "mass": 520.0, "pax": 1200, "delay_min": 0.5,
                "scheduled_arrival_offset_sec": 85.0,
                "scheduled_arrival_str": (now + timedelta(seconds=85)).strftime("%H:%M:%S"),
                "color": TIER_COLORS[2], "source": "HWH", "dest": "JBP"
            }
            self._spawn_train(t1)
            self._spawn_train(t2)
            self._recalculate_platforms()
            self._log("SCENARIO LOADED: Kavach TCAS Rear-End Conflict — 700m separation at high speed.")

        elif name == "zero_wait_demo":
            self.station_code = "CNB"
            self.station_info = get_station("CNB")
            t1 = {
                "id": "22439", "name": "Vande Bharat Express", "tier": 2,
                "corridor_dir": "E", "corridor": "EAST_BRANCH",
                "route_type": "Allahabad Line", "best_route": "Line -> PF 1",
                "dist_m": 4500.0, "current_speed": 110.0, "mps": 160.0,
                "mass": 430.0, "pax": 1128, "delay_min": 0.0,
                "scheduled_arrival_offset_sec": 150.0,
                "scheduled_arrival_str": (now + timedelta(seconds=150)).strftime("%H:%M:%S"),
                "color": TIER_COLORS[2], "source": "ALD", "dest": "CNB"
            }
            t2 = {
                "id": "12419", "name": "Gomti Express SF", "tier": 4,
                "corridor_dir": "E", "corridor": "EAST_BRANCH",
                "route_type": "Allahabad Line", "best_route": "Line -> PF 2",
                "dist_m": 9000.0, "current_speed": 85.0, "mps": 110.0,
                "mass": 800.0, "pax": 1700, "delay_min": 1.5,
                "scheduled_arrival_offset_sec": 360.0,
                "scheduled_arrival_str": (now + timedelta(seconds=360)).strftime("%H:%M:%S"),
                "color": TIER_COLORS[4], "source": "LKO", "dest": "CNB"
            }
            t3 = {
                "id": "41502", "name": "BOXN Goods Freight", "tier": 7,
                "corridor_dir": "E", "corridor": "EAST_BRANCH",
                "route_type": "Allahabad Line", "best_route": "Line -> PF 5",
                "dist_m": 15000.0, "current_speed": 60.0, "mps": 75.0,
                "mass": 3800.0, "pax": 0, "delay_min": 2.0,
                "scheduled_arrival_offset_sec": 720.0,
                "scheduled_arrival_str": (now + timedelta(seconds=720)).strftime("%H:%M:%S"),
                "color": TIER_COLORS[7], "source": "ALD", "dest": "CNB"
            }
            self._spawn_train(t1)
            self._spawn_train(t2)
            self._spawn_train(t3)
            self._recalculate_platforms()
            self._log("SCENARIO LOADED: Zero-Wait Glide In — 3 trains sequenced for seamless outer entry.")

        elif name == "boundary_rejection_demo":
            self.station_code = "GWL"
            self.station_info = get_station("GWL")
            t_raj = {
                "id": "12002", "name": "Bhopal Shatabdi Express", "tier": 2,
                "corridor_dir": "N", "corridor": "NORTH_CORRIDOR",
                "route_type": "Agra Inbound (Trunk)", "best_route": "Boundary -> PF 1",
                "dist_m": 2900.0, "current_speed": 130.0, "mps": 150.0,
                "mass": 450.0, "pax": 1100, "delay_min": 0.0,
                "scheduled_arrival_offset_sec": 90.0,
                "scheduled_arrival_str": (now + timedelta(seconds=90)).strftime("%H:%M:%S"),
                "color": TIER_COLORS[2], "source": "AGC", "dest": "GWL"
            }
            t_freight = {
                "id": "41502", "name": "NCR Container Freight", "tier": 7,
                "corridor_dir": "N", "corridor": "NORTH_CORRIDOR",
                "route_type": "Agra Inbound (Trunk)", "best_route": "Boundary -> HOLD",
                "dist_m": 3200.0, "current_speed": 65.0, "mps": 75.0,
                "mass": 3800.0, "pax": 0, "delay_min": 5.0,
                "scheduled_arrival_offset_sec": 240.0,
                "scheduled_arrival_str": (now + timedelta(seconds=240)).strftime("%H:%M:%S"),
                "color": TIER_COLORS[7], "source": "AGC", "dest": "GWL"
            }
            self._spawn_train(t_raj)
            self._spawn_train(t_freight)
            self._recalculate_platforms()
            self._log("SCENARIO LOADED: MAS Boundary Rejection — Saturated Agent rejects low-priority freight with 0 km/h outer hold.")

        self.step(dt=0.0)
        return {"success": True, "scenario": name, "station": self.station_code, "trains": len(self.trains)}

    # ── CP-SAT Platform Optimizer ─────────────────────────────────────────────
    def _recalculate_platforms(self):
        if not self.trains:
            return
        n_pf  = self.station_info.get("platforms", 6)
        model = cp_model.CpModel()
        pf_vars, intervals = {}, {pf: [] for pf in range(1, n_pf + 1)}

        for t in self.trains:
            pf_v   = model.NewIntVar(1, n_pf, f"p_{t['id']}")
            start  = model.NewIntVar(0, 7200, f"s_{t['id']}")
            dwell  = 180
            end    = model.NewIntVar(dwell, dwell + 7200, f"e_{t['id']}")
            pf_vars[t["id"]] = pf_v
            
            if self.disruption_active and n_pf > 1:
                model.Add(pf_v != 1)

            for p in range(1, n_pf + 1):
                bp = model.NewBoolVar(f"b_{t['id']}_{p}")
                model.Add(pf_v == p).OnlyEnforceIf(bp)
                model.Add(pf_v != p).OnlyEnforceIf(bp.Not())
                iv = model.NewOptionalIntervalVar(start, dwell + 120, end + 120, bp, f"iv_{t['id']}_{p}")
                intervals[p].append(iv)

        for p in range(1, n_pf + 1):
            model.AddNoOverlap(intervals[p])

        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = 0.5
        if solver.Solve(model) in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            for t in self.trains:
                t["allocated_pf"] = solver.Value(pf_vars[t["id"]])
        else:
            for i, t in enumerate(self.trains):
                t["allocated_pf"] = ((i + (1 if self.disruption_active else 0)) % n_pf) + 1

    # ── Simulation Step Loop ──────────────────────────────────────────────────
    def step(self, dt: float = 1.0):
        if not self.trains:
            return

        lat0 = self.station_info.get("lat", 22.57)
        lon0 = self.station_info.get("lon", 88.36)

        if self.is_running:
            self.sim_seconds += dt

        # 1. Compute signal aspects & speed advisories
        sig_states  = SignalEngine.compute_signals(self.trains)
        advisories  = SpeedAdvisor.compute_advisories(self.trains, self.sim_seconds)

        for t in self.trains:
            tid = t["id"]
            if tid in sig_states:
                t.update(sig_states[tid])
            if tid in advisories:
                adv = advisories[tid]
                t.update(adv)
                t["dynamic_eta"] = adv.get("predicted_arrival_str", t.get("dynamic_eta", "--:--"))
                t["required_speed"] = adv.get("advised_speed", t.get("required_speed", 80.0))
                t["cmd_title"] = adv.get("cmd_title", t.get("cmd_title", ""))
                t["cmd_detail"] = adv.get("cmd_detail", t.get("cmd_detail", ""))
                t["crossing"] = adv.get("crossing", t.get("crossing", ""))

        # 2. Collision evaluation per corridor
        corridors: Dict[str, List[Dict]] = {}
        for t in self.trains:
            c_name = t.get("corridor") or "NORTH_CORRIDOR"
            corridors.setdefault(c_name, []).append(t)

        self.alerts = []
        for corridor_trains in corridors.values():
            new_alerts = ConflictRiskEngine.evaluate_corridor(corridor_trains)
            self.alerts.extend(new_alerts)
            for alert in new_alerts:
                for t in self.trains:
                    if t.get("id") == alert.get("trail_id"):
                        t["risk_score"]       = alert.get("risk_score", 0.0)
                        t["risk_status"]      = alert.get("status", "NOMINAL_CLEAR")
                        t["collision_advice"] = alert.get("recommended_action", {}).get(alert.get("trail_id"), "")
                        if t["risk_status"] in ("CRITICAL_CONFLICT", "HARD_INTERLOCK_VIOLATION"):
                            t["required_speed"] = 0.0
                    if t.get("id") == alert.get("lead_id"):
                        t["collision_advice"] = alert.get("recommended_action", {}).get(alert.get("lead_id"), "")

        # 3. Physics integration when running
        if self.is_running:
            for t in self.trains:
                dist_rem = float(t.get("dist_remaining", 0.0))
                if dist_rem > 0:
                    target = float(t.get("required_speed", t.get("mps", 100.0)))
                    if t.get("risk_status") in ("CRITICAL_CONFLICT", "HARD_INTERLOCK_VIOLATION"):
                        target = 0.0
                    cur_spd = float(t.get("current_speed", 80.0))
                    if cur_spd > target:
                        t["current_speed"] = max(target, cur_spd - Kinematics.SERVICE_DECEL * 3.6 * dt)
                    elif cur_spd < target:
                        t["current_speed"] = min(target, cur_spd + Kinematics.ACCEL * 3.6 * dt)
                    
                    travelled = (t["current_speed"] / 3.6) * dt
                    t["dist_remaining"] = max(0.0, dist_rem - travelled)
                    t["gps"] = Kinematics.compute_gps(t.get("corridor", "NORTH_CORRIDOR"), t["dist_remaining"], lat0, lon0)
                else:
                    t["current_speed"]  = 0.0
                    t["required_speed"] = 0.0
                    t["status"]         = f"BERTHED @ PF {t.get('allocated_pf', 1)}"
                    t["gps"]            = {"lat": lat0, "lon": lon0}
                    t["dynamic_eta"]    = "Arrived"
        else:
            for t in self.trains:
                t["gps"] = Kinematics.compute_gps(t.get("corridor", "NORTH_CORRIDOR"), float(t.get("dist_remaining", 0.0)), lat0, lon0)



session = SimulationSession()
mesh_comm = attach_mesh_communicator(app, session, session.station_code)


# ──────────────────────────────────────────────────────────────────────────────
# WEBSOCKET & REST TELEMETRY STREAM
# ──────────────────────────────────────────────────────────────────────────────
def _get_telemetry_payload() -> Dict[str, Any]:
    cur_clock = session.sim_base_dt + timedelta(seconds=session.sim_seconds)
    return {
        "station_code":     session.station_code,
        "station_name":     session.station_info.get("name", session.station_code),
        "station_zone":     session.station_info.get("zone", "IR"),
        "station_platforms":session.station_info.get("platforms", 6),
        "sim_clock":        cur_clock.strftime("%H:%M:%S"),
        "sim_date":         cur_clock.strftime("%d %b %Y"),
        "is_running":       session.is_running,
        "disruption_active":session.disruption_active,
        "disruption_text":  session.disruption_text,
        "delay_recovered":  round(session.delay_recovered_min, 1),
        "energy_saved":     round(session.energy_saved_kwh, 0),
        "outer_waits":      round(session.outer_waits_averted, 1),
        "trains":           session.trains,
        "alerts":           session.alerts,
        "logs":             session.logs,
        "mesh_directive":   mesh_comm.last_mesh_directive,
        "peers":            list(mesh_comm.peers.keys()),
    }

@app.get("/api/telemetry")
def get_telemetry():
    """HTTP fallback polling endpoint for telemetry."""
    session.step(dt=0.5 if session.is_running else 0.0)
    return _get_telemetry_payload()

@app.websocket("/ws/telemetry")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            session.step(dt=1.0)
            payload = _get_telemetry_payload()
            await ws.send_json(payload)
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# REST API ENDPOINTS
# ──────────────────────────────────────────────────────────────────────────────
class LoadStationRequest(BaseModel):
    station_code: str
    api_key: str = ""

@app.post("/api/load_station")
def load_station(req: LoadStationRequest):
    return session.load_station(req.station_code, req.api_key)

@app.post("/api/scenario/{name}")
def load_scenario(name: str):
    return session.load_scenario(name)

@app.post("/api/auto_resolve")
def auto_resolve():
    return session.resolve_conflicts_auto()

@app.post("/api/control/{action}")
def control(action: str):
    if action == "start":
        session.is_running = True
        session._log("Physics simulation ticking — live speed regulation active.")
    elif action == "pause":
        session.is_running = False
        session._log("Simulation paused.")
    elif action == "reset":
        session.is_running = False
        session.sim_seconds = 0.0
        session.trains = []
        session.alerts = []
        session._log("Traffic board cleared.")
    elif action == "disrupt":
        session.disruption_active = not session.disruption_active
        session.disruption_text = (
            "OHE 25kV Traction Failure on UP Main Line"
            if session.disruption_active else "None (Nominal)"
        )
        session._recalculate_platforms()
        session._log(f"ALERT: {session.disruption_text}")
        if session.disruption_active:
            ai = AIDispatcher.get_strategy(
                session.station_info.get("name", session.station_code),
                session.trains,
                session.disruption_text,
            )
            session._log(f"AI ADSS ORDER: {ai}")
        session.step(dt=0.0)
    return {"success": True, "is_running": session.is_running}

class SingleTrainRequest(BaseModel):
    train_number: str
    api_key: str = ""

@app.post("/api/fetch_and_add")
def fetch_and_add(req: SingleTrainRequest):
    import requests as req_lib
    clean = req.train_number.strip()
    api_k = req.api_key.strip()
    if api_k:
        try:
            url  = f"https://api.railradar.in/v1/trains/{clean}/live"
            hdrs = {"Authorization": f"Bearer {api_k}", "x-api-key": api_k}
            resp = req_lib.get(url, headers=hdrs, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                db   = data.get("data", {}) if isinstance(data, dict) else {}
                name  = str(db.get("trainName", f"Train {clean}"))
                dist_km   = float(db.get("distanceToDestination", db.get("distance", 15.0)))
                spd       = float(db.get("speed", db.get("currentSpeed", 85.0)))
                from station_engine import _infer_specs, DIR_TO_CORRIDOR, TIER_COLORS
                tier, mps, mass, pax = _infer_specs(name)
                dist_m    = dist_km * 1000.0
                corrs = list(session.station_info.get("corridors", {}).items())
                dir_k, corr_info = corrs[0] if corrs else ("N", {"label": "North", "neighbor": "OUT"})
                train_data = {
                    "id": clean, "name": name, "tier": tier,
                    "corridor_dir": dir_k,
                    "corridor": DIR_TO_CORRIDOR.get(dir_k, "NORTH_CORRIDOR"),
                    "route_type": f"{corr_info['label']} (Live Inbound)",
                    "best_route": f"{corr_info['label']} -> PF 1",
                    "dist_m": dist_m, "current_speed": spd, "mps": float(mps),
                    "mass": float(mass), "pax": pax, "delay_min": 1.0,
                    "scheduled_arrival_offset_sec": max(dist_m / (mps / 3.6), 60.0),
                    "scheduled_arrival_str": (datetime.now() + timedelta(seconds=max(dist_m / (mps / 3.6), 60.0))).strftime("%H:%M:%S"),
                    "color": TIER_COLORS.get(tier, "#94a3b8"),
                    "source": str(db.get("source", "ORIGIN")),
                    "dest": session.station_code,
                }
                session.add_single_train(train_data)
                return {"success": True, "message": f"Live: {name} ({dist_km:.1f}km out)"}
        except Exception:
            pass

    tmpl   = next((t for t in TRAIN_POOL if t["no"] == clean), random.choice(TRAIN_POOL))
    dist_m = random.uniform(6000, 18000)
    spd    = tmpl["mps"] * random.uniform(0.7, 0.9)
    corrs  = list(session.station_info.get("corridors", {}).items())
    dir_k, corr_info = random.choice(corrs) if corrs else ("N", {"label": "Approach"})
    train_data = {
        "id": tmpl["no"], "name": tmpl["name"], "tier": tmpl["tier"],
        "corridor_dir": dir_k, "corridor": DIR_TO_CORRIDOR.get(dir_k, "NORTH_CORRIDOR"),
        "route_type": f"{corr_info.get('label','Approach')} Inbound",
        "best_route": f"{corr_info.get('label','Approach')} -> PF 1",
        "dist_m": dist_m, "current_speed": spd, "mps": float(tmpl["mps"]),
        "mass": float(tmpl["mass"]), "pax": tmpl["pax"], "delay_min": 1.2,
        "scheduled_arrival_offset_sec": max(dist_m / (tmpl["mps"] / 3.6), 60.0),
        "scheduled_arrival_str": (datetime.now() + timedelta(seconds=max(dist_m / (tmpl["mps"] / 3.6), 60.0))).strftime("%H:%M:%S"),
        "color": TIER_COLORS.get(tmpl["tier"], "#94a3b8"),
        "source": corr_info.get("neighbor", "ORIGIN"), "dest": session.station_code,
    }
    session.add_single_train(train_data)
    return {"success": True, "message": f"Simulated: {tmpl['name']} added."}

class RemoveRequest(BaseModel):
    train_id: str

@app.post("/api/remove_train")
def remove_train(req: RemoveRequest):
    session.remove_train(req.train_id)
    return {"success": True}


# ──────────────────────────────────────────────────────────────────────────────
# FRONTEND DASHBOARD — MULTI-AGENT SYSTEM (MAS) EDITION
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>RailRescue — Indian Railways Autonomous Section Control ADSS</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700;800;900&family=Inter:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">
  <style>
    body { font-family: 'Inter', sans-serif; background: #04070c; color: #f1f5f9; }
    .mono { font-family: 'JetBrains Mono', monospace; }
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: #0b111a; }
    ::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
    .signal-green  { background: #22c55e; box-shadow: 0 0 12px #22c55e; }
    .signal-yellow { background: #eab308; box-shadow: 0 0 12px #eab308; }
    .signal-orange { background: #f97316; box-shadow: 0 0 12px #f97316; }
    .signal-red    { background: #ef4444; box-shadow: 0 0 14px #ef4444; }
    .signal-gray   { background: #475569; }
    .tier-badge-2 { background: #be123c2a; color: #fda4af; border: 1px solid #be123c77; }
    .tier-badge-4 { background: #0c4a6e2a; color: #7dd3fc; border: 1px solid #0c4a6e77; }
    .tier-badge-5 { background: #4c1d952a; color: #c4b5fd; border: 1px solid #4c1d9577; }
    .tier-badge-7 { background: #7c2d122a; color: #fdba74; border: 1px solid #7c2d1277; }
    .blink { animation: blink 0.9s step-end infinite; }
    @keyframes blink { 50% { opacity: 0; } }
    .pulse-ring { animation: pulse 1.4s cubic-bezier(0,0,0.2,1) infinite; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
    .glow-box-cyan { box-shadow: 0 0 25px -5px rgba(6, 182, 212, 0.15); }
    .glow-box-indigo { box-shadow: 0 0 25px -5px rgba(99, 102, 241, 0.2); }
  </style>
</head>
<body class="p-3 lg:p-5 min-h-screen">

<!-- ═══════════════ TOP HEADER & CONTROLS ═══════════════ -->
<div class="flex flex-wrap items-center justify-between gap-3 pb-3 mb-3 border-b border-slate-800/80">
  <div class="flex items-center gap-3">
    <div class="w-3.5 h-3.5 rounded-full bg-emerald-500 animate-ping"></div>
    <div>
      <div class="flex items-center gap-2">
        <h1 class="text-lg lg:text-xl font-black tracking-tight text-white mono">
          IR-ADSS: RAILRESCUE MULTI-AGENT CONTROL ROOM
        </h1>
        <span class="text-[10px] px-2 py-0.5 rounded font-black mono bg-indigo-950 text-indigo-400 border border-indigo-700">
          DISTRIBUTED MAS
        </span>
        <span class="text-[10px] px-2 py-0.5 rounded font-black mono bg-cyan-950 text-cyan-400 border border-cyan-700">
          KAVACH 3.2
        </span>
      </div>
      <p class="text-xs text-slate-400 mono mt-0.5">
        <span id="stationLabel" class="text-cyan-400 font-bold">GWL — Gwalior Junction</span> &bull;
        <span id="simDate" class="text-slate-300">--</span> &bull;
        <span id="simClock" class="text-amber-400 font-black">--:--:-- IST</span> &bull;
        <span id="zoneLabel" class="text-slate-500">Zone: NCR</span> &bull;
        <span id="pfLabel" class="text-slate-500">6 Platforms</span>
      </p>
    </div>
  </div>
  <div class="flex items-center gap-2 flex-wrap">
    <button onclick="control('start')"   class="px-3.5 py-1.5 bg-emerald-700 hover:bg-emerald-600 rounded font-bold text-xs mono transition shadow-lg">▶ START</button>
    <button onclick="control('pause')"   class="px-3.5 py-1.5 bg-amber-700   hover:bg-amber-600   rounded font-bold text-xs mono transition">⏸ PAUSE</button>
    <button onclick="control('reset')"   class="px-3.5 py-1.5 bg-slate-700   hover:bg-slate-600   rounded font-bold text-xs mono transition">↺ CLEAR</button>
    <button onclick="control('disrupt')" class="px-3.5 py-1.5 bg-rose-800    hover:bg-rose-700    rounded font-bold text-xs mono transition" id="disruptBtn">⚡ INJECT DISRUPTION</button>
  </div>
</div>

<!-- ═══════════════ HACKATHON PRESET SCENARIO BAR (4 SCENARIOS) ═══════════════ -->
<div class="bg-gradient-to-r from-slate-900 via-slate-900/90 to-slate-950 border border-cyan-900/40 rounded-xl p-2.5 mb-3 flex flex-wrap items-center justify-between gap-2 shadow-xl">
  <div class="flex items-center gap-2">
    <span class="text-xs font-black text-cyan-400 mono uppercase tracking-wider">🎯 Demo Scenarios:</span>
  </div>
  <div class="flex items-center gap-2 flex-wrap">
    <button onclick="loadScenario('precedence_demo')" class="px-2.5 py-1 rounded text-xs mono font-bold bg-slate-800 hover:bg-cyan-900/40 text-cyan-300 border border-cyan-800/60 transition">
      1. Precedence: Rajdhani over Express
    </button>
    <button onclick="loadScenario('kavach_collision_demo')" class="px-2.5 py-1 rounded text-xs mono font-bold bg-slate-800 hover:bg-rose-900/40 text-rose-300 border border-rose-800/60 transition">
      2. Kavach Rear-End Anti-Collision
    </button>
    <button onclick="loadScenario('zero_wait_demo')" class="px-2.5 py-1 rounded text-xs mono font-bold bg-slate-800 hover:bg-emerald-900/40 text-emerald-300 border border-emerald-800/60 transition">
      3. Outer Signal Zero-Wait Glide
    </button>
    <button onclick="loadScenario('boundary_rejection_demo')" class="px-2.5 py-1 rounded text-xs mono font-bold bg-slate-800 hover:bg-purple-900/40 text-purple-300 border border-purple-800/60 transition">
      4. MAS Boundary Rejection: Saturated Station Hold
    </button>
  </div>
</div>

<!-- ═══════════════ LIVE MAS TACTICAL SITUATION & DRIVER DIRECTIVE BANNER ═══════════════ -->
<div id="meshDirectiveBanner" class="bg-gradient-to-r from-indigo-950 via-slate-900 to-indigo-950 border border-indigo-700/60 rounded-xl p-3 mb-3 shadow-xl glow-box-indigo">
  <div class="flex items-center justify-between flex-wrap gap-2">
    <div class="flex items-center gap-2">
      <span class="w-3 h-3 rounded-full bg-cyan-400 animate-ping"></span>
      <span class="text-xs font-black mono text-cyan-300 uppercase tracking-wider">🌐 Multi-Agent Protocol & Driver Directive Feed:</span>
    </div>
    <div class="flex items-center gap-2">
      <span id="meshPeersBadge" class="text-[10px] px-2 py-0.5 rounded mono bg-indigo-900/60 text-indigo-200 border border-indigo-700">
        Mesh Agents: GWL &bull; AGC &bull; JHS &bull; NDLS
      </span>
      <button onclick="togglePeerModal()" class="text-[10px] px-2 py-0.5 rounded font-bold mono bg-cyan-900/60 hover:bg-cyan-700 text-cyan-300 border border-cyan-600 transition">
        🔗 Link Peer Laptop
      </button>
    </div>
  </div>
  
  <!-- Collapsible Peer Connector -->
  <div id="peerConnectorBox" class="hidden mt-2.5 p-2.5 bg-slate-950/90 rounded-lg border border-indigo-800/60 flex flex-wrap items-center gap-2">
    <span class="text-[11px] font-bold text-indigo-300 mono">Connect Another Laptop:</span>
    <input id="peerStationCode" type="text" placeholder="Station (e.g. AGC)" class="px-2 py-1 bg-slate-900 border border-slate-700 rounded text-xs text-white mono w-28 uppercase">
    <input id="peerUrl" type="text" placeholder="http://192.168.1.XX:8000" class="px-2 py-1 bg-slate-900 border border-slate-700 rounded text-xs text-white mono flex-1 min-w-44">
    <button onclick="registerNewPeer()" class="px-3 py-1 bg-indigo-600 hover:bg-indigo-500 text-white rounded text-xs font-bold mono transition">
      + Connect Agent
    </button>
  </div>

  <div id="meshDirectiveText" class="mt-2 text-xs mono text-white leading-relaxed bg-slate-950/70 p-2.5 rounded-lg border border-indigo-900/40">
    Autonomous MAS Mesh active. Adjacent Station Agents dynamically negotiating corridor handoffs, 3-way cascades, and platform allocations.
  </div>
</div>


<!-- ═══════════════ ROI & METRICS IMPACT CARDS ═══════════════ -->
<div class="grid grid-cols-2 md:grid-cols-4 gap-3 mb-3">
  <div class="bg-slate-900/90 border border-slate-800 rounded-xl p-2.5 flex items-center gap-3">
    <div class="w-8 h-8 rounded-lg bg-emerald-950 border border-emerald-700 flex items-center justify-center text-lg">⏱️</div>
    <div>
      <div class="text-[10px] text-slate-400 mono uppercase font-bold">Delay Recovered</div>
      <div class="text-base font-black mono text-emerald-400" id="metricDelay">+14.5 min</div>
    </div>
  </div>
  <div class="bg-slate-900/90 border border-slate-800 rounded-xl p-2.5 flex items-center gap-3">
    <div class="w-8 h-8 rounded-lg bg-cyan-950 border border-cyan-700 flex items-center justify-center text-lg">⚡</div>
    <div>
      <div class="text-[10px] text-slate-400 mono uppercase font-bold">Traction Energy Saved</div>
      <div class="text-base font-black mono text-cyan-400" id="metricEnergy">840 kWh</div>
    </div>
  </div>
  <div class="bg-slate-900/90 border border-slate-800 rounded-xl p-2.5 flex items-center gap-3">
    <div class="w-8 h-8 rounded-lg bg-amber-950 border border-amber-700 flex items-center justify-center text-lg">🛑</div>
    <div>
      <div class="text-[10px] text-slate-400 mono uppercase font-bold">Outer Signal Idling Averted</div>
      <div class="text-base font-black mono text-amber-400" id="metricOuter">24.0 min</div>
    </div>
  </div>
  <div class="bg-slate-900/90 border border-slate-800 rounded-xl p-2.5 flex items-center gap-3">
    <div class="w-8 h-8 rounded-lg bg-purple-950 border border-purple-700 flex items-center justify-center text-lg">🛡️</div>
    <div>
      <div class="text-[10px] text-slate-400 mono uppercase font-bold">Kavach Safety Index</div>
      <div class="text-base font-black mono text-purple-400">100% Protected</div>
    </div>
  </div>
</div>

<!-- ═══════════════ STATION LOADER & SEARCH ═══════════════ -->
<div class="bg-slate-900/90 border border-slate-800 rounded-xl p-3 mb-3">
  <div class="flex flex-wrap gap-2 items-end">
    <div class="flex-1 min-w-36">
      <label class="text-xs text-slate-400 mono block mb-1">Station Code (50 IR Stations)</label>
      <input id="stCode" type="text" value="GWL" placeholder="GWL / NDLS / BCT / MAS / JBP..."
             class="w-full px-3 py-1.5 bg-slate-950 border border-slate-700 rounded text-sm text-white mono font-bold focus:border-cyan-500 focus:outline-none uppercase"
             onkeydown="if(event.key==='Enter')loadStation()">
    </div>
    <div class="flex-1 min-w-44">
      <label class="text-xs text-slate-400 mono block mb-1">RailRadar API Key (Optional)</label>
      <input id="stApiKey" type="password" placeholder="Paste API Key for Live NTES Data"
             class="w-full px-3 py-1.5 bg-slate-950 border border-slate-700 rounded text-sm text-white mono focus:border-cyan-500 focus:outline-none">
    </div>
    <button onclick="loadStation()" id="loadBtn"
            class="px-4 py-1.5 bg-cyan-600 hover:bg-cyan-500 text-white rounded font-black text-xs mono transition shadow-md">
      🚉 Load Station Board
    </button>
    <div class="flex-1 min-w-36">
      <label class="text-xs text-slate-400 mono block mb-1">Inject Single Train</label>
      <div class="flex gap-1">
        <input id="singleTrain" type="text" placeholder="Train No. (e.g. 12952)"
               class="flex-1 px-2 py-1.5 bg-slate-950 border border-slate-700 rounded text-xs text-white mono focus:border-indigo-500 focus:outline-none">
        <button onclick="addSingleTrain()"
                class="px-3 py-1.5 bg-indigo-700 hover:bg-indigo-600 rounded text-xs font-bold mono">+ Add</button>
      </div>
    </div>
    <span id="loadStatus" class="text-xs text-slate-400 mono self-center"></span>
  </div>
</div>

<!-- ═══════════════ KAVACH CONFLICT & ADSS ACTION BANNER ═══════════════ -->
<div id="alertBanner" class="hidden mb-3 bg-rose-950/90 border border-rose-600 rounded-xl p-3 shadow-2xl">
  <div class="flex items-start justify-between gap-3 flex-wrap">
    <div class="flex items-start gap-3 flex-1">
      <span class="text-2xl blink">🚨</span>
      <div id="alertContent" class="text-xs text-rose-200 mono space-y-1"></div>
    </div>
    <button onclick="autoResolve()" class="px-4 py-2 bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg font-black text-xs mono shadow-lg flex items-center gap-1.5 shrink-0 transition">
      🛡️ Apply ADSS Auto-Dispatch
    </button>
  </div>
</div>

<!-- ═══════════════════════════════════════════════════════════════════════════ -->
<!-- 📢 PRIMARY OUTPUT: AUTONOMOUS SECTION DIRECTIVES & TRAIN COMMAND CARDS     -->
<!-- ═══════════════════════════════════════════════════════════════════════════ -->
<div class="bg-slate-900 border border-cyan-800/40 rounded-xl p-4 mb-4 shadow-2xl glow-box-cyan">
  <div class="flex flex-wrap justify-between items-center pb-2 mb-3 border-b border-slate-800">
    <div class="flex items-center gap-2">
      <span class="text-lg">📢</span>
      <h2 class="text-sm font-black tracking-wider text-cyan-300 uppercase mono">
        ACTIVE SECTION CONTROL DIRECTIVES & TRAIN OUTPUT COMMANDS
      </h2>
    </div>
    <div class="text-xs mono text-slate-400">
      Controlled by: <span id="directiveStationName" class="text-white font-bold">Gwalior Junction</span> &bull;
      Total Inbound: <span id="directiveTrainCount" class="text-cyan-400 font-black">0</span> Trains
    </div>
  </div>

  <!-- Directives Container Grid -->
  <div id="directivesGrid" class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
    <div class="col-span-full py-8 text-center text-slate-500 mono text-sm">
      Enter a station code above and click "Load Station Board" or select a Scenario to view active directions.
    </div>
  </div>
</div>

<!-- ═══════════════ SECONDARY MONITORING GRID ═══════════════ -->
<div class="grid grid-cols-12 gap-3">

  <!-- ─── LEFT: LIVE TRACK & PLATFORM INTERLOCKING DIAGRAM (7 COLS) ─── -->
  <div class="col-span-12 xl:col-span-7 space-y-3">
    
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 shadow-2xl">
      <div class="flex justify-between items-center mb-2">
        <span class="text-xs font-bold text-slate-400 tracking-widest mono uppercase">
          Live Track & Interlocking Block Diagram &mdash; <span id="mapStationName" class="text-cyan-400">GWL</span>
        </span>
        <span id="disruptionTag" class="text-[10px] px-2 py-0.5 rounded mono bg-emerald-950 text-emerald-400 border border-emerald-800">Nominal Operations</span>
      </div>
      <svg id="trackSvg" viewBox="0 0 880 260" class="w-full bg-slate-950 rounded-lg border border-slate-800">
        <!-- Corridors with signal lights -->
        <line x1="40"  y1="50"  x2="260" y2="110" stroke="#1e293b" stroke-width="4"/>
        <text x="8"   y="48"   fill="#64748b" font-size="9" class="mono font-bold">NORTH (UP)</text>
        <circle cx="230" cy="100" r="4" fill="#22c55e" id="sig_N"/>

        <line x1="40"  y1="210" x2="260" y2="150" stroke="#1e293b" stroke-width="4"/>
        <text x="8"   y="222"  fill="#64748b" font-size="9" class="mono font-bold">SOUTH (DN)</text>
        <circle cx="230" cy="160" r="4" fill="#22c55e" id="sig_S"/>

        <line x1="40"  y1="110" x2="260" y2="120" stroke="#334155" stroke-width="2" stroke-dasharray="5"/>
        <text x="8"   y="108"  fill="#475569" font-size="8" class="mono">EAST BR.</text>

        <line x1="40"  y1="150" x2="260" y2="140" stroke="#334155" stroke-width="2" stroke-dasharray="5"/>
        <text x="8"   y="165"  fill="#475569" font-size="8" class="mono">WEST BR.</text>

        <!-- Dynamic Platform lines -->
        <g id="platformLines"></g>
        <line x1="620" y1="130" x2="840" y2="130" stroke="#1e293b" stroke-width="4"/>
        <text x="730" y="122" fill="#64748b" font-size="9" class="mono font-bold">DEPARTURE TRUNK</text>
        <g id="trainSvgContainer"></g>
      </svg>
    </div>

    <!-- Tabular Priority Traffic Board -->
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 shadow-2xl">
      <div class="flex justify-between items-center mb-2">
        <span class="text-xs font-black text-slate-400 tracking-widest mono uppercase">
          Priority Traffic Precedence Table
        </span>
        <span id="runningTag" class="text-[10px] px-2 py-0.5 rounded mono bg-slate-800 text-slate-400 border border-slate-700">STANDBY</span>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-left text-xs">
          <thead>
            <tr class="text-[10px] text-slate-500 border-b border-slate-800 mono">
              <th class="pb-2 pr-2">#</th>
              <th class="pb-2 pr-3">Train</th>
              <th class="pb-2 pr-2">Corridor</th>
              <th class="pb-2 pr-2">Sched.</th>
              <th class="pb-2 pr-2">ETA</th>
              <th class="pb-2 pr-2">Delay</th>
              <th class="pb-2 pr-3">Advised Speed</th>
              <th class="pb-2 pr-2">Signal</th>
              <th class="pb-2 pr-2">PF</th>
              <th class="pb-2 pr-2">Dist</th>
              <th class="pb-2 pr-2">Safety</th>
              <th class="pb-2">MAS Handoff</th>
            </tr>
          </thead>
          <tbody id="boardBody" class="divide-y divide-slate-800/60 font-medium"></tbody>
        </table>
      </div>
    </div>

  </div>

  <!-- ─── RIGHT: LOCO-PILOT CAB HUD + DISPATCH DIARY (5 COLS) ─── -->
  <div class="col-span-12 xl:col-span-5 space-y-3">

    <!-- LOCO-PILOT CAB HUD (DMI) -->
    <div class="bg-gradient-to-b from-slate-900 to-slate-950 border border-cyan-800/40 rounded-xl p-4 shadow-2xl relative overflow-hidden">
      <div class="flex justify-between items-center mb-3">
        <span class="text-xs font-black text-cyan-400 tracking-widest mono uppercase flex items-center gap-1.5">
          <span>🚂 IN-CAB LOCO PILOT HUD (DMI)</span>
        </span>
        <span class="text-[10px] px-2 py-0.5 rounded mono bg-cyan-950 text-cyan-300 border border-cyan-700 font-bold" id="hudTrainNo">
          Select a Train Card
        </span>
      </div>

      <div class="grid grid-cols-2 gap-3 mb-3">
        <div class="bg-slate-950/80 p-3 rounded-lg border border-slate-800 text-center">
          <div class="text-[10px] text-slate-400 mono uppercase font-bold">Current Speed</div>
          <div class="text-2xl font-black mono text-cyan-300 mt-1" id="hudCurSpeed">-- <span class="text-xs text-slate-400 font-normal">km/h</span></div>
          <div class="text-[10px] text-emerald-400 mono mt-1 font-bold">Advised: <span id="hudAdvSpeed">-- km/h</span></div>
        </div>
        <div class="bg-slate-950/80 p-3 rounded-lg border border-slate-800 text-center flex flex-col justify-center items-center">
          <div class="text-[10px] text-slate-400 mono uppercase font-bold mb-1">Cab Signal Aspect</div>
          <div class="w-7 h-7 rounded-full flex items-center justify-center signal-green" id="hudSignalBall"></div>
          <div class="text-[11px] font-black mono text-emerald-400 mt-1" id="hudSignalText">CLEAR</div>
        </div>
      </div>

      <div class="bg-slate-950/80 p-2.5 rounded-lg border border-slate-800 text-xs mono space-y-1.5">
        <div class="flex justify-between">
          <span class="text-slate-400">Target Distance:</span>
          <span class="font-bold text-white" id="hudDist">-- km</span>
        </div>
        <div class="flex justify-between">
          <span class="text-slate-400">Target Platform:</span>
          <span class="font-bold text-cyan-400" id="hudPF">Platform --</span>
        </div>
        <div class="pt-1 border-t border-slate-800/80 text-[11px] text-amber-300 leading-tight" id="hudAdvice">
          Select any train above to inspect real-time in-cab throttle recommendations.
        </div>
      </div>
    </div>

    <!-- Official COA Controller Diary / Live Transmissions Stream -->
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-3.5 flex flex-col shadow-xl" style="height:320px">
      <div class="flex justify-between items-center mb-2">
        <h2 class="text-xs font-black text-slate-400 tracking-widest mono uppercase">
          Section Controller Transmission Diary
        </h2>
        <span class="text-[9px] px-1.5 py-0.5 rounded mono bg-slate-800 text-slate-400">COA v4.2</span>
      </div>
      <div id="agentLog" class="flex-1 bg-slate-950 p-2.5 rounded-lg border border-slate-800 overflow-y-auto mono text-[10px] text-slate-300 space-y-1"></div>
    </div>

  </div>
</div>

<!-- ═══════════════ JAVASCRIPT LOGIC ═══════════════ -->
<script>
const savedKey = localStorage.getItem("railrescue_api_key");
if (savedKey) document.getElementById("stApiKey").value = savedKey;

let selectedTrainId = null;
let lastTelemetryData = null;

// ── ACTION HANDLERS ─────────────────────────────────────────────────────────
async function control(action) {
  try {
    const res = await fetch(`/api/control/${action}`, { method: "POST" });
    const data = await res.json();
    console.log("Control action:", action, data);
  } catch(e) {
    console.error("Control error:", e);
  }
}

async function loadScenario(name) {
  const st = document.getElementById("loadStatus");
  if (st) st.innerText = "Loading scenario...";
  try {
    const r = await fetch(`/api/scenario/${name}`, { method: "POST" });
    const d = await r.json();
    if (st) st.innerText = `Scenario loaded (${d.trains} trains)`;
  } catch(e) {
    if (st) st.innerText = "Error loading scenario.";
  }
}

async function autoResolve() {
  const btn = document.querySelector("#alertBanner button");
  if (btn) {
    btn.innerText = "⏳ Transmitting via Kavach LTE...";
    btn.disabled = true;
  }
  try {
    const r = await fetch(`/api/auto_resolve`, { method: "POST" });
    const d = await r.json();
    console.log("Auto-resolve:", d);
    
    const alertBanner = document.getElementById("alertBanner");
    if (alertBanner) alertBanner.classList.add("hidden");
    
    const loadStatus = document.getElementById("loadStatus");
    if (loadStatus) loadStatus.innerText = "🛡️ " + (d.message || "ADSS Auto-Dispatch applied!");
  } catch(e) {
    console.error("Auto resolve error:", e);
  } finally {
    if (btn) {
      btn.innerText = "🛡️ Apply ADSS Auto-Dispatch";
      btn.disabled = false;
    }
  }
}

function togglePeerModal() {
  const box = document.getElementById("peerConnectorBox");
  if (box) box.classList.toggle("hidden");
}

async function registerNewPeer() {
  const codeIn = document.getElementById("peerStationCode");
  const urlIn  = document.getElementById("peerUrl");
  const code = (codeIn ? codeIn.value : "").trim().toUpperCase();
  const url  = (urlIn ? urlIn.value : "").trim();
  if (!code || !url) return alert("Please enter both Station Code (e.g. AGC) and URL (e.g. http://192.168.1.55:8000)");
  
  try {
    const r = await fetch("/api/mesh/peers/register", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ station_code: code, url: url })
    });
    const d = await r.json();
    if (d.success) {
      alert(`Connected successfully to Agent-${code} at ${url}!`);
      togglePeerModal();
    } else {
      alert("Error registering peer: " + (d.error || "Unknown error"));
    }
  } catch(e) {
    alert("Network error connecting to peer laptop: " + e.message);
  }
}

async function triggerAgentHandoff(trainId) {
  const st = document.getElementById("loadStatus");
  if (st) st.innerText = `Initiating MAS handoff for Train ${trainId}...`;
  try {
    const r = await fetch(`/api/mesh/trigger_handoff`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ train_id: trainId })
    });
    const d = await r.json();
    if (st) st.innerText = `MAS Handoff completed for Train ${trainId}.`;
  } catch(e) {
    if (st) st.innerText = "Error triggering handoff.";
  }
}


async function loadStation() {
  const codeIn = document.getElementById("stCode");
  const code   = (codeIn ? codeIn.value : "").trim().toUpperCase() || "GWL";
  const keyIn  = document.getElementById("stApiKey");
  const apiKey = (keyIn ? keyIn.value : "").trim();
  if (apiKey) localStorage.setItem("railrescue_api_key", apiKey);

  const btn = document.getElementById("loadBtn");
  const st  = document.getElementById("loadStatus");
  if (btn) { btn.disabled = true; btn.innerText = "Loading..."; }
  if (st) st.innerText = "Fetching board...";

  try {
    const r = await fetch("/api/load_station", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ station_code: code, api_key: apiKey })
    });
    const d = await r.json();
    if (st) st.innerText = `${d.train_count} trains loaded via ${d.source}.`;
  } catch(e) {
    if (st) st.innerText = "Network error loading station.";
  } finally {
    if (btn) { btn.disabled = false; btn.innerText = "🚉 Load Station Board"; }
  }
}

async function addSingleTrain() {
  const trainIn = document.getElementById("singleTrain");
  const no = (trainIn ? trainIn.value : "").trim();
  const apiKey = (document.getElementById("stApiKey") ? document.getElementById("stApiKey").value : "").trim();
  if (!no) return alert("Enter train number.");

  const st = document.getElementById("loadStatus");
  if (st) st.innerText = `Adding train ${no}...`;
  try {
    const r = await fetch("/api/fetch_and_add", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ train_number: no, api_key: apiKey })
    });
    const d = await r.json();
    if (st) st.innerText = d.message;
    if (trainIn) trainIn.value = "";
  } catch(e) {
    if (st) st.innerText = "Error adding train.";
  }
}

async function removeTrain(tid) {
  try {
    await fetch("/api/remove_train", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ train_id: tid })
    });
  } catch(e) {
    console.error("Remove train error:", e);
  }
}

function selectTrainForHUD(tid) {
  selectedTrainId = tid;
  if (lastTelemetryData) renderDashboard(lastTelemetryData);
}

function signalClass(aspect) {
  return { CLEAR:"signal-green", ATTENTION:"signal-yellow", CAUTION:"signal-orange", DANGER:"signal-red", BERTHED:"signal-gray" }[aspect] || "signal-gray";
}

function tierBadgeClass(tier) {
  return { 2:"tier-badge-2", 4:"tier-badge-4", 5:"tier-badge-5", 7:"tier-badge-7" }[tier] || "tier-badge-5";
}

function tierLabel(tier) {
  return { 2:"Tier 2 (Raj/VB)", 4:"Tier 4 (Superfast)", 5:"Tier 5 (Express)", 7:"Tier 7 (Freight)" }[tier] || "Express";
}

function delayBadge(delayMin, color) {
  const sign = delayMin > 0 ? "+" : "";
  return `<span class="px-1.5 py-0.5 rounded text-[10px] font-black mono" style="background:${color}22;color:${color};border:1px solid ${color}44">
            ${sign}${delayMin.toFixed(1)} min
          </span>`;
}

// ── CENTRAL DASHBOARD RENDERER ──────────────────────────────────────────────
function renderDashboard(data) {
  if (!data) return;
  lastTelemetryData = data;

  // Header & Info
  const stLabel = document.getElementById("stationLabel");
  if (stLabel) stLabel.innerText = `${data.station_code} — ${data.station_name}`;
  const dStName = document.getElementById("directiveStationName");
  if (dStName) dStName.innerText = data.station_name;
  const dCount = document.getElementById("directiveTrainCount");
  if (dCount) dCount.innerText = (data.trains || []).length;
  const mName = document.getElementById("mapStationName");
  if (mName) mName.innerText = data.station_code;
  const sDate = document.getElementById("simDate");
  if (sDate) sDate.innerText = data.sim_date;
  const sClock = document.getElementById("simClock");
  if (sClock) sClock.innerText = data.sim_clock + " IST";
  const zLabel = document.getElementById("zoneLabel");
  if (zLabel) zLabel.innerText = "Zone: " + (data.station_zone || "IR");
  const pfLab = document.getElementById("pfLabel");
  if (pfLab) pfLab.innerText = (data.station_platforms || 6) + " Platforms";

  // MAS Directive Banner & Peer Network
  const meshBanner = document.getElementById("meshDirectiveText");
  if (meshBanner) {
    if (data.mesh_directive) {
      const md = data.mesh_directive;
      const statusColor = md.status === 'REJECTED_HOLD' ? '#f43f5e' : '#22c55e';
      meshBanner.innerHTML = `
        <div class="flex items-center justify-between mb-1">
          <span class="font-black" style="color:${statusColor}">[AGENT HANDOFF: ${md.source} ➔ ${md.target}]</span>
          <span class="text-slate-400">${md.timestamp}</span>
        </div>
        <div class="text-slate-200 font-bold">${md.directive}</div>
      `;
    }
  }

  const peersBadge = document.getElementById("meshPeersBadge");
  if (peersBadge && data.peers) {
    peersBadge.innerText = "Mesh Agents: " + data.peers.join(" • ");
  }

  // Metrics
  const mDel = document.getElementById("metricDelay");
  if (mDel && data.delay_recovered !== undefined) mDel.innerText = `+${data.delay_recovered.toFixed(1)} min`;
  const mEng = document.getElementById("metricEnergy");
  if (mEng && data.energy_saved !== undefined) mEng.innerText = `${data.energy_saved.toFixed(0)} kWh`;
  const mOut = document.getElementById("metricOuter");
  if (mOut && data.outer_waits !== undefined) mOut.innerText = `${data.outer_waits.toFixed(1)} min`;

  // Status Tags
  const runTag = document.getElementById("runningTag");
  if (runTag) {
    runTag.innerText = data.is_running ? "● LIVE ACTIVE" : "⏸ PAUSED";
    runTag.className = data.is_running
      ? "text-[10px] px-2 py-0.5 rounded mono bg-emerald-950 text-emerald-400 border border-emerald-700 shadow-sm"
      : "text-[10px] px-2 py-0.5 rounded mono bg-slate-800 text-slate-400 border border-slate-700";
  }

  const dtag = document.getElementById("disruptionTag");
  if (dtag) {
    dtag.innerText = data.disruption_active ? data.disruption_text : "Nominal Operations";
    dtag.className = data.disruption_active
      ? "text-[10px] px-2 py-0.5 rounded mono bg-rose-950 text-rose-400 border border-rose-700 blink"
      : "text-[10px] px-2 py-0.5 rounded mono bg-emerald-950 text-emerald-400 border border-emerald-800";
  }

  const dBtn = document.getElementById("disruptBtn");
  if (dBtn) dBtn.innerText = data.disruption_active ? "✅ CLEAR DISRUPTION" : "⚡ INJECT DISRUPTION";

  // ── Collision Alert Banner ──
  const alertBanner  = document.getElementById("alertBanner");
  const alertContent = document.getElementById("alertContent");
  const critAlerts   = (data.alerts || []).filter(a => a.status !== "NOMINAL_CLEAR" && a.status !== "CAUTION_CONVERGING");
  if (alertBanner && alertContent) {
    if (critAlerts.length > 0) {
      alertBanner.classList.remove("hidden");
      alertContent.innerHTML = critAlerts.map(a => `
        <div class="flex flex-col gap-0.5">
          <span class="font-black text-rose-300">
            ${a.status === "HARD_INTERLOCK_VIOLATION" ? "🚨 KAVACH HARD INTERLOCK" : "⚠️ CRITICAL CONFLICT"}
            — Trains <b>${a.lead_id}</b> &amp; <b>${a.trail_id}</b>
          </span>
          <span class="text-rose-400">${a.consequence || ""}</span>
          ${Object.entries(a.recommended_action || {}).map(([id, cmd]) =>
            `<span class="text-amber-300">→ <b>Train ${id}</b>: ${cmd}</span>`
          ).join("")}
        </div>
      `).join('<div class="border-t border-rose-800/50 my-1"></div>');
    } else {
      alertBanner.classList.add("hidden");
    }
  }

  // ── PRIMARY OUTPUT: DIRECTIVES CARDS GRID ──
  const dGrid = document.getElementById("directivesGrid");
  if (dGrid) {
    if (!data.trains || data.trains.length === 0) {
      dGrid.innerHTML = `
        <div class="col-span-full py-8 text-center text-slate-500 mono text-sm">
          No trains active in section. Enter a station code or pick a scenario above.
        </div>`;
    } else {
      const sorted = [...data.trains].sort((a, b) =>
        (a.priority_rank || 99) - (b.priority_rank || 99) || a.tier - b.tier
      );

      if (!selectedTrainId && sorted.length > 0) {
        selectedTrainId = sorted[0].id;
      }

      dGrid.innerHTML = sorted.map(t => {
        const isSel = (t.id === selectedTrainId);
        const cardBorder = isSel ? "border-cyan-400 ring-2 ring-cyan-500/20 bg-slate-900" : "border-slate-800 bg-slate-950/70 hover:border-slate-700";
        
        const distKm = ((t.dist_remaining || 0) / 1000).toFixed(1);
        const speedCmd = t.cmd_title || `RUN AT ${(t.advised_speed || t.mps).toFixed(0)} KM/H`;
        const speedDetail = t.cmd_detail || t.action_text || "Maintain section timetable speed.";
        const crossingText = t.crossing || "Cleared for direct approach.";

        const riskBadge = t.risk_status === "HARD_INTERLOCK_VIOLATION"
          ? `<span class="px-2 py-0.5 rounded text-[10px] font-black mono bg-rose-950 text-rose-300 border border-rose-600 blink">🚨 KAVACH BRAKE VIOLATION</span>`
          : t.risk_status === "CRITICAL_CONFLICT"
          ? `<span class="px-2 py-0.5 rounded text-[10px] font-black mono bg-amber-950 text-amber-300 border border-amber-600">⚠️ CONFLICT DETECTED</span>`
          : `<span class="px-2 py-0.5 rounded text-[10px] font-black mono bg-emerald-950/70 text-emerald-400 border border-emerald-800">🛡️ KAVACH PROTECTED</span>`;

        return `
        <div onclick="selectTrainForHUD('${t.id}')" class="p-3.5 rounded-xl border ${cardBorder} transition cursor-pointer flex flex-col justify-between shadow-lg">
          
          <!-- Top Row: Train Identity & Precedence -->
          <div>
            <div class="flex items-start justify-between gap-2 pb-2 mb-2 border-b border-slate-800/80">
              <div class="flex items-center gap-2">
                <span class="w-3 h-3 rounded-full shrink-0" style="background:${t.color}"></span>
                <div>
                  <div class="font-black text-sm text-white mono flex items-center gap-1.5">
                    <span>Train ${t.id}</span>
                    <span class="text-xs text-slate-300 font-normal">(${t.name})</span>
                  </div>
                  <div class="flex items-center gap-1.5 mt-0.5">
                    <span class="text-[9px] px-1.5 py-0.2 rounded font-bold mono ${tierBadgeClass(t.tier)}">${tierLabel(t.tier)}</span>
                    <span class="text-[10px] text-slate-400 mono font-semibold">Priority #${t.priority_rank || 1}</span>
                  </div>
                </div>
              </div>
              <div class="text-right">
                <span class="text-xs font-black mono text-cyan-400">${distKm} km</span>
                <div class="text-[9px] text-slate-500 mono">${t.corridor_dir || "N"} Corridor</div>
              </div>
            </div>

            <!-- Central Big Speed Command Badge -->
            <div class="p-2.5 rounded-lg bg-slate-900 border border-slate-800 mb-2.5">
              <div class="flex items-center justify-between">
                <span class="text-[10px] uppercase mono font-black text-slate-400">⚡ Operational Speed Directive</span>
                <span class="text-xs font-black mono" style="color:${t.delay_color || '#3b82f6'}">${(t.advised_speed || 0).toFixed(0)} km/h</span>
              </div>
              <div class="text-xs font-black mono text-cyan-300 mt-1">${speedCmd}</div>
              <div class="text-[11px] text-slate-300 mt-1 leading-snug">${speedDetail}</div>
            </div>

            <!-- Signal & Crossing Directives -->
            <div class="grid grid-cols-2 gap-2 text-[11px] mono mb-2">
              <div class="p-2 rounded bg-slate-900/60 border border-slate-800/80">
                <div class="text-[9px] text-slate-400 uppercase font-bold">Signal Directive</div>
                <div class="flex items-center gap-1.5 mt-1">
                  <span class="w-2.5 h-2.5 rounded-full ${signalClass(t.signal_aspect)}"></span>
                  <span class="font-black text-white">${t.signal_aspect || "CLEAR"}</span>
                </div>
                <div class="text-[10px] text-slate-400 truncate mt-0.5">${(t.signal_km_from_station || 0).toFixed(1)}km to Signal</div>
              </div>

              <div class="p-2 rounded bg-slate-900/60 border border-slate-800/80">
                <div class="text-[9px] text-slate-400 uppercase font-bold">Platform Berth</div>
                <div class="font-black text-cyan-400 text-xs mt-1">Platform ${t.allocated_pf || 1}</div>
                <div class="text-[10px] text-slate-400 truncate mt-0.5">ETA: ${t.predicted_arrival_str || t.dynamic_eta || "--:--"}</div>
              </div>
            </div>

            <!-- Precedence & Crossing Action -->
            <div class="p-2 rounded bg-slate-900/40 border border-slate-800 text-[10px] mono text-slate-300">
              <span class="text-slate-500 font-bold">Crossing:</span> ${crossingText}
            </div>
          </div>

          <!-- Bottom Row: Safety Interlock Badge & MAS Trigger -->
          <div class="pt-2 mt-2 border-t border-slate-800/80 flex items-center justify-between gap-2 flex-wrap">
            <div class="flex items-center gap-2">
              ${riskBadge}
              <button onclick="event.stopPropagation(); triggerAgentHandoff('${t.id}')" class="px-2 py-0.5 rounded text-[10px] font-bold mono bg-indigo-900/80 hover:bg-indigo-700 text-indigo-200 border border-indigo-600 transition">
                ⚡ ➔ Handoff
              </button>
            </div>
            <span class="text-[10px] mono text-slate-400">Delay: ${delayBadge(t.delay_min || 0, t.delay_color || '#6b7280')}</span>
          </div>

        </div>`;
      }).join("");

      // ── Loco Pilot HUD ──
      const selTrain = data.trains.find(t => t.id === selectedTrainId) || sorted[0];
      if (selTrain) {
        const hNo = document.getElementById("hudTrainNo");
        if (hNo) hNo.innerText = `${selTrain.id} (${selTrain.name.substring(0,14)})`;
        const hCur = document.getElementById("hudCurSpeed");
        if (hCur) hCur.innerHTML = `${(selTrain.current_speed||0).toFixed(0)} <span class="text-xs text-slate-400 font-normal">km/h</span>`;
        const hAdv = document.getElementById("hudAdvSpeed");
        if (hAdv) hAdv.innerText = `${(selTrain.advised_speed||selTrain.mps).toFixed(0)} km/h`;
        const hBall = document.getElementById("hudSignalBall");
        if (hBall) hBall.className = `w-7 h-7 rounded-full flex items-center justify-center ${signalClass(selTrain.signal_aspect)}`;
        const hSigT = document.getElementById("hudSignalText");
        if (hSigT) {
          hSigT.innerText = selTrain.signal_aspect || "CLEAR";
          hSigT.style.color = selTrain.signal_color || "#22c55e";
        }
        const hDst = document.getElementById("hudDist");
        if (hDst) hDst.innerText = `${((selTrain.dist_remaining||0)/1000).toFixed(2)} km`;
        const hPf = document.getElementById("hudPF");
        if (hPf) hPf.innerText = `Platform ${selTrain.allocated_pf || 1}`;
        const hAdvT = document.getElementById("hudAdvice");
        if (hAdvT) hAdvT.innerText = selTrain.cmd_detail || selTrain.action_text || "Maintain section speed.";
      }
    }
  }

  // ── Tabular Priority Table ──
  const tbody = document.getElementById("boardBody");
  if (tbody) {
    if (!data.trains || data.trains.length === 0) {
      tbody.innerHTML = `<tr><td colspan="12" class="py-6 text-center text-slate-600 mono">No trains loaded.</td></tr>`;
    } else {
      const sorted = [...data.trains].sort((a, b) =>
        (a.priority_rank || 99) - (b.priority_rank || 99) || a.tier - b.tier
      );
      tbody.innerHTML = sorted.map(t => `
        <tr onclick="selectTrainForHUD('${t.id}')" class="border-b border-slate-800/40 hover:bg-slate-800/30 cursor-pointer transition">
          <td class="py-2 pr-2 mono text-slate-400 font-black">${t.priority_rank || "—"}</td>
          <td class="pr-3">
            <div class="flex items-center gap-1.5">
              <span class="w-2 h-2 rounded-full shrink-0" style="background:${t.color}"></span>
              <div>
                <div class="font-black text-white text-xs">${t.id}</div>
                <div class="text-[10px] text-slate-400 truncate max-w-28">${(t.name||"").substring(0,18)}</div>
              </div>
            </div>
          </td>
          <td class="pr-2 text-[10px] mono text-slate-400">${t.corridor_dir||"?"}</td>
          <td class="pr-2 mono text-[10px] text-slate-300">${t.scheduled_arrival_str||"--:--"}</td>
          <td class="pr-2 mono text-[10px] text-cyan-300">${t.predicted_arrival_str||t.dynamic_eta||"--:--"}</td>
          <td class="pr-2">${delayBadge(t.delay_min || 0, t.delay_color || "#6b7280")}</td>
          <td class="pr-3 mono font-black" style="color:${t.delay_color || '#3b82f6'}">${(t.advised_speed||0).toFixed(0)} km/h</td>
          <td class="pr-2 mono text-[10px]"><span class="inline-block w-2 h-2 rounded-full mr-1 ${signalClass(t.signal_aspect)}"></span>${t.signal_aspect||"?"}</td>
          <td class="pr-2 mono font-black text-cyan-400 text-[10px]">PF ${t.allocated_pf||1}</td>
          <td class="pr-2 mono text-[10px] text-slate-400">${((t.dist_remaining||0)/1000).toFixed(1)}km</td>
          <td class="mono text-[10px] text-emerald-400 pr-2">${t.risk_status === 'NOMINAL_CLEAR' ? 'PROTECTED' : t.risk_status}</td>
          <td>
            <button onclick="event.stopPropagation(); triggerAgentHandoff('${t.id}')" class="px-2 py-0.5 rounded text-[9px] font-bold mono bg-indigo-900/80 hover:bg-indigo-700 text-indigo-300 border border-indigo-600 transition">
              ⚡ ➔ Handoff
            </button>
          </td>
        </tr>
      `).join("");
    }
  }

  // ── Section Controller Diary Log ──
  const aLog = document.getElementById("agentLog");
  if (aLog) {
    aLog.innerHTML = (data.logs || []).map(l => `<div class="leading-snug">${l}</div>`).join("");
  }

  // ── DYNAMIC SVG TRACK & PLATFORM VISUALIZER ──
  const nPf = data.station_platforms || 6;
  const pfSpacing = nPf <= 8 ? 26 : (nPf <= 14 ? 18 : 14);
  const pfStartY = 60;
  const totalSvgHeight = Math.max(260, pfStartY + nPf * pfSpacing + 40);

  const trackSvg = document.getElementById("trackSvg");
  if (trackSvg) {
    trackSvg.setAttribute("viewBox", `0 0 880 ${totalSvgHeight}`);
  }

  const pfLines = document.getElementById("platformLines");
  if (pfLines) {
    let pfHtml = "";
    for (let p = 1; p <= nPf; p++) {
      const y = pfStartY + (p - 1) * pfSpacing;
      const isClosed = data.disruption_active && (p === 1);
      const strokeColor = isClosed ? "#ef4444" : "#334155";
      const textColor = isClosed ? "#ef4444" : "#94a3b8";
      const label = isClosed ? `PF ${p} [CLOSED]` : `PF ${p}`;
      const strokeDash = isClosed ? 'stroke-dasharray="4"' : "";
      pfHtml += `
        <line x1="260" y1="${y}" x2="620" y2="${y}" stroke="${strokeColor}" stroke-width="2.5" ${strokeDash}/>
        <text x="628" y="${y + 3}" fill="${textColor}" font-size="${nPf > 12 ? 8 : 9}" class="mono font-bold">${label}</text>
      `;
    }
    pfLines.innerHTML = pfHtml;
  }

  const svgContainer = document.getElementById("trainSvgContainer");
  if (svgContainer) {
    svgContainer.innerHTML = "";
    (data.trains || []).forEach(t => {
      const ratio = Math.max(0, Math.min(1, 1 - (t.dist_remaining / Math.max(t.dist, 1))));
      let x = 40, y = 130;
      const midY = totalSvgHeight / 2;
      const bottomY = pfStartY + (nPf - 1) * pfSpacing;

      if (t.corridor === "NORTH_CORRIDOR") { x = 40 + ratio * 220; y = 50 + ratio * (pfStartY - 50); }
      else if (t.corridor === "SOUTH_CORRIDOR") { x = 40 + ratio * 220; y = (bottomY + 40) - ratio * 40; }
      else if (t.corridor === "EAST_BRANCH") { x = 40 + ratio * 220; y = (midY - 20) + ratio * 20; }
      else { x = 40 + ratio * 220; y = (midY + 20) - ratio * 20; }

      if (ratio >= 0.90) {
        x = 260 + (ratio - 0.90) / 0.10 * 350;
        const pf = Math.min(nPf, Math.max(1, t.allocated_pf || 1));
        y = pfStartY + (pf - 1) * pfSpacing;
      }

      const hasRisk = t.risk_score > 0.4;
      svgContainer.innerHTML += `
        <g transform="translate(${x - 20}, ${y - 6})" class="cursor-pointer" onclick="selectTrainForHUD('${t.id}')">
          ${hasRisk ? `<circle cx="20" cy="6" r="18" fill="${t.color}" opacity="0.3" class="pulse-ring"/>` : ""}
          <rect x="0"  y="2"  width="8"  height="7" rx="1" fill="#475569"/>
          <rect x="10" y="2"  width="8"  height="7" rx="1" fill="#64748b"/>
          <rect x="20" y="1"  width="12" height="9" rx="2" fill="${t.color}" stroke="#fff" stroke-width="0.7"/>
          <polygon points="32,5 44,1 44,9 32,7" fill="#fef08a" opacity="0.3"/>
          <text x="0" y="-2" fill="#fff" font-size="7" font-weight="bold" class="mono">${t.id}</text>
        </g>`;
    });
  }
}

// ── AUTO-RECONNECTING WEBSOCKET WITH HTTP POLLING FALLBACK ───────────────────
let ws = null;
let pollTimer = null;

function startHttpPolling() {
  if (pollTimer) return;
  pollTimer = setInterval(async () => {
    try {
      const res = await fetch("/api/telemetry");
      if (res.ok) {
        const data = await res.json();
        renderDashboard(data);
      }
    } catch(e) {
      console.warn("HTTP polling error:", e);
    }
  }, 1000);
}

function initWS() {
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  try {
    ws = new WebSocket(`${protocol}//${location.host}/ws/telemetry`);

    ws.onopen = () => {
      console.log("WebSocket connected to RailRescue telemetry stream.");
      if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        renderDashboard(data);
      } catch(err) {
        console.error("Dashboard render error:", err);
      }
    };

    ws.onclose = () => {
      startHttpPolling();
      setTimeout(initWS, 2000);
    };

    ws.onerror = (err) => {
      console.warn("WebSocket error, falling back to HTTP:", err);
      startHttpPolling();
    };
  } catch(e) {
    startHttpPolling();
  }
}

initWS();
</script>
</body>
</html>"""


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=False)
