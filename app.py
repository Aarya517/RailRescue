import asyncio
import json
import math
import os
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from google import genai
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from ortools.sat.python import cp_model

app = FastAPI(title="RailRescue Mesh - Advanced Gwalior Digital Twin")

INDIAN_RAIL_API_KEY = os.getenv("INDIAN_RAIL_API_KEY", "")

# Gwalior Junction Reference GPS Anchor
GWL_GPS = {"lat": 26.2163, "lon": 78.1728}


# ==========================================
# 1. API ADAPTER WITH GPS & ROUTE SELECTION
# ==========================================


class RailRadarJSONAdapter:
    @staticmethod
    def parse_railradar_response(resp_json: Dict[str, Any], api_key: str = "") -> tuple[bool, str, Optional[Dict[str, Any]]]:
        if not resp_json.get("success", False):
            return False, "API returned success=false", None

        data_block = resp_json.get("data", {})
        train_number = str(data_block.get("trainNumber", "00000")).strip()
        train_name = str(data_block.get("trainName", f"Train {train_number}")).strip()
        status = str(data_block.get("status", "running")).lower()
        delay_mins = int(data_block.get("delayMinutes", 0))

        # ---------------------------------------------------------
        # 🚨 GRAB TRUE LIVE LOCATION FROM THE API PAYLOAD
        # Checks for standard live distance keys in the API response
        # ---------------------------------------------------------
        live_dist_km = float(data_block.get("distanceToDestination", data_block.get("distance", 15.0)))
        dist_m = live_dist_km * 1000.0  # Convert true km to meters for the physics engine
        
        # Grab true live speed if the API provides it
        live_speed_kmh = float(data_block.get("speed", data_block.get("currentSpeed", 85.0)))

        tier = 5
        mps = 100.0
        mass = 780.0
        pax = 1200
        name_u = train_name.upper()

        if "RAJDHANI" in name_u or "SHATABDI" in name_u or "VANDE" in name_u or "TEJAS" in name_u:
            tier, mps, mass, pax = 2, 130.0, 520.0, 1200
        elif "SF" in name_u or "SUPERFAST" in name_u or "DURONTO" in name_u or "MAIL" in name_u:
            tier, mps, mass, pax = 4, 110.0, 850.0, 1750
        elif "GOODS" in name_u or "BOXN" in name_u or "FREIGHT" in name_u:
            tier, mps, mass, pax = 7, 75.0, 3800.0, 0

        corridor = "NORTH_CORRIDOR"
        route_desc = "UP Fast Trunk (Live Inbound)"
        best_route = "Main UP Line -> Platform 1"

        if any(term in name_u for term in ["MUMBAI", "JHANSI", "BHOPAL", "KERALA", "CHENNAI", "CSMT"]):
            corridor = "SOUTH_CORRIDOR"
            route_desc = "DN Main Trunk (Live Inbound)"
            best_route = "DN Main Track -> Platform 4"

        # Set true ETA based on real distance and speed
        current_time = datetime.now()
        time_to_reach_secs = (dist_m / 1000.0) / mps * 3600
        final_eta_dt = current_time + timedelta(seconds=time_to_reach_secs)
        arr_time_str = final_eta_dt.strftime("%H:%M")

        adapted_train = {
            "id": train_number,
            "name": train_name,
            "tier": tier,
            "corridor": corridor,
            "route_type": route_desc,
            "best_route": best_route,
            "entry": "LIVE_API_INGEST",
            "dist": dist_m,             # <--- TRUE API DISTANCE
            "mps": mps,
            "current_speed": live_speed_kmh if status == "running" else 0.0,
            "required_speed": live_speed_kmh,
            "eta_dt_iso": final_eta_dt.isoformat(),
            "arr_time_str": f"{arr_time_str} (+{delay_mins}m)",
            "dwell": 180.0,
            "mass": mass,
            "pax": pax,
            "delay_minutes": delay_mins,
            "status": "RUNNING" if status == "running" else "HELD",
            "color": "#f43f5e" if tier == 2 else "#38bdf8"
        }

        return True, f"Live Ingest: {train_name} spawned exactly {round(dist_m/1000, 1)}km away.", adapted_train
    
    @classmethod
    def fetch_live_train(
        cls, train_number: str, query_date: str = "", api_key: str = ""
    ) -> tuple[bool, str, Optional[Dict[str, Any]]]:
        clean_train = str(train_number).strip()
        clean_key = str(api_key).strip()

        if clean_key:
            url = f"https://api.railradar.in/v1/trains/{clean_train}/live"
            headers = {"Authorization": f"Bearer {clean_key}", "x-api-key": clean_key}
            try:
                resp = requests.get(url, headers=headers, timeout=8)
                if resp.status_code == 200:
                    return cls.parse_railradar_response(resp.json(), clean_key)
                else:
                    return (
                        False,
                        f"RailRadar returned HTTP status {resp.status_code}",
                        None,
                    )
            except Exception as e:
                return False, f"API Connection error: {e}", None

        sample_json = {
            "success": True,
            "data": {
                "trainNumber": clean_train or "12952",
                "trainName": (
                    "MUMBAI RAJDHANI"
                    if clean_train == "12952"
                    else f"EXPRESS {clean_train}"
                ),
                "status": "running",
                "delayMinutes": 12,
            },
            "meta": {
                "timestamp": datetime.now().isoformat(),
            },
        }
        return cls.parse_railradar_response(sample_json, clean_key)

    @classmethod
    def fetch_station_board(cls, station_code: str, hours: int, api_key: str):
        station = str(station_code).strip().upper()
        
        if not api_key:
            return False, "API Key is required.", []

        # --- USING YOUR EXACT API CONCEPT ---
        url = f"https://api.railradar.in/v1/stations/{station}/live"
        
        headers = {
            "Authorization": f"Bearer {api_key}"
        }

        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            # Print statements exactly as you requested
            print("HTTP Status:", response.status_code)
            data = response.json()
            
            print("\n==============================")
            print(f"      STATION: {station}")
            print("==============================")
            # print(data) # (Optional: uncomment to see the massive raw JSON dump in terminal)

            if response.status_code != 200:
                return False, f"RailRadar returned HTTP {response.status_code}", []

            # Extract the trains from the API response
            raw_trains = data.get("data", []) if isinstance(data, dict) else data
            trains_list = []
            
            # Loop through the live station data and adapt it for the simulation
            for item in raw_trains[:10]: # Limiting to 10 trains so the screen doesn't clutter
                # We package it so your existing parser understands it
                fake_resp = {"success": True, "data": item}
                success, msg, adapted_train = cls.parse_railradar_response(fake_resp, api_key)
                
                if success and adapted_train:
                    trains_list.append(adapted_train)

            return True, f"Successfully fetched live board for {station}.", trains_list

        except Exception as e:
            print(f"API Error: {e}")
            return False, f"API Connection error: {str(e)}", []
        

# ==========================================
# 2. PHYSICS & KINEMATICS ENGINE
# ==========================================


class Kinematics:
    SERVICE_DECEL = 0.65  # m/s^2
    ACCEL = 0.45  # m/s^2

    @staticmethod
    def get_advisory(
        dist_m: float, mps_kmh: float, target_kmh: float = 30.0
    ) -> tuple[float, str]:
        v_target_ms = target_kmh / 3.6
        stopping_dist = ((mps_kmh / 3.6) ** 2) / (2 * Kinematics.SERVICE_DECEL)
        if dist_m <= stopping_dist * 1.15:
            safe_v_ms = math.sqrt(
                max(0.0, (v_target_ms**2) + 2 * Kinematics.SERVICE_DECEL * dist_m)
            )
            return round(min(mps_kmh, safe_v_ms * 3.6), 1), "SERVICE_BRAKE_ACTIVE"
        elif dist_m <= stopping_dist * 2.2:
            return round(mps_kmh * 0.8, 1), "COASTING"
        return round(mps_kmh, 1), "MAX_SECTION_SPEED"

    @staticmethod
    def compute_gps(corridor: str, dist_remaining_m: float) -> Dict[str, float]:
        lat_per_m = 1.0 / 111139.0
        lon_per_m = 1.0 / (111139.0 * math.cos(math.radians(26.2163)))

        if corridor == "NORTH_CORRIDOR":
            lat = GWL_GPS["lat"] + (dist_remaining_m * lat_per_m * 0.95)
            lon = GWL_GPS["lon"] - (dist_remaining_m * lon_per_m * 0.3)
        elif corridor == "SOUTH_CORRIDOR":
            lat = GWL_GPS["lat"] - (dist_remaining_m * lat_per_m * 0.95)
            lon = GWL_GPS["lon"] + (dist_remaining_m * lon_per_m * 0.25)
        elif corridor == "EAST_BRANCH":
            lat = GWL_GPS["lat"] + (dist_remaining_m * lat_per_m * 0.2)
            lon = GWL_GPS["lon"] + (dist_remaining_m * lon_per_m * 0.95)
        else:
            lat = GWL_GPS["lat"] - (dist_remaining_m * lat_per_m * 0.3)
            lon = GWL_GPS["lon"] - (dist_remaining_m * lon_per_m * 0.95)

        return {"lat": round(lat, 5), "lon": round(lon, 5)}


# ==========================================
# 2.5 AI DISPATCHER COPILOT (NEW GEMINI API)
# ==========================================

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "YOUR_GEMINI_API_KEY")


class AIDispatcher:
    @staticmethod
    def get_strategy(train_data: List[Dict], disruption: str) -> str:
        if not GEMINI_API_KEY or GEMINI_API_KEY == "YOUR_GEMINI_API_KEY":
            return "AI API Key missing. Defaulting to standard holding patterns."

        try:
            client = genai.Client(api_key=GEMINI_API_KEY)

            context = f"You are the Chief Train Controller. There is a disruption: {disruption}. "
            context += "Current active trains in your sector:\n"
            for t in train_data:
                context += f"- Train {t['id']} ({t['name']}) at speed {t['current_speed']}km/h, distance {t['dist_remaining']}m.\n"
            context += "Provide a 2-sentence maximum, strict routing dispatch command to resolve conflicts and prevent gridlock."

            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=context,
            )
            return response.text.strip()
        except Exception as e:
            return f"AI Copilot Error: {str(e)}"


# ==========================================
# 3. ADVANCED SIMULATION SESSION
# ==========================================


class SimulationSession:
    def __init__(self):
        self.is_running = False
        self.sim_seconds = 0.0
        self.sim_base_datetime = datetime.now() 
        self.trains: List[Dict[str, Any]] = []  # Starts completely empty
        self.disruption_active = False
        self.disruption_text = "None (Nominal)"
        self.logs: List[str] = []
        self.active_train_dict: Dict[str, Dict[str, Any]] = {}

        self.log("System Ready. Waiting for live API ingestion...")
        

    def log(self, msg: str):
        cur_time = (
            self.sim_base_datetime + timedelta(seconds=self.sim_seconds)
        ).strftime("%H:%M:%S")
        self.logs.insert(0, f"[{cur_time}] {msg}")
        if len(self.logs) > 35:
            self.logs.pop()

    def add_or_update_train(self, train_data: Dict[str, Any]):
        tid = train_data["id"]
        self.active_train_dict[tid] = train_data
        
        existing = [t for t in self.trains if t["id"] == tid]
        if not existing:
            instance = dict(train_data)
            instance["dist_remaining"] = float(instance["dist"])
            instance["current_speed"] = min(float(instance["mps"]), 85.0)
            instance["status"] = "EN_ROUTE"
            instance["risk_score"] = 0.0
            instance["risk_status"] = "NOMINAL"
            instance["original_route"] = train_data["best_route"]
            instance["allocated_pf"] = 1
            instance["gps"] = Kinematics.compute_gps(instance["corridor"], instance["dist_remaining"])
            self.trains.append(instance)
        else:
            # FORCE UPDATE EXISTING TRAIN WITH LIVE API DATA
            target = existing[0]
            target["dist"] = float(train_data["dist"])
            target["dist_remaining"] = float(train_data["dist"])
            target["current_speed"] = float(train_data["current_speed"])
            target["required_speed"] = float(train_data.get("required_speed", target["current_speed"]))
            target["eta_dt_iso"] = train_data.get("eta_dt_iso")
            target["arr_time_str"] = train_data.get("arr_time_str")
            
            # Wipe away stale collision or arrival statuses
            target["status"] = "EN_ROUTE"
            target["risk_score"] = 0.0
            target["risk_status"] = "NOMINAL"
            target["best_route"] = train_data["best_route"]
            target["original_route"] = train_data["best_route"]
            
            # Destroy the "Arrived" bug if distance is now > 0
            if "dynamic_eta" in target and target["dist_remaining"] > 0:
                del target["dynamic_eta"] 

        self.recalculate_schedule()
        self.log(f"Mesh Initialized/Updated: Train {tid} -> Assigned {train_data['best_route']}")
        
    def recalculate_schedule(self):
        model = cp_model.CpModel()
        intervals_per_pf = {pf: [] for pf in range(1, 7)}
        train_pfs = {}
        for t in self.trains:
            pf_v = model.NewIntVar(1, 6, f"p_{t['id']}")
            train_pfs[t["id"]] = pf_v
            start_v = model.NewIntVar(0, 3600, f"s_{t['id']}")
            dwell_v = int(t.get("dwell", 180))
            end_v = model.NewIntVar(dwell_v, dwell_v + 3600, f"e_{t['id']}")
            for p in range(1, 7):
                is_p = model.NewBoolVar(f"t_{t['id']}_p_{p}")
                model.Add(pf_v == p).OnlyEnforceIf(is_p)
                model.Add(pf_v != p).OnlyEnforceIf(is_p.Not())
                iv = model.NewOptionalIntervalVar(
                    start_v, dwell_v + 120, end_v + 120, is_p, f"iv_{t['id']}_{p}"
                )
                intervals_per_pf[p].append(iv)

        for p in range(1, 7):
            model.AddNoOverlap(intervals_per_pf[p])

        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = 0.5
        if solver.Solve(model) in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            for t in self.trains:
                t["allocated_pf"] = solver.Value(train_pfs[t["id"]])
        else:
            for idx, t in enumerate(self.trains):
                t["allocated_pf"] = (idx % 6) + 1

    def remove_train(self, tid: str):
        self.trains = [t for t in self.trains if t["id"] != tid]
        if tid in self.active_train_dict:
            del self.active_train_dict[tid]
        self.recalculate_schedule()
        self.log(f"Removed Train {tid} from simulation loop.")

    def step(self, dt: float = 1.0):
        if not self.is_running or not self.trains:
            return

        self.sim_seconds += dt

        corridors: Dict[str, List[Dict[str, Any]]] = {}
        for t in self.trains:
            corridors.setdefault(t["corridor"], []).append(t)

        for c_name, c_trains in corridors.items():
            if len(c_trains) >= 2:
                c_trains.sort(key=lambda x: x["dist_remaining"])
                for i in range(len(c_trains) - 1):
                    lead = c_trains[i]
                    trail = c_trains[i + 1]
                    sep = abs(trail["dist_remaining"] - lead["dist_remaining"])
                    v_trail_ms = trail["current_speed"] / 3.6
                    braking_buffer = (v_trail_ms**2) / (
                        2 * Kinematics.SERVICE_DECEL
                    ) + 300.0
                    if sep < 300.0 and lead["dist_remaining"] > 0:
                        trail["risk_score"] = 1.0
                        trail["risk_status"] = "HARD_INTERLOCK_VIOLATION"
                        trail["best_route"] = "EMERGENCY HOLD AT OUTER SIGNAL"
                    elif sep < braking_buffer and lead["dist_remaining"] > 0:
                        trail["risk_score"] = round(min(0.95, (braking_buffer - sep) / braking_buffer + 0.4), 2)
                        trail["risk_status"] = "CRITICAL_CONFLICT"
                        trail["best_route"] = "Switch to Rayaru Bi-Directional Loop 2"
                    else:
                        trail["risk_score"] = 0.0
                        trail["risk_status"] = "NOMINAL"
                        # Reset the route text back to normal when safe!
                        if "original_route" in trail:
                            trail["best_route"] = trail["original_route"]
                            
        trains_to_handoff = []

        for t in self.trains:
            for t in self.trains:
                if t["dist_remaining"] > 0:
                    target_spd, advisory = Kinematics.get_advisory(
                        t["dist_remaining"], t["mps"], 30.0
                    )

                    if (
                        self.disruption_active
                        and t["corridor"] == "SOUTH_CORRIDOR"
                        and t["dist_remaining"] > 1500
                    ):
                        target_spd = 0.0
                        advisory = "HOLD (OHE Block at Sithouli)"
                        t["best_route"] = "Reroute via Goods Loop Siding"

                    if t["current_speed"] > target_spd:
                        t["current_speed"] = max(
                            target_spd,
                            t["current_speed"] - (Kinematics.SERVICE_DECEL * 3.6 * dt),
                        )
                    elif t["current_speed"] < target_spd:
                        t["current_speed"] = min(
                            target_spd,
                            t["current_speed"] + (Kinematics.ACCEL * 3.6 * dt),
                        )

                    travelled = (t["current_speed"] / 3.6) * dt
                    t["dist_remaining"] = max(0.0, t["dist_remaining"] - travelled)
                    t["status"] = advisory
                    t["gps"] = Kinematics.compute_gps(
                        t["corridor"], t["dist_remaining"]
                    )

                    # --- LIVE ETA & REQUIRED SPEED CALCULATION ---
                    if "eta_dt_iso" in t:
                        target_eta = datetime.fromisoformat(t["eta_dt_iso"])
                        current_sim_time = self.sim_base_datetime + timedelta(
                            seconds=self.sim_seconds
                        )
                        time_left_secs = (target_eta - current_sim_time).total_seconds()

                        if time_left_secs > 0:
                            # Required Speed (km/h) = Distance (km) / Time (hours)
                            req_spd = (t["dist_remaining"] / 1000.0) / (
                                time_left_secs / 3600.0
                            )
                            t["required_speed"] = min(
                                req_spd, t["mps"] * 1.2
                            )  # Cap at 120% of MPS
                        else:
                            t["required_speed"] = 0.0

                        t["dynamic_eta"] = target_eta.strftime("%H:%M:%S")
                    else:
                        t["required_speed"] = t["mps"]
                        t["dynamic_eta"] = t["arr_time_str"]

            else:
                t["current_speed"] = 0.0
                t["required_speed"] = 0.0
                t["status"] = f"BERTHED @ PF {t['allocated_pf']}"
                t["gps"] = GWL_GPS
                t["dynamic_eta"] = "Arrived"

                if (
                    t.get("is_outbound_candidate", False)
                    or t["corridor"] == "SOUTH_CORRIDOR"
                ):
                    trains_to_handoff.append(t)

        for t in trains_to_handoff:
            target_ip = "http://192.168.1.103:8000/p2p/receive_handoff"
            payload = {"source_agent_ip": "192.168.1.102", "train_data": t}
            try:
                resp = requests.post(target_ip, json=payload, timeout=1.0)
                if resp.status_code == 200:
                    self.log(f"🌐 P2P Handoff Success: Train {t['id']} handed off.")
                    self.remove_train(t["id"])
            except Exception:
                pass


session = SimulationSession()

# ==========================================
# 4. WEBSOCKET & REST API
# ==========================================


@app.websocket("/ws/telemetry")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            session.step(dt=1.0)
            cur_clock = session.sim_base_datetime + timedelta(
                seconds=session.sim_seconds
            )
            payload = {
                "sim_clock": cur_clock.strftime("%H:%M:%S"),
                "sim_date": cur_clock.strftime("%d %b %Y"),
                "is_running": session.is_running,
                "disruption_active": session.disruption_active,
                "disruption_text": session.disruption_text,
                "trains": session.trains,
                "logs": session.logs,
            }
            await websocket.send_json(payload)
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        pass


@app.post("/api/control/{action}")
def control_simulation(action: str):
    if action == "start":
        session.is_running = True
        session.log("Kinematics simulation running.")

    elif action == "pause":
        session.is_running = False
        session.log("Simulation engine paused.")

    elif action == "reset":
        session.is_running = False  # Auto-pause when reset is clicked
        session.sim_seconds = 0.0
        session.trains = []  # Clear the current board

        # Respawn all trains completely fresh from the original data
        for t_data in session.active_train_dict.values():
            instance = dict(t_data)
            instance["dist_remaining"] = float(instance["dist"])
            instance["current_speed"] = min(float(instance["mps"]), 85.0)
            instance["status"] = "EN_ROUTE"
            instance["risk_score"] = 0.0
            instance["risk_status"] = "NOMINAL"
            instance["allocated_pf"] = 1
            instance["gps"] = Kinematics.compute_gps(
                instance["corridor"], instance["dist_remaining"]
            )
            session.trains.append(instance)

        session.recalculate_schedule()
        session.log("Corridor states completely reset to initial positions.")

    elif action == "disrupt":
        session.disruption_active = not session.disruption_active
        session.disruption_text = (
            "OHE Traction Breakdown at Sithouli"
            if session.disruption_active
            else "None (Nominal)"
        )
        session.log(f"Agent Alert: Disruption status = {session.disruption_text}")

        if session.disruption_active:
            session.log("🤖 Querying AI Dispatcher for resolution strategy...")
            ai_advice = AIDispatcher.get_strategy(
                train_data=session.trains, disruption=session.disruption_text
            )
            session.log(f"🤖 AI DISPATCH DECISION: {ai_advice}")

    return {
        "status": "success",
        "is_running": session.is_running,
        "disruption": session.disruption_active,
    }


class LiveQueryRequest(BaseModel):
    train_number: str
    date_yyyymmdd: str
    api_key: str


@app.post("/api/fetch_and_add")
def fetch_and_add(req: LiveQueryRequest):
    success, msg, train_data = RailRadarJSONAdapter.fetch_live_train(
        train_number=req.train_number, query_date=req.date_yyyymmdd, api_key=req.api_key
    )
    if success and train_data:
        session.add_or_update_train(train_data)
        return {"success": True, "message": msg, "train": train_data}
    else:
        return {"success": False, "message": msg, "train": None}


class RemoveRequest(BaseModel):
    train_id: str


@app.post("/api/remove_train")
def remove_train_endpoint(req: RemoveRequest):
    session.remove_train(req.train_id)
    return {"success": True}


class StationWindowRequest(BaseModel):
    station_code: str
    time_window_hours: int
    api_key: str


@app.post("/api/ingest_station")
def ingest_station_window(req: StationWindowRequest):
    success, msg, trains_list = RailRadarJSONAdapter.fetch_station_board(
        req.station_code, req.time_window_hours, req.api_key
    )
    if success:
        for train in trains_list:
            session.add_or_update_train(train)
        session.log(f"Ingested {len(trains_list)} trains for {req.station_code}")
        return {"success": True, "message": msg}
    return {"success": False, "message": msg}


class HandoffPayload(BaseModel):
    source_agent_ip: str
    train_data: dict


@app.post("/p2p/receive_handoff")
def receive_train_from_neighbor(payload: HandoffPayload):
    incoming_train = payload.train_data
    incoming_train["dist_remaining"] = incoming_train["dist"]
    incoming_train["entry"] = "NEIGHBOR_HANDOFF"

    session.add_or_update_train(incoming_train)
    session.log(
        f"P2P HANDOFF RECEIVED: {incoming_train['id']} from {payload.source_agent_ip}"
    )

    return {"status": "accepted", "message": "Train entered sector successfully."}


# ==========================================
# 5. ADVANCED INTEGRATED FRONTEND
# ==========================================


@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Gwalior Jn - RailRescue Mesh Advanced Digital Twin</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700;800&family=Inter:wght@400;500;600;700;900&display=swap" rel="stylesheet">
  <style>
    body { font-family: 'Inter', sans-serif; background-color: #080c14; color: #f1f5f9; }
    .mono { font-family: 'JetBrains Mono', monospace; }
    .track-main { stroke: #1e293b; stroke-width: 4; }
    .track-loop { stroke: #334155; stroke-width: 3; stroke-dasharray: 4; }
  </style>
</head>
<body class="p-6">

  <!-- TOP HUD BAR -->
  <div class="flex justify-between items-center pb-4 mb-6 border-b border-slate-800">
    <div class="flex items-center gap-4">
      <div class="w-4 h-4 rounded-full bg-emerald-500 animate-ping"></div>
      <div>
        <h1 class="text-2xl font-black tracking-tight text-white flex items-center gap-3">
          GWALIOR JUNCTION (GWL) &mdash; RAILRESCUE MESH DIGITAL TWIN
        </h1>
        <p class="text-xs text-slate-400 mt-0.5 flex items-center gap-2">
          <span>Lat: 26.2163° N, Lon: 78.1728° E</span>
          <span class="text-slate-600">&bull;</span>
          <span id="simDateText" class="text-cyan-400 font-semibold mono">22 Aug 2026</span>
          <span class="text-slate-600">&bull;</span>
          <span id="simClockText" class="text-amber-400 font-black mono text-sm">14:30:00 IST</span>
        </p>
      </div>
    </div>
    
    <!-- Controls -->
    <div class="flex items-center gap-2">
      <button onclick="control('start')" class="px-4 py-2 bg-emerald-600 hover:bg-emerald-500 rounded font-bold text-xs transition shadow-lg shadow-emerald-950">▶ START</button>
      <button onclick="control('pause')" class="px-4 py-2 bg-amber-600 hover:bg-amber-500 rounded font-bold text-xs transition shadow-lg shadow-amber-950">⏸ PAUSE</button>
      <button onclick="control('reset')" class="px-3 py-2 bg-slate-800 hover:bg-slate-700 rounded font-bold text-xs transition">↺ RESET</button>
      <button onclick="control('disrupt')" class="px-4 py-2 bg-rose-600 hover:bg-rose-500 rounded font-bold text-xs transition shadow-lg shadow-rose-950">⚡ INJECT DISRUPTION</button>
    </div>
  </div>

  <!-- LIVE API & TRAIN INGESTION BAR -->
  <div class="bg-slate-900/90 border border-slate-800 rounded-xl p-4 mb-6 shadow-xl backdrop-blur">
    <div class="flex justify-between items-center mb-2.5">
      <span class="text-xs font-bold text-cyan-400 tracking-wider mono uppercase flex items-center gap-2">
        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 011.447-.894L9 7m0 13l6-3m-6 3V7m6 10l4.553 2.276A1 1 0 0021 18.382V7.618a1 1 0 00-.553-.894L15 4m0 13V4m0 0L9 7"/></svg>
        Live Telemetry Ingestion (RailRadar / IndianRailAPI)
      </span>
      <span id="apiStatusLabel" class="text-xs mono text-slate-400">Ready</span>
    </div>

    <div class="grid grid-cols-12 gap-3">
      <div class="col-span-3">
        <input type="text" id="apiTrainNo" placeholder="Train No (e.g. 12952, 12002)" class="w-full px-3 py-2 bg-slate-950 border border-slate-700 rounded text-xs text-white mono font-bold focus:border-cyan-500 focus:outline-none">
      </div>
      <div class="col-span-3">
        <input type="text" id="apiDate" class="w-full px-3 py-2 bg-slate-950 border border-slate-700 rounded text-xs text-white mono focus:border-cyan-500 focus:outline-none">
      </div>
      <div class="col-span-4">
        <input type="password" id="apiKey" placeholder="Paste your API Key" class="w-full px-3 py-2 bg-slate-950 border border-slate-700 rounded text-xs text-white mono focus:border-cyan-500 focus:outline-none">
      </div>
      <div class="col-span-2">
        <button id="fetchBtn" onclick="fetchLiveTrain()" class="w-full py-2 bg-cyan-600 hover:bg-cyan-500 text-white rounded font-bold text-xs transition">
          + Ingest Live Train
        </button>
      </div>
    </div>
  </div>

  <!-- MAIN DUAL-VIEW CONTAINER -->
  <div class="grid grid-cols-12 gap-6">

    <!-- Left Column: Graphical Track Vector Schematic & Detailed Table -->
    <div class="col-span-8 space-y-6">

      <!-- Track Vector Schematic -->
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-2xl relative">
        <div class="flex justify-between items-center mb-3">
          <span class="text-xs font-bold text-slate-400 tracking-wider mono uppercase">Gwalior Hub Vector Track Map & GPS Envelopes</span>
          <span id="disruptionStatusTag" class="text-[11px] px-2.5 py-0.5 rounded mono bg-emerald-950 text-emerald-400 border border-emerald-800">Nominal Operations</span>
        </div>

        <svg id="trackSvg" viewBox="0 0 820 330" class="w-full h-80 bg-slate-950 rounded-lg border border-slate-800">
          <!-- Tracks Layout -->
          <!-- North Corridors (Banmore) -->
          <line x1="40" y1="45" x2="280" y2="120" class="track-main"/>
          <text x="40" y="35" fill="#64748b" class="mono text-[10px] font-bold">BANMORE (NORTH TRUNK)</text>
          
          <!-- South Corridors (Sithouli) -->
          <line x1="40" y1="285" x2="280" y2="210" class="track-main"/>
          <text x="40" y="305" fill="#64748b" class="mono text-[10px] font-bold">SITHOULI (SOUTH TRUNK)</text>

          <!-- Branch Lines -->
          <line x1="40" y1="125" x2="280" y2="145" class="track-loop"/>
          <text x="40" y="118" fill="#475569" class="mono text-[9px]">PANIHAR (GUNA BRANCH)</text>
          <line x1="40" y1="205" x2="280" y2="185" class="track-loop"/>
          <text x="40" y="222" fill="#475569" class="mono text-[9px]">MALANPUR (BHIND BRANCH)</text>

          <!-- Platform Tracks 1 to 6 -->
          <g id="pf_lines">
            <line x1="280" y1="90" x2="580" y2="90" stroke="#334155" stroke-width="4"/>
            <text x="595" y="94" fill="#94a3b8" class="mono text-[10px] font-bold">PF 1 (Up Fast)</text>
            <line x1="280" y1="120" x2="580" y2="120" stroke="#334155" stroke-width="4"/>
            <text x="595" y="124" fill="#94a3b8" class="mono text-[10px] font-bold">PF 2 (Up Main)</text>
            <line x1="280" y1="150" x2="580" y2="150" stroke="#334155" stroke-width="4"/>
            <text x="595" y="154" fill="#94a3b8" class="mono text-[10px] font-bold">PF 3 (Branch)</text>
            <line x1="280" y1="180" x2="580" y2="180" stroke="#334155" stroke-width="4"/>
            <text x="595" y="184" fill="#94a3b8" class="mono text-[10px] font-bold">PF 4 (Dn Main)</text>
            <line x1="280" y1="210" x2="580" y2="210" stroke="#334155" stroke-width="4"/>
            <text x="595" y="214" fill="#94a3b8" class="mono text-[10px] font-bold">PF 5 (Dn Fast)</text>
            <line x1="280" y1="240" x2="580" y2="240" stroke="#334155" stroke-width="4"/>
            <text x="595" y="244" fill="#94a3b8" class="mono text-[10px] font-bold">PF 6 (Goods Bypass)</text>
          </g>

          <!-- Outbound Trunk to Jhansi / Agra -->
          <line x1="580" y1="165" x2="780" y2="165" class="track-main"/>
          <text x="690" y="155" fill="#64748b" class="mono text-[10px] font-bold">JHANSI / AGRA</text>

          <!-- Dynamic Animated Train Groups -->
          <g id="trainSvgContainer"></g>
        </svg>
      </div>

      <!-- Live Kinematics, GPS & Collision Table -->
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-2xl">
        <h2 class="text-xs font-bold text-slate-400 tracking-wider mono uppercase mb-3">Live Kinematics, GPS Telemetry & Conflict Gauge</h2>
        <div class="overflow-x-auto">
          <table class="w-full text-left text-xs">
            <thead>
              <tr class="text-slate-500 border-b border-slate-800 pb-2">
                <th class="pb-2">Train</th>
                <th class="pb-2">Speed & MPS</th>
                <th class="pb-2">Live GPS (Lat, Lon)</th>
                <th class="pb-2">Distance</th>
                <th class="pb-2">Dynamic ETA</th>
                <th class="pb-2">Best Route Selection</th>
                <th class="pb-2">Kavach Risk</th>
              </tr>
            </thead>
            <tbody id="telemetryTableBody" class="divide-y divide-slate-800 font-medium"></tbody>
          </table>
        </div>
      </div>

    </div>

    <!-- Right Column: Active Trains & Agent Negotiation Logs -->
    <div class="col-span-4 space-y-6">

      <!-- Active Trains Card List -->
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <h2 class="text-xs font-bold text-slate-400 tracking-wider mono uppercase mb-3">Simulated Corridor Trains</h2>
        <div id="activeTrainsContainer" class="space-y-2.5 max-h-64 overflow-y-auto pr-1"></div>
      </div>

      <!-- Agent Negotiation & Disruption Status -->
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 flex flex-col h-80">
        <span class="text-xs font-bold text-slate-400 tracking-wider mono uppercase mb-3">RailRescue Mesh Agent Coordination Log</span>
        <div id="agentLogBox" class="flex-1 bg-slate-950 p-3 rounded-lg border border-slate-800/80 overflow-y-auto mono text-[11px] text-slate-300 space-y-1.5"></div>
      </div>

    </div>

  </div>

  <script>
    const today = new Date();
    const yyyy = today.getFullYear();
    const mm = String(today.getMonth() + 1).padStart(2, '0');
    const dd = String(today.getDate()).padStart(2, '0');
    document.getElementById("apiDate").value = `${yyyy}${mm}${dd}`;

    const savedKey = localStorage.getItem("rail_api_key");
    if (savedKey) document.getElementById("apiKey").value = savedKey;

    async function control(action) {
      await fetch(`/api/control/${action}`, { method: "POST" });
    }

    async function removeTrain(tid) {
      await fetch('/api/remove_train', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ train_id: tid })
      });
    }

    async function fetchLiveTrain() {
      const trainNo = document.getElementById("apiTrainNo").value.trim();
      const queryDate = document.getElementById("apiDate").value.trim();
      const apiKey = document.getElementById("apiKey").value.trim();
      const statusLabel = document.getElementById("apiStatusLabel");
      const btn = document.getElementById("fetchBtn");

      if (!trainNo) return alert("Enter a Train Number!");

      localStorage.setItem("rail_api_key", apiKey);
      btn.disabled = true;
      btn.innerText = "Querying...";
      statusLabel.innerText = "Querying live route from API...";

      try {
        const resp = await fetch("/api/fetch_and_add", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ train_number: trainNo, date_yyyymmdd: queryDate, api_key: apiKey })
        });
        const data = await resp.json();
        if (data.success) {
          statusLabel.innerText = "Train added to Gwalior simulation!";
          document.getElementById("apiTrainNo").value = "";
        } else {
          statusLabel.innerText = data.message;
          alert(data.message);
        }
      } catch (err) {
        statusLabel.innerText = "Network Error.";
      } finally {
        btn.disabled = false;
        btn.innerText = "+ Ingest Live Train";
      }
    }

    // Connect Telemetry WebSocket
    const ws = new WebSocket(`ws://${location.host}/ws/telemetry`);
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);

      document.getElementById("simDateText").innerText = data.sim_date;
      document.getElementById("simClockText").innerText = data.sim_clock + " IST";

      const tag = document.getElementById("disruptionStatusTag");
      if (data.disruption_active) {
        tag.className = "text-[11px] px-2.5 py-0.5 rounded mono bg-rose-950 text-rose-400 border border-rose-800 font-bold animate-pulse";
        tag.innerText = data.disruption_text;
      } else {
        tag.className = "text-[11px] px-2.5 py-0.5 rounded mono bg-emerald-950 text-emerald-400 border border-emerald-800";
        tag.innerText = "Nominal Operations";
      }

      // Render Active Trains List
      const activeList = document.getElementById("activeTrainsContainer");
      activeList.innerHTML = "";
      data.trains.forEach(t => {
        activeList.innerHTML += `
          <div class="p-2.5 rounded bg-slate-800/60 border border-slate-700/60 text-xs">
            <div class="flex items-center justify-between mb-1">
              <div class="flex items-center gap-2">
                <span class="w-2.5 h-2.5 rounded-full" style="background-color: ${t.color}"></span>
                <span class="font-bold text-white">${t.id}</span>
                <span class="text-slate-400 truncate w-32">${t.name.split('(')[0]}</span>
              </div>
              <button onclick="removeTrain('${t.id}')" class="text-slate-500 hover:text-rose-400 font-bold px-1">&times;</button>
            </div>
            <div class="flex justify-between items-center text-[10px] mono text-slate-400 mt-1">
              <span class="text-cyan-400">PF ${t.allocated_pf}</span>
              <span>Sch: ${t.arr_time_str}</span>
              <span class="text-amber-300">${t.current_speed.toFixed(0)} km/h</span>
            </div>
          </div>
        `;
      });

// Render Telemetry Table
      const tbody = document.getElementById("telemetryTableBody");
      tbody.innerHTML = "";
      data.trains.forEach(t => {
        const riskColor = t.risk_status === "HARD_INTERLOCK_VIOLATION" ? "text-rose-400 font-bold animate-pulse" :
                          t.risk_status === "CRITICAL_CONFLICT" ? "text-amber-400 font-bold" : "text-emerald-400";
        
        // ---> REPLACE THE TABLE ROW (<tr>...</tr>) BELOW <---
        tbody.innerHTML += `
          <tr class="hover:bg-slate-800/40 transition">
            <td class="py-2.5 font-bold text-white flex items-center gap-2">
              <span class="w-2 h-2 rounded-full" style="background-color: ${t.color}"></span>
              <div>
                <div>${t.id} - ${t.name.substring(0, 16)}</div>
                <div class="text-[10px] text-slate-500 font-normal mono">${t.route_type}</div>
              </div>
            </td>
            <td class="mono font-semibold text-[11px]">
              Live: <span class="text-white">${t.current_speed.toFixed(1)}</span> km/h<br>
              Req:  <span class="text-amber-400">${(t.required_speed || 0).toFixed(1)}</span> km/h
            </td>
            <td class="mono text-cyan-300 text-[11px]">${t.gps.lat.toFixed(4)}°, ${t.gps.lon.toFixed(4)}°</td>
            <td class="mono">${(t.dist_remaining / 1000).toFixed(2)} km</td>
            <td class="mono text-emerald-400 font-semibold">${t.dynamic_eta}</td>
            <td class="mono text-[11px] text-indigo-300 font-medium">${t.best_route}</td>
            <td class="mono ${riskColor}">${t.risk_status} (${t.risk_score})</td>
          </tr>
        `;
      });
      
      // Render Dynamic Train Shapes onto Track Map
      const trainGroup = document.getElementById("trainSvgContainer");
      trainGroup.innerHTML = "";
      data.trains.forEach(t => {
        let x = 40, y = 160;
        const ratio = Math.max(0, Math.min(1, 1 - (t.dist_remaining / t.dist)));

        if (t.corridor === "NORTH_CORRIDOR") {
          x = 40 + ratio * 240;
          y = 45 + ratio * 75;
        } else if (t.corridor === "SOUTH_CORRIDOR") {
          x = 40 + ratio * 240;
          y = 285 - ratio * 75;
        } else if (t.corridor === "EAST_BRANCH") {
          x = 40 + ratio * 240;
          y = 205 - ratio * 20;
        } else {
          x = 40 + ratio * 240;
          y = 125 + ratio * 20;
        }

        if (ratio >= 0.94) {
          x = 280 + (ratio - 0.94) * 16.6 * 300;
          y = 90 + (t.allocated_pf - 1) * 30;
        }

        // Draw Detailed Train Consist
        trainGroup.innerHTML += `
          <g transform="translate(${x - 22}, ${y - 7})">
            <!-- Dynamic Halo -->
            <circle cx="22" cy="7" r="${t.risk_score > 0.5 ? '18' : '10'}" fill="${t.color}" opacity="${t.risk_score > 0.5 ? '0.35' : '0.15'}" class="${t.risk_score > 0.5 ? 'animate-ping' : ''}"/>
            <!-- Coach 2 -->
            <rect x="0" y="3" width="9" height="8" rx="1.5" fill="#475569" />
            <!-- Coach 1 -->
            <rect x="11" y="3" width="9" height="8" rx="1.5" fill="#64748b" />
            <!-- Locomotive -->
            <rect x="22" y="2" width="13" height="10" rx="2" fill="${t.color}" stroke="#ffffff" stroke-width="0.8" />
            <!-- Cab Window -->
            <rect x="29" y="4" width="4" height="4" fill="#0f172a" rx="0.5" />
            <!-- Headlamp Beam -->
            <polygon points="35,6 48,2 48,12 35,8" fill="#fef08a" opacity="0.3" />
            <!-- Tag -->
            <text x="0" y="-3" fill="#ffffff" class="mono text-[8px] font-black">${t.id}</text>
          </g>
        `;
      });

      // Render Logs
      const logBox = document.getElementById("agentLogBox");
      logBox.innerHTML = data.logs.map(l => `<div>${l}</div>`).join("");
    };
  </script>
</body>
</html>
    """


if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
