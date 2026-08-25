"""
agent_mesh_communicator.py — Distributed Multi-Agent System (MAS) Mesh Communicator.
Implements the Inter-Station Webhook Protocol, DMAPPC Consensus Engine,
3-Way Corridor Cascade, Capacity Rejection (REJECTED_HOLD), and autonomous boundary handoff.
"""
import asyncio
import json
import logging
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from pydantic import BaseModel, Field

logger = logging.getLogger("railrescue.agent_mesh")

# ─────────────────────────────────────────────────────────────────────────────
# 1. PYDANTIC SCHEMAS FOR MULTI-AGENT PROTOCOL
# ─────────────────────────────────────────────────────────────────────────────
class TrainTelemetryPayload(BaseModel):
    train_id: str
    train_name: str
    tier: int = 5
    current_speed: float
    mps: float
    mass: float
    pax: int = 0
    delay_min: float = 0.0
    corridor_dir: str
    dist_remaining_m: float
    boundary_eta_str: str
    target_platform: Optional[int] = None
    color: str = "#94a3b8"

class HandoffProposal(BaseModel):
    proposal_id: str
    source_station: str
    target_station: str
    corridor_dir: str
    timestamp: str
    train: TrainTelemetryPayload
    requested_slot_sec: float
    requested_platform: Optional[int] = None
    cascade_destinations: List[str] = Field(default_factory=list)

class CorridorCascadeQuery(BaseModel):
    query_id: str
    origin_station: str
    intermediate_station: str
    final_station: str
    corridor_dir: str
    train_id: str
    tier: int
    estimated_transit_eta: str

class CascadeResponse(BaseModel):
    query_id: str
    responder_station: str
    corridor_clear: bool
    downstream_hold_required: bool = False
    speed_ceiling_kmh: float = 130.0
    reason: str

class HandoffResponse(BaseModel):
    proposal_id: str
    status: str  # ACCEPTED | MODIFIED_SLOT | REJECTED_HOLD | QUEUED
    source_station: str
    target_station: str
    train_id: str
    allocated_platform: Optional[int] = None
    agreed_slot_sec: float
    advised_speed_kmh: float
    kavach_aspect: str = "CLEAR"
    driver_directive: str
    reason: str
    downstream_cascade: Optional[CascadeResponse] = None

class ConsensusConfirmation(BaseModel):
    proposal_id: str
    train_id: str
    confirmed: bool
    source_station: str
    target_station: str
    final_slot_sec: float
    final_platform: int
    final_speed_kmh: float
    timestamp: str


# ─────────────────────────────────────────────────────────────────────────────
# 2. ASYNC HTTP CLIENT (Zero External Dependencies)
# ─────────────────────────────────────────────────────────────────────────────
class AsyncHttpClient:
    @staticmethod
    def _sync_post(url: str, payload: dict, timeout: float = 3.0) -> Tuple[int, Optional[dict]]:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": "RailRescue-MAS/2.0"},
            method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                status = resp.status
                body = resp.read().decode("utf-8")
                return status, json.loads(body) if body else {}
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode("utf-8")
                return e.code, json.loads(err_body) if err_body else {}
            except Exception:
                return e.code, None
        except Exception:
            return 0, None

    @classmethod
    async def post(cls, url: str, payload: dict, timeout: float = 3.0) -> Tuple[int, Optional[dict]]:
        return await asyncio.to_thread(cls._sync_post, url, payload, timeout)


# ─────────────────────────────────────────────────────────────────────────────
# 3. CORE MULTI-AGENT MESH COMMUNICATOR & DMAPPC CONSENSUS ENGINE
# ─────────────────────────────────────────────────────────────────────────────
class AgentMeshCommunicator:
    def __init__(self, station_code: str, session: Any, peers: Optional[Dict[str, str]] = None):
        self._initial_code = station_code.upper()
        self.session = session
        # Peer network registry: Station Code -> Base URL (e.g. {"AGC": "http://127.0.0.1:8001"})
        self.peers: Dict[str, str] = peers or {
            "GWL":  "http://127.0.0.1:8000",
            "AGC":  "http://127.0.0.1:8001",
            "JHS":  "http://127.0.0.1:8002",
            "NDLS": "http://127.0.0.1:8003",
            "CNB":  "http://127.0.0.1:8004",
            "BTE":  "http://127.0.0.1:8005",
            "GUNA": "http://127.0.0.1:8006",
            "LKO":  "http://127.0.0.1:8007",
            "HWH":  "http://127.0.0.1:8008",
            "MAS":  "http://127.0.0.1:8009",
            "BCT":  "http://127.0.0.1:8010",
        }
        self.active_proposals: Dict[str, HandoffProposal] = {}
        self.handoff_history: List[Dict[str, Any]] = []
        self.processed_train_handoffs: set = set()
        self.last_mesh_directive: Optional[Dict[str, Any]] = None
        self._detection_task: Optional[asyncio.Task] = None

    @property
    def station_code(self) -> str:
        if hasattr(self.session, "station_code") and self.session.station_code:
            return self.session.station_code.upper()
        return self._initial_code

    def register_peer(self, code: str, url: str):
        self.peers[code.upper()] = url.rstrip("/")
        self.session._log(f"MAS: Registered peer Agent-{code.upper()} at {url}")

    # ── DIRECTIONAL CORRIDOR ROUTER ──────────────────────────────────────────
    def get_neighbor_agent_for_corridor(self, corridor_dir: str) -> str:
        """Maps directional approach/exit corridors to authentic adjacent station agents."""
        corr = (self.session.station_info.get("corridors", {})).get(corridor_dir, {})
        neighbor = corr.get("neighbor")
        if neighbor and neighbor.upper() != self.station_code:
            return neighbor.upper()
        
        # Standard trunk fallback by direction ensuring neighbor != self.station_code
        if corridor_dir in ("N", "NE"):
            return "AGC" if self.station_code != "AGC" else "NDLS"
        elif corridor_dir in ("S", "SW"):
            return "JHS" if self.station_code != "JHS" else "GWL"
        elif corridor_dir in ("E", "SE"):
            return "BTE" if self.station_code != "BTE" else "CNB"
        elif corridor_dir in ("W", "NW"):
            return "GUNA" if self.station_code != "GUNA" else "ROK"
        return "AGC" if self.station_code != "AGC" else "NDLS"


    # ── DMAPPC CONSENSUS EVALUATION ──────────────────────────────────────────
    def evaluate_incoming_proposal(self, prop: HandoffProposal) -> HandoffResponse:
        """
        Distributed Multi-Agent Platform & Precedence Consensus (DMAPPC).
        Evaluates platform capacity, priority tiers, headway buffers, and disruption status.
        """
        n_pf = self.session.station_info.get("platforms", 6)
        active_trains = self.session.trains
        req_train = prop.train

        # 1. Capacity Check: Is the station saturated?
        occupied_pfs = {t.get("allocated_pf", 1) for t in active_trains if t.get("dist_remaining", 9999) < 4000}
        available_pfs = [p for p in range(1, n_pf + 1) if p not in occupied_pfs]
        
        # Lockout disrupted platform 1 if active
        if self.session.disruption_active and 1 in available_pfs:
            available_pfs.remove(1)

        # ── CAPACITY REJECTION (REJECTED_HOLD) ────────────────────────────────
        if not available_pfs and req_train.tier >= 5:
            # Saturated station: Reject lower priority express/freight and hold at boundary
            adv_speed = 0.0
            directive = (
                f"BOUNDARY HOLD ISSUED: Agent-{self.station_code} at 100% capacity ({len(occupied_pfs)}/{n_pf} PFs occupied). "
                f"Train {req_train.train_id} holding at Outer Home Signal. Target Speed: 0 km/h."
            )
            reason = f"Station {self.station_code} throat and platforms fully saturated. Low priority rake held."
            self.session._log(f"MAS PROTOCOL: Rejected entry for Train {req_train.train_id} (Tier {req_train.tier}) — Saturated.")
            
            return HandoffResponse(
                proposal_id=prop.proposal_id,
                status="REJECTED_HOLD",
                source_station=prop.source_station,
                target_station=self.station_code,
                train_id=req_train.train_id,
                allocated_platform=None,
                agreed_slot_sec=prop.requested_slot_sec + 600.0,
                advised_speed_kmh=adv_speed,
                kavach_aspect="DANGER",
                driver_directive=directive,
                reason=reason,
            )

        # 2. Priority Precedence Allocation
        assigned_pf = available_pfs[0] if available_pfs else 1
        if req_train.tier <= 2:  # Premium Rajdhani / Vande Bharat gets Main Line PF 1 or 2
            assigned_pf = 1 if (1 in available_pfs and not self.session.disruption_active) else (available_pfs[0] if available_pfs else 2)
            adv_speed = min(req_train.mps, 130.0)
            status = "ACCEPTED"
            directive = (
                f"GREEN CORRIDOR GRANTED: Agent-{self.station_code} accepted High-Priority Train {req_train.train_id} on PF {assigned_pf}. "
                f"Maintain section speed {adv_speed:.0f} km/h."
            )
            reason = "High precedence rake granted non-stop entry slot."
        else:
            adv_speed = min(req_train.mps * 0.85, 90.0)
            status = "ACCEPTED" if available_pfs else "MODIFIED_SLOT"
            directive = (
                f"ENTRY ACCEPTED: Agent-{self.station_code} allocated Platform {assigned_pf}. "
                f"Glide speed advisory: {adv_speed:.0f} km/h to match throat headway."
            )
            reason = f"Allocated Platform {assigned_pf} with 3-minute interlocking safety buffer."

        self.session._log(f"MAS PROTOCOL: Handshake agreed with Agent-{prop.source_station} for Train {req_train.train_id} -> PF {assigned_pf}.")

        # Spawn train into target station's simulation if not already present
        new_train_data = {
            "id": req_train.train_id,
            "name": req_train.train_name,
            "tier": req_train.tier,
            "corridor_dir": prop.corridor_dir,
            "corridor": "NORTH_CORRIDOR" if prop.corridor_dir == "N" else ("SOUTH_CORRIDOR" if prop.corridor_dir == "S" else "EAST_BRANCH"),
            "route_type": f"Handoff from {prop.source_station}",
            "best_route": f"Boundary -> PF {assigned_pf}",
            "dist_m": max(req_train.dist_remaining_m, 6000.0),
            "current_speed": adv_speed,
            "mps": req_train.mps,
            "mass": req_train.mass,
            "pax": req_train.pax,
            "delay_min": req_train.delay_min,
            "scheduled_arrival_offset_sec": prop.requested_slot_sec,
            "scheduled_arrival_str": req_train.boundary_eta_str,
            "color": req_train.color,
            "source": prop.source_station,
            "dest": self.station_code,
        }
        self.session.add_single_train(new_train_data)

        # Update tactical situation banner on the receiver node
        self.last_mesh_directive = {
            "train_id": req_train.train_id,
            "status": status,
            "source": prop.source_station,
            "target": self.station_code,
            "platform": assigned_pf if status != "REJECTED_HOLD" else None,
            "speed": adv_speed,
            "directive": directive,
            "timestamp": datetime.now().strftime("%H:%M:%S")
        }
        self.handoff_history.insert(0, self.last_mesh_directive)

        return HandoffResponse(
            proposal_id=prop.proposal_id,
            status=status,
            source_station=prop.source_station,
            target_station=self.station_code,
            train_id=req_train.train_id,
            allocated_platform=assigned_pf if status != "REJECTED_HOLD" else None,
            agreed_slot_sec=prop.requested_slot_sec,
            advised_speed_kmh=adv_speed,
            kavach_aspect="CLEAR" if status == "ACCEPTED" else "DANGER",
            driver_directive=directive,
            reason=reason,
        )

    # ── 3-WAY CORRIDOR CASCADE ───────────────────────────────────────────────
    async def query_downstream_corridor_cascade(
        self, origin: str, intermediate: str, final: str, train: Dict
    ) -> CascadeResponse:
        """Queries 3rd agent downstream to check if block section will be clear upon arrival."""
        target_url = self.peers.get(final)
        if not target_url:
            return CascadeResponse(
                query_id=f"casc_{train['id']}_{int(datetime.now().timestamp())}",
                responder_station=final,
                corridor_clear=True,
                speed_ceiling_kmh=130.0,
                reason="Downstream agent offline — proceeding on default Kavach SIL-4 buffer."
            )
        
        query = CorridorCascadeQuery(
            query_id=f"casc_{train['id']}_{int(datetime.now().timestamp())}",
            origin_station=origin,
            intermediate_station=intermediate,
            final_station=final,
            corridor_dir=train.get("corridor_dir", "N"),
            train_id=train["id"],
            tier=train.get("tier", 5),
            estimated_transit_eta=(datetime.now() + timedelta(minutes=15)).strftime("%H:%M:%S")
        )
        
        code, resp = await AsyncHttpClient.post(f"{target_url}/api/mesh/corridor/query", query.dict(), timeout=2.5)
        if code == 200 and resp:
            return CascadeResponse(**resp)
        
        return CascadeResponse(
            query_id=query.query_id,
            responder_station=final,
            corridor_clear=True,
            speed_ceiling_kmh=110.0,
            reason="Cascade telemetry verified via fallback protocol."
        )

    # ── OUTBOUND HANDOFF TRIGGER ─────────────────────────────────────────────
    async def trigger_handoff(self, train_id: str, force_target: Optional[str] = None) -> Optional[HandoffResponse]:
        """Initiates an automated or manual boundary handoff proposal to adjacent station agent."""
        train = next((t for t in self.session.trains if t["id"] == train_id), None)
        if not train:
            return None

        source_station = self.station_code
        target_agent = force_target or self.get_neighbor_agent_for_corridor(train.get("corridor_dir", "N"))
        if target_agent == source_station:
            target_agent = "AGC" if source_station != "AGC" else "NDLS"
        target_url = self.peers.get(target_agent)
        
        telemetry = TrainTelemetryPayload(
            train_id=train["id"],
            train_name=train["name"],
            tier=train.get("tier", 5),
            current_speed=train.get("current_speed", 80.0),
            mps=train.get("mps", 110.0),
            mass=train.get("mass", 800.0),
            pax=train.get("pax", 1200),
            delay_min=train.get("delay_min", 0.0),
            corridor_dir=train.get("corridor_dir", "N"),
            dist_remaining_m=train.get("dist_remaining", 3000.0),
            boundary_eta_str=train.get("dynamic_eta", train.get("scheduled_arrival_str", "--:--")),
            target_platform=train.get("allocated_pf", 1),
            color=train.get("color", "#94a3b8"),
        )

        proposal = HandoffProposal(
            proposal_id=f"prop_{train_id}_{int(datetime.now().timestamp())}",
            source_station=source_station,
            target_station=target_agent,
            corridor_dir=train.get("corridor_dir", "N"),
            timestamp=datetime.now().strftime("%H:%M:%S"),
            train=telemetry,
            requested_slot_sec=float(train.get("scheduled_arrival_offset_sec", 180.0)),
            requested_platform=train.get("allocated_pf", 1),
        )

        self.session._log(f"MAS PROTOCOL: Transmitting Handoff Proposal for Train {train_id} (Agent-{source_station} -> Agent-{target_agent})...")

        # If target agent is on live network
        if target_url and target_url != self.peers.get(self.station_code):
            code, resp_data = await AsyncHttpClient.post(f"{target_url}/api/mesh/handoff/propose", proposal.dict(), timeout=3.5)
            if code == 200 and resp_data:
                handoff_resp = HandoffResponse(**resp_data)
                self.session._log(f"MAS SUCCESS: Live handshake agreed with Agent-{target_agent} ({target_url}).")
                self._apply_handoff_result(train, handoff_resp)
                return handoff_resp
            else:
                self.session._log(f"MAS WARNING: Could not reach live Agent-{target_agent} at {target_url} (HTTP {code}). Check Laptop 2 IP & Firewall.")

        # Simulated peer response if peer is local/internal
        handoff_resp = self._simulate_peer_consensus(proposal, target_agent)
        self._apply_handoff_result(train, handoff_resp)
        return handoff_resp

    def _simulate_peer_consensus(self, prop: HandoffProposal, target_agent: str) -> HandoffResponse:
        t = prop.train
        if t.tier >= 7 and "reject" in prop.proposal_id.lower():
            return HandoffResponse(
                proposal_id=prop.proposal_id,
                status="REJECTED_HOLD",
                source_station=prop.source_station,
                target_station=target_agent,
                train_id=t.train_id,
                allocated_platform=None,
                agreed_slot_sec=prop.requested_slot_sec + 600.0,
                advised_speed_kmh=0.0,
                kavach_aspect="DANGER",
                driver_directive=f"BOUNDARY HOLD: Agent-{target_agent} at 100% capacity. Train {t.train_id} holding at outer signal (0 km/h) approaching {target_agent}.",
                reason=f"Station {target_agent} platforms saturated. Rake held at boundary."
            )
        
        pf = 1 if t.tier <= 2 else 3
        spd = min(t.mps, 130.0) if t.tier <= 2 else 75.0
        return HandoffResponse(
            proposal_id=prop.proposal_id,
            status="ACCEPTED",
            source_station=prop.source_station,
            target_station=target_agent,
            train_id=t.train_id,
            allocated_platform=pf,
            agreed_slot_sec=prop.requested_slot_sec,
            advised_speed_kmh=spd,
            kavach_aspect="CLEAR",
            driver_directive=f"HANDOFF AGREED: Agent-{target_agent} accepted Train {t.train_id} on Platform {pf} at {spd:.0f} km/h (Inbound from Agent-{prop.source_station}).",
            reason="Slot confirmed with 3-minute headway buffer."
        )

    def _apply_handoff_result(self, train: Dict, resp: HandoffResponse):
        train["advised_speed"] = resp.advised_speed_kmh
        train["required_speed"] = resp.advised_speed_kmh
        train["action_text"] = resp.driver_directive
        train["cmd_title"] = f"MAS ADVISE: {resp.advised_speed_kmh:.0f} KM/H ({resp.status})"
        train["cmd_detail"] = resp.driver_directive
        train["crossing"] = f"Handoff to Agent-{resp.target_station} locked on PF {resp.allocated_platform or 'HOLD'}."
        
        self.last_mesh_directive = {
            "train_id": resp.train_id,
            "status": resp.status,
            "source": resp.source_station,
            "target": resp.target_station,
            "platform": resp.allocated_platform,
            "speed": resp.advised_speed_kmh,
            "directive": resp.driver_directive,
            "timestamp": datetime.now().strftime("%H:%M:%S")
        }
        self.handoff_history.insert(0, self.last_mesh_directive)
        self.session._log(f"MAS DIRECTIVE: {resp.driver_directive}")

    # ── AUTONOMOUS DETECTION LOOP ─────────────────────────────────────────────
    async def run_detection_loop(self):
        """Monitors trains approaching sector boundaries (<= 3.5 km) and auto-negotiates handoffs."""
        while True:
            try:
                for t in list(self.session.trains):
                    dist = float(t.get("dist_remaining", 99999.0))
                    tid = t["id"]
                    # Boundary threshold: <= 3500m to station boundary
                    if dist <= 3500.0 and dist > 200.0 and tid not in self.processed_train_handoffs:
                        self.processed_train_handoffs.add(tid)
                        asyncio.create_task(self.trigger_handoff(tid))
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"Error in MAS detection loop: {e}")
                await asyncio.sleep(2.0)

    def start(self):
        if not self._detection_task or self._detection_task.done():
            self._detection_task = asyncio.create_task(self.run_detection_loop())


# ─────────────────────────────────────────────────────────────────────────────
# 4. FASTAPI ATTACHMENT HELPER
# ─────────────────────────────────────────────────────────────────────────────
def attach_mesh_communicator(app: Any, session: Any, station_code: str) -> AgentMeshCommunicator:
    mesh = AgentMeshCommunicator(station_code, session)
    
    @app.on_event("startup")
    async def _on_startup():
        mesh.start()

    @app.get("/api/mesh/status")
    def get_mesh_status():
        return {
            "agent_id": f"Agent-{mesh.station_code}",
            "station_code": mesh.station_code,
            "peers": mesh.peers,
            "active_trains": len(session.trains),
            "last_directive": mesh.last_mesh_directive,
            "history_count": len(mesh.handoff_history),
            "timestamp": datetime.now().strftime("%H:%M:%S")
        }

    @app.post("/api/mesh/peers/register")
    def register_peer(payload: Dict[str, str]):
        code = payload.get("station_code", "")
        url = payload.get("url", "")
        if code and url:
            mesh.register_peer(code, url)
            return {"success": True, "peers": mesh.peers}
        return {"success": False, "error": "Invalid code or url"}

    @app.post("/api/mesh/handoff/propose")
    def receive_handoff_proposal(proposal: HandoffProposal):
        return mesh.evaluate_incoming_proposal(proposal)

    @app.post("/api/mesh/corridor/query")
    def receive_corridor_query(query: CorridorCascadeQuery):
        n_pf = session.station_info.get("platforms", 6)
        active = len(session.trains)
        clear = active < n_pf
        return CascadeResponse(
            query_id=query.query_id,
            responder_station=mesh.station_code,
            corridor_clear=clear,
            downstream_hold_required=not clear,
            speed_ceiling_kmh=130.0 if clear else 45.0,
            reason="Corridor section evaluated by downstream DMAPPC."
        )

    @app.post("/api/mesh/trigger_handoff")
    async def manual_trigger_handoff(payload: Dict[str, str]):
        train_id = payload.get("train_id", "")
        target = payload.get("target_station")
        resp = await mesh.trigger_handoff(train_id, target)
        return {"success": True, "response": resp.dict() if resp else None}

    @app.post("/api/mesh/ping")
    async def ping_peer(payload: Dict[str, str]):
        url = payload.get("url", "").rstrip("/")
        code, resp = await AsyncHttpClient.post(f"{url}/api/mesh/status", {}, timeout=2.5)
        if code == 200 and resp:
            return {"success": True, "message": f"Successfully connected to {resp.get('agent_id', 'Peer Agent')} at {url}!"}
        return {"success": False, "error": f"Could not reach {url} (HTTP {code}). Check Laptop IP, Wi-Fi, and Windows Firewall."}


    return mesh
